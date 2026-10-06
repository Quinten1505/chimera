use std::path::PathBuf;
use std::sync::Arc;

use chimera_core::error::PortError;
use chimera_core::repository::Repository;
use chimera_core::run_store::RunStore;
use chimera_core::terminal::{Terminal, TurnStatus};
use chimera_core::{BranchName, CommitId, PaneId, Role, RunId, WorkspaceId};
use serde::{Deserialize, Serialize};

use crate::error::PipelineError;
use crate::policy::{Budget, Policy, RetryRefused};

/// A role and the ready command line its agent is started with. The caller builds the command
/// line; this crate knows nothing about provider arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLaunch {
    pub role: Role,
    pub command_line: String,
}

/// What to provision: the worktree and one pane with an agent per entry of `agents` (the
/// three-agent triplet, or a single review agent).
#[derive(Debug, Clone)]
pub struct ProvisionSpec {
    pub worktree: PathBuf,
    pub task_branch: BranchName,
    pub feature: BranchName,
    pub agents: Vec<AgentLaunch>,
}

/// The pane of one role and whether its agent has been launched.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentAgent {
    pub role: Role,
    pub pane: PaneId,
    pub launched: bool,
}

/// A provisioning effect that creates something only the response would name, so a restart
/// cannot tell from the saved progress alone whether it happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnvironmentEffect {
    CreateWorktree,
    CreateWorkspace,
    SplitPane,
    Launch(Role),
}

/// Worktree, workspace, and agents of one task. Doubles as the provisioning progress record:
/// it is saved before and after every effect, and each effect is skipped once it shows as done.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Environment {
    pub worktree: PathBuf,
    pub task_branch: BranchName,
    pub worktree_created: bool,
    /// The verified feature head the new worktree was moved onto before any agent started.
    #[serde(default)]
    pub synced_to: Option<CommitId>,
    pub workspace: Option<WorkspaceId>,
    pub agents: Vec<EnvironmentAgent>,
    /// The effect that was started but whose outcome is not saved: the process stopped or the
    /// response was lost. It is reconciled through the ports before anything else.
    #[serde(default)]
    pub started: Option<EnvironmentEffect>,
}

impl Environment {
    pub fn new(spec: &ProvisionSpec) -> Self {
        Self {
            worktree: spec.worktree.clone(),
            task_branch: spec.task_branch.clone(),
            worktree_created: false,
            synced_to: None,
            workspace: None,
            agents: Vec::new(),
            started: None,
        }
    }

    /// The pane of `role`'s agent.
    pub fn pane(&self, role: Role) -> Option<&PaneId> {
        self.agents
            .iter()
            .find(|agent| agent.role == role)
            .map(|agent| &agent.pane)
    }

    pub fn is_provisioned(&self, spec: &ProvisionSpec) -> bool {
        self.worktree_created
            && self.synced_to.is_some()
            && self.workspace.is_some()
            && self.agents.len() == spec.agents.len()
            && self.agents.iter().all(|agent| agent.launched)
            && self.started.is_none()
    }
}

/// The next provisioning step: an effect to record first, or moving the new worktree.
enum Next {
    Effect(EnvironmentEffect),
    Sync,
}

/// Provisions and cleans up the worktree, workspace, and agents of a task.
pub struct EnvironmentService {
    repository: Arc<dyn Repository>,
    terminal: Arc<dyn Terminal>,
    policy: Arc<Policy>,
}

impl EnvironmentService {
    pub fn new(
        repository: Arc<dyn Repository>,
        terminal: Arc<dyn Terminal>,
        policy: Arc<Policy>,
    ) -> Self {
        Self {
            repository,
            terminal,
            policy,
        }
    }

    /// The environment saved under `instance`, or a new one for `spec`.
    pub async fn load(
        store: &dyn RunStore,
        run: &RunId,
        instance: &str,
        spec: &ProvisionSpec,
    ) -> Result<Environment, PipelineError> {
        Ok(match store.load_pipeline_state(run, instance).await? {
            Some(saved) => serde_json::from_value(saved)?,
            None => Environment::new(spec),
        })
    }

    async fn save(
        store: &dyn RunStore,
        run: &RunId,
        instance: &str,
        environment: &Environment,
    ) -> Result<(), PipelineError> {
        store
            .save_pipeline_state(run, instance, serde_json::to_value(environment)?)
            .await?;
        Ok(())
    }

    /// Advances the environment saved under `instance` by one external effect and saves it;
    /// call again until it [`Environment::is_provisioned`]. The order is: worktree, moving it
    /// onto the verified feature head `base`, workspace, one split pane per further role, then
    /// each agent launch, with the policy checked before a launch.
    ///
    /// An effect is recorded as started before it runs. One still recorded when this is called
    /// again (the process stopped, or the response was lost) is reconciled instead: the worktree
    /// and workspace are looked up, a launch is read from the pane's status. Once the worktree
    /// was moved and agents may have worked in it, it is never moved again.
    pub async fn provision_step(
        &self,
        store: &dyn RunStore,
        run: &RunId,
        instance: &str,
        spec: &ProvisionSpec,
        base: &CommitId,
    ) -> Result<Environment, PipelineError> {
        let mut environment = Self::load(store, run, instance, spec).await?;
        if environment.is_provisioned(spec) {
            return Ok(environment);
        }
        if let Some(effect) = environment.started {
            self.reconcile(spec, &mut environment, effect).await?;
            environment.started = None;
            Self::save(store, run, instance, &environment).await?;
            return Ok(environment);
        }
        let effect = match self.next(spec, &environment) {
            Next::Sync => {
                // Moving the branch is idempotent and nothing has worked in the worktree yet.
                self.repository
                    .update_worktree(&spec.worktree, base)
                    .await?;
                environment.synced_to = Some(base.clone());
                Self::save(store, run, instance, &environment).await?;
                return Ok(environment);
            }
            Next::Effect(effect) => effect,
        };
        if matches!(effect, EnvironmentEffect::Launch(_)) {
            self.policy.check_start().map_err(PipelineError::Paused)?;
        }
        environment.started = Some(effect);
        Self::save(store, run, instance, &environment).await?;
        match self.perform(spec, &mut environment, effect).await {
            Ok(()) => {}
            // Known not to have happened: nothing to reconcile.
            Err(error) if error.is_failed() => {
                environment.started = None;
                Self::save(store, run, instance, &environment).await?;
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        }
        environment.started = None;
        Self::save(store, run, instance, &environment).await?;
        Ok(environment)
    }

    /// Provisions the environment of `spec` completely, one [`Self::provision_step`] at a time.
    pub async fn provision(
        &self,
        store: &dyn RunStore,
        run: &RunId,
        instance: &str,
        spec: &ProvisionSpec,
        base: &CommitId,
    ) -> Result<Environment, PipelineError> {
        loop {
            let environment = self
                .provision_step(store, run, instance, spec, base)
                .await?;
            if environment.is_provisioned(spec) {
                return Ok(environment);
            }
        }
    }

    fn next(&self, spec: &ProvisionSpec, environment: &Environment) -> Next {
        if !environment.worktree_created {
            Next::Effect(EnvironmentEffect::CreateWorktree)
        } else if environment.synced_to.is_none() {
            Next::Sync
        } else if environment.workspace.is_none() {
            Next::Effect(EnvironmentEffect::CreateWorkspace)
        } else if environment.agents.len() < spec.agents.len() {
            Next::Effect(EnvironmentEffect::SplitPane)
        } else {
            let agent = environment
                .agents
                .iter()
                .find(|agent| !agent.launched)
                .expect("an environment that is not provisioned has an agent to launch");
            Next::Effect(EnvironmentEffect::Launch(agent.role))
        }
    }

    async fn perform(
        &self,
        spec: &ProvisionSpec,
        environment: &mut Environment,
        effect: EnvironmentEffect,
    ) -> Result<(), PortError> {
        match effect {
            EnvironmentEffect::CreateWorktree => {
                self.repository
                    .create_worktree(&spec.worktree, &spec.task_branch, &spec.feature)
                    .await?;
                environment.worktree_created = true;
            }
            EnvironmentEffect::CreateWorkspace => {
                let (workspace, pane) = self.terminal.create_workspace(&spec.worktree).await?;
                environment.workspace = Some(workspace);
                environment.agents.push(EnvironmentAgent {
                    role: spec.agents[0].role,
                    pane,
                    launched: false,
                });
            }
            EnvironmentEffect::SplitPane => {
                let workspace = environment.workspace.as_ref().expect("workspace exists");
                let pane = self
                    .terminal
                    .split_pane(workspace, &environment.agents[0].pane)
                    .await?;
                environment.agents.push(EnvironmentAgent {
                    role: spec.agents[environment.agents.len()].role,
                    pane,
                    launched: false,
                });
            }
            EnvironmentEffect::Launch(role) => {
                let launch = spec
                    .agents
                    .iter()
                    .find(|launch| launch.role == role)
                    .expect("agent role comes from the spec");
                let agent = environment
                    .agents
                    .iter_mut()
                    .find(|agent| agent.role == role)
                    .expect("the agent's pane was created");
                self.terminal
                    .launch_agent(&agent.pane, &launch.command_line)
                    .await?;
                agent.launched = true;
            }
        }
        Ok(())
    }

    /// Records what `effect` left behind, if anything; one that did not happen is performed
    /// again by the next step.
    async fn reconcile(
        &self,
        spec: &ProvisionSpec,
        environment: &mut Environment,
        effect: EnvironmentEffect,
    ) -> Result<(), PortError> {
        match effect {
            EnvironmentEffect::CreateWorktree => {
                environment.worktree_created =
                    self.repository.worktree_exists(&spec.worktree).await?;
            }
            EnvironmentEffect::CreateWorkspace => {
                if let Some((workspace, panes)) =
                    self.terminal.find_workspace(&spec.worktree).await?
                    && let Some(pane) = panes.into_iter().next()
                {
                    environment.workspace = Some(workspace);
                    environment.agents.push(EnvironmentAgent {
                        role: spec.agents[0].role,
                        pane,
                        launched: false,
                    });
                }
            }
            EnvironmentEffect::SplitPane => {
                let found = self.terminal.find_workspace(&spec.worktree).await?;
                let known: Vec<PaneId> = environment
                    .agents
                    .iter()
                    .map(|agent| agent.pane.clone())
                    .collect();
                if let Some(pane) = found
                    .into_iter()
                    .flat_map(|(_, panes)| panes)
                    .find(|pane| !known.contains(pane))
                {
                    environment.agents.push(EnvironmentAgent {
                        role: spec.agents[environment.agents.len()].role,
                        pane,
                        launched: false,
                    });
                }
            }
            EnvironmentEffect::Launch(role) => {
                let agent = environment
                    .agents
                    .iter_mut()
                    .find(|agent| agent.role == role)
                    .expect("the agent's pane was created");
                // A pane whose agent never started has no agent to report on.
                agent.launched = self.terminal.read_status(&agent.pane).await? != TurnStatus::Gone;
            }
        }
        Ok(())
    }

    /// Launches `role`'s agent again in its pane after it is `Gone`, consuming one agent
    /// recovery from the policy. The consumed budget, or the pause its exhaustion caused, is
    /// saved under `run` before the launch, so a restart cannot relaunch on a restored budget.
    pub async fn relaunch(
        &self,
        store: &dyn RunStore,
        run: &RunId,
        environment: &Environment,
        role: Role,
        command_line: &str,
    ) -> Result<(), PipelineError> {
        let pane = environment.pane(role).ok_or_else(|| {
            PipelineError::Environment(format!("no agent for role {role:?} in this environment"))
        })?;
        if self.terminal.read_status(pane).await? != TurnStatus::Gone {
            return Err(PipelineError::Environment(format!(
                "the {role:?} agent is not gone"
            )));
        }
        let permit = self.policy.permit_retry(
            Budget::AgentRecovery,
            &PortError::failed("agent is gone"),
            false,
        );
        self.policy.save(store, run).await?;
        match permit {
            Ok(()) => {}
            Err(RetryRefused::Paused(reason)) => return Err(PipelineError::Paused(reason)),
            Err(RetryRefused::NeedsReconciliation) => unreachable!("the error is not uncertain"),
        }
        self.terminal.launch_agent(pane, command_line).await?;
        Ok(())
    }

    /// Performs the next cleanup effect on the environment saved under `instance` and saves
    /// what was removed; call again until it returns `true`. Closes the workspace, removes the
    /// worktree, then prunes worktree metadata. The ports succeed for a resource that is
    /// already gone, so repeating an interrupted removal is its reconciliation, and any error
    /// leaves the resource recorded for a retry. History is not touched.
    pub async fn cleanup_step(
        &self,
        store: &dyn RunStore,
        run: &RunId,
        instance: &str,
    ) -> Result<bool, PipelineError> {
        let mut environment: Environment = serde_json::from_value(
            store
                .load_pipeline_state(run, instance)
                .await?
                .ok_or_else(|| PipelineError::Environment("nothing was provisioned".into()))?,
        )?;
        if let Some(workspace) = &environment.workspace {
            self.terminal.close_workspace(workspace).await?;
            environment.workspace = None;
        } else if environment.worktree_created {
            self.repository
                .remove_worktree(&environment.worktree, &environment.task_branch)
                .await?;
            environment.worktree_created = false;
        } else {
            self.repository.prune_worktrees().await?;
            return Ok(true);
        }
        Self::save(store, run, instance, &environment).await?;
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chimera_core::repository::FakeRepository;
    use chimera_core::run_store::{EffectRecord, FakeRunStore};
    use chimera_core::terminal::FakeTerminal;
    use chimera_core::{Limits, TurnResult};
    use futures_executor::block_on;

    use super::*;
    use crate::PauseReason;

    fn branch(name: &str) -> BranchName {
        BranchName::new(name).unwrap()
    }

    fn commit(id: &str) -> CommitId {
        CommitId::new(id).unwrap()
    }

    /// How a scripted call misbehaves.
    #[derive(Clone, Copy, PartialEq)]
    enum Lost {
        /// The call takes effect but its response is lost.
        Response,
        /// The call does not take effect, but the caller cannot tell.
        Request,
    }

    /// Counts the creating calls made through to a [`FakeTerminal`]. A pane reports `Gone` until
    /// an agent was launched in it.
    struct CountingTerminal {
        inner: FakeTerminal,
        calls: Mutex<Vec<&'static str>>,
        close_failure: Mutex<Option<PortError>>,
        launch_failure: Mutex<Option<PortError>>,
        /// The next call of this name is lost.
        lose: Mutex<Option<(&'static str, Lost)>>,
    }

    impl CountingTerminal {
        fn count(&self, call: &str) -> usize {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|c| **c == call)
                .count()
        }

        /// Logs the call; `Err` if it is lost before taking effect.
        fn log(&self, call: &'static str) -> Result<Option<Lost>, PortError> {
            let mut lose = self.lose.lock().unwrap();
            let lost = match *lose {
                Some((name, lost)) if name == call => {
                    *lose = None;
                    Some(lost)
                }
                _ => None,
            };
            if lost == Some(Lost::Request) {
                return Err(PortError::uncertain("request lost"));
            }
            self.calls.lock().unwrap().push(call);
            Ok(lost)
        }

        fn respond<T>(lost: Option<Lost>, result: Result<T, PortError>) -> Result<T, PortError> {
            match lost {
                Some(_) => result.and(Err(PortError::uncertain("response lost"))),
                None => result,
            }
        }
    }

    #[async_trait]
    impl Terminal for CountingTerminal {
        async fn create_workspace(
            &self,
            directory: &Path,
        ) -> Result<(WorkspaceId, PaneId), PortError> {
            let lost = self.log("workspace")?;
            Self::respond(lost, self.inner.create_workspace(directory).await)
        }
        async fn find_workspace(
            &self,
            directory: &Path,
        ) -> Result<Option<(WorkspaceId, Vec<PaneId>)>, PortError> {
            self.inner.find_workspace(directory).await
        }
        async fn split_pane(
            &self,
            workspace: &WorkspaceId,
            pane: &PaneId,
        ) -> Result<PaneId, PortError> {
            let lost = self.log("split")?;
            Self::respond(lost, self.inner.split_pane(workspace, pane).await)
        }
        async fn launch_agent(&self, pane: &PaneId, command_line: &str) -> Result<(), PortError> {
            let lost = self.log("launch")?;
            if let Some(error) = self.launch_failure.lock().unwrap().take() {
                return Err(error);
            }
            Self::respond(lost, self.inner.launch_agent(pane, command_line).await)
        }
        async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError> {
            self.inner.send_prompt(pane, prompt).await
        }
        async fn read_status(&self, pane: &PaneId) -> Result<TurnStatus, PortError> {
            let status = self.inner.read_status(pane).await?;
            Ok(match self.inner.launched_command(pane) {
                Some(_) => status,
                None => TurnStatus::Gone,
            })
        }
        async fn read_output(&self, pane: &PaneId) -> Result<String, PortError> {
            self.inner.read_output(pane).await
        }
        async fn close_workspace(&self, workspace: &WorkspaceId) -> Result<(), PortError> {
            if let Some(error) = self.close_failure.lock().unwrap().take() {
                return Err(error);
            }
            self.inner.close_workspace(workspace).await
        }
    }

    /// Wraps the fake store; the save with the armed number fails without storing anything, as
    /// if the process stopped right before it.
    #[derive(Default)]
    struct CrashingStore {
        inner: FakeRunStore,
        saves: Mutex<usize>,
        crash_at: Mutex<Option<usize>>,
    }

    #[async_trait]
    impl RunStore for CrashingStore {
        async fn save_pipeline_state(
            &self,
            run: &RunId,
            pipeline: &str,
            state: serde_json::Value,
        ) -> Result<(), PortError> {
            let number = {
                let mut saves = self.saves.lock().unwrap();
                *saves += 1;
                *saves
            };
            if *self.crash_at.lock().unwrap() == Some(number) {
                return Err(PortError::failed("process stopped"));
            }
            self.inner.save_pipeline_state(run, pipeline, state).await
        }
        async fn load_pipeline_state(
            &self,
            run: &RunId,
            pipeline: &str,
        ) -> Result<Option<serde_json::Value>, PortError> {
            self.inner.load_pipeline_state(run, pipeline).await
        }
        async fn save_run_data(
            &self,
            run: &RunId,
            data: serde_json::Value,
        ) -> Result<(), PortError> {
            self.inner.save_run_data(run, data).await
        }
        async fn load_run_data(&self, run: &RunId) -> Result<Option<serde_json::Value>, PortError> {
            self.inner.load_run_data(run).await
        }
        async fn append_turn(&self, run: &RunId, turn: TurnResult) -> Result<(), PortError> {
            self.inner.append_turn(run, turn).await
        }
        async fn load_history(&self, run: &RunId) -> Result<Vec<TurnResult>, PortError> {
            self.inner.load_history(run).await
        }
        async fn record_effect_intent(
            &self,
            run: &RunId,
            key: &str,
            intent: &str,
        ) -> Result<(), PortError> {
            self.inner.record_effect_intent(run, key, intent).await
        }
        async fn record_effect_outcome(
            &self,
            run: &RunId,
            key: &str,
            outcome: &str,
        ) -> Result<(), PortError> {
            self.inner.record_effect_outcome(run, key, outcome).await
        }
        async fn load_effects(&self, run: &RunId) -> Result<Vec<EffectRecord>, PortError> {
            self.inner.load_effects(run).await
        }
    }

    struct Fixture {
        repository: Arc<FakeRepository>,
        terminal: Arc<CountingTerminal>,
        policy: Arc<Policy>,
        service: EnvironmentService,
        store: CrashingStore,
    }

    fn fixture(agent_recovery: u32) -> Fixture {
        let repository = Arc::new(FakeRepository::new(branch("main"), commit("c0")));
        repository.add_branch(branch("feat"), commit("c0"));
        let terminal = Arc::new(CountingTerminal {
            inner: FakeTerminal::new(),
            calls: Mutex::new(Vec::new()),
            close_failure: Mutex::new(None),
            launch_failure: Mutex::new(None),
            lose: Mutex::new(None),
        });
        let policy = Arc::new(Policy::new(&Limits {
            agent_recovery,
            ..Limits::default()
        }));
        let service = EnvironmentService::new(repository.clone(), terminal.clone(), policy.clone());
        Fixture {
            repository,
            terminal,
            policy,
            service,
            store: CrashingStore::default(),
        }
    }

    fn launch(role: Role) -> AgentLaunch {
        AgentLaunch {
            role,
            command_line: format!("agent --role {role:?}"),
        }
    }

    fn triplet() -> ProvisionSpec {
        ProvisionSpec {
            worktree: PathBuf::from("/wt/29"),
            task_branch: branch("task-29"),
            feature: branch("feat"),
            agents: vec![
                launch(Role::Implementation),
                launch(Role::Review),
                launch(Role::Merge),
            ],
        }
    }

    fn single() -> ProvisionSpec {
        ProvisionSpec {
            agents: vec![launch(Role::Review)],
            ..triplet()
        }
    }

    fn run() -> RunId {
        RunId::new("run").unwrap()
    }

    fn provision(f: &Fixture, spec: &ProvisionSpec) -> Result<Environment, PipelineError> {
        block_on(
            f.service
                .provision(&f.store, &run(), "env", spec, &commit("c0")),
        )
    }

    /// Cleans up completely; returns the number of steps it took.
    fn clean_up(f: &Fixture) -> Result<usize, PipelineError> {
        let mut steps = 1;
        while !block_on(f.service.cleanup_step(&f.store, &run(), "env"))? {
            steps += 1;
        }
        Ok(steps)
    }

    fn saved(f: &Fixture) -> Environment {
        let saved = block_on(f.store.load_pipeline_state(&run(), "env")).unwrap();
        serde_json::from_value(saved.unwrap()).unwrap()
    }

    fn assert_provisioned_once(f: &Fixture, spec: &ProvisionSpec, environment: &Environment) {
        assert!(environment.is_provisioned(spec));
        assert_eq!(
            f.repository.worktree_branch(&spec.worktree),
            Some(spec.task_branch.clone())
        );
        assert_eq!(f.terminal.count("workspace"), 1);
        assert_eq!(f.terminal.count("split"), spec.agents.len() - 1);
        assert_eq!(f.terminal.count("launch"), spec.agents.len());
        for agent in &spec.agents {
            let pane = environment.pane(agent.role).unwrap();
            assert_eq!(
                f.terminal.inner.launched_command(pane).as_deref(),
                Some(agent.command_line.as_str())
            );
        }
        assert_eq!(&saved(f), environment);
    }

    #[test]
    fn provisions_the_triplet() {
        let f = fixture(1);
        let spec = triplet();
        let environment = provision(&f, &spec).unwrap();
        assert_provisioned_once(&f, &spec, &environment);
        assert_eq!(
            f.terminal
                .inner
                .workspace_directory(environment.workspace.as_ref().unwrap()),
            Some(spec.worktree.clone())
        );
    }

    #[test]
    fn provisions_a_single_review_agent() {
        let f = fixture(1);
        let spec = single();
        let environment = provision(&f, &spec).unwrap();
        assert_provisioned_once(&f, &spec, &environment);
        assert_eq!(environment.agents.len(), 1);
        assert!(environment.pane(Role::Merge).is_none());
    }

    #[test]
    fn each_step_performs_one_effect() {
        let f = fixture(1);
        let spec = triplet();
        let mut effects = Vec::new();
        loop {
            let before = (
                f.terminal.calls.lock().unwrap().len(),
                saved_or_new(&f, &spec),
            );
            let environment =
                block_on(
                    f.service
                        .provision_step(&f.store, &run(), "env", &spec, &commit("c0")),
                )
                .unwrap();
            assert!(f.terminal.calls.lock().unwrap().len() - before.0 <= 1);
            effects.push(environment.clone());
            if environment.is_provisioned(&spec) {
                break;
            }
        }
        // Worktree, move onto the base, workspace, two splits, three launches.
        assert_eq!(effects.len(), 8);
    }

    fn saved_or_new(f: &Fixture, spec: &ProvisionSpec) -> Environment {
        match block_on(f.store.load_pipeline_state(&run(), "env")).unwrap() {
            Some(saved) => serde_json::from_value(saved).unwrap(),
            None => Environment::new(spec),
        }
    }

    #[test]
    fn the_new_worktree_is_moved_onto_the_verified_head() {
        let f = fixture(1);
        let spec = triplet();
        let environment =
            block_on(
                f.service
                    .provision(&f.store, &run(), "env", &spec, &commit("c1")),
            )
            .unwrap();
        assert_eq!(
            f.repository.worktree_head(&spec.worktree),
            Some(commit("c1"))
        );
        assert_eq!(environment.synced_to, Some(commit("c1")));

        // Work in the provisioned worktree is never moved away again.
        block_on(f.repository.update_worktree(&spec.worktree, &commit("w1"))).unwrap();
        block_on(
            f.service
                .provision(&f.store, &run(), "env", &spec, &commit("c2")),
        )
        .unwrap();
        assert_eq!(
            f.repository.worktree_head(&spec.worktree),
            Some(commit("w1"))
        );
    }

    #[test]
    fn environment_serde_round_trip() {
        let f = fixture(1);
        let environment = provision(&f, &triplet()).unwrap();
        let json = serde_json::to_value(&environment).unwrap();
        assert_eq!(
            serde_json::from_value::<Environment>(json).unwrap(),
            environment
        );
    }

    #[test]
    fn a_crash_at_every_save_boundary_creates_no_duplicates() {
        for spec in [triplet(), single()] {
            // Before and after each effect, plus the move of the worktree.
            let saves = 2 * (2 + (spec.agents.len() - 1) + spec.agents.len()) + 1;
            for crash_at in 1..=saves {
                let f = fixture(1);
                *f.store.crash_at.lock().unwrap() = Some(crash_at);
                let context = format!("{} agents, crash at save {crash_at}", spec.agents.len());
                assert!(provision(&f, &spec).is_err(), "{context}");

                *f.store.crash_at.lock().unwrap() = None;
                let resumed = provision(&f, &spec).unwrap();

                assert_provisioned_once(&f, &spec, &resumed);
            }
        }
    }

    #[test]
    fn a_lost_response_or_request_is_reconciled_without_duplicates() {
        for call in ["workspace", "split", "launch"] {
            for lost in [Lost::Response, Lost::Request] {
                let f = fixture(1);
                let spec = triplet();
                *f.terminal.lose.lock().unwrap() = Some((call, lost));

                let error = provision(&f, &spec).unwrap_err();
                assert!(error.is_uncertain(), "{call}");
                assert!(saved(&f).started.is_some(), "{call}");

                let resumed = provision(&f, &spec).unwrap();
                assert_provisioned_once(&f, &spec, &resumed);
            }
        }
    }

    #[test]
    fn a_failed_effect_is_not_reconciled() {
        let f = fixture(1);
        let spec = triplet();
        *f.terminal.launch_failure.lock().unwrap() = Some(PortError::failed("refused"));

        assert!(provision(&f, &spec).unwrap_err().is_failed());
        assert_eq!(saved(&f).started, None);

        let resumed = provision(&f, &spec).unwrap();
        assert!(resumed.is_provisioned(&spec));
        assert_eq!(f.terminal.count("workspace"), 1);
    }

    #[test]
    fn paused_policy_blocks_launching() {
        let f = fixture(1);
        f.policy.pause(PauseReason::GlobalPause);
        let error = provision(&f, &triplet()).unwrap_err();
        assert!(matches!(
            error,
            PipelineError::Paused(PauseReason::GlobalPause)
        ));
        assert_eq!(f.terminal.count("launch"), 0);
        assert_eq!(saved(&f).started, None);
    }

    #[test]
    fn relaunch_consumes_one_agent_recovery() {
        let f = fixture(1);
        let spec = triplet();
        let environment = provision(&f, &spec).unwrap();
        let pane = environment.pane(Role::Review).unwrap().clone();
        let store = FakeRunStore::new();

        let error =
            block_on(
                f.service
                    .relaunch(&store, &run(), &environment, Role::Review, "again"),
            )
            .unwrap_err();
        assert!(matches!(error, PipelineError::Environment(_)));
        assert_eq!(f.policy.snapshot().agent_recovery_remaining, 1);

        f.terminal.inner.script_statuses(&pane, [TurnStatus::Gone]);
        block_on(
            f.service
                .relaunch(&store, &run(), &environment, Role::Review, "again"),
        )
        .unwrap();
        assert_eq!(
            f.terminal.inner.launched_command(&pane).as_deref(),
            Some("again")
        );
        assert_eq!(f.policy.snapshot().agent_recovery_remaining, 0);

        let error =
            block_on(
                f.service
                    .relaunch(&store, &run(), &environment, Role::Review, "third"),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            PipelineError::Paused(PauseReason::AgentRecoveryExhausted)
        ));
        assert_eq!(
            f.terminal.inner.launched_command(&pane).as_deref(),
            Some("again")
        );
        let restored = block_on(Policy::load_or_new(&store, &run(), &Limits::default())).unwrap();
        assert_eq!(
            restored.check_start(),
            Err(PauseReason::AgentRecoveryExhausted)
        );
    }

    #[test]
    fn a_consumed_recovery_is_saved_before_the_launch() {
        let f = fixture(2);
        let environment = provision(&f, &triplet()).unwrap();
        let pane = environment.pane(Role::Review).unwrap().clone();
        f.terminal.inner.script_statuses(&pane, [TurnStatus::Gone]);
        *f.terminal.launch_failure.lock().unwrap() = Some(PortError::uncertain("lost"));
        let store = FakeRunStore::new();

        let error =
            block_on(
                f.service
                    .relaunch(&store, &run(), &environment, Role::Review, "again"),
            )
            .unwrap_err();

        assert!(error.is_uncertain());
        let restored = block_on(Policy::load_or_new(&store, &run(), &Limits::default())).unwrap();
        assert_eq!(restored.snapshot().agent_recovery_remaining, 1);
    }

    #[test]
    fn cleanup_releases_everything_one_effect_per_step() {
        let f = fixture(1);
        let spec = triplet();
        let environment = provision(&f, &spec).unwrap();
        let workspace = environment.workspace.clone().unwrap();

        // Close the workspace, remove the worktree, prune.
        assert_eq!(clean_up(&f).unwrap(), 3);

        assert_eq!(f.repository.worktree_branch(&spec.worktree), None);
        assert!(!f.repository.has_branch(&spec.task_branch));
        assert_eq!(f.terminal.inner.workspace_directory(&workspace), None);
        let environment = saved(&f);
        assert!(environment.workspace.is_none() && !environment.worktree_created);
    }

    #[test]
    fn cleanup_skips_resources_that_are_already_gone() {
        let f = fixture(1);
        let spec = triplet();
        let environment = provision(&f, &spec).unwrap();
        // Remove both behind the service's back, then clean up; and clean up twice.
        block_on(
            f.terminal
                .close_workspace(environment.workspace.as_ref().unwrap()),
        )
        .unwrap();
        block_on(
            f.repository
                .remove_worktree(&spec.worktree, &spec.task_branch),
        )
        .unwrap();
        clean_up(&f).unwrap();
        clean_up(&f).unwrap();
    }

    #[test]
    fn cleanup_keeps_a_workspace_whose_close_failed() {
        let f = fixture(1);
        let environment = provision(&f, &triplet()).unwrap();
        let workspace = environment.workspace.clone().unwrap();
        *f.terminal.close_failure.lock().unwrap() = Some(PortError::failed("permission denied"));
        let error = clean_up(&f).unwrap_err();
        assert!(error.is_failed());
        assert_eq!(saved(&f).workspace.as_ref(), Some(&workspace));
        assert!(f.terminal.inner.workspace_directory(&workspace).is_some());
        clean_up(&f).unwrap();
        assert_eq!(f.terminal.inner.workspace_directory(&workspace), None);
        assert!(saved(&f).workspace.is_none());
    }

    #[test]
    fn cleanup_keeps_a_worktree_whose_removal_failed() {
        let f = fixture(1);
        let spec = triplet();
        provision(&f, &spec).unwrap();
        block_on(f.service.cleanup_step(&f.store, &run(), "env")).unwrap();
        f.repository
            .fail_next(PortError::failed("permission denied"));
        let error = clean_up(&f).unwrap_err();
        assert!(error.is_failed());
        assert!(saved(&f).worktree_created);
        assert!(f.repository.worktree_branch(&spec.worktree).is_some());
        assert!(f.repository.has_branch(&spec.task_branch));
        clean_up(&f).unwrap();
        assert_eq!(f.repository.worktree_branch(&spec.worktree), None);
        assert!(!f.repository.has_branch(&spec.task_branch));
        assert!(!saved(&f).worktree_created);
    }

    #[test]
    fn cleanup_returns_uncertain_errors_and_repeats_the_removal() {
        let f = fixture(1);
        let spec = triplet();
        provision(&f, &spec).unwrap();
        block_on(f.service.cleanup_step(&f.store, &run(), "env")).unwrap();
        f.repository.fail_next(PortError::uncertain("lost"));
        let error = clean_up(&f).unwrap_err();
        assert!(error.is_uncertain());
        let environment = saved(&f);
        assert!(environment.workspace.is_none() && environment.worktree_created);
        clean_up(&f).unwrap();
        assert_eq!(f.repository.worktree_branch(&spec.worktree), None);
    }

    #[test]
    fn a_crash_at_every_cleanup_boundary_finishes_the_cleanup() {
        for crash_at in 1..=2 {
            let f = fixture(1);
            let spec = triplet();
            provision(&f, &spec).unwrap();
            let saves = *f.store.saves.lock().unwrap();
            *f.store.crash_at.lock().unwrap() = Some(saves + crash_at);
            assert!(clean_up(&f).is_err(), "crash at {crash_at}");

            clean_up(&f).unwrap();
            assert_eq!(f.repository.worktree_branch(&spec.worktree), None);
            assert!(saved(&f).workspace.is_none());
        }
    }
}

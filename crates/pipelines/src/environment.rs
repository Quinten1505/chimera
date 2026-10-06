use std::path::PathBuf;
use std::sync::Arc;

use chimera_core::error::PortError;
use chimera_core::repository::Repository;
use chimera_core::run_store::RunStore;
use chimera_core::terminal::{Terminal, TurnStatus};
use chimera_core::{BranchName, PaneId, Role, RunId, WorkspaceId};
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

/// Worktree, workspace, and agents of one task. Doubles as the provisioning progress record:
/// it is saved after every effect, and each effect is skipped once it shows as done.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Environment {
    pub worktree: PathBuf,
    pub task_branch: BranchName,
    pub worktree_created: bool,
    pub workspace: Option<WorkspaceId>,
    pub agents: Vec<EnvironmentAgent>,
}

impl Environment {
    pub fn new(spec: &ProvisionSpec) -> Self {
        Self {
            worktree: spec.worktree.clone(),
            task_branch: spec.task_branch.clone(),
            worktree_created: false,
            workspace: None,
            agents: Vec::new(),
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
            && self.workspace.is_some()
            && self.agents.len() == spec.agents.len()
            && self.agents.iter().all(|agent| agent.launched)
    }
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

    /// Provisions the environment of `spec`, resuming from the progress saved under `instance`.
    /// The progress is saved after every effect.
    pub async fn provision(
        &self,
        store: &dyn RunStore,
        run: &RunId,
        instance: &str,
        spec: &ProvisionSpec,
    ) -> Result<Environment, PipelineError> {
        let mut environment = match store.load_pipeline_state(run, instance).await? {
            Some(saved) => serde_json::from_value(saved)?,
            None => Environment::new(spec),
        };
        while !environment.is_provisioned(spec) {
            environment = self.provision_step(spec, environment).await?;
            store
                .save_pipeline_state(run, instance, serde_json::to_value(&environment)?)
                .await?;
        }
        Ok(environment)
    }

    /// Performs the next missing effect and returns the updated progress: worktree, workspace,
    /// one split pane per further role, then each agent launch. Policy is checked before a
    /// launch.
    pub async fn provision_step(
        &self,
        spec: &ProvisionSpec,
        mut environment: Environment,
    ) -> Result<Environment, PipelineError> {
        if !environment.worktree_created {
            self.repository
                .create_worktree(&spec.worktree, &spec.task_branch, &spec.feature)
                .await?;
            environment.worktree_created = true;
        } else if environment.workspace.is_none() {
            let (workspace, pane) = self.terminal.create_workspace(&spec.worktree).await?;
            environment.workspace = Some(workspace);
            environment.agents.push(EnvironmentAgent {
                role: spec.agents[0].role,
                pane,
                launched: false,
            });
        } else if environment.agents.len() < spec.agents.len() {
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
        } else if let Some(agent) = environment.agents.iter_mut().find(|agent| !agent.launched) {
            self.policy.check_start().map_err(PipelineError::Paused)?;
            let launch = spec
                .agents
                .iter()
                .find(|launch| launch.role == agent.role)
                .expect("agent role comes from the spec");
            self.terminal
                .launch_agent(&agent.pane, &launch.command_line)
                .await?;
            agent.launched = true;
        }
        Ok(environment)
    }

    /// Launches `role`'s agent again in its pane after it is `Gone`, consuming one agent
    /// recovery from the policy.
    pub async fn relaunch(
        &self,
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
        match self.policy.permit_retry(
            Budget::AgentRecovery,
            &PortError::failed("agent is gone"),
            false,
        ) {
            Ok(()) => {}
            Err(RetryRefused::Paused(reason)) => return Err(PipelineError::Paused(reason)),
            Err(RetryRefused::NeedsReconciliation) => unreachable!("the error is not uncertain"),
        }
        self.terminal.launch_agent(pane, command_line).await?;
        Ok(())
    }

    /// Closes the workspace, removes the worktree, and prunes worktree metadata. A resource whose
    /// removal fails with a known-not-happened error is taken to be already gone and skipped;
    /// uncertain errors are returned. History is not touched.
    pub async fn cleanup(&self, environment: &mut Environment) -> Result<(), PipelineError> {
        if let Some(workspace) = &environment.workspace {
            skip_failed(self.terminal.close_workspace(workspace).await)?;
            environment.workspace = None;
        }
        if environment.worktree_created {
            skip_failed(
                self.repository
                    .remove_worktree(&environment.worktree, &environment.task_branch)
                    .await,
            )?;
            environment.worktree_created = false;
        }
        self.repository.prune_worktrees().await?;
        Ok(())
    }
}

fn skip_failed(result: Result<(), PortError>) -> Result<(), PortError> {
    match result {
        Err(error) if error.is_failed() => Ok(()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chimera_core::repository::FakeRepository;
    use chimera_core::run_store::FakeRunStore;
    use chimera_core::terminal::FakeTerminal;
    use chimera_core::{CommitId, Limits};
    use futures_executor::block_on;

    use super::*;
    use crate::PauseReason;

    fn branch(name: &str) -> BranchName {
        BranchName::new(name).unwrap()
    }

    /// Counts the creating calls made through to a [`FakeTerminal`].
    struct CountingTerminal {
        inner: FakeTerminal,
        calls: Mutex<Vec<&'static str>>,
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
        fn log(&self, call: &'static str) {
            self.calls.lock().unwrap().push(call);
        }
    }

    #[async_trait]
    impl Terminal for CountingTerminal {
        async fn create_workspace(
            &self,
            directory: &Path,
        ) -> Result<(WorkspaceId, PaneId), PortError> {
            self.log("workspace");
            self.inner.create_workspace(directory).await
        }
        async fn split_pane(
            &self,
            workspace: &WorkspaceId,
            pane: &PaneId,
        ) -> Result<PaneId, PortError> {
            self.log("split");
            self.inner.split_pane(workspace, pane).await
        }
        async fn launch_agent(&self, pane: &PaneId, command_line: &str) -> Result<(), PortError> {
            self.log("launch");
            self.inner.launch_agent(pane, command_line).await
        }
        async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError> {
            self.inner.send_prompt(pane, prompt).await
        }
        async fn read_status(&self, pane: &PaneId) -> Result<TurnStatus, PortError> {
            self.inner.read_status(pane).await
        }
        async fn read_output(&self, pane: &PaneId) -> Result<String, PortError> {
            self.inner.read_output(pane).await
        }
        async fn close_workspace(&self, workspace: &WorkspaceId) -> Result<(), PortError> {
            self.inner.close_workspace(workspace).await
        }
    }

    struct Fixture {
        repository: Arc<FakeRepository>,
        terminal: Arc<CountingTerminal>,
        policy: Arc<Policy>,
        service: EnvironmentService,
    }

    fn fixture(agent_recovery: u32) -> Fixture {
        let repository = Arc::new(FakeRepository::new(
            branch("main"),
            CommitId::new("c0").unwrap(),
        ));
        repository.add_branch(branch("feat"), CommitId::new("c0").unwrap());
        let terminal = Arc::new(CountingTerminal {
            inner: FakeTerminal::new(),
            calls: Mutex::new(Vec::new()),
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
                .provision(&FakeRunStore::new(), &run(), "env", spec),
        )
    }

    fn assert_provisioned_once(f: &Fixture, spec: &ProvisionSpec, environment: &Environment) {
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
    }

    #[test]
    fn provisions_the_triplet() {
        let f = fixture(1);
        let spec = triplet();
        let environment = provision(&f, &spec).unwrap();
        assert!(environment.is_provisioned(&spec));
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
    fn resume_after_interruption_at_each_substep_creates_no_duplicates() {
        for spec in [triplet(), single()] {
            // worktree + workspace + splits + launches
            let steps = 2 + (spec.agents.len() - 1) + spec.agents.len();
            for interrupted_after in 0..steps {
                let f = fixture(1);
                let mut environment = Environment::new(&spec);
                for _ in 0..interrupted_after {
                    environment = block_on(f.service.provision_step(&spec, environment)).unwrap();
                }
                // Saved progress is all that survives the restart.
                let store = FakeRunStore::new();
                block_on(store.save_pipeline_state(
                    &run(),
                    "env",
                    serde_json::to_value(&environment).unwrap(),
                ))
                .unwrap();
                let resumed = block_on(f.service.provision(&store, &run(), "env", &spec)).unwrap();
                assert_provisioned_once(&f, &spec, &resumed);
                let saved = store.pipeline_states();
                assert_eq!(
                    saved[&(run(), "env".to_string())],
                    serde_json::to_value(&resumed).unwrap()
                );
            }
        }
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
    }

    #[test]
    fn relaunch_consumes_one_agent_recovery() {
        let f = fixture(1);
        let spec = triplet();
        let environment = provision(&f, &spec).unwrap();
        let pane = environment.pane(Role::Review).unwrap().clone();

        let error = block_on(f.service.relaunch(&environment, Role::Review, "again")).unwrap_err();
        assert!(matches!(error, PipelineError::Environment(_)));
        assert_eq!(f.policy.snapshot().agent_recovery_remaining, 1);

        f.terminal.inner.script_statuses(&pane, [TurnStatus::Gone]);
        block_on(f.service.relaunch(&environment, Role::Review, "again")).unwrap();
        assert_eq!(
            f.terminal.inner.launched_command(&pane).as_deref(),
            Some("again")
        );
        assert_eq!(f.policy.snapshot().agent_recovery_remaining, 0);

        let error = block_on(f.service.relaunch(&environment, Role::Review, "third")).unwrap_err();
        assert!(matches!(
            error,
            PipelineError::Paused(PauseReason::AgentRecoveryExhausted)
        ));
        assert_eq!(
            f.terminal.inner.launched_command(&pane).as_deref(),
            Some("again")
        );
    }

    #[test]
    fn cleanup_releases_everything() {
        let f = fixture(1);
        let spec = triplet();
        let mut environment = provision(&f, &spec).unwrap();
        let workspace = environment.workspace.clone().unwrap();
        block_on(f.service.cleanup(&mut environment)).unwrap();
        assert_eq!(f.repository.worktree_branch(&spec.worktree), None);
        assert!(!f.repository.has_branch(&spec.task_branch));
        assert_eq!(f.terminal.inner.workspace_directory(&workspace), None);
        assert!(environment.workspace.is_none() && !environment.worktree_created);
    }

    #[test]
    fn cleanup_skips_resources_that_are_already_gone() {
        let f = fixture(1);
        let spec = triplet();
        let mut environment = provision(&f, &spec).unwrap();
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
        block_on(f.service.cleanup(&mut environment)).unwrap();
        block_on(f.service.cleanup(&mut environment)).unwrap();
    }

    #[test]
    fn cleanup_returns_uncertain_errors() {
        let f = fixture(1);
        let mut environment = provision(&f, &triplet()).unwrap();
        f.repository.fail_next(PortError::uncertain("lost"));
        let error = block_on(f.service.cleanup(&mut environment)).unwrap_err();
        assert!(error.is_uncertain());
        assert!(environment.workspace.is_none() && environment.worktree_created);
    }
}

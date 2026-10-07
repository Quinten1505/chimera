use crate::{HerdrClient, HerdrError, herdr::Effect};
use chimera_core::PaneId;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout_at};

/// How long [`HerdrClient::launch_command_line`] waits for Herdr to report an agent on the pane.
pub(crate) const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(30);

const POLL_INTERVAL: Duration = Duration::from_millis(250);

impl HerdrClient {
    /// Launch an agent in an existing shell pane and wait until Herdr reports one there.
    ///
    /// `line` is a ready command line, typed into the pane's shell exactly as given; it must be
    /// a single line. The adapter knows nothing about providers: `agent.start` needs an agent
    /// kind, so the line is typed into the pane instead, and readiness is detected by polling
    /// `pane.get` for an agent.
    ///
    /// Invalid input is *failed*. Once the line has been sent, a timeout, a dropped connection
    /// or any other error is *uncertain*, because the process may already be running. Nothing is
    /// retried. The launch requests use a socket timeout longer than `ready_timeout` (the client
    /// timeout on top of it), and each readiness poll is also bounded by the remaining wait, so
    /// the socket timeout never cuts the wait short.
    pub(crate) async fn launch_command_line(
        &self,
        pane: &PaneId,
        line: &str,
        ready_timeout: Duration,
    ) -> Result<(), HerdrError> {
        if pane.as_str().trim().is_empty() {
            return Err(HerdrError::InvalidInput("pane ID must not be empty"));
        }
        if line.trim().is_empty() {
            return Err(HerdrError::InvalidInput("command line must not be empty"));
        }
        if line.contains(['\0', '\n', '\r']) {
            return Err(HerdrError::InvalidInput(
                "command line must be a single line without NUL",
            ));
        }
        let deadline = Instant::now() + ready_timeout;
        let socket_timeout = ready_timeout.saturating_add(self.timeout());

        let _: serde_json::Value = self
            .request_within(
                "pane.send_input",
                &serde_json::json!({"pane_id": pane, "text": line, "keys": ["enter"]}),
                "ok",
                Effect::Change,
                socket_timeout,
            )
            .await?;

        // The command is on its way: every outcome from here on is uncertain.
        loop {
            if self.agent_present(pane, deadline, socket_timeout).await? {
                return Ok(());
            }
            if Instant::now() + POLL_INTERVAL >= deadline {
                return Err(HerdrError::Timeout { uncertain: true });
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn agent_present(
        &self,
        pane: &PaneId,
        deadline: Instant,
        socket_timeout: Duration,
    ) -> Result<bool, HerdrError> {
        #[derive(Deserialize)]
        struct Info {
            agent: Option<String>,
        }
        #[derive(Deserialize)]
        struct Found {
            pane: Info,
        }
        let params = serde_json::json!({"pane_id": pane});
        let poll = self.request_within::<Found>(
            "pane.get",
            &params,
            "pane_info",
            Effect::Read,
            socket_timeout,
        );
        match timeout_at(deadline, poll).await {
            Err(_) => Err(HerdrError::Timeout { uncertain: true }),
            Ok(Ok(found)) => Ok(found.pane.agent.is_some()),
            Ok(Err(HerdrError::Timeout { .. })) => Err(HerdrError::Timeout { uncertain: true }),
            Ok(Err(error)) if error.is_uncertain() => Err(error),
            Ok(Err(error)) => Err(HerdrError::Protocol {
                message: format!("readiness check failed after launch: {error}"),
                uncertain: true,
            }),
        }
    }
}

#[cfg(all(test, unix))]
mod tests;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentPhase {
    Created,
    Running,
    WaitingForPermission,
    Finished,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionPhase {
    Created,
    Running,
    Finished,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionState {
    pub session_id: String,
    pub phase: SessionPhase,
    pub revision: u64,
}

impl SessionState {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            phase: SessionPhase::Created,
            revision: 0,
        }
    }

    pub fn start(&mut self) -> Result<(), SessionTransitionError> {
        self.transition(SessionPhase::Running, &[SessionPhase::Created])
    }

    pub fn finish(&mut self) -> Result<(), SessionTransitionError> {
        self.transition(SessionPhase::Finished, &[SessionPhase::Running])
    }

    pub fn fail(&mut self) -> Result<(), SessionTransitionError> {
        self.transition(
            SessionPhase::Failed,
            &[SessionPhase::Created, SessionPhase::Running],
        )
    }

    fn transition(
        &mut self,
        next: SessionPhase,
        allowed_from: &[SessionPhase],
    ) -> Result<(), SessionTransitionError> {
        if !allowed_from.contains(&self.phase) {
            return Err(SessionTransitionError {
                from: self.phase,
                to: next,
            });
        }
        self.phase = next;
        self.revision += 1;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid session transition from {from:?} to {to:?}")]
pub struct SessionTransitionError {
    pub from: SessionPhase,
    pub to: SessionPhase,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_happy_path_and_increments_revision() {
        let mut session = SessionState::new("session-1");
        session.start().expect("new sessions can start");
        session.finish().expect("running sessions can finish");

        assert_eq!(session.phase, SessionPhase::Finished);
        assert_eq!(session.revision, 2);
    }

    #[test]
    fn rejects_transitions_from_a_terminal_phase() {
        let mut session = SessionState::new("session-1");
        session.fail().expect("new sessions can fail");

        assert!(session.start().is_err());
        assert_eq!(session.revision, 1);
    }
}

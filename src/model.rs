//! Provider-independent data model: projects, pipelines and jobs.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Normalized CI status shared by all providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Status {
    /// Waiting to start: created, pending, waiting for a runner, ...
    Queued,
    Running,
    /// Waiting for a human (manual job, deployment approval).
    Manual,
    Success,
    Failed,
    Canceled,
    Skipped,
    Unknown,
}

impl Status {
    pub fn is_active(self) -> bool {
        matches!(self, Status::Queued | Status::Running)
    }

    /// Combines the statuses of several pipelines on the same commit (GitHub
    /// runs one per workflow) into one. Failure wins: a red build is the most
    /// important thing to see, even while other workflows are still running.
    pub fn aggregate(statuses: impl IntoIterator<Item = Status>) -> Status {
        statuses
            .into_iter()
            .min_by_key(|s| match s {
                Status::Failed => 0,
                Status::Running => 1,
                Status::Queued => 2,
                Status::Manual => 3,
                Status::Unknown => 4,
                Status::Canceled => 5,
                Status::Success => 6,
                Status::Skipped => 7,
            })
            .unwrap_or(Status::Unknown)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(match self {
            Status::Queued => "queued",
            Status::Running => "running",
            Status::Manual => "manual",
            Status::Success => "success",
            Status::Failed => "failed",
            Status::Canceled => "canceled",
            Status::Skipped => "skipped",
            Status::Unknown => "unknown",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    /// Name of the config source this project belongs to.
    pub source: String,
    /// Provider-specific identifier used in API calls.
    pub id: String,
    /// Full path, e.g. `group/subgroup/project`.
    pub path: String,
    pub web_url: String,
    /// `None` for empty repositories.
    pub default_branch: Option<String>,
    pub archived: bool,
    pub fork: bool,
    /// Whether CI/CD is enabled. Projects without it cannot have pipelines.
    pub ci_enabled: bool,
}

/// What a pipeline was run for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineKind {
    Branch(String),
    Tag(String),
    MergeRequest(u64),
    PullRequest(u64),
}

impl fmt::Display for PipelineKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineKind::Branch(name) => write!(f, "branch {name}"),
            PipelineKind::Tag(name) => write!(f, "tag {name}"),
            PipelineKind::MergeRequest(number) => write!(f, "MR !{number}"),
            PipelineKind::PullRequest(number) => write!(f, "PR #{number}"),
        }
    }
}

/// One GitLab pipeline or one GitHub workflow run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: u64,
    /// Per-project number shown in the web UI, if the provider has one.
    pub number: Option<u64>,
    /// Pipeline or workflow name, if any.
    pub name: Option<String>,
    pub kind: PipelineKind,
    pub sha: String,
    pub status: Status,
    /// Provider-specific trigger, e.g. `push`, `merge_request_event`, `schedule`.
    pub trigger: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub web_url: String,
}

/// The CI state of a single commit, made up of one or more pipelines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitStatus {
    pub sha: String,
    pub status: Status,
    pub pipelines: Vec<Pipeline>,
}

/// Whether anything is running or queued, on the default branch or elsewhere.
pub fn is_busy(default: Option<&CommitStatus>, active: &[Pipeline]) -> bool {
    default.is_some_and(|c| c.status.is_active()) || active.iter().any(|p| p.status.is_active())
}

impl CommitStatus {
    /// Returns `None` if there are no pipelines.
    pub fn new(pipelines: Vec<Pipeline>) -> Option<Self> {
        let sha = pipelines.first()?.sha.clone();
        Some(Self {
            sha,
            status: Status::aggregate(pipelines.iter().map(|p| p.status)),
            pipelines,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: u64,
    pub name: String,
    pub stage: Option<String>,
    pub status: Status,
    /// A failure of this job does not fail the pipeline.
    pub allow_failure: bool,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub web_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_prefers_failure_then_activity() {
        use Status::*;
        assert_eq!(Status::aggregate([Success, Running, Failed]), Failed);
        assert_eq!(Status::aggregate([Success, Queued, Running]), Running);
        assert_eq!(Status::aggregate([Success, Skipped]), Success);
        assert_eq!(Status::aggregate([]), Unknown);
    }
}

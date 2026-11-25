//! CI providers, behind a common interface.

mod gitlab;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};

use crate::config::{SourceConfig, SourceKind};
use crate::filter::ProjectFilter;
use crate::http::RateLimit;
use crate::model::{CommitStatus, Job, Pipeline, Project};

/// A configured source (one account on one CI instance).
pub struct Source {
    pub name: String,
    pub kind: SourceKind,
    filter: ProjectFilter,
    exclude_archived: bool,
    exclude_forks: bool,
    backend: Backend,
}

enum Backend {
    GitLab(gitlab::GitLab),
}

pub struct Discovery {
    pub projects: Vec<Project>,
    /// Non-fatal problems, e.g. an included project that does not exist.
    pub warnings: Vec<String>,
    /// Projects that matched the filters but were hidden because CI/CD is disabled.
    pub without_ci: usize,
}

impl Source {
    pub fn new(config: &SourceConfig) -> Result<Self> {
        let backend = match config.kind {
            SourceKind::GitLab => Backend::GitLab(gitlab::GitLab::new(
                &config.name,
                &config.url,
                &config.token,
            )?),
            SourceKind::GitHub => bail!(
                "source `{}`: GitHub support is not implemented yet",
                config.name
            ),
        };
        Ok(Self {
            name: config.name.clone(),
            kind: config.kind,
            filter: config.filter.clone(),
            exclude_archived: config.exclude_archived,
            exclude_forks: config.exclude_forks,
            backend,
        })
    }

    /// Finds all projects of this source that pass the configured filters.
    pub async fn discover(&self) -> Result<Discovery> {
        let targets = self.filter.discovery_targets();
        let (projects, warnings) = match &self.backend {
            Backend::GitLab(gl) => gl.discover(&targets, self.exclude_archived).await,
        }
        .with_context(|| format!("source `{}`: could not list projects", self.name))?;
        let (mut projects, without_ci): (Vec<Project>, Vec<Project>) = projects
            .into_iter()
            .filter(|p| {
                self.filter.matches(&p.path)
                    && !(self.exclude_archived && p.archived)
                    && !(self.exclude_forks && p.fork)
            })
            .partition(|p| p.ci_enabled);
        projects.sort_by_key(|p| p.path.to_lowercase());
        Ok(Discovery {
            projects,
            warnings,
            without_ci: without_ci.len(),
        })
    }

    /// CI status of the default branch, from its newest pipeline.
    pub async fn default_branch(&self, project: &Project) -> Result<Option<CommitStatus>> {
        let Some(branch) = &project.default_branch else {
            return Ok(None);
        };
        let pipelines = match &self.backend {
            Backend::GitLab(gl) => gl
                .latest_pipeline(project, branch)
                .await?
                .into_iter()
                .collect(),
        };
        Ok(CommitStatus::new(pipelines))
    }

    /// Pipelines on any branch, tag or merge request that are running, or
    /// that changed since `since`. Newest first.
    pub async fn active_pipelines(
        &self,
        project: &Project,
        since: DateTime<Utc>,
    ) -> Result<Vec<Pipeline>> {
        match &self.backend {
            Backend::GitLab(gl) => gl.active_pipelines(project, since).await,
        }
    }

    pub async fn jobs(&self, project: &Project, pipeline: &Pipeline) -> Result<Vec<Job>> {
        match &self.backend {
            Backend::GitLab(gl) => gl.jobs(project, pipeline.id).await,
        }
    }

    pub fn rate_limit(&self) -> Option<RateLimit> {
        match &self.backend {
            Backend::GitLab(gl) => gl.rate_limit(),
        }
    }
}

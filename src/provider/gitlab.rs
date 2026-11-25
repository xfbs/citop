//! GitLab REST API (v4) backend.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use anyhow::{Result, anyhow};
use chrono::{DateTime, SecondsFormat, Utc};
use futures::future::try_join_all;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use serde::de::IgnoredAny;
use url::Url;

use crate::filter::Target;
use crate::http::{HttpError, RateLimit, ReadOnlyClient};
use crate::model::{Job, Pipeline, PipelineKind, Project, Status};
use crate::secret::Secret;

const PER_PAGE: &str = "100";
/// Upper bound for paginated lists, to stay sane on huge instances.
const MAX_PAGES: usize = 50;
/// Running pipelines without any update for this long are considered stuck.
const STALE_AFTER: chrono::TimeDelta = chrono::TimeDelta::days(1);

pub struct GitLab {
    source: String,
    api: Url,
    client: ReadOnlyClient,
    /// Detailed pipelines by project id and pipeline id. List endpoints lack
    /// some fields (e.g. whether the ref is a tag), so details are fetched
    /// once per pipeline and reused while the pipeline is unchanged.
    details: Mutex<HashMap<String, HashMap<u64, Pipeline>>>,
    /// The newest default-branch pipeline by project id, likewise.
    latest: Mutex<HashMap<String, Pipeline>>,
}

enum Found {
    Projects(Vec<Project>),
    Missing(String),
}

impl GitLab {
    pub fn new(source: &str, web_url: &Url, token: &Secret) -> Result<Self> {
        let mut api = web_url.clone();
        api.path_segments_mut()
            .map_err(|_| anyhow!("`{web_url}` cannot be used as a base URL"))?
            .pop_if_empty()
            .extend(["api", "v4", ""]);
        Ok(Self {
            source: source.to_string(),
            api,
            client: ReadOnlyClient::new(token, HeaderMap::new())?,
            details: Mutex::default(),
            latest: Mutex::default(),
        })
    }

    pub fn rate_limit(&self) -> Option<RateLimit> {
        self.client.rate_limit()
    }

    /// Builds an API URL. Segments are percent-encoded, including `/`, so a
    /// project path can be used as a segment (`group%2Fproject`).
    fn endpoint(&self, segments: &[&str]) -> Url {
        let mut url = self.api.clone();
        url.path_segments_mut()
            .expect("API URL is a base URL")
            .pop_if_empty()
            .extend(segments);
        url
    }

    /// Lists candidate projects for the given targets. Returns the projects
    /// plus warnings for targets that did not exist.
    pub async fn discover(
        &self,
        targets: &[Target],
        exclude_archived: bool,
    ) -> Result<(Vec<Project>, Vec<String>)> {
        let found = try_join_all(
            targets
                .iter()
                .map(|t| self.discover_target(t, exclude_archived)),
        )
        .await?;
        let mut seen = HashSet::new();
        let mut projects = Vec::new();
        let mut warnings = Vec::new();
        for found in found {
            match found {
                Found::Projects(list) => {
                    projects.extend(list.into_iter().filter(|p| seen.insert(p.id.clone())));
                }
                Found::Missing(warning) => warnings.push(warning),
            }
        }
        Ok((projects, warnings))
    }

    async fn discover_target(
        &self,
        target: &Target,
        exclude_archived: bool,
    ) -> Result<Found, HttpError> {
        match target {
            Target::All => {
                let mut url = self.endpoint(&["projects"]);
                url.query_pairs_mut().append_pair("membership", "true");
                self.list_projects(url, exclude_archived)
                    .await
                    .map(Found::Projects)
            }
            Target::Project(path) => match self
                .client
                .get_json::<GlProject>(self.endpoint(&["projects", path]))
                .await
            {
                Ok(project) => Ok(Found::Projects(vec![project.into_project(&self.source)])),
                Err(e) if e.is_not_found() => Ok(Found::Missing(format!(
                    "project `{path}` not found or not accessible"
                ))),
                Err(e) => Err(e),
            },
            Target::Namespace { path, recursive } => {
                let mut url = self.endpoint(&["groups", path, "projects"]);
                url.query_pairs_mut().append_pair(
                    "include_subgroups",
                    if *recursive { "true" } else { "false" },
                );
                match self.list_projects(url, exclude_archived).await {
                    Ok(list) => Ok(Found::Projects(list)),
                    // Not a group; a top-level namespace may also be a user.
                    Err(e) if e.is_not_found() && !path.contains('/') => {
                        let url = self.endpoint(&["users", path, "projects"]);
                        match self.list_projects(url, exclude_archived).await {
                            Ok(list) => Ok(Found::Projects(list)),
                            Err(e) if e.is_not_found() => Ok(Found::Missing(format!(
                                "group or user `{path}` not found or not accessible"
                            ))),
                            Err(e) => Err(e),
                        }
                    }
                    Err(e) if e.is_not_found() => Ok(Found::Missing(format!(
                        "group `{path}` not found or not accessible"
                    ))),
                    Err(e) => Err(e),
                }
            }
        }
    }

    async fn list_projects(
        &self,
        mut url: Url,
        exclude_archived: bool,
    ) -> Result<Vec<Project>, HttpError> {
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("per_page", PER_PAGE);
            if exclude_archived {
                query.append_pair("archived", "false");
            }
        }
        let projects: Vec<GlProject> = self.client.get_all(url, MAX_PAGES).await?;
        Ok(projects
            .into_iter()
            .map(|p| p.into_project(&self.source))
            .collect())
    }

    /// The newest pipeline that ran on `branch`, if any.
    pub async fn latest_pipeline(
        &self,
        project: &Project,
        branch: &str,
    ) -> Result<Option<Pipeline>> {
        // Not `pipelines/latest`: that answers 403 instead of 404 when the
        // branch has no pipelines.
        let mut url = self.endpoint(&["projects", &project.id, "pipelines"]);
        url.query_pairs_mut()
            .append_pair("ref", branch)
            .append_pair("per_page", "1");
        let Some(summary) = self
            .client
            .get_json_cached::<Vec<GlPipeline>>(url)
            .await?
            .into_iter()
            .next()
        else {
            self.latest.lock().unwrap().remove(&project.id);
            return Ok(None);
        };

        let cached = self
            .latest
            .lock()
            .unwrap()
            .get(&project.id)
            .filter(|p| p.id == summary.id && summary.is_unchanged(p))
            .cloned();
        if cached.is_some() {
            return Ok(cached);
        }
        let pipeline = self.pipeline(project, summary.id).await?;
        let mut latest = self.latest.lock().unwrap();
        match &pipeline {
            Some(p) => latest.insert(project.id.clone(), p.clone()),
            None => latest.remove(&project.id),
        };
        Ok(pipeline)
    }

    /// Pipelines on any ref that are running, or were updated since `since`.
    pub async fn active_pipelines(
        &self,
        project: &Project,
        since: DateTime<Utc>,
    ) -> Result<Vec<Pipeline>> {
        let base = self.endpoint(&["projects", &project.id, "pipelines"]);
        let mut recent = base.clone();
        recent
            .query_pairs_mut()
            .append_pair(
                "updated_after",
                &since.to_rfc3339_opts(SecondsFormat::Secs, true),
            )
            .append_pair("per_page", "50");
        // Long-running pipelines are not necessarily updated recently.
        let mut running = base;
        running
            .query_pairs_mut()
            .append_pair("scope", "running")
            .append_pair("per_page", "50");
        let (recent, running) = futures::try_join!(
            self.client.get_json::<Vec<GlPipeline>>(recent),
            self.client.get_json::<Vec<GlPipeline>>(running),
        )?;
        // GitLab occasionally leaves pipelines "running" forever after their
        // jobs finished; don't let those clutter the list.
        let stale = Utc::now() - STALE_AFTER;
        let summaries: HashMap<u64, GlPipeline> = recent
            .into_iter()
            .chain(running.into_iter().filter(|p| p.updated_at > stale))
            .map(|p| (p.id, p))
            .collect();

        let known = self
            .details
            .lock()
            .unwrap()
            .get(&project.id)
            .cloned()
            .unwrap_or_default();
        let pipelines = try_join_all(summaries.into_values().map(|summary| {
            let cached = known
                .get(&summary.id)
                .filter(|p| summary.is_unchanged(p))
                .cloned();
            async move {
                match cached {
                    Some(pipeline) => Ok(Some(pipeline)),
                    None => self.pipeline(project, summary.id).await,
                }
            }
        }))
        .await?;
        let mut pipelines: Vec<Pipeline> = pipelines.into_iter().flatten().collect();
        pipelines.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id)));

        self.details.lock().unwrap().insert(
            project.id.clone(),
            pipelines.iter().map(|p| (p.id, p.clone())).collect(),
        );
        Ok(pipelines)
    }

    /// A single pipeline, or `None` if it no longer exists.
    async fn pipeline(&self, project: &Project, id: u64) -> Result<Option<Pipeline>> {
        let url = self.endpoint(&["projects", &project.id, "pipelines", &id.to_string()]);
        match self.client.get_json::<GlPipeline>(url).await {
            Ok(pipeline) => Ok(Some(pipeline.into_pipeline())),
            Err(e) if e.is_not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// The jobs of a pipeline, without retried ones, in pipeline order.
    pub async fn jobs(&self, project: &Project, pipeline_id: u64) -> Result<Vec<Job>> {
        let mut url = self.endpoint(&[
            "projects",
            &project.id,
            "pipelines",
            &pipeline_id.to_string(),
            "jobs",
        ]);
        url.query_pairs_mut().append_pair("per_page", PER_PAGE);
        let jobs: Vec<GlJob> = self.client.get_all(url, MAX_PAGES).await?;
        let mut jobs: Vec<Job> = jobs.into_iter().map(GlJob::into_job).collect();
        jobs.sort_by_key(|j| j.id);
        Ok(jobs)
    }
}

fn map_status(status: &str) -> Status {
    match status {
        "created"
        | "waiting_for_resource"
        | "preparing"
        | "pending"
        | "scheduled"
        | "waiting_for_callback" => Status::Queued,
        "running" | "canceling" => Status::Running,
        "manual" => Status::Manual,
        "success" => Status::Success,
        "failed" => Status::Failed,
        "canceled" => Status::Canceled,
        "skipped" => Status::Skipped,
        _ => Status::Unknown,
    }
}

fn classify(git_ref: &str, tag: bool) -> PipelineKind {
    if let Some(rest) = git_ref.strip_prefix("refs/merge-requests/")
        && let Some(Ok(iid)) = rest.split('/').next().map(str::parse)
    {
        return PipelineKind::MergeRequest(iid);
    }
    if tag {
        PipelineKind::Tag(git_ref.to_string())
    } else {
        PipelineKind::Branch(git_ref.to_string())
    }
}

#[derive(Deserialize)]
struct GlProject {
    id: u64,
    path_with_namespace: String,
    web_url: String,
    default_branch: Option<String>,
    #[serde(default)]
    archived: bool,
    /// Only present for forks.
    forked_from_project: Option<IgnoredAny>,
    /// `enabled`, `private` or `disabled`; absent on older instances.
    builds_access_level: Option<String>,
    /// Deprecated predecessor of `builds_access_level`.
    jobs_enabled: Option<bool>,
}

impl GlProject {
    fn into_project(self, source: &str) -> Project {
        Project {
            source: source.to_string(),
            id: self.id.to_string(),
            path: self.path_with_namespace,
            web_url: self.web_url,
            default_branch: self.default_branch,
            archived: self.archived,
            fork: self.forked_from_project.is_some(),
            ci_enabled: self
                .builds_access_level
                .map(|level| level != "disabled")
                .or(self.jobs_enabled)
                .unwrap_or(true),
        }
    }
}

/// A pipeline from either a list (fewer fields) or a detail endpoint.
#[derive(Deserialize)]
struct GlPipeline {
    id: u64,
    iid: Option<u64>,
    name: Option<String>,
    #[serde(rename = "ref")]
    git_ref: String,
    sha: String,
    status: String,
    source: Option<String>,
    /// Only in detail responses.
    #[serde(default)]
    tag: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    web_url: String,
}

impl GlPipeline {
    /// Whether `pipeline`, fetched earlier, still matches this summary.
    fn is_unchanged(&self, pipeline: &Pipeline) -> bool {
        pipeline.updated_at == self.updated_at && pipeline.status == map_status(&self.status)
    }

    fn into_pipeline(self) -> Pipeline {
        Pipeline {
            id: self.id,
            number: self.iid,
            name: self.name.filter(|n| !n.is_empty()),
            kind: classify(&self.git_ref, self.tag),
            sha: self.sha,
            status: map_status(&self.status),
            trigger: self.source.unwrap_or_default(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            web_url: self.web_url,
        }
    }
}

#[derive(Deserialize)]
struct GlJob {
    id: u64,
    name: String,
    stage: Option<String>,
    status: String,
    #[serde(default)]
    allow_failure: bool,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    web_url: String,
}

impl GlJob {
    fn into_job(self) -> Job {
        Job {
            id: self.id,
            name: self.name,
            stage: self.stage,
            status: map_status(&self.status),
            allow_failure: self.allow_failure,
            started_at: self.started_at,
            finished_at: self.finished_at,
            web_url: self.web_url,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gitlab(url: &str) -> GitLab {
        GitLab::new("test", &Url::parse(url).unwrap(), &Secret::new("t".into())).unwrap()
    }

    #[test]
    fn builds_endpoints() {
        let gl = gitlab("https://gitlab.com/");
        assert_eq!(
            gl.endpoint(&["projects", "group/sub/project"]).as_str(),
            "https://gitlab.com/api/v4/projects/group%2Fsub%2Fproject"
        );
        let gl = gitlab("https://example.com/gitlab/");
        assert_eq!(
            gl.endpoint(&["projects", "1", "pipelines"]).as_str(),
            "https://example.com/gitlab/api/v4/projects/1/pipelines"
        );
    }

    #[test]
    fn classifies_refs() {
        assert_eq!(classify("main", false), PipelineKind::Branch("main".into()));
        assert_eq!(classify("v1.0.0", true), PipelineKind::Tag("v1.0.0".into()));
        assert_eq!(
            classify("refs/merge-requests/42/head", false),
            PipelineKind::MergeRequest(42)
        );
        assert_eq!(
            classify("refs/merge-requests/7/merge", false),
            PipelineKind::MergeRequest(7)
        );
        assert_eq!(
            classify("refs/merge-requests/x/head", false),
            PipelineKind::Branch("refs/merge-requests/x/head".into())
        );
    }

    #[test]
    fn parses_api_responses() {
        let project: GlProject = serde_json::from_str(
            r#"{"id": 3, "path_with_namespace": "g/p", "web_url": "https://gitlab.com/g/p",
                "default_branch": "main", "archived": false, "forked_from_project": {"id": 1}, "builds_access_level": "disabled", "extra": 1}"#,
        )
        .unwrap();
        let project = project.into_project("src");
        assert!(project.fork);
        assert!(!project.ci_enabled);
        assert_eq!(project.id, "3");

        let pipeline: GlPipeline = serde_json::from_str(
            r#"{"id": 10, "iid": 2, "project_id": 3, "ref": "v1", "sha": "abc", "status": "waiting_for_resource",
                "source": "push", "tag": true, "created_at": "2026-11-25T10:00:00.123Z",
                "updated_at": "2026-11-25T10:01:00Z", "started_at": null, "finished_at": null,
                "web_url": "https://gitlab.com/g/p/-/pipelines/10"}"#,
        )
        .unwrap();
        let pipeline = pipeline.into_pipeline();
        assert_eq!(pipeline.kind, PipelineKind::Tag("v1".into()));
        assert_eq!(pipeline.status, Status::Queued);
        assert_eq!(pipeline.number, Some(2));
    }
}

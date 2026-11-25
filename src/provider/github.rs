//! GitHub REST API backend (github.com and GitHub Enterprise Server).
//!
//! GitHub Actions has no pipelines, only one workflow run per workflow and
//! commit. Each run becomes a [`Pipeline`]; the status of a commit is the
//! combination of all its runs.
//!
//! The rate limit (5000 requests per hour) is much tighter than GitLab's.
//! Lists that are polled repeatedly are therefore requested conditionally,
//! as a `304 Not Modified` does not count against it, and their URLs are
//! kept stable between polls.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use chrono::{DateTime, DurationRound, SecondsFormat, TimeDelta, Utc};
use futures::future::try_join_all;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use url::Url;

use crate::filter::Target;
use crate::http::{HttpError, RateLimit, ReadOnlyClient, ResponseCache};
use crate::model::{Job, Pipeline, PipelineKind, Project, Status};
use crate::secret::Secret;

const PER_PAGE: &str = "100";
/// Upper bound for paginated lists, to stay sane on huge organizations.
const MAX_PAGES: usize = 50;
/// Runs looked at to find the newest commit on the default branch.
const DEFAULT_BRANCH_RUNS: &str = "30";
/// Runs without any update for this long are considered stuck.
const STALE_AFTER: TimeDelta = TimeDelta::days(1);
/// The start of the window of recent runs is rounded down to this, so that
/// the URL, and with it the conditional request, stays the same for a while.
const WINDOW_STEP: TimeDelta = TimeDelta::minutes(15);

pub struct GitHub {
    source: String,
    api: Url,
    client: ReadOnlyClient,
    /// The login of the token's user, once known.
    login: Mutex<Option<String>>,
    /// Whether a ref name is a tag, by project and name. Runs only name their
    /// ref (`head_branch`), not whether it is a branch or a tag.
    tags: Mutex<HashMap<String, HashMap<String, bool>>>,
    /// Creation time of the oldest run that was active in the last poll, by
    /// project, so that long-running runs stay in the window.
    oldest_active: Mutex<HashMap<String, DateTime<Utc>>>,
}

enum Found {
    Projects(Vec<Project>),
    Missing(String),
}

impl GitHub {
    pub fn new(
        source: &str,
        web_url: &Url,
        token: &Secret,
        cache: Arc<dyn ResponseCache>,
    ) -> Result<Self> {
        let api = api_url(web_url)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            HeaderValue::from_static("application/vnd.github+json"),
        );
        headers.insert(
            HeaderName::from_static("x-github-api-version"),
            HeaderValue::from_static("2022-11-28"),
        );
        Ok(Self {
            source: source.to_string(),
            api,
            client: ReadOnlyClient::new(token, headers, cache)?,
            login: Mutex::default(),
            tags: Mutex::default(),
            oldest_active: Mutex::default(),
        })
    }

    pub fn rate_limit(&self) -> Option<RateLimit> {
        self.client.rate_limit()
    }

    /// Builds an API URL from percent-encoded segments.
    fn endpoint(&self, segments: &[&str]) -> Url {
        let mut url = self.api.clone();
        url.path_segments_mut()
            .expect("API URL is a base URL")
            .pop_if_empty()
            .extend(segments);
        url
    }

    /// An endpoint below `repos/{owner}/{repo}/`.
    fn repo_endpoint(&self, project: &Project, segments: &[&str]) -> Url {
        let mut parts: Vec<&str> = vec!["repos"];
        parts.extend(project.id.splitn(2, '/'));
        parts.extend(segments);
        self.endpoint(&parts)
    }

    /// Lists candidate repositories for the given targets. Returns them plus
    /// warnings for targets that did not exist.
    pub async fn discover(
        &self,
        targets: &[Target],
        exclude_archived: bool,
    ) -> Result<(Vec<Project>, Vec<String>)> {
        let found = try_join_all(targets.iter().map(|t| self.discover_target(t))).await?;
        let mut seen = HashSet::new();
        let mut projects = Vec::new();
        let mut warnings = Vec::new();
        for found in found {
            match found {
                Found::Projects(list) => projects.extend(
                    list.into_iter()
                        .filter(|p| !(exclude_archived && p.archived))
                        .filter(|p| seen.insert(p.id.to_lowercase())),
                ),
                Found::Missing(warning) => warnings.push(warning),
            }
        }
        Ok((projects, warnings))
    }

    async fn discover_target(&self, target: &Target) -> Result<Found, HttpError> {
        match target {
            Target::All => self
                .list_repos(self.endpoint(&["user", "repos"]))
                .await
                .map(Found::Projects),
            Target::Project(path) => {
                let Some((owner, repo)) = path.split_once('/').filter(|(_, r)| !r.contains('/'))
                else {
                    return Ok(Found::Missing(format!(
                        "`{path}` is not a GitHub repository path (owner/repo)"
                    )));
                };
                match self
                    .client
                    .get_json::<GhRepo>(self.endpoint(&["repos", owner, repo]))
                    .await
                {
                    Ok(repo) => Ok(Found::Projects(vec![repo.into_project(&self.source)])),
                    Err(e) if e.is_not_found() => Ok(Found::Missing(format!(
                        "repository `{path}` not found or not accessible"
                    ))),
                    Err(e) => Err(e),
                }
            }
            Target::Namespace { path, .. } if path.contains('/') => Ok(Found::Missing(format!(
                "`{path}` is not a GitHub organization or user; patterns look like `owner/repo`"
            ))),
            Target::Namespace { path, .. } => {
                let mut url = self.endpoint(&["orgs", path, "repos"]);
                url.query_pairs_mut().append_pair("type", "all");
                match self.list_repos(url).await {
                    Ok(list) => Ok(Found::Projects(list)),
                    Err(e) if e.is_not_found() => self.user_repos(path).await,
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Repositories owned by a user. `users/{name}/repos` only lists public
    /// ones, so the token's own user is listed through `user/repos`.
    async fn user_repos(&self, user: &str) -> Result<Found, HttpError> {
        if self.login().await?.eq_ignore_ascii_case(user) {
            let mut url = self.endpoint(&["user", "repos"]);
            url.query_pairs_mut().append_pair("affiliation", "owner");
            return self.list_repos(url).await.map(Found::Projects);
        }
        let mut url = self.endpoint(&["users", user, "repos"]);
        url.query_pairs_mut().append_pair("type", "owner");
        match self.list_repos(url).await {
            Ok(list) => Ok(Found::Projects(list)),
            Err(e) if e.is_not_found() => Ok(Found::Missing(format!(
                "organization or user `{user}` not found or not accessible"
            ))),
            Err(e) => Err(e),
        }
    }

    async fn login(&self) -> Result<String, HttpError> {
        if let Some(login) = self.login.lock().unwrap().clone() {
            return Ok(login);
        }
        #[derive(Deserialize)]
        struct User {
            login: String,
        }
        let user: User = self.client.get_json(self.endpoint(&["user"])).await?;
        *self.login.lock().unwrap() = Some(user.login.clone());
        Ok(user.login)
    }

    async fn list_repos(&self, mut url: Url) -> Result<Vec<Project>, HttpError> {
        url.query_pairs_mut().append_pair("per_page", PER_PAGE);
        let repos: Vec<GhRepo> = self.client.get_all(url, MAX_PAGES).await?;
        Ok(repos
            .into_iter()
            .map(|r| r.into_project(&self.source))
            .collect())
    }

    /// The runs of the newest pushed commit on `branch` that has any, one
    /// per workflow.
    ///
    /// Only runs triggered by a push count. Many workflows run "on" the
    /// default branch without testing it: issue and comment bots, manually
    /// dispatched chores, dependency graph updates. A failing triage bot
    /// should not turn the branch red, and their runs would otherwise crowd
    /// the actual CI runs out of the list.
    pub async fn latest_runs(&self, project: &Project, branch: &str) -> Result<Vec<Pipeline>> {
        let mut url = self.repo_endpoint(project, &["actions", "runs"]);
        url.query_pairs_mut()
            .append_pair("branch", branch)
            .append_pair("event", "push")
            .append_pair("per_page", DEFAULT_BRANCH_RUNS);
        let runs: GhRuns = self.client.get_json_cached(url).await?;
        let runs = runs.workflow_runs;
        let Some(sha) = runs.first().map(|r| r.head_sha.clone()) else {
            return Ok(Vec::new());
        };
        // Newest first, so the first run of each workflow is its latest.
        let mut workflows = HashSet::new();
        Ok(runs
            .into_iter()
            .filter(|r| r.head_sha == sha && workflows.insert(r.workflow_id))
            .map(|r| r.into_pipeline(PipelineKind::Branch(branch.to_string())))
            .collect())
    }

    /// Runs on any ref that are active, or were updated since `since`.
    pub async fn active_runs(
        &self,
        project: &Project,
        since: DateTime<Utc>,
    ) -> Result<Vec<Pipeline>> {
        // Runs are only filtered by creation time. Start the window early
        // enough to include runs that were still active at the last poll.
        let mut start = since;
        if let Some(oldest) = self.oldest_active.lock().unwrap().get(&project.id) {
            start = start.min(*oldest);
        }
        let start = start.duration_trunc(WINDOW_STEP).unwrap_or(start);
        let mut url = self.repo_endpoint(project, &["actions", "runs"]);
        url.query_pairs_mut()
            .append_pair(
                "created",
                &format!(">={}", start.to_rfc3339_opts(SecondsFormat::Secs, true)),
            )
            .append_pair("per_page", PER_PAGE);
        let runs: GhRuns = self.client.get_json_cached(url).await?;

        let stale = Utc::now() - STALE_AFTER;
        let runs: Vec<GhRun> = runs
            .workflow_runs
            .into_iter()
            .filter(|r| {
                let active = r.status().is_active();
                (active && r.updated_at > stale) || (!active && r.updated_at >= since)
            })
            .collect();

        let mut pipelines = try_join_all(runs.into_iter().map(|run| async move {
            let kind = self.kind(project, &run).await;
            Ok::<_, anyhow::Error>(run.into_pipeline(kind))
        }))
        .await?;
        pipelines.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id)));

        let oldest = pipelines
            .iter()
            .filter(|p| p.status.is_active())
            .map(|p| p.created_at)
            .min();
        let mut oldest_active = self.oldest_active.lock().unwrap();
        match oldest {
            Some(oldest) => oldest_active.insert(project.id.clone(), oldest),
            None => oldest_active.remove(&project.id),
        };
        Ok(pipelines)
    }

    /// What a run was for. A run for a tag push names the tag as its
    /// `head_branch`, so names other than the default branch are looked up.
    async fn kind(&self, project: &Project, run: &GhRun) -> PipelineKind {
        let name = run.head_branch.clone().unwrap_or_default();
        if run.is_pull_request() {
            if let Some(pr) = run
                .pull_requests
                .iter()
                .find(|pr| pr.base.repo.id == run.repository.id)
            {
                return PipelineKind::PullRequest(pr.number);
            }
            // Pull requests from forks are not linked to their runs.
            return PipelineKind::Branch(name);
        }
        if run.event == "release" {
            return PipelineKind::Tag(name);
        }
        if name.is_empty() || project.default_branch.as_deref() == Some(name.as_str()) {
            return PipelineKind::Branch(name);
        }
        if self.is_tag(project, &name).await {
            PipelineKind::Tag(name)
        } else {
            PipelineKind::Branch(name)
        }
    }

    async fn is_tag(&self, project: &Project, name: &str) -> bool {
        let known = self
            .tags
            .lock()
            .unwrap()
            .get(&project.id)
            .and_then(|tags| tags.get(name).copied());
        if let Some(known) = known {
            return known;
        }
        #[derive(Deserialize)]
        struct GitRef {
            #[serde(rename = "ref")]
            name: String,
        }
        let url = self.repo_endpoint(project, &["git", "ref", "tags", name]);
        let is_tag = match self.client.get_json::<GitRef>(url).await {
            Ok(found) => found.name == format!("refs/tags/{name}"),
            Err(e) if e.is_not_found() => false,
            // Try again next time.
            Err(_) => return false,
        };
        self.tags
            .lock()
            .unwrap()
            .entry(project.id.clone())
            .or_default()
            .insert(name.to_string(), is_tag);
        is_tag
    }

    /// The jobs of the latest attempt of a run, in the order they were created.
    pub async fn jobs(&self, project: &Project, run_id: u64) -> Result<Vec<Job>> {
        #[derive(Deserialize)]
        struct GhJobs {
            jobs: Vec<GhJob>,
        }
        let mut url =
            self.repo_endpoint(project, &["actions", "runs", &run_id.to_string(), "jobs"]);
        url.query_pairs_mut().append_pair("per_page", PER_PAGE);
        // Paginated, but wrapped in an object instead of being an array.
        let mut jobs = Vec::new();
        let mut next = Some(url);
        for _ in 0..MAX_PAGES {
            let Some(url) = next.take() else { break };
            let (page, link): (GhJobs, _) = self.client.get_json_page(url).await?;
            jobs.extend(page.jobs.into_iter().map(GhJob::into_job));
            next = link;
        }
        jobs.sort_by_key(|j| j.id);
        Ok(jobs)
    }
}

/// The REST API root for a GitHub web URL.
fn api_url(web_url: &Url) -> Result<Url> {
    if matches!(web_url.host_str(), Some("github.com" | "www.github.com")) {
        return Ok(Url::parse("https://api.github.com/").expect("valid URL"));
    }
    // GitHub Enterprise Server.
    let mut api = web_url.clone();
    api.path_segments_mut()
        .map_err(|_| anyhow!("`{web_url}` cannot be used as a base URL"))?
        .pop_if_empty()
        .extend(["api", "v3", ""]);
    Ok(api)
}

fn map_status(status: &str, conclusion: Option<&str>) -> Status {
    match status {
        "queued" | "requested" | "pending" => Status::Queued,
        "in_progress" => Status::Running,
        // Waiting for a deployment to be approved.
        "waiting" | "action_required" => Status::Manual,
        "completed" => match conclusion {
            Some("success" | "neutral") => Status::Success,
            Some("failure" | "timed_out" | "startup_failure") => Status::Failed,
            Some("cancelled") => Status::Canceled,
            Some("skipped") => Status::Skipped,
            Some("action_required") => Status::Manual,
            _ => Status::Unknown,
        },
        _ => Status::Unknown,
    }
}

#[derive(Deserialize)]
struct GhRepo {
    full_name: String,
    html_url: String,
    default_branch: Option<String>,
    #[serde(default)]
    archived: bool,
    #[serde(default)]
    fork: bool,
}

impl GhRepo {
    fn into_project(self, source: &str) -> Project {
        Project {
            source: source.to_string(),
            id: self.full_name.clone(),
            path: self.full_name,
            web_url: self.html_url,
            default_branch: self.default_branch,
            archived: self.archived,
            fork: self.fork,
            // Not visible without admin access; repositories without
            // workflows simply have no runs.
            ci_enabled: true,
        }
    }
}

#[derive(Deserialize)]
struct GhRuns {
    workflow_runs: Vec<GhRun>,
}

#[derive(Deserialize)]
struct GhRun {
    id: u64,
    name: Option<String>,
    run_number: u64,
    workflow_id: u64,
    head_branch: Option<String>,
    head_sha: String,
    event: String,
    status: String,
    conclusion: Option<String>,
    #[serde(default)]
    pull_requests: Vec<GhPullRequest>,
    repository: GhRepoId,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    run_started_at: Option<DateTime<Utc>>,
    html_url: String,
}

#[derive(Deserialize)]
struct GhPullRequest {
    number: u64,
    base: GhPullRequestBase,
}

#[derive(Deserialize)]
struct GhPullRequestBase {
    repo: GhRepoId,
}

#[derive(Deserialize)]
struct GhRepoId {
    id: u64,
}

impl GhRun {
    fn status(&self) -> Status {
        map_status(&self.status, self.conclusion.as_deref())
    }

    fn is_pull_request(&self) -> bool {
        self.event.starts_with("pull_request")
    }

    fn into_pipeline(self, kind: PipelineKind) -> Pipeline {
        let status = self.status();
        let finished = self.status == "completed";
        Pipeline {
            id: self.id,
            number: Some(self.run_number),
            name: self.name.filter(|n| !n.is_empty()),
            kind,
            sha: self.head_sha,
            status,
            trigger: self.event,
            created_at: self.created_at,
            updated_at: self.updated_at,
            started_at: self.run_started_at,
            finished_at: finished.then_some(self.updated_at),
            web_url: self.html_url,
        }
    }
}

#[derive(Deserialize)]
struct GhJob {
    id: u64,
    name: String,
    status: String,
    conclusion: Option<String>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    html_url: Option<String>,
}

impl GhJob {
    fn into_job(self) -> Job {
        let status = map_status(&self.status, self.conclusion.as_deref());
        Job {
            id: self.id,
            name: self.name,
            stage: None,
            // Queued jobs report their creation time as `started_at`.
            started_at: self.started_at.filter(|_| status != Status::Queued),
            status,
            allow_failure: false,
            finished_at: self.completed_at,
            web_url: self.html_url.unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn github(url: &str) -> GitHub {
        GitHub::new(
            "test",
            &Url::parse(url).unwrap(),
            &Secret::new("t".into()),
            Arc::new(crate::http::NoCache),
        )
        .unwrap()
    }

    fn project(id: &str) -> Project {
        Project {
            source: "test".into(),
            id: id.into(),
            path: id.into(),
            web_url: String::new(),
            default_branch: Some("main".into()),
            archived: false,
            fork: false,
            ci_enabled: true,
        }
    }

    #[test]
    fn builds_endpoints() {
        let gh = github("https://github.com/");
        assert_eq!(
            gh.repo_endpoint(&project("o/r"), &["actions", "runs"])
                .as_str(),
            "https://api.github.com/repos/o/r/actions/runs"
        );
        assert_eq!(
            gh.repo_endpoint(&project("o/r"), &["git", "ref", "tags", "v1/beta"])
                .as_str(),
            "https://api.github.com/repos/o/r/git/ref/tags/v1%2Fbeta"
        );
        let gh = github("https://ghe.example.com/");
        assert_eq!(
            gh.endpoint(&["user", "repos"]).as_str(),
            "https://ghe.example.com/api/v3/user/repos"
        );
    }

    #[test]
    fn maps_statuses() {
        assert_eq!(map_status("in_progress", None), Status::Running);
        assert_eq!(map_status("queued", None), Status::Queued);
        assert_eq!(map_status("waiting", None), Status::Manual);
        assert_eq!(map_status("completed", Some("timed_out")), Status::Failed);
        assert_eq!(map_status("completed", Some("neutral")), Status::Success);
        assert_eq!(map_status("completed", Some("cancelled")), Status::Canceled);
        assert_eq!(map_status("completed", Some("stale")), Status::Unknown);
    }

    fn run(id: u64, workflow_id: u64, sha: &str, event: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id, "name": "CI", "run_number": id, "workflow_id": workflow_id,
            "head_branch": "main", "head_sha": sha, "event": event,
            "status": "completed", "conclusion": "success", "pull_requests": [],
            "repository": {"id": 1, "full_name": "o/r"},
            "created_at": "2026-11-25T10:00:00Z", "updated_at": "2026-11-25T10:05:00Z",
            "run_started_at": "2026-11-25T10:00:01Z",
            "html_url": format!("https://github.com/o/r/actions/runs/{id}"),
            "extra": {"ignored": true},
        })
    }

    #[test]
    fn parses_runs() {
        let runs: GhRuns = serde_json::from_value(serde_json::json!({
            "total_count": 2,
            "workflow_runs": [run(3, 10, "b", "push"), run(2, 10, "a", "pull_request")],
        }))
        .unwrap();
        let mut runs = runs.workflow_runs.into_iter();
        let first = runs.next().unwrap();
        assert!(!first.is_pull_request());
        let pipeline = first.into_pipeline(PipelineKind::Branch("main".into()));
        assert_eq!(pipeline.status, Status::Success);
        assert_eq!(pipeline.number, Some(3));
        assert!(pipeline.finished_at.is_some());
        assert!(runs.next().unwrap().is_pull_request());
    }

    #[test]
    fn parses_jobs() {
        let job: GhJob = serde_json::from_value(serde_json::json!({
            "id": 5, "run_id": 3, "name": "build (ubuntu)", "status": "queued",
            "conclusion": null, "started_at": "2026-11-25T10:00:00Z", "completed_at": null,
            "html_url": "https://github.com/o/r/actions/runs/3/job/5", "steps": [],
        }))
        .unwrap();
        let job = job.into_job();
        assert_eq!(job.status, Status::Queued);
        assert_eq!(job.started_at, None);
    }
}

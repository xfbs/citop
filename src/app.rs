//! Application state, refresh scheduling and key handling for the TUI.
//!
//! All state lives in [`App`] and is only touched from the UI loop. Network
//! requests run as tokio tasks that report back through an [`Update`]
//! channel, so the UI never blocks on the network.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use ratatui::crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::widgets::TableState;
use tokio::sync::{Semaphore, mpsc};

use crate::config::Config;
use crate::model::{CommitStatus, Job, Pipeline, Project, Status};
use crate::provider::{Discovery, Source};

/// Concurrent requests per source, to be gentle on rate limits.
const CONCURRENCY_PER_SOURCE: usize = 8;
/// How often the project list of a source is fetched again.
const REDISCOVER_EVERY: Duration = Duration::from_secs(10 * 60);
/// How soon to retry a failed project discovery.
const DISCOVERY_RETRY: Duration = Duration::from_secs(30);
/// How long a status message stays in the footer.
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(8);
const PAGE: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectKey {
    /// Index into [`App::sources`].
    pub source: usize,
    pub id: String,
}

/// The result of a background request.
pub enum Update {
    Discovered {
        source: usize,
        result: anyhow::Result<Discovery>,
    },
    Project {
        key: ProjectKey,
        default: anyhow::Result<Option<CommitStatus>>,
        active: anyhow::Result<Vec<Pipeline>>,
    },
    Jobs {
        key: ProjectKey,
        pipeline: u64,
        result: anyhow::Result<Vec<Job>>,
    },
}

pub struct SourceState {
    pub source: Arc<Source>,
    semaphore: Arc<Semaphore>,
    pub discovered: bool,
    discovering: bool,
    next_discovery: Instant,
    pub error: Option<String>,
}

pub struct ProjectState {
    pub key: ProjectKey,
    pub project: Project,
    /// Status of the default branch.
    pub default: Option<CommitStatus>,
    /// Running or recently finished pipelines on any ref.
    pub active: Vec<Pipeline>,
    /// Whether data has been fetched at least once.
    pub loaded: bool,
    /// Error of the most recent refresh. Earlier data is kept.
    pub error: Option<String>,
    next_refresh: Instant,
    in_flight: bool,
}

impl ProjectState {
    fn new(key: ProjectKey, project: Project) -> Self {
        Self {
            key,
            project,
            default: None,
            active: Vec::new(),
            loaded: false,
            error: None,
            next_refresh: Instant::now(),
            in_flight: false,
        }
    }

    pub fn default_status(&self) -> Option<Status> {
        self.default.as_ref().map(|c| c.status)
    }

    fn is_default_pipeline(&self, id: u64) -> bool {
        self.default
            .as_ref()
            .is_some_and(|c| c.pipelines.iter().any(|p| p.id == id))
    }

    /// Number of running or queued pipelines on refs other than the default branch.
    pub fn active_elsewhere(&self) -> usize {
        self.active
            .iter()
            .filter(|p| p.status.is_active() && !self.is_default_pipeline(p.id))
            .count()
    }

    /// Whether anything is running or queued, on any ref.
    pub fn is_busy(&self) -> bool {
        self.default_status().is_some_and(Status::is_active) || self.active_elsewhere() > 0
    }

    /// Pipelines for the detail view: the default branch first, then the others.
    pub fn pipelines(&self) -> Vec<(&Pipeline, bool)> {
        let mut pipelines: Vec<(&Pipeline, bool)> = self
            .default
            .iter()
            .flat_map(|c| &c.pipelines)
            .map(|p| (p, true))
            .collect();
        pipelines.extend(
            self.active
                .iter()
                .filter(|p| !self.is_default_pipeline(p.id))
                .map(|p| (p, false)),
        );
        pipelines
    }

    /// Sort order, like htop sorting by CPU: what needs attention comes first.
    fn rank(&self) -> u8 {
        if !self.loaded {
            return if self.error.is_some() { 1 } else { 6 };
        }
        match self.default_status() {
            Some(Status::Failed) => 0,
            _ if self.error.is_some() => 1,
            _ if self.is_busy() => 2,
            Some(Status::Manual) => 3,
            Some(Status::Canceled | Status::Unknown) => 4,
            Some(Status::Success | Status::Skipped | Status::Queued | Status::Running) => 5,
            None => 7,
        }
    }
}

#[derive(Default)]
pub struct JobsState {
    pub jobs: Vec<Job>,
    pub loaded: bool,
    pub error: Option<String>,
    fetched: Option<Instant>,
    in_flight: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Projects,
    Pipelines,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Normal,
    Filter,
}

pub struct App {
    refresh: Duration,
    idle_refresh: Duration,
    keep_finished: chrono::TimeDelta,
    pub sources: Vec<SourceState>,
    pub projects: HashMap<ProjectKey, ProjectState>,
    pub jobs: HashMap<(ProjectKey, u64), JobsState>,
    pub selected: Option<ProjectKey>,
    pub pipeline_index: usize,
    pub detail: bool,
    pub focus: Focus,
    pub mode: Mode,
    pub filter: String,
    message: Option<(String, Instant)>,
    pub quit: bool,
    pub table_state: TableState,
    pub pipeline_state: TableState,
    tx: mpsc::UnboundedSender<Update>,
}

impl App {
    pub fn new(config: &Config, sources: Vec<Source>, tx: mpsc::UnboundedSender<Update>) -> Self {
        let now = Instant::now();
        Self {
            refresh: config.refresh,
            idle_refresh: config.idle_refresh,
            keep_finished: chrono::TimeDelta::from_std(config.keep_finished)
                .unwrap_or(chrono::TimeDelta::hours(1)),
            sources: sources
                .into_iter()
                .map(|source| SourceState {
                    source: Arc::new(source),
                    semaphore: Arc::new(Semaphore::new(CONCURRENCY_PER_SOURCE)),
                    discovered: false,
                    discovering: false,
                    next_discovery: now,
                    error: None,
                })
                .collect(),
            projects: HashMap::new(),
            jobs: HashMap::new(),
            selected: None,
            pipeline_index: 0,
            detail: false,
            focus: Focus::Projects,
            mode: Mode::Normal,
            filter: String::new(),
            message: None,
            quit: false,
            table_state: TableState::default(),
            pipeline_state: TableState::default(),
            tx,
        }
    }

    pub fn set_message(&mut self, message: impl Into<String>) {
        self.message = Some((message.into(), Instant::now()));
    }

    pub fn message(&self) -> Option<&str> {
        self.message
            .as_ref()
            .filter(|(_, at)| at.elapsed() < MESSAGE_TIMEOUT)
            .map(|(m, _)| m.as_str())
    }

    /// Projects that pass the filter, in display order.
    pub fn visible(&self) -> Vec<&ProjectState> {
        let filter = self.filter.to_lowercase();
        let mut projects: Vec<&ProjectState> = self
            .projects
            .values()
            .filter(|p| filter.is_empty() || p.project.path.to_lowercase().contains(&filter))
            .collect();
        projects.sort_by(|a, b| {
            a.rank()
                .cmp(&b.rank())
                .then_with(|| {
                    a.project
                        .path
                        .to_lowercase()
                        .cmp(&b.project.path.to_lowercase())
                })
                .then_with(|| a.key.source.cmp(&b.key.source))
        });
        projects
    }

    pub fn selected_project(&self) -> Option<&ProjectState> {
        self.projects.get(self.selected.as_ref()?)
    }

    pub fn selected_pipeline(&self) -> Option<(&Pipeline, bool)> {
        self.selected_project()?
            .pipelines()
            .get(self.pipeline_index)
            .copied()
    }

    /// Starts any requests that are due. Called on every loop iteration.
    pub fn tick(&mut self) {
        let now = Instant::now();
        self.fix_selection();

        for (index, state) in self.sources.iter_mut().enumerate() {
            if state.discovering || now < state.next_discovery {
                continue;
            }
            state.discovering = true;
            let source = state.source.clone();
            let tx = self.tx.clone();
            tokio::spawn(async move {
                let result = source.discover().await;
                let _ = tx.send(Update::Discovered {
                    source: index,
                    result,
                });
            });
        }

        let since = Utc::now() - self.keep_finished;
        for project in self.projects.values_mut() {
            if project.in_flight || now < project.next_refresh {
                continue;
            }
            project.in_flight = true;
            let state = &self.sources[project.key.source];
            let source = state.source.clone();
            let semaphore = state.semaphore.clone();
            let tx = self.tx.clone();
            let key = project.key.clone();
            let project = project.project.clone();
            tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await;
                let (default, active) = futures::join!(
                    source.default_branch(&project),
                    source.active_pipelines(&project, since)
                );
                let _ = tx.send(Update::Project {
                    key,
                    default,
                    active,
                });
            });
        }

        self.refresh_jobs(now);
    }

    /// Fetches jobs of the pipeline shown in the detail view, and keeps them
    /// fresh while it runs. Not rate limited by the semaphore: the user is
    /// looking at it, so it should not queue behind background refreshes.
    fn refresh_jobs(&mut self, now: Instant) {
        if !self.detail {
            return;
        }
        let Some(project) = self.selected_project() else {
            return;
        };
        let Some((pipeline, _)) = self.selected_pipeline() else {
            return;
        };
        let key = (project.key.clone(), pipeline.id);
        let active = pipeline.status.is_active();
        let project = project.project.clone();
        let pipeline = pipeline.clone();

        // Only keep jobs for what is on screen.
        self.jobs.retain(|k, _| *k == key);
        let state = self.jobs.entry(key.clone()).or_default();
        let due = match state.fetched {
            None => true,
            Some(at) => active && now.duration_since(at) >= self.refresh,
        };
        if state.in_flight || !due {
            return;
        }
        state.in_flight = true;
        let source = self.sources[key.0.source].source.clone();
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let result = source.jobs(&project, &pipeline).await;
            let _ = tx.send(Update::Jobs {
                key: key.0,
                pipeline: key.1,
                result,
            });
        });
    }

    pub fn handle_update(&mut self, update: Update) {
        let now = Instant::now();
        match update {
            Update::Discovered { source, result } => {
                let state = &mut self.sources[source];
                state.discovering = false;
                match result {
                    Ok(discovery) => {
                        state.discovered = true;
                        state.error = None;
                        state.next_discovery = now + REDISCOVER_EVERY;
                        if let Some(warning) = discovery.warnings.first() {
                            let more = discovery.warnings.len() - 1;
                            let warning = if more > 0 {
                                format!("{warning} (and {more} more)")
                            } else {
                                warning.clone()
                            };
                            self.set_message(warning);
                        }
                        self.merge_projects(source, discovery.projects);
                    }
                    Err(e) => {
                        state.error = Some(format!("{e:#}"));
                        state.next_discovery = now + DISCOVERY_RETRY;
                    }
                }
            }
            Update::Project {
                key,
                default,
                active,
            } => {
                let (refresh, idle_refresh) = (self.refresh, self.idle_refresh);
                let Some(project) = self.projects.get_mut(&key) else {
                    return;
                };
                project.in_flight = false;
                let mut errors = Vec::new();
                match default {
                    Ok(default) => project.default = default,
                    Err(e) => errors.push(format!("{e:#}")),
                }
                match active {
                    Ok(active) => project.active = active,
                    Err(e) => errors.push(format!("{e:#}")),
                }
                project.error = (!errors.is_empty()).then(|| errors.join("; "));
                project.loaded |= project.error.is_none();
                let interval = if project.is_busy() {
                    refresh
                } else {
                    idle_refresh
                };
                project.next_refresh = now + interval;
            }
            Update::Jobs {
                key,
                pipeline,
                result,
            } => {
                let Some(state) = self.jobs.get_mut(&(key, pipeline)) else {
                    return;
                };
                state.in_flight = false;
                state.fetched = Some(now);
                match result {
                    Ok(jobs) => {
                        state.jobs = jobs;
                        state.loaded = true;
                        state.error = None;
                    }
                    Err(e) => state.error = Some(format!("{e:#}")),
                }
            }
        }
    }

    /// Replaces the projects of a source with a freshly discovered list,
    /// keeping the state of projects that are still there.
    fn merge_projects(&mut self, source: usize, projects: Vec<Project>) {
        let keys: HashSet<ProjectKey> = projects
            .iter()
            .map(|p| ProjectKey {
                source,
                id: p.id.clone(),
            })
            .collect();
        self.projects
            .retain(|key, _| key.source != source || keys.contains(key));
        for project in projects {
            let key = ProjectKey {
                source,
                id: project.id.clone(),
            };
            match self.projects.get_mut(&key) {
                Some(state) => state.project = project,
                None => {
                    self.projects
                        .insert(key.clone(), ProjectState::new(key, project));
                }
            }
        }
    }

    /// Keeps the selection on a visible project, and the pipeline index in range.
    fn fix_selection(&mut self) {
        let visible = self.visible();
        let selected_visible = self
            .selected
            .as_ref()
            .is_some_and(|s| visible.iter().any(|p| &p.key == s));
        if !selected_visible {
            let first = visible.first().map(|p| p.key.clone());
            self.select(first);
        }
        let count = self.selected_project().map_or(0, |p| p.pipelines().len());
        self.pipeline_index = self.pipeline_index.min(count.saturating_sub(1));
        if self.selected.is_none() {
            self.detail = false;
            self.focus = Focus::Projects;
        }
    }

    fn select(&mut self, key: Option<ProjectKey>) {
        if key != self.selected {
            self.selected = key;
            self.pipeline_index = 0;
        }
    }

    fn move_project(&mut self, delta: isize) {
        let visible = self.visible();
        if visible.is_empty() {
            return;
        }
        let current = self
            .selected
            .as_ref()
            .and_then(|s| visible.iter().position(|p| &p.key == s))
            .unwrap_or(0);
        let index = current.saturating_add_signed(delta).min(visible.len() - 1);
        let key = visible[index].key.clone();
        self.select(Some(key));
    }

    fn move_pipeline(&mut self, delta: isize) {
        let count = self.selected_project().map_or(0, |p| p.pipelines().len());
        if count > 0 {
            self.pipeline_index = self
                .pipeline_index
                .saturating_add_signed(delta)
                .min(count - 1);
        }
    }

    fn move_selection(&mut self, delta: isize) {
        match self.focus {
            Focus::Projects => self.move_project(delta),
            Focus::Pipelines => self.move_pipeline(delta),
        }
    }

    pub fn handle_event(&mut self, event: Event) {
        if let Event::Key(key) = event
            && key.kind != KeyEventKind::Release
        {
            match self.mode {
                Mode::Normal => self.handle_key(key),
                Mode::Filter => self.handle_filter_key(key),
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Char('c') if ctrl => self.quit = true,
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::PageUp => self.move_selection(-(PAGE as isize)),
            KeyCode::PageDown => self.move_selection(PAGE as isize),
            KeyCode::Home | KeyCode::Char('g') => self.move_selection(isize::MIN),
            KeyCode::End | KeyCode::Char('G') => self.move_selection(isize::MAX),
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                if self.selected.is_some() {
                    self.detail = true;
                    self.focus = Focus::Pipelines;
                }
            }
            KeyCode::Tab if self.detail => {
                self.focus = match self.focus {
                    Focus::Projects => Focus::Pipelines,
                    Focus::Pipelines => Focus::Projects,
                };
            }
            KeyCode::Left | KeyCode::Char('h') if self.focus == Focus::Pipelines => {
                self.focus = Focus::Projects;
            }
            KeyCode::Esc => {
                if self.focus == Focus::Pipelines {
                    self.focus = Focus::Projects;
                } else if self.detail {
                    self.detail = false;
                } else {
                    self.filter.clear();
                }
            }
            KeyCode::Char('/') => self.mode = Mode::Filter,
            KeyCode::Char('o') => self.open_selected(),
            KeyCode::Char('r') => self.refresh_selected(),
            _ => {}
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Enter => self.mode = Mode::Normal,
            KeyCode::Esc => {
                self.filter.clear();
                self.mode = Mode::Normal;
            }
            KeyCode::Char('c') if ctrl => self.quit = true,
            KeyCode::Char('u') if ctrl => self.filter.clear(),
            KeyCode::Backspace => {
                self.filter.pop();
            }
            KeyCode::Up => self.move_project(-1),
            KeyCode::Down => self.move_project(1),
            KeyCode::Char(c) if !ctrl => self.filter.push(c),
            _ => {}
        }
    }

    /// Opens the selected pipeline (in the detail view) or project in the browser.
    fn open_selected(&mut self) {
        let url = match (
            self.focus,
            self.selected_pipeline(),
            self.selected_project(),
        ) {
            (Focus::Pipelines, Some((pipeline, _)), _) => pipeline.web_url.clone(),
            (_, _, Some(project)) => project.project.web_url.clone(),
            _ => return,
        };
        match open_url(&url) {
            Ok(()) => self.set_message(format!("opened {url}")),
            Err(e) => self.set_message(format!("could not open {url}: {e}")),
        }
    }

    fn refresh_selected(&mut self) {
        let now = Instant::now();
        let Some(key) = self.selected.clone() else {
            return;
        };
        if let Some(project) = self.projects.get_mut(&key) {
            project.next_refresh = now;
        }
        for ((project, _), state) in &mut self.jobs {
            if *project == key {
                state.fetched = None;
            }
        }
    }

    /// Counts for the header: (projects, busy, failing).
    pub fn counts(&self) -> (usize, usize, usize) {
        let busy = self.projects.values().filter(|p| p.is_busy()).count();
        let failing = self
            .projects
            .values()
            .filter(|p| p.default_status() == Some(Status::Failed))
            .count();
        (self.projects.len(), busy, failing)
    }
}

fn open_url(url: &str) -> std::io::Result<()> {
    use std::process::{Command, Stdio};
    let mut command = if cfg!(target_os = "macos") {
        Command::new("open")
    } else if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.args(["/C", "start", ""]);
        c
    } else {
        Command::new("xdg-open")
    };
    let mut child = command
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    // Reap the process so it does not linger as a zombie.
    std::thread::spawn(move || child.wait());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::PipelineKind;

    fn project(path: &str) -> Project {
        Project {
            source: "s".into(),
            id: path.into(),
            path: path.into(),
            web_url: String::new(),
            default_branch: Some("main".into()),
            archived: false,
            fork: false,
            ci_enabled: true,
        }
    }

    fn pipeline(id: u64, status: Status, kind: PipelineKind) -> Pipeline {
        Pipeline {
            id,
            number: None,
            name: None,
            kind,
            sha: "abc".into(),
            status,
            trigger: "push".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            started_at: None,
            finished_at: None,
            web_url: String::new(),
        }
    }

    fn state(path: &str, default: Option<Status>, active: Vec<Pipeline>) -> ProjectState {
        let key = ProjectKey {
            source: 0,
            id: path.into(),
        };
        let mut state = ProjectState::new(key, project(path));
        state.loaded = true;
        state.default = default.and_then(|s| {
            CommitStatus::new(vec![pipeline(1, s, PipelineKind::Branch("main".into()))])
        });
        state.active = active;
        state
    }

    #[test]
    fn ranks_failures_then_activity() {
        let failed = state("a", Some(Status::Failed), vec![]);
        let running_mr = state(
            "b",
            Some(Status::Success),
            vec![pipeline(2, Status::Running, PipelineKind::MergeRequest(1))],
        );
        let ok = state("c", Some(Status::Success), vec![]);
        let none = state("d", None, vec![]);
        assert!(failed.rank() < running_mr.rank());
        assert!(running_mr.rank() < ok.rank());
        assert!(ok.rank() < none.rank());
        assert_eq!(running_mr.active_elsewhere(), 1);
        assert!(running_mr.is_busy());
        assert!(!ok.is_busy());
    }

    #[test]
    fn default_pipeline_is_not_duplicated_in_detail() {
        let mut s = state(
            "a",
            Some(Status::Running),
            vec![
                pipeline(1, Status::Running, PipelineKind::Branch("main".into())),
                pipeline(2, Status::Success, PipelineKind::Tag("v1".into())),
            ],
        );
        s.default.as_mut().unwrap().pipelines[0].status = Status::Running;
        let pipelines = s.pipelines();
        assert_eq!(pipelines.len(), 2);
        assert!(pipelines[0].1);
        assert_eq!(pipelines[1].0.id, 2);
        assert_eq!(s.active_elsewhere(), 0);
    }
}

//! Application state and key handling for the TUI.
//!
//! [`App`] is a view of the [`Store`](crate::store::Store): it applies the
//! [`Changes`] that the watcher sends and rebuilds its project list from them.
//! Fetching happens in the [`poller`](crate::poller), which [`App`] only
//! talks to through [`Command`]s, so the UI never blocks on the network.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ratatui::crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::widgets::TableState;
use tokio::sync::mpsc;

use crate::http::RateLimit;
use crate::model::{CommitStatus, Job, Pipeline, Project, Status};
use crate::poller::Command;
use crate::provider::Source;
use crate::store::{Changes, DiscoveryRow, JobsRow, ProjectRow};

/// How long a status message stays in the footer.
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(8);
/// How often the poller is reminded which jobs are on screen.
const WANT_JOBS_EVERY: Duration = Duration::from_secs(1);
const PAGE: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectKey {
    /// Index into [`App::sources`].
    pub source: usize,
    pub id: String,
}

pub enum Update {
    /// Data that changed in the store.
    Changes(Changes),
    /// Something to tell the user.
    Message(String),
}

pub struct SourceState {
    pub name: String,
    pub scope: String,
    pub instance: String,
    pub discovery: Option<DiscoveryRow>,
    pub rate_limit: Option<RateLimit>,
}

impl SourceState {
    pub fn discovered(&self) -> bool {
        self.discovery
            .as_ref()
            .is_some_and(|d| d.projects.is_some())
    }

    pub fn error(&self) -> Option<&str> {
        self.discovery.as_ref()?.error.as_deref()
    }
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
}

impl ProjectState {
    fn new(key: ProjectKey, project: Project, row: Option<&ProjectRow>) -> Self {
        Self {
            key,
            project,
            default: row.and_then(|r| r.default.clone()),
            active: row.map(|r| r.active.clone()).unwrap_or_default(),
            loaded: row.is_some_and(|r| r.loaded),
            error: row.and_then(|r| r.error.clone()),
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

pub struct JobsState {
    pub jobs: Vec<Job>,
    pub loaded: bool,
    pub error: Option<String>,
}

impl From<JobsRow> for JobsState {
    fn from(row: JobsRow) -> Self {
        Self {
            jobs: row.jobs,
            loaded: row.loaded,
            error: row.error,
        }
    }
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
    pub sources: Vec<SourceState>,
    /// Project state from the store, by instance and project id.
    rows: HashMap<(String, String), ProjectRow>,
    /// The projects of all sources, built from discoveries and `rows`.
    pub projects: HashMap<ProjectKey, ProjectState>,
    /// Jobs from the store, by instance, project id and pipeline id.
    jobs: HashMap<(String, String, u64), JobsState>,
    pub selected: Option<ProjectKey>,
    /// Whether the user moved the selection. Until then it stays on the top
    /// row, instead of following a project down the list as states load.
    moved: bool,
    pub pipeline_index: usize,
    pub detail: bool,
    pub focus: Focus,
    pub mode: Mode,
    pub filter: String,
    message: Option<(String, Instant)>,
    pub quit: bool,
    pub table_state: TableState,
    pub pipeline_state: TableState,
    commands: mpsc::UnboundedSender<Command>,
    /// The pipeline whose jobs were last requested, and when.
    wanted_jobs: Option<((ProjectKey, u64), Instant)>,
}

impl App {
    pub fn new(sources: &[Arc<Source>], commands: mpsc::UnboundedSender<Command>) -> Self {
        Self {
            sources: sources
                .iter()
                .map(|source| SourceState {
                    name: source.name.clone(),
                    scope: source.scope.clone(),
                    instance: source.instance.clone(),
                    discovery: None,
                    rate_limit: None,
                })
                .collect(),
            rows: HashMap::new(),
            projects: HashMap::new(),
            jobs: HashMap::new(),
            selected: None,
            moved: false,
            pipeline_index: 0,
            detail: false,
            focus: Focus::Projects,
            mode: Mode::Normal,
            filter: String::new(),
            message: None,
            quit: false,
            table_state: TableState::default(),
            pipeline_state: TableState::default(),
            commands,
            wanted_jobs: None,
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

    /// The jobs of a pipeline of a project, if the store has them.
    pub fn jobs_for(&self, project: &ProjectState, pipeline_id: u64) -> Option<&JobsState> {
        let instance = self.sources[project.key.source].instance.clone();
        self.jobs
            .get(&(instance, project.key.id.clone(), pipeline_id))
    }

    /// Called on every loop iteration.
    pub fn tick(&mut self) {
        self.fix_selection();
        self.want_jobs();
    }

    /// Tells the poller which jobs are on screen, so it keeps them fresh.
    fn want_jobs(&mut self) {
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
        let recently = self
            .wanted_jobs
            .as_ref()
            .is_some_and(|(k, at)| *k == key && at.elapsed() < WANT_JOBS_EVERY);
        if recently {
            return;
        }
        let command = Command::WantJobs {
            source: key.0.source,
            project: Box::new(project.project.clone()),
            pipeline: Box::new(pipeline.clone()),
        };
        let _ = self.commands.send(command);
        self.wanted_jobs = Some((key, Instant::now()));
    }

    pub fn handle_update(&mut self, update: Update) {
        match update {
            Update::Message(message) => self.set_message(message),
            Update::Changes(changes) => self.apply(changes),
        }
    }

    fn apply(&mut self, changes: Changes) {
        for (scope, discovery) in changes.discoveries {
            if let Some(warning) = discovery.warnings.first() {
                let more = discovery.warnings.len() - 1;
                let warning = if more > 0 {
                    format!("{warning} (and {more} more)")
                } else {
                    warning.clone()
                };
                self.set_message(warning);
            }
            for state in self.sources.iter_mut().filter(|s| s.scope == scope) {
                state.discovery = Some(discovery.clone());
            }
        }
        for (scope, rate) in changes.rate_limits {
            for state in self.sources.iter_mut().filter(|s| s.scope == scope) {
                state.rate_limit = Some(rate);
            }
        }
        for row in changes.projects {
            self.rows
                .insert((row.instance.clone(), row.project_id.clone()), row);
        }
        for row in changes.jobs {
            let key = (
                row.instance.clone(),
                row.project_id.clone(),
                row.pipeline_id,
            );
            self.jobs.insert(key, row.into());
        }
        self.rebuild();
    }

    /// Rebuilds the project list from the discoveries and project rows.
    fn rebuild(&mut self) {
        let mut projects = HashMap::new();
        for (index, state) in self.sources.iter().enumerate() {
            let discovered = state.discovery.as_ref().and_then(|d| d.projects.as_ref());
            for project in discovered.into_iter().flatten() {
                let key = ProjectKey {
                    source: index,
                    id: project.id.clone(),
                };
                let row = self.rows.get(&(state.instance.clone(), project.id.clone()));
                projects.insert(key.clone(), ProjectState::new(key, project.clone(), row));
            }
        }
        self.projects = projects;
    }

    /// Keeps the selection on a visible project, and the pipeline index in range.
    fn fix_selection(&mut self) {
        let visible = self.visible();
        let selected_visible = self
            .selected
            .as_ref()
            .is_some_and(|s| visible.iter().any(|p| &p.key == s));
        if !selected_visible || !self.moved {
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
        self.moved = true;
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
                    self.moved = true;
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
        let Some(key) = self.selected.clone() else {
            return;
        };
        let _ = self.commands.send(Command::Refresh {
            source: key.source,
            project_id: key.id,
        });
        self.set_message("refresh requested");
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
    use chrono::Utc;

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
        let mut state = ProjectState::new(key, project(path), None);
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

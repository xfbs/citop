//! Rendering of the TUI.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Paragraph, Row, Table, Wrap};

use crate::app::{App, Focus, Mode, ProjectState};
use crate::format::{ago, elapsed, short_sha};
use crate::model::{Job, Pipeline, Status};

pub fn draw(frame: &mut Frame, app: &mut App) {
    let now = Utc::now();
    let detail = app.detail && app.selected_project().is_some();
    let [header, main, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Fill(1),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_header(frame, header, app);
    if detail {
        let [list, detail] =
            Layout::vertical([Constraint::Percentage(45), Constraint::Percentage(55)]).areas(main);
        draw_projects(frame, list, app, now);
        draw_detail(frame, detail, app, now);
    } else {
        draw_projects(frame, main, app, now);
    }
    draw_footer(frame, footer, app);
}

fn status_style(status: Status) -> Style {
    match status {
        Status::Failed => Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
        Status::Running => Style::new().fg(Color::Cyan),
        Status::Queued => Style::new().fg(Color::Blue),
        Status::Manual => Style::new().fg(Color::Magenta),
        Status::Success => Style::new().fg(Color::Green),
        Status::Canceled | Status::Skipped | Status::Unknown => Style::new().fg(Color::DarkGray),
    }
}

fn status_symbol(status: Status) -> &'static str {
    match status {
        Status::Failed => "✗",
        Status::Running => "●",
        Status::Queued => "◌",
        Status::Manual => "◆",
        Status::Success => "✓",
        Status::Canceled => "⊘",
        Status::Skipped => "»",
        Status::Unknown => "?",
    }
}

fn status_cell(status: Status) -> Cell<'static> {
    Cell::from(format!("{} {status}", status_symbol(status))).style(status_style(status))
}

fn draw_header(frame: &mut Frame, area: Rect, app: &App) {
    let (total, busy, failing) = app.counts();
    let mut spans = vec![
        Span::from(" citop ").bold().reversed(),
        Span::from(format!("  {total} projects")),
    ];
    if busy > 0 {
        spans.push(Span::from(" · "));
        spans.push(Span::styled(
            format!("{busy} running"),
            status_style(Status::Running),
        ));
    }
    if failing > 0 {
        spans.push(Span::from(" · "));
        spans.push(Span::styled(
            format!("{failing} failing"),
            status_style(Status::Failed),
        ));
    }

    let mut right = Vec::new();
    for state in &app.sources {
        right.push(Span::from("  "));
        right.push(Span::from(state.name.clone()).bold());
        let text = if state.error().is_some() {
            Span::styled(" error", status_style(Status::Failed))
        } else if !state.discovered() {
            Span::from(" loading…").dim()
        } else if let Some(rate) = state.rate_limit {
            let style = if rate.remaining * 10 < rate.limit {
                Style::new().fg(Color::Red)
            } else {
                Style::new().dim()
            };
            Span::styled(format!(" {}/{}", rate.remaining, rate.limit), style)
        } else {
            Span::from(" ok").dim()
        };
        right.push(text);
    }
    right.push(Span::from(" "));
    let right = Line::from(right);
    let width = right.width() as u16;
    let [left_area, right_area] =
        Layout::horizontal([Constraint::Fill(1), Constraint::Length(width)]).areas(area);
    frame.render_widget(Line::from(spans), left_area);
    frame.render_widget(right, right_area);
}

fn draw_projects(frame: &mut Frame, area: Rect, app: &mut App, now: DateTime<Utc>) {
    let visible = app.visible();
    let selected = app
        .selected
        .as_ref()
        .and_then(|s| visible.iter().position(|p| &p.key == s));
    let rows: Vec<Row> = visible.iter().map(|p| project_row(p, now)).collect();
    let empty = rows.is_empty();

    let focused = app.focus == Focus::Projects;
    let title = if app.filter.is_empty() {
        " Projects ".to_string()
    } else {
        format!(" Projects matching \"{}\" ", app.filter)
    };
    let table = Table::new(
        rows,
        [
            Constraint::Length(11),
            Constraint::Fill(3),
            Constraint::Fill(1),
            Constraint::Length(10),
            Constraint::Length(5),
            Constraint::Length(7),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new([
            "STATUS", "PROJECT", "BRANCH", "COMMIT", "+ACT", "AGE", "DURATION",
        ])
        .style(Style::new().bold()),
    )
    .block(panel(title, focused))
    .row_highlight_style(highlight(focused));

    app.table_state.select(selected);
    frame.render_stateful_widget(table, area, &mut app.table_state);

    if empty {
        let text = if app
            .sources
            .iter()
            .all(|s| s.discovered() || s.error().is_some())
        {
            "No projects."
        } else {
            "Discovering projects…"
        };
        let inner = area.inner(ratatui::layout::Margin::new(2, 2));
        frame.render_widget(Paragraph::new(text).dim(), inner);
    }
}

fn project_row(p: &ProjectState, now: DateTime<Utc>) -> Row<'static> {
    let status = if !p.loaded && p.error.is_some() {
        Cell::from("! error").style(status_style(Status::Failed))
    } else if !p.loaded {
        Cell::from("… loading").dim()
    } else {
        match p.default_status() {
            Some(status) => status_cell(status),
            None => Cell::from("· none").dim(),
        }
    };
    let pipeline = p.default.as_ref().and_then(|c| c.pipelines.first());
    let elsewhere = p.active_elsewhere();
    let mut path = Line::from(p.project.path.clone());
    if p.loaded && p.error.is_some() {
        path.push_span(Span::styled(" !", status_style(Status::Failed)));
    }
    Row::new([
        status,
        Cell::from(path),
        Cell::from(p.project.default_branch.clone().unwrap_or_default()).dim(),
        Cell::from(
            p.default
                .as_ref()
                .map_or("", |c| short_sha(&c.sha))
                .to_string(),
        )
        .dim(),
        if elsewhere > 0 {
            Cell::from(format!("+{elsewhere}")).style(status_style(Status::Running))
        } else {
            Cell::from("")
        },
        Cell::from(pipeline.map(|p| ago(p.created_at, now)).unwrap_or_default()),
        Cell::from(pipeline_elapsed(pipeline, now)),
    ])
}

fn pipeline_elapsed(pipeline: Option<&Pipeline>, now: DateTime<Utc>) -> String {
    pipeline
        .and_then(|p| elapsed(p.started_at, p.finished_at, now))
        .unwrap_or_default()
}

fn draw_detail(frame: &mut Frame, area: Rect, app: &mut App, now: DateTime<Utc>) {
    let Some(project) = app.selected_project() else {
        return;
    };
    let focused = app.focus == Focus::Pipelines;
    let title = format!(" {} ", project.project.path);
    let pipelines: Vec<Row> = project
        .pipelines()
        .into_iter()
        .map(|(p, default)| pipeline_row(p, default, now))
        .collect();
    let error = project.error.clone();
    let no_pipelines = pipelines.is_empty();

    let [left, right] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(area);

    let block = panel(title, focused);
    let inner = block.inner(left);
    frame.render_widget(block, left);
    let inner = match error {
        Some(error) => {
            let [error_area, rest] =
                Layout::vertical([Constraint::Length(2), Constraint::Fill(1)]).areas(inner);
            frame.render_widget(
                Paragraph::new(format!("! {error}"))
                    .style(status_style(Status::Failed))
                    .wrap(Wrap { trim: true }),
                error_area,
            );
            rest
        }
        None => inner,
    };
    if no_pipelines {
        frame.render_widget(Paragraph::new("No recent pipelines.").dim(), inner);
    } else {
        let table = Table::new(
            pipelines,
            [
                Constraint::Length(11),
                Constraint::Fill(3),
                Constraint::Fill(2),
                Constraint::Length(7),
                Constraint::Length(8),
            ],
        )
        .header(Row::new(["STATUS", "REF", "PIPELINE", "AGE", "DURATION"]).bold())
        .row_highlight_style(highlight(focused));
        app.pipeline_state.select(Some(app.pipeline_index));
        frame.render_stateful_widget(table, inner, &mut app.pipeline_state);
    }

    draw_jobs(frame, right, app, now);
}

fn pipeline_row(p: &Pipeline, default: bool, now: DateTime<Utc>) -> Row<'static> {
    // Marks the default branch; in front so that truncation cannot hide it.
    let marker = if default {
        Span::from("★ ").fg(Color::Yellow)
    } else {
        Span::from("  ")
    };
    let what = Line::from(vec![marker, Span::from(p.kind.to_string())]);
    let mut label = p.number.map(|n| format!("#{n}")).unwrap_or_default();
    if let Some(name) = &p.name {
        label = format!("{label} {name}");
    }
    let label = format!("{} {}", label.trim(), p.trigger);
    Row::new([
        status_cell(p.status),
        Cell::from(what),
        Cell::from(label.trim().to_string()).dim(),
        Cell::from(ago(p.created_at, now)),
        Cell::from(pipeline_elapsed(Some(p), now)),
    ])
}

fn draw_jobs(frame: &mut Frame, area: Rect, app: &App, now: DateTime<Utc>) {
    let Some(project) = app.selected_project() else {
        return;
    };
    let Some((pipeline, _)) = app.selected_pipeline() else {
        frame.render_widget(panel(" Jobs ".into(), false), area);
        return;
    };
    let title = format!(" Jobs @ {} ", short_sha(&pipeline.sha));
    let block = panel(title, false);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let Some(state) = app.jobs_for(project, pipeline.id) else {
        frame.render_widget(Paragraph::new("Loading…").dim(), inner);
        return;
    };
    if let Some(error) = &state.error {
        frame.render_widget(
            Paragraph::new(format!("! {error}"))
                .style(status_style(Status::Failed))
                .wrap(Wrap { trim: true }),
            inner,
        );
        return;
    }
    if !state.loaded {
        frame.render_widget(Paragraph::new("Loading…").dim(), inner);
        return;
    }
    if state.jobs.is_empty() {
        frame.render_widget(Paragraph::new("No jobs.").dim(), inner);
        return;
    }
    // GitHub has no stages.
    let stages = state.jobs.iter().any(|j| j.stage.is_some());
    let rows: Vec<Row> = state.jobs.iter().map(|j| job_row(j, stages, now)).collect();
    let (widths, header) = if stages {
        (
            vec![
                Constraint::Length(11),
                Constraint::Fill(1),
                Constraint::Fill(2),
                Constraint::Length(8),
            ],
            vec!["STATUS", "STAGE", "JOB", "DURATION"],
        )
    } else {
        (
            vec![
                Constraint::Length(11),
                Constraint::Fill(1),
                Constraint::Length(8),
            ],
            vec!["STATUS", "JOB", "DURATION"],
        )
    };
    let table = Table::new(rows, widths).header(Row::new(header).bold());
    frame.render_widget(table, inner);
}

fn job_row(job: &Job, stages: bool, now: DateTime<Utc>) -> Row<'static> {
    let status = if job.allow_failure && job.status == Status::Failed {
        Cell::from(format!("{} failed", status_symbol(Status::Failed))).fg(Color::Yellow)
    } else {
        status_cell(job.status)
    };
    let mut name = Line::from(job.name.clone());
    if job.allow_failure && job.status == Status::Failed {
        name.push_span(Span::from(" (allowed)").dim());
    }
    let mut cells = vec![status];
    if stages {
        cells.push(Cell::from(job.stage.clone().unwrap_or_default()).dim());
    }
    cells.push(Cell::from(name));
    cells.push(Cell::from(
        elapsed(job.started_at, job.finished_at, now).unwrap_or_default(),
    ));
    Row::new(cells)
}

fn draw_footer(frame: &mut Frame, area: Rect, app: &App) {
    let line = if app.mode == Mode::Filter {
        Line::from(vec![
            Span::from(" filter: ").bold(),
            Span::from(app.filter.clone()),
            Span::from("█").slow_blink(),
            Span::from("   enter: apply  esc: clear").dim(),
        ])
    } else if let Some(message) = app.message() {
        Line::from(format!(" {message}"))
    } else if let Some(error) = app
        .sources
        .iter()
        .find_map(|s| s.error().map(|e| format!(" {e}")))
    // names the source already
    {
        Line::from(error).style(status_style(Status::Failed))
    } else {
        let keys: &[(&str, &str)] = if app.detail {
            &[
                ("q", "quit"),
                ("↑↓", "move"),
                ("tab", "switch pane"),
                ("o", "open in browser"),
                ("r", "refresh"),
                ("esc", "back"),
            ]
        } else {
            &[
                ("q", "quit"),
                ("↑↓", "move"),
                ("enter", "details"),
                ("/", "filter"),
                ("o", "open in browser"),
                ("r", "refresh"),
            ]
        };
        let mut spans = vec![Span::from(" ")];
        for (key, action) in keys {
            spans.push(Span::from(*key).bold());
            spans.push(Span::from(format!(" {action}  ")).dim());
        }
        Line::from(spans)
    };
    frame.render_widget(line, area);
}

fn panel(title: String, focused: bool) -> Block<'static> {
    let block = Block::bordered().title(title);
    if focused {
        block.border_style(Style::new().fg(Color::Cyan))
    } else {
        block.border_style(Style::new().dim())
    }
}

fn highlight(focused: bool) -> Style {
    if focused {
        Style::new().add_modifier(Modifier::REVERSED)
    } else {
        Style::new().add_modifier(Modifier::UNDERLINED)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::crossterm::event::{Event, KeyCode, KeyEvent};

    use super::*;
    use std::sync::Arc;

    use crate::app::Update;
    use crate::config::Config;
    use crate::http::NoCache;
    use crate::model::{CommitStatus, Job, PipelineKind, Project};
    use crate::poller::Command;
    use crate::provider::{Discovery, Source};
    use crate::store::Store;

    fn project(path: &str) -> Project {
        Project {
            source: "gitlab".into(),
            id: path.into(),
            path: path.into(),
            web_url: format!("https://gitlab.com/{path}"),
            default_branch: Some("main".into()),
            archived: false,
            fork: false,
            ci_enabled: true,
        }
    }

    fn pipeline(id: u64, status: Status, kind: PipelineKind, minutes_ago: i64) -> Pipeline {
        let created = Utc::now() - TimeDelta::minutes(minutes_ago);
        Pipeline {
            id,
            number: Some(id),
            name: None,
            kind,
            sha: format!("{id:0>40}"),
            status,
            trigger: "push".into(),
            created_at: created,
            updated_at: created,
            started_at: Some(created + TimeDelta::seconds(10)),
            finished_at: (!status.is_active()).then(|| created + TimeDelta::seconds(95)),
            web_url: String::new(),
        }
    }

    fn render(app: &mut App) -> String {
        let mut terminal = Terminal::new(TestBackend::new(110, 24)).unwrap();
        terminal.draw(|frame| draw(frame, app)).unwrap();
        let buffer = terminal.backend().buffer();
        (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn key(code: KeyCode) -> Event {
        Event::Key(KeyEvent::from(code))
    }

    #[tokio::test]
    async fn renders_list_and_detail() {
        let config =
            Config::parse("[[source]]\nname = \"gitlab\"\ntype = \"gitlab\"\ntoken = \"x\"")
                .unwrap();
        let source = Arc::new(Source::new(&config.sources[0], Arc::new(NoCache)).unwrap());
        let (tx, mut commands) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(std::slice::from_ref(&source), tx);
        let store = Store::in_memory().unwrap();
        let (scope, instance) = (source.scope.clone(), source.instance.clone());
        let mut seq = 0;
        let mut sync = async |app: &mut App| {
            let changes = store
                .changes_since(seq, vec![scope.clone()], vec![instance.clone()])
                .await
                .unwrap();
            seq = changes.seq;
            app.handle_update(Update::Changes(changes));
        };

        let paths = ["xfbs/ok", "xfbs/broken", "xfbs/busy", "xfbs/empty"];
        store.claim_discovery(&source.scope).await.unwrap();
        store
            .save_discovery(
                &source.scope,
                &source.instance,
                Ok(Discovery {
                    projects: paths.iter().map(|p| project(p)).collect(),
                    warnings: vec![],
                    without_ci: 0,
                }),
            )
            .await
            .unwrap();
        let main = || PipelineKind::Branch("main".into());
        let updates = [
            (
                "xfbs/ok",
                Some(pipeline(1, Status::Success, main(), 90)),
                vec![],
            ),
            (
                "xfbs/broken",
                Some(pipeline(2, Status::Failed, main(), 5)),
                vec![],
            ),
            (
                "xfbs/busy",
                Some(pipeline(3, Status::Success, main(), 600)),
                vec![
                    pipeline(4, Status::Running, PipelineKind::Tag("v1.2.0".into()), 2),
                    pipeline(5, Status::Queued, PipelineKind::MergeRequest(17), 1),
                ],
            ),
            ("xfbs/empty", None, vec![]),
        ];
        for (path, default, active) in updates {
            let default = default.and_then(|p| CommitStatus::new(vec![p]));
            store
                .save_project(&source.instance, path, Ok(default), Ok(active))
                .await
                .unwrap();
        }
        sync(&mut app).await;
        app.tick();

        let list = render(&mut app);
        println!("{list}");
        let order: Vec<usize> = ["xfbs/broken", "xfbs/busy", "xfbs/ok", "xfbs/empty"]
            .iter()
            .map(|p| list.find(p).unwrap())
            .collect();
        assert!(order.is_sorted(), "failing, then busy, then ok, then none");
        assert!(list.contains("1 running") && list.contains("1 failing"));
        assert!(list.contains("+2"));

        // Select the busy project and open the detail view on its tag pipeline.
        app.handle_event(key(KeyCode::Down));
        app.handle_event(key(KeyCode::Enter));
        app.handle_event(key(KeyCode::Down));
        assert_eq!(app.selected_pipeline().unwrap().0.id, 4);

        // The app asks the poller for the jobs on screen.
        app.tick();
        let Some(Command::WantJobs {
            pipeline, project, ..
        }) = commands.try_recv().ok()
        else {
            panic!("expected a request for jobs");
        };
        assert_eq!((project.id.as_str(), pipeline.id), ("xfbs/busy", 4));
        // Do what the poller would.
        assert!(
            store
                .claim_jobs(&source.instance, "xfbs/busy", 4, true, 0)
                .await
                .unwrap()
        );
        let jobs = vec![
            Job {
                id: 1,
                name: "build".into(),
                stage: Some("build".into()),
                status: Status::Success,
                allow_failure: false,
                started_at: Some(Utc::now() - TimeDelta::seconds(100)),
                finished_at: Some(Utc::now() - TimeDelta::seconds(40)),
                web_url: String::new(),
            },
            Job {
                id: 2,
                name: "publish".into(),
                stage: Some("release".into()),
                status: Status::Running,
                allow_failure: false,
                started_at: Some(Utc::now() - TimeDelta::seconds(30)),
                finished_at: None,
                web_url: String::new(),
            },
        ];
        store
            .save_jobs(&source.instance, "xfbs/busy", 4, Ok(jobs))
            .await
            .unwrap();
        sync(&mut app).await;

        let detail = render(&mut app);
        println!("{detail}");
        assert!(detail.contains("tag v1.2.0"));
        assert!(detail.contains("MR !17"));
        assert!(detail.contains("★ branch main"));
        assert!(detail.contains("publish"));
    }
}

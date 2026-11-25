mod app;
mod config;
mod filter;
mod format;
mod http;
mod model;
mod poller;
mod provider;
mod secret;
mod store;
mod ui;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use futures::StreamExt;

use crate::app::App;
use crate::config::Config;
use crate::format::{ago, elapsed, short_sha};
use crate::http::{NoCache, ResponseCache};
use crate::model::Pipeline;
use crate::poller::Poller;
use crate::provider::Source;
use crate::store::Store;

/// Like htop, but for CI pipelines.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Config file [default: ~/.config/citop/config.toml]
    #[arg(short, long, env = "CITOP_CONFIG")]
    config: Option<PathBuf>,

    /// Check the config file (including token commands) and exit.
    #[arg(long)]
    check: bool,

    /// Query every source once and print the result (development aid).
    #[arg(long, hide = true)]
    dump: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<()> {
    let path = match args.config {
        Some(path) => path,
        None => config::default_path()
            .context("could not determine the config directory; use --config")?,
    };
    let config = Config::load(&path)?;
    for warning in &config.warnings {
        eprintln!("warning: {warning}");
    }

    if args.check {
        println!("{}: ok", path.display());
        println!(
            "  refresh every {} (active) / {} (idle), keep finished pipelines for {}",
            humantime::format_duration(config.refresh),
            humantime::format_duration(config.idle_refresh),
            humantime::format_duration(config.keep_finished),
        );
        for source in &config.sources {
            println!(
                "  {} ({}, {}, {})",
                source.name, source.kind, source.url, source.token_origin
            );
            if !source.filter.include().is_empty() {
                println!("    include: {}", source.filter.include().join(", "));
            }
            if !source.filter.exclude().is_empty() {
                println!("    exclude: {}", source.filter.exclude().join(", "));
            }
        }
        return Ok(());
    }
    if args.dump {
        return dump(&config).await;
    }
    run_tui(&config).await
}

async fn run_tui(config: &Config) -> Result<()> {
    // The store is shared with other citop processes. If it cannot be
    // opened, run with a private in-memory one instead.
    let mut store_problem = None;
    let store = match store_path().map(|path| Store::open(&path)) {
        Some(Ok(store)) => store,
        Some(Err(e)) => {
            store_problem = Some(format!("{e:#}; not saving state"));
            Store::in_memory()?
        }
        None => {
            store_problem = Some("no cache directory; not saving state".to_string());
            Store::in_memory()?
        }
    };
    let store = Arc::new(store);

    let mut sources = Vec::new();
    let mut skipped = Vec::new();
    for source_config in &config.sources {
        let cache: Arc<dyn ResponseCache> = store.clone();
        match Source::new(source_config, cache) {
            Ok(source) => sources.push(Arc::new(source)),
            Err(e) => skipped.push(format!("{e:#}")),
        }
    }
    if sources.is_empty() {
        anyhow::bail!("no usable sources: {}", skipped.join("; "));
    }

    let (commands_tx, commands) = tokio::sync::mpsc::unbounded_channel();
    let (updates_tx, mut updates) = tokio::sync::mpsc::unbounded_channel();
    let mut app = App::new(&sources, commands_tx);
    let problems: Vec<String> = store_problem
        .into_iter()
        .chain(skipped.into_iter().map(|s| format!("skipped: {s}")))
        .collect();
    if !problems.is_empty() {
        app.set_message(problems.join("; "));
    }
    let poller = Poller::new(config, store.clone(), &sources, updates_tx.clone());
    tokio::spawn(poller.run(commands));
    tokio::spawn(poller::watch(store, sources, updates_tx));

    // crossterm's event reading blocks, so it gets its own thread.
    let (event_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(move || {
        while let Ok(event) = ratatui::crossterm::event::read() {
            if event_tx.send(event).is_err() {
                break;
            }
        }
    });

    let mut terminal = ratatui::init();
    let result: Result<()> = async {
        // Redraw at least once a second so that ages and durations tick.
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        while !app.quit {
            app.tick();
            terminal.draw(|frame| ui::draw(frame, &mut app))?;
            tokio::select! {
                _ = tick.tick() => {}
                Some(update) = updates.recv() => {
                    app.handle_update(update);
                    // Apply everything that is ready before redrawing.
                    while let Ok(update) = updates.try_recv() {
                        app.handle_update(update);
                    }
                }
                Some(event) = events.recv() => app.handle_event(event),
            }
        }
        Ok(())
    }
    .await;
    ratatui::restore();
    result
}

async fn dump(config: &Config) -> Result<()> {
    let since = Utc::now() - chrono::Duration::from_std(config.keep_finished)?;
    for source_config in &config.sources {
        let source = match Source::new(source_config, Arc::new(NoCache)) {
            Ok(source) => source,
            Err(e) => {
                eprintln!("warning: {e:#}");
                continue;
            }
        };
        let discovery = match source.discover().await {
            Ok(discovery) => discovery,
            Err(e) => {
                eprintln!("error: {e:#}");
                continue;
            }
        };
        println!(
            "== {} ({}): {} projects ({} hidden because CI/CD is disabled)",
            source.name,
            source.kind,
            discovery.projects.len(),
            discovery.without_ci,
        );
        for warning in &discovery.warnings {
            eprintln!("warning: {warning}");
        }

        let source = &source;
        let mut rows = futures::stream::iter(&discovery.projects)
            .map(|project| async move {
                let (default, active) = futures::join!(
                    source.default_branch(project),
                    source.active_pipelines(project, since)
                );
                (project, default, active)
            })
            .buffered(8);

        while let Some((project, default, active)) = rows.next().await {
            let branch = project.default_branch.as_deref().unwrap_or("-");
            match default {
                Ok(Some(commit)) => println!(
                    "{:<9} {}  {} @ {}",
                    commit.status,
                    project.path,
                    branch,
                    short_sha(&commit.sha)
                ),
                Ok(None) => println!("{:<9} {}  {}", "-", project.path, branch),
                Err(e) => println!("{:<9} {}  {e:#}", "error", project.path),
            }
            match active {
                Ok(pipelines) => {
                    for pipeline in pipelines {
                        print_pipeline(&pipeline);
                        if pipeline.status.is_active() {
                            match source.jobs(project, &pipeline).await {
                                Ok(jobs) => {
                                    for job in jobs {
                                        println!(
                                            "            {:<9} {}{}{}",
                                            job.status,
                                            job.stage.map(|s| format!("{s}: ")).unwrap_or_default(),
                                            job.name,
                                            if job.allow_failure {
                                                " (allowed to fail)"
                                            } else {
                                                ""
                                            },
                                        );
                                    }
                                }
                                Err(e) => println!("            error: {e:#}"),
                            }
                        }
                    }
                }
                Err(e) => println!("    error: {e:#}"),
            }
        }

        if let Some(rate) = source.rate_limit() {
            println!("   rate limit: {}/{} remaining", rate.remaining, rate.limit);
        }
    }
    Ok(())
}

fn print_pipeline(pipeline: &Pipeline) {
    let number = pipeline
        .number
        .map(|n| format!("#{n} "))
        .unwrap_or_default();
    let name = pipeline
        .name
        .as_deref()
        .map(|n| format!("\"{n}\" "))
        .unwrap_or_default();
    let now = Utc::now();
    let duration = elapsed(pipeline.started_at, pipeline.finished_at, now)
        .map(|d| format!(", ran for {d}"))
        .unwrap_or_default();
    println!(
        "    {:<9} {}{}({}, {}) {} @ {}, created {} ago{}",
        pipeline.status,
        number,
        name,
        pipeline.kind,
        pipeline.trigger,
        pipeline.web_url,
        short_sha(&pipeline.sha),
        ago(pipeline.created_at, now),
        duration,
    );
}

/// `$XDG_CACHE_HOME/citop/state.db` or the platform equivalent.
fn store_path() -> Option<PathBuf> {
    Some(dirs::cache_dir()?.join("citop").join("state.db"))
}

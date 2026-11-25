mod config;
mod filter;
mod http;
mod model;
mod provider;
mod secret;

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures::StreamExt;

use crate::config::Config;
use crate::model::Pipeline;
use crate::provider::Source;

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

    /// Query every source once and print the result. A development aid until
    /// the interactive UI exists.
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
    bail!("the interactive UI is not implemented yet; try --check")
}

async fn dump(config: &Config) -> Result<()> {
    let since = Utc::now() - chrono::Duration::from_std(config.keep_finished)?;
    for source_config in &config.sources {
        let source = match Source::new(source_config) {
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
    let duration = match (pipeline.started_at, pipeline.finished_at) {
        (Some(start), Some(end)) => format!(", took {}", human(end - start)),
        (Some(start), None) => format!(", running for {}", human(Utc::now() - start)),
        _ => String::new(),
    };
    println!(
        "    {:<9} {}{}({}, {}) {} @ {}, created {}{}",
        pipeline.status,
        number,
        name,
        pipeline.kind,
        pipeline.trigger,
        pipeline.web_url,
        short_sha(&pipeline.sha),
        ago(pipeline.created_at),
        duration,
    );
}

fn short_sha(sha: &str) -> &str {
    sha.get(..8).unwrap_or(sha)
}

fn ago(time: DateTime<Utc>) -> String {
    format!("{} ago", human(Utc::now() - time))
}

fn human(duration: chrono::TimeDelta) -> String {
    let secs = duration.num_seconds().max(0);
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m{:02}s", secs / 60, secs % 60),
        3600..86400 => format!("{}h{:02}m", secs / 3600, secs % 3600 / 60),
        _ => format!("{}d", secs / 86400),
    }
}

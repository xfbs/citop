//! Background fetching. The poller decides what is due, claims it in the
//! [`Store`] and writes results back there; it never talks to the UI
//! directly. The [`watch`] task tells the UI when the store changed, no
//! matter which process changed it.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use crate::app::Update;
use crate::config::Config;
use crate::http::RateLimit;
use crate::model::{Pipeline, Project};
use crate::provider::Source;
use crate::store::Store;

/// Concurrent project refreshes per source, to be gentle on rate limits.
const CONCURRENCY_PER_SOURCE: usize = 8;
/// How often the poller looks for due work.
const TICK: Duration = Duration::from_secs(1);
/// How often the store is checked for changes.
const WATCH_INTERVAL: Duration = Duration::from_millis(500);

/// Requests from the UI.
pub enum Command {
    /// The user is looking at the jobs of this pipeline.
    WantJobs {
        source: usize,
        project: Box<Project>,
        pipeline: Box<Pipeline>,
    },
    /// Refresh a project (and its jobs) as soon as possible.
    Refresh { source: usize, project_id: String },
}

struct PolledSource {
    source: Arc<Source>,
    in_flight: Arc<AtomicUsize>,
    /// The latest project list, and the store sequence number it is from.
    projects: Vec<Project>,
    discovery_seq: i64,
}

pub struct Poller {
    store: Arc<Store>,
    sources: Vec<PolledSource>,
    refresh: chrono::TimeDelta,
    idle_refresh: chrono::TimeDelta,
    keep_finished: chrono::TimeDelta,
    /// For reporting problems with the store itself.
    updates: mpsc::UnboundedSender<Update>,
}

impl Poller {
    pub fn new(
        config: &Config,
        store: Arc<Store>,
        sources: &[Arc<Source>],
        updates: mpsc::UnboundedSender<Update>,
    ) -> Self {
        let delta = |d| chrono::TimeDelta::from_std(d).unwrap_or(chrono::TimeDelta::MAX);
        Self {
            store,
            sources: sources
                .iter()
                .map(|source| PolledSource {
                    source: source.clone(),
                    in_flight: Arc::default(),
                    projects: Vec::new(),
                    discovery_seq: 0,
                })
                .collect(),
            refresh: delta(config.refresh),
            idle_refresh: delta(config.idle_refresh),
            keep_finished: delta(config.keep_finished),
            updates,
        }
    }

    pub async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let result = tokio::select! {
                _ = tick.tick() => self.schedule().await,
                command = commands.recv() => match command {
                    Some(command) => self.handle(command).await,
                    None => return,
                },
            };
            if let Err(e) = result {
                let _ = self
                    .updates
                    .send(Update::Message(format!("state database: {e:#}")));
            }
        }
    }

    /// Starts everything that is due and not claimed by another process.
    async fn schedule(&mut self) -> anyhow::Result<()> {
        let now = Utc::now();
        let busy_before = (now - self.refresh).timestamp_millis();
        let idle_before = (now - self.idle_refresh).timestamp_millis();
        let since = now - self.keep_finished;

        for polled in &mut self.sources {
            let source = polled.source.clone();
            let pace = Pace::of(source.rate_limit(), now);
            if pace == Pace::Paused {
                continue;
            }
            // Short on requests: refresh busy projects as rarely as idle ones.
            let busy_before = match pace {
                Pace::Slow => idle_before,
                _ => busy_before,
            };

            if self.store.claim_discovery(&source.scope).await? {
                let store = self.store.clone();
                let source = source.clone();
                tokio::spawn(async move {
                    let result = source.discover().await.map_err(|e| format!("{e:#}"));
                    let _ = store
                        .save_discovery(&source.scope, &source.instance, result)
                        .await;
                    save_rate_limit(&store, &source).await;
                });
            }

            if let Some(row) = self
                .store
                .discovery_since(&source.scope, polled.discovery_seq)
                .await?
            {
                polled.discovery_seq = row.seq;
                if let Some(projects) = row.projects {
                    polled.projects = projects;
                }
            }

            let free =
                CONCURRENCY_PER_SOURCE.saturating_sub(polled.in_flight.load(Ordering::SeqCst));
            if free == 0 || polled.projects.is_empty() {
                continue;
            }
            let ids: HashSet<String> = polled.projects.iter().map(|p| p.id.clone()).collect();
            let claimed = self
                .store
                .claim_due_projects(&source.instance, ids, busy_before, idle_before, free)
                .await?;
            for id in claimed {
                let Some(project) = polled.projects.iter().find(|p| p.id == id).cloned() else {
                    continue;
                };
                let store = self.store.clone();
                let source = source.clone();
                let in_flight = polled.in_flight.clone();
                in_flight.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let (default, active) = futures::join!(
                        source.default_branch(&project),
                        source.active_pipelines(&project, since)
                    );
                    let _ = store
                        .save_project(
                            &source.instance,
                            &project.id,
                            default.map_err(|e| format!("{e:#}")),
                            active.map_err(|e| format!("{e:#}")),
                        )
                        .await;
                    save_rate_limit(&store, &source).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                });
            }
        }
        Ok(())
    }

    async fn handle(&mut self, command: Command) -> anyhow::Result<()> {
        match command {
            Command::WantJobs {
                source,
                project,
                pipeline,
            } => {
                let Some(polled) = self.sources.get(source) else {
                    return Ok(());
                };
                let source = polled.source.clone();
                let refresh_before = (Utc::now() - self.refresh).timestamp_millis();
                let claimed = self
                    .store
                    .claim_jobs(
                        &source.instance,
                        &project.id,
                        pipeline.id,
                        pipeline.status.is_active(),
                        refresh_before,
                    )
                    .await?;
                if claimed {
                    let store = self.store.clone();
                    tokio::spawn(async move {
                        let result = source
                            .jobs(&project, &pipeline)
                            .await
                            .map_err(|e| format!("{e:#}"));
                        let _ = store
                            .save_jobs(&source.instance, &project.id, pipeline.id, result)
                            .await;
                        save_rate_limit(&store, &source).await;
                    });
                }
            }
            Command::Refresh { source, project_id } => {
                if let Some(polled) = self.sources.get(source) {
                    self.store
                        .invalidate_project(&polled.source.instance, &project_id)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

/// How fast a source may be polled, given its rate limit.
#[derive(Debug, PartialEq, Eq)]
enum Pace {
    Normal,
    /// Less than [`SLOW_BELOW`] of the limit left.
    Slow,
    /// Nearly nothing left; wait for the reset.
    Paused,
}

/// Fraction of the rate limit below which polling slows down.
const SLOW_BELOW: f64 = 0.2;
/// Requests kept in reserve, e.g. for opening jobs.
const PAUSE_BELOW: u64 = 20;

impl Pace {
    fn of(rate: Option<RateLimit>, now: DateTime<Utc>) -> Self {
        let Some(rate) = rate else {
            return Pace::Normal;
        };
        // After the reset, the remembered numbers are outdated.
        if rate.reset.is_some_and(|reset| reset <= now) {
            return Pace::Normal;
        }
        if rate.remaining < PAUSE_BELOW {
            Pace::Paused
        } else if (rate.remaining as f64) < rate.limit as f64 * SLOW_BELOW {
            Pace::Slow
        } else {
            Pace::Normal
        }
    }
}

async fn save_rate_limit(store: &Store, source: &Source) {
    if let Some(rate) = source.rate_limit() {
        let _ = store.save_rate_limit(&source.scope, rate).await;
    }
}

/// Sends everything relevant from the store to the UI, starting with what is
/// already there, then whatever changes, from this or any other process.
pub async fn watch(
    store: Arc<Store>,
    sources: Vec<Arc<Source>>,
    updates: mpsc::UnboundedSender<Update>,
) {
    let scopes: Vec<String> = sources.iter().map(|s| s.scope.clone()).collect();
    let instances: Vec<String> = sources.iter().map(|s| s.instance.clone()).collect();
    let mut seen = 0;
    let mut interval = tokio::time::interval(WATCH_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let current = match store.seq().await {
            Ok(seq) => seq,
            Err(e) => {
                let _ = updates.send(Update::Message(format!("state database: {e:#}")));
                continue;
            }
        };
        if current == seen && seen != 0 {
            continue;
        }
        match store
            .changes_since(seen, scopes.clone(), instances.clone())
            .await
        {
            Ok(changes) => {
                seen = changes.seq;
                if updates.send(Update::Changes(changes)).is_err() {
                    return;
                }
            }
            Err(e) => {
                let _ = updates.send(Update::Message(format!("state database: {e:#}")));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paces_by_rate_limit() {
        let now = Utc::now();
        let rate = |remaining, reset_in: i64| {
            Some(RateLimit {
                limit: 5000,
                remaining,
                reset: Some(now + chrono::TimeDelta::seconds(reset_in)),
            })
        };
        assert_eq!(Pace::of(None, now), Pace::Normal);
        assert_eq!(Pace::of(rate(4000, 60), now), Pace::Normal);
        assert_eq!(Pace::of(rate(500, 60), now), Pace::Slow);
        assert_eq!(Pace::of(rate(3, 60), now), Pace::Paused);
        assert_eq!(Pace::of(rate(3, -1), now), Pace::Normal);
    }
}

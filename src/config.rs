//! Loading and validating the TOML config file.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use url::Url;

use crate::filter::ProjectFilter;
use crate::secret::Secret;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    GitHub,
    GitLab,
}

impl SourceKind {
    fn default_url(self) -> Url {
        let url = match self {
            SourceKind::GitHub => "https://github.com/",
            SourceKind::GitLab => "https://gitlab.com/",
        };
        Url::parse(url).expect("valid default URL")
    }
}

impl fmt::Display for SourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SourceKind::GitHub => "github",
            SourceKind::GitLab => "gitlab",
        })
    }
}

/// Where a source's token came from, for display purposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenOrigin {
    Inline,
    Env(String),
    Command(String),
}

impl fmt::Display for TokenOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenOrigin::Inline => f.write_str("inline token"),
            TokenOrigin::Env(var) => write!(f, "token from ${var}"),
            TokenOrigin::Command(cmd) => write!(f, "token from `{cmd}`"),
        }
    }
}

/// A validated config file, with tokens resolved.
#[derive(Debug)]
pub struct Config {
    /// How often to refresh projects that have running pipelines.
    pub refresh: Duration,
    /// How often to refresh projects where nothing is running.
    pub idle_refresh: Duration,
    /// How long finished pipelines stay in the list of active pipelines.
    pub keep_finished: Duration,
    pub sources: Vec<SourceConfig>,
    /// Non-fatal problems worth telling the user about.
    pub warnings: Vec<String>,
}

#[derive(Debug)]
pub struct SourceConfig {
    pub name: String,
    pub kind: SourceKind,
    /// Web URL of the instance, e.g. `https://gitlab.com/`.
    pub url: Url,
    pub token: Secret,
    pub token_origin: TokenOrigin,
    pub filter: ProjectFilter,
    pub exclude_archived: bool,
    pub exclude_forks: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default = "default_refresh", with = "humantime_serde")]
    refresh: Duration,
    #[serde(default = "default_idle_refresh", with = "humantime_serde")]
    idle_refresh: Duration,
    #[serde(default = "default_keep_finished", with = "humantime_serde")]
    keep_finished: Duration,
    #[serde(default, rename = "source")]
    sources: Vec<RawSource>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSource {
    name: String,
    #[serde(rename = "type")]
    kind: SourceKind,
    url: Option<Url>,
    token: Option<String>,
    token_env: Option<String>,
    token_cmd: Option<String>,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default = "yes")]
    exclude_archived: bool,
    #[serde(default)]
    exclude_forks: bool,
}

fn default_refresh() -> Duration {
    Duration::from_secs(5)
}

fn default_idle_refresh() -> Duration {
    Duration::from_secs(60)
}

fn default_keep_finished() -> Duration {
    Duration::from_secs(60 * 60)
}

fn yes() -> bool {
    true
}

/// `$CITOP_CONFIG` is handled by the CLI; this is the fallback.
pub fn default_path() -> Option<PathBuf> {
    Some(dirs::config_dir()?.join("citop").join("config.toml"))
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("could not read config file {}", path.display()))?;
        let mut config = Self::parse(&text)
            .with_context(|| format!("invalid config file {}", path.display()))?;
        if config
            .sources
            .iter()
            .any(|s| s.token_origin == TokenOrigin::Inline)
            && let Some(warning) = permission_warning(path)
        {
            config.warnings.push(warning);
        }
        Ok(config)
    }

    pub fn parse(text: &str) -> Result<Self> {
        let raw: RawConfig = toml::from_str(text)?;

        if raw.sources.is_empty() {
            bail!("no sources configured; add at least one [[source]] section");
        }
        if raw.refresh < Duration::from_secs(1) || raw.idle_refresh < Duration::from_secs(1) {
            bail!("`refresh` and `idle_refresh` must be at least 1s");
        }

        let mut names = HashSet::new();
        let mut sources = Vec::with_capacity(raw.sources.len());
        for source in raw.sources {
            let name = source.name.trim().to_string();
            if name.is_empty() {
                bail!("source names must not be empty");
            }
            if !names.insert(name.clone()) {
                bail!("duplicate source name `{name}`");
            }
            let source = SourceConfig::from_raw(name.clone(), source)
                .with_context(|| format!("source `{name}`"))?;
            sources.push(source);
        }

        Ok(Self {
            refresh: raw.refresh,
            idle_refresh: raw.idle_refresh,
            keep_finished: raw.keep_finished,
            sources,
            warnings: Vec::new(),
        })
    }
}

impl SourceConfig {
    fn from_raw(name: String, raw: RawSource) -> Result<Self> {
        let url = match raw.url {
            Some(url) => validate_url(url)?,
            None => raw.kind.default_url(),
        };
        let filter = ProjectFilter::new(&raw.include, &raw.exclude).map_err(|e| anyhow!(e))?;
        let (token, token_origin) = resolve_token(raw.token, raw.token_env, raw.token_cmd)?;
        Ok(Self {
            name,
            kind: raw.kind,
            url,
            token,
            token_origin,
            filter,
            exclude_archived: raw.exclude_archived,
            exclude_forks: raw.exclude_forks,
        })
    }
}

fn validate_url(mut url: Url) -> Result<Url> {
    if !matches!(url.scheme(), "http" | "https") {
        bail!("`url` must be an http(s) URL, got `{url}`");
    }
    if url.query().is_some() || url.fragment().is_some() || !url.username().is_empty() {
        bail!("`url` must not contain credentials, a query or a fragment");
    }
    // Normalize to a trailing slash so instances under a sub-path work.
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url)
}

fn resolve_token(
    token: Option<String>,
    token_env: Option<String>,
    token_cmd: Option<String>,
) -> Result<(Secret, TokenOrigin)> {
    let (value, origin) = match (token, token_env, token_cmd) {
        (Some(token), None, None) => (token, TokenOrigin::Inline),
        (None, Some(var), None) => {
            let value = std::env::var(&var).with_context(|| {
                format!("environment variable `{var}` is not set or not valid UTF-8")
            })?;
            (value, TokenOrigin::Env(var))
        }
        (None, None, Some(cmd)) => (run_token_cmd(&cmd)?, TokenOrigin::Command(cmd)),
        _ => bail!("set exactly one of `token`, `token_env` or `token_cmd`"),
    };
    let value = value.trim();
    if value.is_empty() {
        bail!("{origin} is empty");
    }
    Ok((Secret::new(value.to_string()), origin))
}

fn run_token_cmd(cmd: &str) -> Result<String> {
    let mut command = if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.arg("/C");
        c
    } else {
        let mut c = Command::new("sh");
        c.arg("-c");
        c
    };
    // Inherit stdin/stderr so password managers can prompt.
    let output = command
        .arg(cmd)
        .stdin(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .with_context(|| format!("could not run token command `{cmd}`"))?;
    if !output.status.success() {
        bail!("token command `{cmd}` failed ({})", output.status);
    }
    String::from_utf8(output.stdout)
        .map_err(|_| anyhow!("token command `{cmd}` printed invalid UTF-8"))
}

#[cfg(unix)]
fn permission_warning(path: &Path) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path).ok()?.permissions().mode();
    (mode & 0o077 != 0).then(|| {
        format!(
            "{} contains a token but is readable by other users; run `chmod 600` on it, or use `token_env`/`token_cmd`",
            path.display()
        )
    })
}

#[cfg(not(unix))]
fn permission_warning(_path: &Path) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(text: &str) -> String {
        format!("{:#}", Config::parse(text).unwrap_err())
    }

    #[test]
    fn minimal_config_uses_defaults() {
        let config = Config::parse(
            r#"
            [[source]]
            name = "gl"
            type = "gitlab"
            token = "abc"
            "#,
        )
        .unwrap();
        assert_eq!(config.refresh, Duration::from_secs(5));
        assert_eq!(config.idle_refresh, Duration::from_secs(60));
        assert_eq!(config.keep_finished, Duration::from_secs(3600));
        let source = &config.sources[0];
        assert_eq!(source.kind, SourceKind::GitLab);
        assert_eq!(source.url.as_str(), "https://gitlab.com/");
        assert_eq!(source.token.expose(), "abc");
        assert!(source.exclude_archived);
        assert!(!source.exclude_forks);
    }

    #[test]
    fn full_config() {
        let config = Config::parse(
            r#"
            refresh = "3s"
            idle_refresh = "2m"
            keep_finished = "30m"

            [[source]]
            name = "work"
            type = "gitlab"
            url = "https://git.example.com/gitlab"
            token_cmd = "echo '  secret-token  '"
            include = ["group/**", "other/project"]
            exclude = ["group/archive/**"]
            exclude_archived = false
            exclude_forks = true

            [[source]]
            name = "gh"
            type = "github"
            token = "ghp"
            "#,
        )
        .unwrap();
        assert_eq!(config.refresh, Duration::from_secs(3));
        assert_eq!(config.idle_refresh, Duration::from_secs(120));
        let work = &config.sources[0];
        assert_eq!(work.url.as_str(), "https://git.example.com/gitlab/");
        assert_eq!(work.token.expose(), "secret-token");
        assert!(matches!(work.token_origin, TokenOrigin::Command(_)));
        assert!(work.filter.matches("group/a/b"));
        assert!(!work.filter.matches("group/archive/x"));
        assert!(work.exclude_forks && !work.exclude_archived);
        assert_eq!(config.sources[1].url.as_str(), "https://github.com/");
    }

    #[test]
    fn token_from_env() {
        // SAFETY: the variable name is unique to this test.
        unsafe { std::env::set_var("CITOP_TEST_TOKEN_FROM_ENV", "from-env") };
        let config = Config::parse(
            r#"
            [[source]]
            name = "gl"
            type = "gitlab"
            token_env = "CITOP_TEST_TOKEN_FROM_ENV"
            "#,
        )
        .unwrap();
        assert_eq!(config.sources[0].token.expose(), "from-env");
    }

    #[test]
    fn rejects_bad_configs() {
        assert!(err("").contains("no sources"));
        assert!(err("[[source]]\nname = \"a\"\ntype = \"gitlab\"").contains("exactly one"));
        assert!(
            err("[[source]]\nname = \"a\"\ntype = \"gitlab\"\ntoken = \"x\"\ntoken_env = \"Y\"")
                .contains("exactly one")
        );
        assert!(
            err("[[source]]\nname = \"a\"\ntype = \"bitbucket\"\ntoken = \"x\"")
                .contains("bitbucket")
        );
        assert!(
            err("[[source]]\nname = \"a\"\ntype = \"gitlab\"\ntoken = \"x\"\ntypo = 1")
                .contains("typo")
        );
        assert!(
            err("[[source]]\nname = \"a\"\ntype = \"gitlab\"\ntoken = \"x\"\n[[source]]\nname = \"a\"\ntype = \"gitlab\"\ntoken = \"x\"")
                .contains("duplicate")
        );
        assert!(
            err("[[source]]\nname = \"a\"\ntype = \"gitlab\"\ntoken = \"x\"\nurl = \"ftp://x\"")
                .contains("http(s)")
        );
        assert!(
            err("[[source]]\nname = \"a\"\ntype = \"gitlab\"\ntoken = \"x\"\ninclude = [\"a/[\"]")
                .contains("invalid pattern")
        );
        assert!(
            err("[[source]]\nname = \"a\"\ntype = \"gitlab\"\ntoken_cmd = \"exit 3\"")
                .contains("failed")
        );
        assert!(
            err("refresh = \"100ms\"\n[[source]]\nname = \"a\"\ntype = \"gitlab\"\ntoken = \"x\"")
                .contains("1s")
        );
    }

    #[test]
    fn errors_do_not_leak_tokens() {
        let msg = err(
            "[[source]]\nname = \"a\"\ntype = \"gitlab\"\ntoken = \"hunter2\"\nurl = \"ftp://x\"",
        );
        assert!(!msg.contains("hunter2"));
    }
}

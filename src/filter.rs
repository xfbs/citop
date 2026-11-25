//! Include/exclude filtering of projects, and working out where to look for them.

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

/// Decides which projects of a source are shown, based on glob patterns
/// matched against the full project path (`group/subgroup/project`).
///
/// `*` matches within one path component, `**` matches across components.
/// Matching is case-insensitive. Exclude patterns win over include patterns,
/// and an empty include list includes everything.
#[derive(Debug, Clone)]
pub struct ProjectFilter {
    include: Vec<String>,
    include_set: GlobSet,
    exclude: Vec<String>,
    exclude_set: GlobSet,
}

/// A place a provider has to query to find candidate projects.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Target {
    /// Every project the token is a member of.
    All,
    /// A single project, by full path.
    Project(String),
    /// The projects in a group/organization/user namespace.
    Namespace { path: String, recursive: bool },
}

impl ProjectFilter {
    pub fn new(include: &[String], exclude: &[String]) -> Result<Self, String> {
        let include = normalize(include)?;
        let exclude = normalize(exclude)?;
        Ok(Self {
            include_set: build_set(&include)?,
            exclude_set: build_set(&exclude)?,
            include,
            exclude,
        })
    }

    pub fn include(&self) -> &[String] {
        &self.include
    }

    pub fn exclude(&self) -> &[String] {
        &self.exclude
    }

    pub fn matches(&self, path: &str) -> bool {
        let path = path.trim_matches('/');
        (self.include.is_empty() || self.include_set.is_match(path))
            && !self.exclude_set.is_match(path)
    }

    /// Where to look for projects so that every include pattern can match.
    ///
    /// Patterns with a literal namespace prefix (`group/sub/**`) only need that
    /// namespace listed, and fully literal patterns name a single project. This
    /// keeps discovery cheap and also finds public projects the token is not a
    /// member of. A pattern starting with a wildcard needs the membership list.
    pub fn discovery_targets(&self) -> Vec<Target> {
        if self.include.is_empty() {
            return vec![Target::All];
        }
        let mut targets: Vec<Target> = self
            .include
            .iter()
            .map(|pattern| {
                let components: Vec<&str> = pattern.split('/').collect();
                match components.iter().position(|c| has_glob_meta(c)) {
                    None => Target::Project(pattern.clone()),
                    Some(0) => Target::All,
                    Some(i) => Target::Namespace {
                        path: components[..i].join("/"),
                        recursive: components.len() - i > 1
                            || components[i..].iter().any(|c| c.contains("**")),
                    },
                }
            })
            .collect();
        targets.sort();
        targets.dedup();
        targets
    }
}

fn has_glob_meta(component: &str) -> bool {
    component.contains(['*', '?', '[', '{', '\\'])
}

fn normalize(patterns: &[String]) -> Result<Vec<String>, String> {
    patterns
        .iter()
        .map(|p| {
            let p = p.trim().trim_matches('/');
            if p.is_empty() {
                Err("empty project pattern".to_string())
            } else {
                Ok(p.to_string())
            }
        })
        .collect()
}

fn build_set(patterns: &[String]) -> Result<GlobSet, String> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern)
            .literal_separator(true)
            .case_insensitive(true)
            .backslash_escape(true)
            .build()
            .map_err(|e| format!("invalid pattern `{pattern}`: {e}"))?;
        builder.add(glob);
    }
    builder.build().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(include: &[&str], exclude: &[&str]) -> ProjectFilter {
        let s = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        ProjectFilter::new(&s(include), &s(exclude)).unwrap()
    }

    #[test]
    fn empty_include_matches_everything() {
        let f = filter(&[], &["*/secret"]);
        assert!(f.matches("a/b"));
        assert!(f.matches("a/b/c"));
        assert!(!f.matches("a/secret"));
    }

    #[test]
    fn single_star_stays_in_one_component() {
        let f = filter(&["group/*"], &[]);
        assert!(f.matches("group/project"));
        assert!(!f.matches("group/sub/project"));
        assert!(!f.matches("other/project"));
    }

    #[test]
    fn double_star_crosses_components() {
        let f = filter(&["group/**"], &["group/archive/**"]);
        assert!(f.matches("group/project"));
        assert!(f.matches("group/sub/project"));
        assert!(!f.matches("group/archive/old"));
        assert!(!f.matches("group"));
    }

    #[test]
    fn matching_is_case_insensitive() {
        assert!(filter(&["XFBS/*"], &[]).matches("xfbs/citop"));
    }

    #[test]
    fn invalid_and_empty_patterns_are_rejected() {
        assert!(ProjectFilter::new(&["a/[".into()], &[]).is_err());
        assert!(ProjectFilter::new(&[" / ".into()], &[]).is_err());
    }

    #[test]
    fn discovery_targets() {
        assert_eq!(filter(&[], &[]).discovery_targets(), vec![Target::All]);
        assert_eq!(
            filter(&["a/b", "/g/**", "g/*", "g/sub/*/x", "*/foo", "u/*"], &[]).discovery_targets(),
            vec![
                Target::All,
                Target::Project("a/b".into()),
                Target::Namespace {
                    path: "g".into(),
                    recursive: false
                },
                Target::Namespace {
                    path: "g".into(),
                    recursive: true
                },
                Target::Namespace {
                    path: "g/sub".into(),
                    recursive: true
                },
                Target::Namespace {
                    path: "u".into(),
                    recursive: false
                },
            ]
        );
    }
}

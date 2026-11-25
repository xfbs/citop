# citop

Interactive CI pipeline viewer for GitHub Actions and GitLab CI. citop runs in
the terminal and lists your projects with the CI status of their default
branch, along with the pipelines and jobs that are running right now.

- The status of each project's default branch, and pipelines running on merge
  requests, pull requests, tags and other branches.
- Pipelines and their jobs, and opening any of them in the browser.
- Projects chosen by glob patterns (`my-group/**`, `someone/project`).
- **Read-only.** citop only ever sends GET requests (and GraphQL queries that
  are checked to not be mutations). It cannot retry, cancel or change anything.

## Install

```sh
cargo install citop
```

## Configure

Create `~/.config/citop/config.toml` (or pass `--config`, or set `$CITOP_CONFIG`):

```toml
[[source]]
name = "gitlab"
type = "gitlab"
token_env = "GITLAB_TOKEN"        # or token_cmd = "pass show gitlab", or token = "..."
include = ["my-group/**"]

[[source]]
name = "github"
type = "github"
token_env = "GITHUB_TOKEN"
include = ["my-org/*", "someone/project"]
exclude = ["my-org/legacy-*"]
```

Use read-only tokens: the `read_api` scope on GitLab, and a fine-grained PAT
with read access to "Actions" and "Metadata" on GitHub. Self-hosted GitLab and
GitHub Enterprise work by setting `url`. See
[`config.example.toml`](config.example.toml) for all options.

`citop --check` validates the config without contacting any server.

## Use

| Key              | Action                               |
|------------------|--------------------------------------|
| `↑` `↓` / `j` `k` | Move                                |
| `enter`          | Show pipelines and jobs              |
| `tab`            | Switch between pipelines and projects |
| `esc`            | Back                                 |
| `/`              | Filter projects                      |
| `s`              | Change the sort order (age, status, name, source) |
| `o`              | Open the project or pipeline in the browser |
| `r`              | Refresh now                          |
| `q`              | Quit                                 |

## How it works

State is kept in a SQLite database in your cache directory
(`~/.cache/citop/state.db`), so citop shows the last known state immediately
on start. Several citop instances share it and split the polling work between
them instead of all making the same requests. Unchanged data is fetched with
conditional requests, which do not count against GitHub's rate limit, and
polling slows down when the rate limit runs low.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT), at your option.

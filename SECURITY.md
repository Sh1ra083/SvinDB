# Security policy

SvinDB is a very early prototype and is not meant for production use or for
exposure to untrusted networks. No version is currently supported with
security fixes; fixes land on the main branch.

## Reporting a vulnerability

Please **do not open a public issue** for a security problem.

Use GitHub's private reporting instead: open the repository's **Security** tab
and choose **Report a vulnerability**. Include the build or commit you tested,
the command line and configuration, and the smallest request or file that
reproduces the problem.

Reports are handled on a best-effort basis. There is no bug bounty.

## What is in scope

- authentication, API keys and role checks
- memory safety and crashes reachable through the HTTP API or through index
  files
- path handling in snapshots, restore and the offline tools
- data loss or silent corruption after an acknowledged write

Known gaps that do not need a report are listed under *Security* and *Known
limitations* in the [README](README.md): there is no per-index permission
model, and traffic between nodes is neither encrypted nor authenticated.

# Contributing to SvinDB

**English** · [Русский](CONTRIBUTING.ru.md)

Thank you for wanting to help. SvinDB is an early prototype, which is exactly
the stage where outside eyes are most valuable. You do not need to write Rust
to contribute.

## Ways to help

- **Test it** on your own data and tell us what happened: what worked, what was
  slow, what broke. Honest "it did not work for me" reports are very welcome.
- **Report a bug** or a missing feature as a GitHub issue.
- **Fix or translate documentation.** The reference docs in `docs/` are
  currently in Russian; English translations are a great first contribution.
- **Add a test** for a case that is not covered yet.
- **Pick an item from *Known limitations*** in the [README](README.md) and
  work on it. Open an issue first for anything big, so we can agree on the
  approach before you spend a week on it.

## Reporting a bug

Open an issue and include:

- the version or commit (`svindb --version`, or `git rev-parse HEAD`);
- your operating system;
- the command you ran and the configuration you used;
- the smallest request, document or file that reproduces the problem;
- what you expected and what happened instead.

Please do not paste real customer data or credentials. Security problems go
through the private channel described in [SECURITY.md](SECURITY.md), not a
public issue.

## Sending a change (pull request)

A *pull request* is a proposal to merge your change into the project. The
maintainer reviews it, may ask for adjustments, and then merges it. The steps:

1. **Fork** the repository: press **Fork** at the top right of the GitHub page.
   This creates your own copy.
2. **Clone** your copy and make a branch for your change:

   ```bash
   git clone https://github.com/<your-name>/SvinDB.git
   cd SvinDB
   git switch -c short-name-of-your-change
   ```

3. **Make the change.** Keep it focused: one pull request, one purpose.
4. **Check it** before you send it:

   ```bash
   cargo fmt --all
   cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
   cargo test --workspace --locked --all-features -- --test-threads=1
   ```

   The full test run can take a long time on a laptop. For a small change it is
   fine to run only the tests next to your change and let the automatic checks
   on GitHub run the rest.
5. **Commit and push:**

   ```bash
   git add -A
   git commit -m "Say in one line what the change does"
   git push -u origin short-name-of-your-change
   ```

6. **Open the pull request:** GitHub shows a **Compare & pull request** button
   on your fork right after the push. Describe what you changed and why, and
   how you checked it.

If something in this list is unclear, open an issue or a draft pull request and
ask. Nobody is expected to know git perfectly.

## What a good change looks like

- It does one thing and explains why.
- It comes with a test when it changes behaviour.
- Anything SvinDB does not support stays an **explicit error**. A query or
  setting that is not understood must never be quietly turned into a weaker one.
- It does not contain secrets, real personal data or machine-specific paths.
- Numbers in claims about speed or size come from a command someone else can
  repeat (see [`tools/bench_vs_es.py`](tools/bench_vs_es.py)).

## License

By contributing you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE-APACHE-2.0), the same as the rest of the project.

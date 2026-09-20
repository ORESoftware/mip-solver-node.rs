# Global Codex Agent Instructions

## Command Whitelist (Safe - Always Allowed)

The following commands are safe to use without restriction:
- `find` - locate files and directories
- `grep` / `git grep` - search file contents
- `curl` - make HTTP requests
- `read` - read input / variables
- `cat` - display file contents
- `bash` - run shell scripts (but NEVER use `bash` to invoke blacklisted commands below)
- `sh` - run shell scripts (but NEVER use `sh` to invoke blacklisted commands below)

## Command Blacklist (NEVER Use)

The following commands are permanently banned:
- `rm` - use `git rm` instead for tracked files
- `sed` - causes codebase corruption; use targeted in-place edits
- `mv` - use `git mv` instead for tracked files

These bans also apply when invoked via bash or sh:
- `bash -c 'rm ...'` - BANNED
- `bash -c 'sed ...'` - BANNED
- `bash -c 'mv ...'` - BANNED
- `sh -c 'rm ...'` - BANNED
- `sh -c 'sed ...'` - BANNED
- `sh -c 'mv ...'` - BANNED

## Single-File Edit Policy

1. Never batch edits across multiple files in one operation.
2. Always use `grep` or `git grep` to locate target lines before editing.
3. Edit files in place one file at a time, then validate before moving to the next file.
4. Never use the `sed` CLI command.

## Task Completion Git Policy

When done with a task, add all changes, commit them, and push them to the configured remote. If the remote has new commits, pull them in, resolve any conflicts semantically, commit the resolution if needed, and re-push.

## Syncing with the remote

"Sync with the remote" (or just "sync") is a **two-way** exchange — pull the
remote's commits down **and** push yours up. It is never push-only, and a clean
local tree does not by itself mean "synced": you are done only once local and
the remote hold the same commits.

To sync:

1. **Commit your work first** (`git add` + `git commit`) so the tree is clean —
   pull/merge only into a clean tree. `git pull` / `git merge` aborts when an
   incoming change touches a file you have edited, and even when it doesn't it
   buries the merge in your uncommitted work. (Can't commit yet? `git stash`,
   then `git stash pop` after step 3.)
2. `git fetch --all --prune` — safe any time; it only updates tracking refs.
3. `git pull` (fetch + merge) — or `git merge` the upstream branch — to
   integrate the remote's commits.
4. `git push` to publish yours.

Integrate with **`git merge` / `git pull`**. **Never `git rebase` to sync** — it
rewrites history and breaks shared branches.

<!-- BEGIN ores-agents-pointer: managed by ORESoftware/my-ai; edit there, not here -->

## Canonical agent instructions

Before doing anything else in this repository, also read:

    .ores/agents/AGENTS.md

That path is a symlink to `~/codes/oresoftware/my-ai/AGENTS.md`, whose canonical copy is
<https://github.com/ORESoftware/my-ai/blob/main/AGENTS.md>.

It exists at a fixed path *inside* the repository because some agents cannot walk up past
the repository root, so machine-wide instructions one or more directories above are
invisible to them. This pointer plus that path make the same file reachable from a working
directory anywhere in the tree.

The symlink is deliberately **not committed**: it names an absolute path that is only valid
on a machine with `~/codes/oresoftware/my-ai` checked out, so committing it would produce a
broken link for everyone else and for CI. `.ores/` is git-ignored for that reason. If
`.ores/agents/AGENTS.md` is missing on your machine, create it with:

    mkdir -p .ores/agents
    ln -sfn "$HOME/codes/oresoftware/my-ai/AGENTS.md" .ores/agents/AGENTS.md

or run `~/codes/oresoftware/my-ai/scripts/link-repo-agents.sh` once to do it for every git
repository under `~/codes`, and `--check` to verify them.

A missing `.ores/agents/AGENTS.md` is a setup gap on the reader's machine, never a reason to
skip the canonical instructions: fetch them from the URL above instead.

<!-- END ores-agents-pointer -->

---
name: commit
description: Create focused local Git commits for completed, verified work in this repository. Use when the user asks to commit or when a completed change is ready under the repository auto-commit policy; do not use for push, merge, rebase, tags, or releases.
---

# Commit completed work

Read [`../../../docs/commit-conventions.md`](../../../docs/commit-conventions.md) before
creating a commit. Treat it as the source of truth for message format and commit boundaries.

The user has authorized local commits for completed repository work. This authorization does
not include push, merge, rebase, amend, tag creation, or publishing.

1. Inspect `git status --short` and the relevant staged and unstaged diffs. Separate changes
   created for the current task from pre-existing or unrelated work.
2. Run validation appropriate to the change. Do not commit a known failing implementation
   unless the user explicitly requests a WIP commit.
3. Stage only explicit files or hunks for one coherent change. Never use `git add .` or
   `git add -A`.
4. Review `git diff --cached --check`, `git diff --cached`, and the resulting commit message
   before committing. Do not include secrets, local data, caches, or unrelated edits.
5. Create the local commit using the repository convention. Split multiple purposes when each
   commit can remain reviewable and valid on its own.
6. Report the commit hash and subject, validation performed, and any changes intentionally left
   uncommitted.

If current-task changes cannot be isolated safely from existing edits, leave them uncommitted
and explain the overlap instead of guessing ownership.

---
name: fix-pr-comments
description: Fetch PR review comments from GitHub, apply code fixes based on reviewer feedback, and produce a concise summary of all changes made. Use when the user asks to fix PR comments, address review feedback, resolve PR review issues, or mentions a GitHub PR number/URL that needs to be addressed.
---

# Fix PR Comments

Fetch GitHub PR review comments, apply code fixes, and summarize all changes.

## Workflow

### Step 1: Resolve PR info

Accept either a full GitHub PR URL or a PR number.

- Full URL: `https://github.com/{owner}/{repo}/pull/{number}` → extract owner, repo, number
- Number only: infer owner/repo from `git remote get-url origin`

```bash
git remote get-url origin
# e.g. git@github.com:apache/fluss.git  →  owner=apache, repo=fluss
```

### Step 2: Fetch PR comments

Fetch all review comments (inline code comments):

```bash
# Review comments (inline, on specific lines)
curl -s -H "Accept: application/vnd.github+json" \
  [-H "Authorization: Bearer $GITHUB_TOKEN"] \
  "https://api.github.com/repos/{owner}/{repo}/pulls/{number}/comments"

# General issue comments (top-level PR discussion)
curl -s -H "Accept: application/vnd.github+json" \
  [-H "Authorization: Bearer $GITHUB_TOKEN"] \
  "https://api.github.com/repos/{owner}/{repo}/issues/{number}/comments"
```

If `$GITHUB_TOKEN` is unset, try without auth first (works for public repos). If rate-limited or 401, prompt the user to set `GITHUB_TOKEN`.

### Step 3: Parse and triage comments

From the response JSON, extract:

| Field | Source |
|-------|--------|
| File path | `pull_request_review_comment.path` |
| Line number | `pull_request_review_comment.line` (or `original_line`) |
| Reviewer | `user.login` |
| Comment body | `body` |
| Resolved | `pull_request_review_comment.resolved` |

Filter to **unresolved, actionable** comments. Skip bot comments (e.g., `checkstyle-github-action`) unless they contain specific fix instructions.

Group comments by file for efficient editing.

### Step 4: Apply fixes

For each unresolved comment:

1. Read the referenced file and surrounding context (± 20 lines around the indicated line)
2. Understand the reviewer's intent:
   - **Style/formatting** → apply directly
   - **Logic change** → reason through the code then apply
   - **Question/clarification** → add a code comment or rename for clarity; note in summary
   - **Ambiguous** → apply the most reasonable interpretation and flag in summary
3. Apply the fix using file editing tools
4. Move on to the next comment

**Priority order**: resolve comments in file order, then by line number.

### Step 5: Summarize changes

After all fixes are applied, output a structured summary:

```
## PR #<number> — Fix Summary

### Changes Applied

| File | Line | Reviewer | Comment (brief) | Fix Applied |
|------|------|----------|-----------------|-------------|
| path/to/file.java | 42 | reviewer | "rename variable x" | Renamed `x` to `count` |
| path/to/other.java | 87 | reviewer | "extract method" | Extracted `processRecord()` |

### Notes

- <any ambiguous comments and what interpretation was chosen>
- <any comments skipped and why>

### Files Modified

- `path/to/file.java`
- `path/to/other.java`
```

## Rules

- **Do not commit or push** — only apply edits, leave git state to the user
- **One comment, one fix** — don't bundle unrelated changes
- **Preserve existing style** — match indentation, brace style, import order of the file
- **If a comment is already addressed** in the current code, mark it as "already resolved" in the summary, skip editing
- **If a file cannot be found** (renamed/moved), note it in the summary and skip

## Example Invocation

```
/fix-pr-comments 1234
/fix-pr-comments https://github.com/apache/fluss/pull/1234
```

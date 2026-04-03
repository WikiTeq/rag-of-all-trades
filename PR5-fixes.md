# PR #5 Fixes Plan

Based on the 13 items from the Jira comment (https://wikiteq.atlassian.net/browse/MAIT-41?focusedCommentId=118492).

---

## Commit 1 — Rewrite connector `__init__` to match actual `MediaWikiReader` API + restore `_metadata_cache`

The reader's constructor changed — it no longer accepts `api_url`, `request_delay`,
`user_agent`, `batch_size`, `max_retries`, `timeout`. It now takes `host`, `path`,
`scheme`, `page_limit`, `namespaces`, `filter_redirects`.

- [ ] Replace `api_url` config key with `host`, `path`, `scheme` in `tasks/mediawiki_ingestion.py`
- [ ] Remove `_check_positive` and `_check_positive_int` helpers (AI overengineering)
- [ ] Remove `request_delay` from connector `__init__` (handled by separate PR #39)
- [ ] Update `MediaWikiReader(...)` call to pass correct args
- [ ] Restore `_metadata_cache` in `tasks/helper_classes/ingestion_item.py` — used by `jira_ingestion.py` and `web_ingestion.py` on `main`; keep the new `url` field alongside it
- [ ] Rewrite the `mediawiki` block in `config.yaml.example` to use `host`/`path`/`scheme` keys instead of `api_url` (supersedes the env rename in item #3)

**Links:**
- [#7 — constructor args don't match reader](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r3025070190)
- [#9 — remove request_delay from connector](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r3024863623)
- [#2 — request_delay default should be 0, not 0.1](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r2823672656)
- [r2796363860 — _check_positive overengineering](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r2796363860)
- [#4 — _metadata_cache removed but used by other connectors](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r2823469462)

---

## Commit 2 — Fix `get_raw_content` (remove `load_resource`, use `_page_to_document`)

`load_resource` has been removed from the reader. Switch to `_page_to_document`.
Also pass `pageid` and `namespace` through `IngestionItem` (or store on item) so
`get_extra_metadata` can include them.

- [ ] Replace `self._reader.load_resource(...)` with `self._reader._page_to_document(title, url, last_modified, pageid, namespace)` in `get_raw_content`
- [ ] Pass `pageid` and `namespace` from `list_items()` into `IngestionItem` (store as extra attrs or use existing fields)
- [ ] Add `namespace` and `page_id` to the dict returned by `get_extra_metadata`

**Links:**
- [#11 — load_resource has been removed](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r3025097735)
- [#12 — add namespace and page_id to metadata](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r3025115770)

---

## Commit 3 — `base.py`: move `RESERVED_METADATA_KEYS` to class constant + allow format override

- [ ] Move `RESERVED_METADATA_KEYS` from module level into `IngestionJob` as a class constant
- [ ] Update all references in `process_item` and docstrings to `self.RESERVED_METADATA_KEYS`
- [ ] Add a `content_format` property to `IngestionJob` that returns `"markdown"` by default
- [ ] Replace the hardcoded `"format": "markdown"` in `process_item` with `"format": self.content_format`

**Links:**
- [#5 — make RESERVED_METADATA_KEYS a class constant](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r2823473262)
- [#8 — format is hardcoded; allow connectors to override](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r3024846321)

---

## Commit 4 — Rename `get_document_metadata` → `get_extra_metadata` in other connectors

Other connectors on `main` (e.g. `jira_ingestion.py`) still use the old method name
`get_document_metadata`. Update them to `get_extra_metadata` so they work with the
updated base class.

- [ ] Rename `get_document_metadata` → `get_extra_metadata` in `tasks/jira_ingestion.py` (and any other connector on main that uses the old name)

**Links:**
- [#10 — other connectors depend on the old method name](https://github.com/WikiTeq/rag-of-all-trades/pull/5#pullrequestreview-4047524630)
- [#7b — jira_ingestion.py#L199 referenced](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r3024839641)

---

## Commit 5 — Update tests to match all above changes

- [ ] Fix stale comment `# get_document_metadata` → `# get_extra_metadata` in test class header
- [ ] Update `_make_job` helper to use new reader constructor args (`host` etc.)
- [ ] Update `list_items` assertions to include `pageid` and `namespace` fields
- [ ] Update `get_raw_content` test to mock `_page_to_document` instead of `load_resource`
- [ ] Update `get_extra_metadata` assertions to include `namespace` and `page_id`

**Links:**
- [#6 — test method name stale](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r3024824749)

---

## Step 6 — Rebase onto `main` and resolve conflicts

Do this **after** all fix commits are done.

```bash
git fetch origin
git rebase origin/main
# resolve conflicts, git add, git rebase --continue
```

**Why last:** Rebasing before fixes means resolving conflicts twice if the same
lines are touched. Fix first, then rebase once cleanly.

**Link:** Item #13 from Jira comment — "Please also rebase onto master and resolve the conflicts"

---

## Noted / no action needed

- [r2796348778] — filesystem-safe names suggestion: not in the 12 open items, dismissed
- [r2823451581] — pytest migration should be separate PR: already merged, can't undo
- [r2823457494], [r2823464202] — `celery_app.py` items: "can stay" / dismissed by reviewer
- [#3 — r2823465782](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r2823465782) — env renames in `config.yaml.example`: no standalone action needed — the entire mediawiki block will be rewritten in Commit 1 to use `host`/`path`/`scheme`, making the rename question moot
- [#4 — r2823469462](https://github.com/WikiTeq/rag-of-all-trades/pull/5#discussion_r2823469462) — moved to Commit 1: `_metadata_cache` confirmed used by `jira_ingestion.py` and `web_ingestion.py` on `main`; must be restored alongside the new `url` field
- [r2823663637], [r2823667135], [r2823679865], [r2890643277] — `_check_positive*` complexity / pydantic: covered by Commit 1
- [r2890694621], [r2965482684] — move `pytest` to `requirements-dev.txt`: not in the 12 open items
- [r2890841607], [r2890934700], [r2965480214] — `filtered_extra` pattern: already implemented in current `base.py`
- [r3024788820], [r3024789622], [r3024796047] — dismissed by reviewer for later

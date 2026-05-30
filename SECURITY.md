# SECURITY.md — Phase 03-01: Contact Resync + Advert Reflection

**Audit date:** 2026-05-29
**Phase:** 03-01 — Contact Resync + Advert Reflection
**ASVS Level:** 1
**Auditor:** gsd-security-auditor (claude-sonnet-4-6)
**Disposition:** SECURED — 3/3 threats closed

---

## Threat Verification

| Threat ID | Category | Disposition | Status | Evidence |
|-----------|----------|-------------|--------|----------|
| T-03-01 | Denial of Service | mitigate | CLOSED | `meshcore_handler.py:175` — `async with self._path_fetch_sem:` wraps every resync task; `asyncio.create_task` at line 184 never awaited before `_load_channels()` at line 186. Semaphore(2) initialized at line 51. |
| T-03-02 | Information Disclosure | accept (structurally prevented) | CLOSED | `_maybe_autoshare` called only at `meshcore_handler.py:471` inside `_handle_advertisement`. `_fetch_path_and_announce` (line 507) contains no call to `_maybe_autoshare`. `_resync_fetch` (line 174) calls only `_fetch_path_and_announce(pk, c, silent=True)`. Upload path is structurally unreachable from the resync code path. |
| T-03-03 | Tampering | n/a | CLOSED | Both phase commits (4c166f7, c2c20dd) touch only `meshcore_handler.py`. No changes to `requirements.txt`. No new imports beyond those already present at module load. |

---

## Verification Detail

### T-03-01 — DoS: Semaphore-bounded burst

Declared mitigation: each resync fetch wrapped in `async with self._path_fetch_sem` (Semaphore(2)); tasks dispatched via `create_task` without inline await.

Verified:
- `self._path_fetch_sem = asyncio.Semaphore(2)` at line 51 (Phase-1 infra, unchanged).
- Inner coroutine `_resync_fetch` at line 174 acquires the semaphore (`async with self._path_fetch_sem:` at line 175) then calls `_fetch_path_and_announce`. This is mandatory because `_fetch_path_and_announce` does not acquire the semaphore itself (only `_handle_advertisement` does at line 451).
- Dispatch at line 184: `asyncio.create_task(_resync_fetch(pk, c))` — no `await` on the task handle.
- `await self._load_channels()` at line 186 follows the dispatch loop immediately, confirming the connect sequence is not blocked by resync tasks.
- Contact dict `c` captured synchronously at `create_task` time (line 179: `c = self.bridge.contacts.get(pk)`), passed as argument — not looked up lazily inside the coroutine (ADVT-02 pattern satisfied).

Conclusion: CLOSED.

### T-03-02 — Information Disclosure: Stale GPS upload to map.meshcore.io

Declared disposition: accept (structurally prevented). No plumbing added; upload path unreachable from resync path.

Verified call graph:
- Resync path: `_resync_fetch` (line 174) → `_fetch_path_and_announce(..., silent=True)` (line 507).
- `_fetch_path_and_announce` body (lines 508–566): performs path fetch, cache update, internal dedup; terminates at `if should_announce and not silent: self._announce_advert(contact)` (line 565). No call to `_maybe_autoshare` exists anywhere in this function.
- `_maybe_autoshare` (line 475) has exactly one call site: `meshcore_handler.py:471`, inside `_handle_advertisement`, guarded by `if not is_backlog:`.
- The `silent=True` kwarg suppresses `_announce_advert` but is orthogonal to the autoshare isolation — autoshare is simply not reachable from the resync code path at all.

Conclusion: CLOSED. The structural isolation claim is verified.

### T-03-03 — Tampering: No new dependencies

Declared disposition: n/a — no package installs this phase.

Verified:
- `git show --name-only 4c166f7 c2c20dd` lists only `meshcore_handler.py` for both commits.
- `requirements.txt` has no commits touching it in this phase (git log shows no output).
- Module-level imports in `meshcore_handler.py` (lines 1–12): `asyncio`, `logging`, `re`, `time`, `collections.OrderedDict`, `datetime`, `meshcore`, `map_uploader`, `bridge` — all pre-existing. No new import added.

Conclusion: CLOSED.

---

## Unregistered Threat Flags

SUMMARY.md `## Threat Flags` section reports no new external input surface and maps all three threats to existing register IDs (T-03-01, T-03-02, T-03-03). No unregistered flags.

---

## Accepted Risks Log

None. T-03-02 is documented as "structurally prevented" in the threat register, not a residual risk requiring operator acceptance. The upload path is absent from the resync code path by construction, verified above.

---

## Notes

- The `silent: bool = False` kwarg on `_fetch_path_and_announce` (line 507) preserves backward compatibility: existing call sites at `_handle_advertisement` (line 468) and `_on_new_contact` (line 651) pass no `silent` argument and continue to behave as before.
- The before-snapshot is captured before `await mc.ensure_contacts()` (line 139) and before `self.bridge.contacts.update(mc.contacts)` (line 140), satisfying the ordering invariant.
- Cold-start fallback uses `node_cache.all_items()` with timestamp `0` (lines 132–135), preventing spurious "updated" detections across the incompatible `last_seen` vs `last_advert` clock domains.
- Node_cache None guard is present: `elif self.bridge.node_cache is not None:` (line 131).
- All new logger calls use `%`-style positional args (lines 165–166, 183) — no f-strings in logger calls.

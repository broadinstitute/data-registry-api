# kp4cd.org CMS fixture provenance

Captured 2026-08-07 from the live kp4cd.org Drupal 10 REST views, using:

```
UA="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
curl -sf -A "$UA" "<url>" -o <file>
```

All URLs below are exactly as fetched (copy/paste reproducible). Every committed file
was validated to parse as JSON and be a non-empty list:

```
for f in *.json; do python3 -c "
import json,sys
d=json.load(open('$f'))
assert isinstance(d,list) and d, '$f empty or not a list'
print('$f', len(d), 'rows')"; done
```

## Captured fixtures

| File | Source URL | Rows |
|---|---|---|
| `news2vueportal_md.json` | `https://kp4cd.org/rest/views/news2vueportal?portal=md` | 10 |
| `portal_front_md.json` | `https://kp4cd.org/reset/views/portal_front?portal=md` | 1 |
| `a2f_community_kps.json` | `https://kp4cd.org/rest/views/a2f_community_kps` | 20 |
| `help_book.json` | `https://kp4cd.org/rest/views/help_book` | 34 |
| `newfeatures_md.json` | `https://kp4cd.org/rest/views/newfeatures?portal=md` | 163 |
| `eglmethodsperportal_md.json` | `https://kp4cd.org/rest/views/eglmethodsperportal?portal=md` | 27 |
| `content_by_id_sample.json` | `https://kp4cd.org/rest/views/content_by_id?nid=1770` | 1 |
| `static_content_apis.json` | `https://kp4cd.org/rest/views/static_content?field_page=apis` | 1 |
| `datasetinfo_sample.json` | `https://kp4cd.org/rest/views/datasetinfo?datasetid=Small2025_AorticStenosis` | 1 |
| `paperheadermenu_sample.json` | `https://kp4cd.org/rest/views/paperheadermenu?paper=apol1_portal` | 1 |
| `eglmethod_sample.json` | `https://kp4cd.org/rest/views/eglmethod?from=cardiogram` | 1 |

Notes on the `/reset/views/portal_front` path: this is not a typo. `/rest/views/portal_front`
returns a Drupal 404 page; `/reset/views/portal_front` returns the real JSON payload. Confirmed
by diffing both responses and by finding the exact same `reset/views/portal_front` string
(paired with a `rest/views/portal_front` fallback) in the production frontend bundle
`https://hugeamp.org/js/chunk-common.ecc40e98.js`. Used verbatim per the task brief.

### How the keyed values were chosen

- `nid=1770`: taken from `news2vueportal_md.json[0].nid` (per brief instructions).
- `field_page=apis`: brief-suggested candidate; verified it returns 1 row (not empty).
- `datasetid=Small2025_AorticStenosis`: `kpdatasets` (the view the brief expected to supply
  dataset ids — see Skips below) is unavailable, so ids were instead discovered by probing
  `https://kp4cd.org/rest/views/datasetinfo?portal=md` with no `datasetid` filter, which
  returns full `kp_dataset` node objects for portal `md`, including a `field_dataset_id`
  value per row (e.g. `Small2025_AorticStenosis`, `Salo2024_LumDiscHer_EU`,
  `Weng2024_Brady_Mixed`, ...). The first id was used to capture `datasetinfo_sample.json`
  with the real keyed filter.
- `paper=apol1_portal`: found as a real, currently-linked value on kp4cd.org
  (`https://hugeampkpn.org/paper.html?paper=apol1_portal`, linked from the kp4cd.org
  homepage). Verified `paperheadermenu?paper=apol1_portal` returns 1 row.
- `from=cardiogram`: `eglmethod`'s `from` parameter is populated from a page's `dataset`
  query parameter in the production Vue frontend
  (`https://hugeamp.org/js/eglmethod.46e96fa6.js`:
  `this.$store.dispatch("kp4cd/getResearchMethod", _e["a"].dataset)`), confirmed against a
  real linked page `https://hugeamp.org/method.html?trait=cad&dataset=cardiogram`
  (linked from `https://kp4cd.org/research_portals`). Verified
  `eglmethod?from=cardiogram` returns 1 row. Two other real `dataset` values probed the
  same way also returned rows (`loci_clustering` -> 3 rows, `apol1_diff_exp` -> 1 row),
  confirming the view and parameter are live and working; `cardiogram` was kept as the
  committed sample.

## Skipped views (not committed) and evidence

### `kpdatasets_md.json` — SKIPPED (view unreachable, 404 on every attempt)

The brief's exact command,
`curl -sf -A "$UA" "https://kp4cd.org/rest/views/kpdatasets?portal=md" -o kpdatasets_md.json`,
returns Drupal's own 404 page (`X-Drupal-Cache: HIT`, title "Page not found | Knowledge
Portal Network"), not an empty JSON array — `curl -f` correctly refused to write the file.

Evidence gathered before giving up:
- Retried 3x (not transient): all `404`.
- Tried the `/reset/views/` prefix used successfully for `portal_front`: `404`.
- Tried ~25 plausible alternate view-name spellings/casings (`kp_datasets`, `kpdataset`,
  `kp-datasets`, `datasets`, `dataset`, `KPDatasets`, `kp_dataset`, `dataset_info`,
  `datasetsperportal`, etc.) against both `/rest/views/` and `/reset/views/`: all `404`.
- Tried `_format=json` and `.json` suffix variants: all `404`.
- Extracted the real production call from `https://hugeamp.org/js/chunk-common.ecc40e98.js`
  (the compiled Vue frontend that actually drives kp4cd content, e.g. `hugeamp.org` = the
  `md` portal). The `getDatasetsInfo` action does:
  `r = ("md" == e) ? "" : e` then calls
  `https://hugeampkpncms.org/rest/views/kpdatasets?portal=` + r with a fallback to
  `https://kp4cd.org/rest/views/kpdatasets?portal=` + r (proxy-on-miss pattern, primary
  host `hugeampkpncms.org` with `kp4cd.org` as fallback). This means for portal `md` the
  real call sends an *empty* `portal=` value, not `portal=md`. Tried that exact call
  (`portal=`) against both `hugeampkpncms.org` and `kp4cd.org`: all `404`.
- Confirmed `hugeampkpncms.org` itself is reachable (its homepage returns `200`), but its
  `/rest/views/news2vueportal?portal=md` (a view proven to work on `kp4cd.org`) also `404`s
  there, i.e. `hugeampkpncms.org` is not currently serving these REST views at all —
  independent of the `kpdatasets` question.
- `getDatasetsInfo`/`getDatasetsInfo` is defined once in `chunk-common.ecc40e98.js` but no
  dispatch call site was found in the page bundles inspected (`eglmethod.46e96fa6.js`,
  `index.e8368479.js`) — consistent with this being dead/unused code in production, not a
  transient outage.

Conclusion: the `kpdatasets` view is not currently reachable on kp4cd.org or its documented
fallback host, under the parameters the production frontend itself sends, or any reasonable
variant. This is a genuine production gap, not a probing miss. No file was committed for
`kpdatasets_md.json`.

**Downstream impact**: real `kp_dataset` ids for test fixtures were still obtainable via
`datasetinfo?portal=md` (see "How the keyed values were chosen" above) and that same
approach remains available to Tasks 5/6 if a dataset-listing-by-portal fixture is needed —
it was not captured as a separate committed fixture here because renaming it to
`kpdatasets_md.json` would misrepresent its source view/URL.

### `newresources_md.json` — SKIPPED (view works, but returns 0 rows for every portal)

`https://kp4cd.org/rest/views/newresources?portal=md` returns `200` with body `[]` (2
bytes) — a genuinely empty result, not an error. Probed all 12 known portal codes
(`md, cvd, cd, sleep, t1d, t2d, lung, nage, msk, a2f, kidney, aging` — taken from the
`field_portals` value in `news2vueportal_md.json`): every portal currently returns 0 rows.
This is real production state (no "new resources" content exists site-wide right now), not
a routing problem. Per the task's non-empty-array constraint, no file was committed for
`newresources_md.json`.

## Portal codes observed (for reference)

From `news2vueportal_md.json[0].field_portals`:
`md, cvd, cd, sleep, t1d, t2d, lung, nage, msk, a2f, kidney, aging`

# CalR Server-Side Data Enrichment

**Date:** 2026-04-09
**Status:** Approved

## Problem

The client-side Vue app (`calr-vue`) contains a data enrichment pipeline (`processDetail` in `process.js`) that runs after loading a converted CalR CSV. This pipeline adds derived columns (light/dark flags, cycle day, energy balance, accumulated energy, group metadata, kcal conversion, etc.) that are required for all analysis and plotting.

The same enrichment logic is partially and inconsistently duplicated in the server-side analysis endpoints (`run_ancova`, `run_quality_control`, `run_power_calc`). This creates a maintenance burden and a risk of divergence between what the client computes and what the server computes.

## Goal

- Move the enrichment pipeline fully to the server
- Create a single Python helper `_enrich_df(df, session)` as the canonical implementation
- Have all analysis endpoints call it instead of their ad-hoc enrichment code
- Expose a new API endpoint so the Vue client can fetch the enriched file and delete its local implementation

## Architecture

```
_enrich_df(df: DataFrame, session: dict) → DataFrame
    ↓ called by
run_ancova               (replaces inline enrichment)
run_power_calc           (replaces inline enrichment)
run_quality_control      (replaces inline enrichment; QC-specific zero-basing stays after)
GET /calr/sessions/{session_id}/enriched   (new endpoint, streams full enriched CSV)
```

## `_enrich_df(df, session)` — Enrichment Steps

Mirrors the `processDetail` pipeline from `calr-vue/src/utils/process.js`, applied in this order:

### 1. Numeric parsing
- Cast `exp.minute` to float (NaN → None)
- Derive `hour` and `exp.hour` as `exp.minute / 60`

### 2. `enviro.light` inference
- If all rows have blank `enviro.light`, infer it from the `Date.Time` or `Time.Date` timestamp column using the session's `light_cycle_start` / `dark_cycle_start` hours (default 7 / 19)
- Light = 5, dark = 0 (matching JS defaults)

### 3. Derived time columns
- `light` — 1 if `enviro.light > 1`, else 0; falls back to clock hour vs. cycle if `enviro.light` is null
- `dark` — inverse of `light`
- `clockHour` — `(exp.minute / 60) % 24`
- `day` / `exp.day` — `floor((exp.hour - light_cycle_start) / 24)`

### 4. Session subject fallbacks
Per subject (matched by `subject.id`), fill blank values from the session's `subjects` list:
- `subject.mass` ← `total_mass`
- `subject.lean.mass` ← `lean_mass`
- `subject.fat.mass` ← `fat_mass`

### 5. Group metadata
Join session groups/subjects onto each row:
- `group` — group name
- `groupIndex` — integer index into groups list
- `color` — hex color
- `diet` — diet name

### 6. Kcal conversion
For each subject's group, if `diet_kcal` is set:
- `feed` *= `diet_kcal`
- `feed.acc` *= `diet_kcal`

### 7. Accumulator fill
Per subject (sorted by `exp.minute`):
- If `ee.acc` is absent, compute it as cumulative sum of `ee / minute_bin` per row (where `minute_bin` = 60 / modal row spacing in minutes)
- `eb` = `feed - ee` (if both present)
- `eb.acc` = `feed.acc - ee.acc` (if both present)

> **Note:** Zero-basing of accumulators (subtracting the first value within an analysis window) is **not** done here. It remains a QC-specific step in `run_quality_control`, applied after hour-range filtering.

## New API Endpoint

```
GET /calr/sessions/{session_id}/enriched
```

- **Auth:** Optional (same pattern as analysis endpoints — public sessions accessible without token)
- **Loads:** session JSON + standard file from S3 via `_load_session_and_standard_df`
- **Returns:** Streaming CSV response of the full enriched DataFrame (all rows, no filtering)
- **No caching:** Session is mutable (groups, diet_kcal, subjects, exclusions all editable); computing on the fly avoids stale cache invalidation complexity. S3 fetch is the dominant latency, not the enrichment computation.

## Refactored Analysis Endpoints

Each endpoint calls `_enrich_df(df, session)` immediately after `_load_session_and_standard_df`, then proceeds with its existing filtering and analysis logic.

Inline code removed from each endpoint:
- `run_ancova`: group assignment, `eb` computation, caloric conversion, light/dark inference
- `run_quality_control`: group assignment, `ee.acc` cumsum, caloric conversion
- `run_power_calc`: group assignment, any inline enrichment present

The QC-specific zero-basing (`fixFeed` / `setZero`) stays in `run_quality_control` and runs after the hour-range filter is applied to the enriched df.

## Client-Side Cleanup (`calr-vue`)

Once the endpoint is live, the following can be removed from `process.js`:
- `enrichDetailRows`
- `fillAccumulatorColumns`
- `convertFeedColumns`
- `applySessionFieldFallbacks`
- `attachSessionMetadata`
- `ensureEnviroLight` (if only used in enrichment path)

`processDetail` is replaced by a fetch to `GET /calr/sessions/{session_id}/enriched`. The parsed CSV rows from the API become the input to downstream plotting/aggregation code directly.

## What Is NOT Changing

- `ensureExpMinute` in the JS (may still be needed for locally-loaded files before upload)
- `preprocessSession`, `mergeSessionCsvIntoPayload` (session-loading, not enrichment)
- `applyExclusions`, `cropDetailRows` (analysis-specific filtering, stays client-side or per-endpoint)
- `aggregateDetailRows` and all plotting utilities

## Out of Scope

- Persisting the enriched file to S3
- Caching
- Changing the enrichment logic itself (this is a port, not a redesign)

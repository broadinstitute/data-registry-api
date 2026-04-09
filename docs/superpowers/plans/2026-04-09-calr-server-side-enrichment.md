# CalR Server-Side Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the client-side CalR data enrichment pipeline (`processDetail` in `process.js`) to a single Python helper `_enrich_df`, wire it into all analysis endpoints, expose a new enriched-file API endpoint, then delete the duplicate JS implementation.

**Architecture:** `_enrich_df(df, session)` is the single server-side implementation. All three analysis endpoints (`run_ancova`, `run_power_calc`, `run_quality_control`) call it immediately after `_load_session_and_standard_df`. A new `GET /calr/sessions/{session_id}/enriched` endpoint calls it and streams the full result. The Vue client fetches the enriched file from that endpoint instead of computing enrichment locally.

**Tech Stack:** Python / pandas / FastAPI (server); Vue 3 / fetch API (client). Test runner: `pytest` from repo root.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `dataregistry/api/calr.py` | Add `_enrich_df`, new endpoint, refactor 3 analysis endpoints |
| Create | `calr/tests/test_enrich_df.py` | Unit tests for `_enrich_df` |
| Modify | `calr-vue/src/services/registryService.js` | Add `fetchEnrichedData` |
| Modify | `calr-vue/src/views/AnalysisView.vue` | Swap `prepForAnalysis` for API fetch |
| Modify | `calr-vue/src/views/AccountView.vue` | Same swap as AnalysisView |
| Modify | `calr-vue/src/utils/process.js` | Delete enrichment functions |
| Modify | `calr-vue/src/utils/prep-for-analysis.js` | Simplify — remove enrichment call |

---

## Task 1: Implement `_enrich_df` (TDD)

**Files:**
- Create: `calr/tests/test_enrich_df.py`
- Modify: `dataregistry/api/calr.py` (add helper before `_load_session_and_standard_df` at line ~894)

### Step 1: Create the test file

```python
# calr/tests/test_enrich_df.py
"""
Unit tests for _enrich_df() in dataregistry/api/calr.py.

Verifies that each enrichment step matches the JS processDetail pipeline:
  1. exp.hour / hour derived from exp.minute
  2. enviro.light inferred from timestamp when absent
  3. light / dark / clockHour / day / exp.day derived
  4. subject mass fallbacks applied from session subjects
  5. group / color / diet / groupIndex joined from session
  6. feed / feed.acc multiplied by diet_kcal per group
  7. ee.acc filled by per-subject cumulative sum when absent
  8. eb / eb.acc computed from feed - ee / feed.acc - ee.acc
"""

import sys
import pytest
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataregistry.api.calr import _enrich_df


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _session(
    groups=None,
    subjects=None,
    light_cycle_start=7,
    dark_cycle_start=19,
):
    return {
        'groups': groups or [
            {'name': 'GroupA', 'color': '#3B73C7', 'diet_name': 'Diet A', 'diet_kcal': 3.5},
            {'name': 'GroupB', 'color': '#ED5F00', 'diet_name': 'Diet B', 'diet_kcal': None},
        ],
        'subjects': subjects or [
            {'subject': 'A1', 'groupIndex': 0, 'total_mass': 25.0, 'lean_mass': 18.0, 'fat_mass': 7.0},
            {'subject': 'B1', 'groupIndex': 1, 'total_mass': 28.0, 'lean_mass': None, 'fat_mass': None},
        ],
        'light_cycle_start': light_cycle_start,
        'dark_cycle_start': dark_cycle_start,
    }


def _df():
    """Minimal standard-format DataFrame with two subjects, 15-min intervals."""
    return pd.DataFrame({
        'subject.id': ['A1', 'A1', 'A1', 'B1', 'B1', 'B1'],
        'exp.minute': [15.0, 30.0, 45.0, 15.0, 30.0, 45.0],
        'feed':       [0.5,  0.6,  0.4,  0.8,  0.7,  0.9],
        'feed.acc':   [0.5,  1.1,  1.5,  0.8,  1.5,  2.4],
        'ee':         [2.0,  2.1,  2.2,  1.8,  1.9,  2.0],
        'ee.acc':     [2.0,  4.1,  6.3,  1.8,  3.7,  5.7],
        'subject.mass': [25.0, 24.9, 24.8, 28.0, 27.9, 27.8],
    })


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestTimeColumns:
    def test_exp_hour_derived_from_exp_minute(self):
        result = _enrich_df(_df(), _session())
        assert pytest.approx(result['exp.hour'].iloc[0]) == 15.0 / 60
        assert pytest.approx(result['hour'].iloc[0]) == 15.0 / 60

    def test_clock_hour(self):
        result = _enrich_df(_df(), _session())
        # exp.minute=15 → clockHour = (15/60) % 24 = 0.25
        assert pytest.approx(result['clockHour'].iloc[0]) == (15.0 / 60) % 24

    def test_day_derived_from_exp_hour_and_light_cycle(self):
        result = _enrich_df(_df(), _session(light_cycle_start=7))
        # exp.hour=0.25, light_cycle_start=7 → day = floor((0.25-7)/24) = floor(-0.28) = -1
        assert result['day'].iloc[0] == -1
        assert result['exp.day'].iloc[0] == -1


class TestEnviroLight:
    def test_light_flag_from_enviro_light_column(self):
        df = _df()
        df['enviro.light'] = [5.0, 5.0, 0.0, 0.0, 5.0, 0.0]
        result = _enrich_df(df, _session())
        assert result['light'].iloc[0] == 1   # enviro.light=5 > 1 → light
        assert result['light'].iloc[2] == 0   # enviro.light=0 → dark
        assert result['dark'].iloc[0] == 0
        assert result['dark'].iloc[2] == 1

    def test_light_inferred_from_clock_hour_when_enviro_light_absent(self):
        df = _df()
        # exp.minute=15 → clockHour≈0.25, dark (< light_cycle_start=7)
        result = _enrich_df(df, _session(light_cycle_start=7, dark_cycle_start=19))
        assert result['light'].iloc[0] == 0

    def test_enviro_light_inferred_from_timestamp_when_column_missing(self):
        df = _df()
        # Timestamps at 08:00 (light) and 20:00 (dark)
        df['Date.Time'] = [
            '2024-01-01 08:00:00', '2024-01-01 08:15:00', '2024-01-01 08:30:00',
            '2024-01-01 20:00:00', '2024-01-01 20:15:00', '2024-01-01 20:30:00',
        ]
        result = _enrich_df(df, _session(light_cycle_start=7, dark_cycle_start=19))
        assert result['enviro.light'].iloc[0] == 5   # 08:00 → light
        assert result['enviro.light'].iloc[3] == 0   # 20:00 → dark


class TestSubjectMassFallbacks:
    def test_subject_mass_filled_from_session_when_blank(self):
        df = _df()
        df['subject.mass'] = np.nan  # all blank
        result = _enrich_df(df, _session())
        a1_rows = result[result['subject.id'] == 'A1']
        assert (a1_rows['subject.mass'] == 25.0).all()
        b1_rows = result[result['subject.id'] == 'B1']
        assert (b1_rows['subject.mass'] == 28.0).all()

    def test_lean_mass_filled_from_session(self):
        df = _df()
        result = _enrich_df(df, _session())
        a1_rows = result[result['subject.id'] == 'A1']
        assert (a1_rows['subject.lean.mass'] == 18.0).all()

    def test_fat_mass_not_filled_when_session_value_is_none(self):
        df = _df()
        result = _enrich_df(df, _session())
        b1_rows = result[result['subject.id'] == 'B1']
        assert b1_rows['subject.fat.mass'].isna().all()

    def test_existing_mass_value_not_overwritten(self):
        df = _df()  # subject.mass already present
        result = _enrich_df(df, _session())
        # A1 row 0 already has 25.0 — should remain 25.0, not overwritten by session 25.0
        assert result.loc[result['subject.id'] == 'A1', 'subject.mass'].iloc[0] == 25.0


class TestGroupMetadata:
    def test_group_name_assigned(self):
        result = _enrich_df(_df(), _session())
        assert (result[result['subject.id'] == 'A1']['group'] == 'GroupA').all()
        assert (result[result['subject.id'] == 'B1']['group'] == 'GroupB').all()

    def test_group_color_assigned(self):
        result = _enrich_df(_df(), _session())
        assert (result[result['subject.id'] == 'A1']['color'] == '#3B73C7').all()

    def test_diet_assigned(self):
        result = _enrich_df(_df(), _session())
        assert (result[result['subject.id'] == 'A1']['diet'] == 'Diet A').all()

    def test_group_index_assigned(self):
        result = _enrich_df(_df(), _session())
        assert (result[result['subject.id'] == 'A1']['groupIndex'] == 0).all()
        assert (result[result['subject.id'] == 'B1']['groupIndex'] == 1).all()

    def test_unknown_subject_gets_null_group(self):
        df = _df()
        df.loc[0, 'subject.id'] = 'UNKNOWN'
        result = _enrich_df(df, _session())
        assert pd.isna(result.loc[0, 'group'])


class TestKcalConversion:
    def test_feed_multiplied_by_diet_kcal_for_group_with_value(self):
        result = _enrich_df(_df(), _session())
        a1_original_feed = 0.5  # first row
        assert pytest.approx(result[result['subject.id'] == 'A1']['feed'].iloc[0]) == a1_original_feed * 3.5

    def test_feed_acc_multiplied_by_diet_kcal(self):
        result = _enrich_df(_df(), _session())
        assert pytest.approx(result[result['subject.id'] == 'A1']['feed.acc'].iloc[0]) == 0.5 * 3.5

    def test_no_conversion_when_diet_kcal_is_none(self):
        result = _enrich_df(_df(), _session())
        b1_original_feed = 0.8
        assert pytest.approx(result[result['subject.id'] == 'B1']['feed'].iloc[0]) == b1_original_feed


class TestAccumulatorFill:
    def test_ee_acc_preserved_when_already_present(self):
        df = _df()
        result = _enrich_df(df, _session())
        # ee.acc was already in the df — should remain unchanged
        assert pytest.approx(result[result['subject.id'] == 'A1']['ee.acc'].iloc[0]) == 2.0

    def test_ee_acc_filled_when_absent(self):
        df = _df().drop(columns=['ee.acc'])
        result = _enrich_df(df, _session())
        # 15-min intervals → minute_bin = 60/15 = 4
        # First row ee=2.0 → ee.acc = 2.0/4 = 0.5
        assert pytest.approx(result[result['subject.id'] == 'A1']['ee.acc'].iloc[0], abs=1e-6) == 2.0 / 4

    def test_eb_computed_as_feed_minus_ee(self):
        result = _enrich_df(_df(), _session())
        a1 = result[result['subject.id'] == 'A1'].iloc[0]
        assert pytest.approx(a1['eb']) == a1['feed'] - a1['ee']

    def test_eb_acc_computed_as_feed_acc_minus_ee_acc(self):
        result = _enrich_df(_df(), _session())
        a1 = result[result['subject.id'] == 'A1'].iloc[0]
        assert pytest.approx(a1['eb.acc']) == a1['feed.acc'] - a1['ee.acc']
```

- [ ] **Step 2: Run tests to confirm they fail (function not yet defined)**

```bash
cd /home/dhite/code-repos/broad/data-registry-api
python -m pytest calr/tests/test_enrich_df.py -v 2>&1 | head -30
```

Expected: `ImportError` or `AttributeError: module 'dataregistry.api.calr' has no attribute '_enrich_df'`

- [ ] **Step 3: Implement `_enrich_df` in `dataregistry/api/calr.py`**

Add this function immediately before `_load_session_and_standard_df` (before line 894). Add `import math` at the top of the file with the other stdlib imports.

```python
def _enrich_df(df: 'pd.DataFrame', session: dict) -> 'pd.DataFrame':
    """
    Port of the JS processDetail pipeline (calr-vue/src/utils/process.js).

    Adds derived columns to the standard DataFrame using session metadata.
    Steps match the JS order exactly:
      1. Numeric parsing of exp.minute → hour / exp.hour
      2. enviro.light inference from timestamp when all values blank
      3. light / dark / clockHour / day / exp.day
      4. Subject mass fallbacks from session subjects
      5. Group metadata (group, groupIndex, color, diet)
      6. Kcal conversion on feed / feed.acc per group diet_kcal
      7. ee.acc fill (per-subject cumulative sum) when absent
      8. eb = feed - ee; eb.acc = feed.acc - ee.acc

    Does NOT zero-base accumulators — that is QC-specific and stays in
    run_quality_control after the hour-range window is applied.
    """
    import pandas as pd
    import numpy as np

    df = df.copy()

    light_cycle_start = session.get('light_cycle_start', 7)
    dark_cycle_start = session.get('dark_cycle_start', 19)
    groups = session.get('groups', [])
    subjects = session.get('subjects', [])

    # ── 1. Numeric parsing ────────────────────────────────────────────────────
    df['exp.minute'] = pd.to_numeric(df.get('exp.minute'), errors='coerce')
    df['hour'] = df['exp.minute'] / 60
    df['exp.hour'] = df['hour']

    # ── 2. enviro.light inference ─────────────────────────────────────────────
    enviro_col = 'enviro.light'
    if enviro_col not in df.columns:
        df[enviro_col] = np.nan
    df[enviro_col] = pd.to_numeric(df[enviro_col], errors='coerce')

    if df[enviro_col].isna().all():
        time_col = next((c for c in ('Date.Time', 'Time.Date') if c in df.columns), None)
        if time_col:
            ts = pd.to_datetime(df[time_col], errors='coerce')
            h = ts.dt.hour
            df[enviro_col] = np.where(
                (h >= light_cycle_start) & (h < dark_cycle_start), 5.0, 0.0
            )

    # ── 3. Derived time columns ───────────────────────────────────────────────
    clock_hour = (df['exp.minute'] / 60) % 24
    enviro_light = df[enviro_col]
    light_from_enviro = (enviro_light > 1).astype(float)
    light_from_clock = (
        (clock_hour >= light_cycle_start) & (clock_hour < dark_cycle_start)
    ).astype(float)
    df['light'] = np.where(enviro_light.notna(), light_from_enviro, light_from_clock)
    df['dark'] = 1.0 - df['light']
    df['clockHour'] = clock_hour
    df['day'] = np.floor((df['exp.hour'] - light_cycle_start) / 24)
    df['exp.day'] = df['day']

    # ── 4. Subject mass fallbacks ─────────────────────────────────────────────
    subject_map = {str(s['subject']): s for s in subjects}
    for out_col, src_key in [
        ('subject.mass', 'total_mass'),
        ('subject.lean.mass', 'lean_mass'),
        ('subject.fat.mass', 'fat_mass'),
    ]:
        if out_col not in df.columns:
            df[out_col] = np.nan
        df[out_col] = pd.to_numeric(df[out_col], errors='coerce')
        for subj_id, subj in subject_map.items():
            val = subj.get(src_key)
            if val is not None:
                mask = (df['subject.id'].astype(str) == subj_id) & df[out_col].isna()
                df.loc[mask, out_col] = float(val)

    # ── 5. Group metadata ─────────────────────────────────────────────────────
    def _group_attr(idx, attr, default=None):
        try:
            i = int(idx)
            return groups[i].get(attr, default) if 0 <= i < len(groups) else default
        except (TypeError, ValueError, IndexError):
            return default

    subject_to_group_idx = {str(s['subject']): s['groupIndex'] for s in subjects}
    df['groupIndex'] = df['subject.id'].astype(str).map(subject_to_group_idx)
    df['group'] = df['groupIndex'].map(lambda i: _group_attr(i, 'name'))
    df['color'] = df['groupIndex'].map(lambda i: _group_attr(i, 'color', '#888'))
    df['diet'] = df['groupIndex'].map(lambda i: _group_attr(i, 'diet_name'))

    # ── 6. Kcal conversion ────────────────────────────────────────────────────
    for g in groups:
        kcal = g.get('diet_kcal')
        if kcal:
            mask = df['group'] == g['name']
            if 'feed' in df.columns:
                df.loc[mask, 'feed'] = (
                    pd.to_numeric(df.loc[mask, 'feed'], errors='coerce') * kcal
                )
            if 'feed.acc' in df.columns:
                df.loc[mask, 'feed.acc'] = (
                    pd.to_numeric(df.loc[mask, 'feed.acc'], errors='coerce') * kcal
                )

    # ── 7. Accumulator fill ───────────────────────────────────────────────────
    # minute_bin = 60 / modal row-to-row interval in minutes (JS: computeMinuteBin)
    minute_bin = 1.0
    valid_minutes = df['exp.minute'].dropna().sort_values()
    if len(valid_minutes) >= 2:
        diffs = valid_minutes.diff().dropna()
        pos_diffs = diffs[diffs > 0]
        if not pos_diffs.empty:
            modal_diff = float(pos_diffs.mode().iloc[0])
            minute_bin = 60.0 / modal_diff if modal_diff > 0 else 1.0

    # Fill ee.acc per subject when absent or entirely null
    if 'ee.acc' not in df.columns or df['ee.acc'].isna().all():
        if 'ee' in df.columns:
            def _cumsum_ee(grp):
                grp = grp.sort_values('exp.minute').copy()
                ee = pd.to_numeric(grp['ee'], errors='coerce')
                grp['ee.acc'] = (ee / minute_bin).cumsum()
                return grp
            df = df.groupby('subject.id', group_keys=False).apply(_cumsum_ee)

    # ── 8. eb / eb.acc ────────────────────────────────────────────────────────
    feed = pd.to_numeric(df['feed'], errors='coerce') if 'feed' in df.columns else None
    ee = pd.to_numeric(df['ee'], errors='coerce') if 'ee' in df.columns else None
    feed_acc = pd.to_numeric(df['feed.acc'], errors='coerce') if 'feed.acc' in df.columns else None
    ee_acc = pd.to_numeric(df['ee.acc'], errors='coerce') if 'ee.acc' in df.columns else None

    if feed is not None and ee is not None:
        df['eb'] = np.where(feed.notna() & ee.notna(), feed - ee, np.nan)
    if feed_acc is not None and ee_acc is not None:
        df['eb.acc'] = np.where(
            feed_acc.notna() & ee_acc.notna(), feed_acc - ee_acc, np.nan
        )

    return df
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd /home/dhite/code-repos/broad/data-registry-api
python -m pytest calr/tests/test_enrich_df.py -v
```

Expected: all tests PASS. If any fail, debug before continuing.

- [ ] **Step 5: Run full test suite to confirm no regressions**

```bash
python -m pytest calr/tests/ -q
```

Expected: 22 previously passing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add calr/tests/test_enrich_df.py dataregistry/api/calr.py
git commit -m "feat: add _enrich_df helper — port of JS processDetail pipeline"
```

---

## Task 2: Add `GET /calr/sessions/{session_id}/enriched` endpoint

**Files:**
- Modify: `dataregistry/api/calr.py`

- [ ] **Step 1: Add the endpoint after `_load_session_and_standard_df` (after line ~930)**

Find the `@router.post("/calr/analysis/ancova")` decorator (currently around line 933) and insert the following immediately before it:

```python
@router.get("/calr/sessions/{session_id}/enriched")
async def get_enriched_session_data(
    session_id: str,
    user: Optional[User] = Depends(get_calr_user_optional)
):
    """
    Return the full enriched standard file for a session as CSV.

    Applies the _enrich_df pipeline (derived columns, group metadata, kcal
    conversion, accumulator fill) to the raw converted file and streams the
    result. All rows are returned — no hour-range or exclusion filtering.

    Auth: public sessions are accessible without a token; non-public sessions
    require a valid token from the owning user (enforced by
    _load_session_and_standard_df).
    """
    import pandas as pd

    session, df = _load_session_and_standard_df(session_id, user.user_name if user else None)
    enriched = _enrich_df(df, session)

    csv_buffer = io.StringIO()
    enriched.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode('utf-8')

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type='text/csv',
        headers={
            'Content-Disposition': f'attachment; filename="enriched_{session_id}.csv"',
            'Content-Length': str(len(csv_bytes)),
        }
    )
```

- [ ] **Step 2: Write a smoke test for the endpoint**

Add a new test class to `calr/tests/test_enrich_df.py`:

```python
# Add these imports at the top of test_enrich_df.py
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
import json


def _make_enriched_endpoint_test():
    """Imports deferred to avoid circular-import issues at module load."""
    from dataregistry.main import app
    return TestClient(app)


class TestEnrichedEndpoint:
    """Smoke-tests for GET /calr/sessions/{session_id}/enriched."""

    def _session_dict(self):
        return {
            'groups': [{'name': 'G1', 'color': '#000', 'diet_name': 'D1', 'diet_kcal': None}],
            'subjects': [{'subject': 'S1', 'groupIndex': 0, 'total_mass': 20.0, 'lean_mass': None, 'fat_mass': None}],
            'light_cycle_start': 7,
            'dark_cycle_start': 19,
            'hour_range': [0, 24],
        }

    def _standard_df_csv(self):
        df = pd.DataFrame({
            'subject.id': ['S1', 'S1'],
            'exp.minute': [15.0, 30.0],
            'feed': [0.5, 0.6],
            'feed.acc': [0.5, 1.1],
            'ee': [2.0, 2.1],
            'ee.acc': [2.0, 4.1],
            'subject.mass': [20.0, 19.9],
        })
        return df.to_csv(index=False).encode()

    @patch('dataregistry.api.calr._load_session_and_standard_df')
    def test_returns_csv_with_enriched_columns(self, mock_load):
        import pandas as pd
        mock_load.return_value = (self._session_dict(), pd.read_csv(
            __import__('io').BytesIO(self._standard_df_csv())
        ))

        from dataregistry.main import app
        client = TestClient(app)
        response = client.get('/api/calr/sessions/test-session-id/enriched')

        assert response.status_code == 200
        assert 'text/csv' in response.headers['content-type']

        result_df = pd.read_csv(__import__('io').StringIO(response.text))
        for col in ('exp.hour', 'light', 'dark', 'day', 'group', 'eb'):
            assert col in result_df.columns, f"Expected column '{col}' in enriched output"
```

- [ ] **Step 3: Find the FastAPI app entrypoint and confirm the route prefix**

```bash
grep -rn "calr\|router\|include_router" /home/dhite/code-repos/broad/data-registry-api/dataregistry/main.py | head -20
```

If the prefix is `/api` and the router is included, the test URL `/api/calr/sessions/{id}/enriched` should match. Adjust the URL in the test if needed.

- [ ] **Step 4: Run the smoke test**

```bash
python -m pytest calr/tests/test_enrich_df.py::TestEnrichedEndpoint -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dataregistry/api/calr.py calr/tests/test_enrich_df.py
git commit -m "feat: add GET /calr/sessions/{session_id}/enriched endpoint"
```

---

## Task 3: Refactor `run_ancova`

**Files:**
- Modify: `dataregistry/api/calr.py` (lines ~952–1012)

The inline group assignment and caloric conversion are replaced by a single `_enrich_df` call. The `eb` computation is now provided by `_enrich_df`; the comment on the docstring can be cleaned up.

- [ ] **Step 1: Replace the body of `run_ancova`**

Find and replace the body between `session, df = _load_session_and_standard_df(...)` and `return result` (currently lines ~952–1012). The new body:

```python
    session, df = _load_session_and_standard_df(request.session_id, user.user_name if user else None)

    if request.mass_variable not in df.columns:
        raise fastapi.HTTPException(
            status_code=422,
            detail=f"Mass variable '{request.mass_variable}' not found in standard file"
        )

    import pandas as pd
    df = _enrich_df(df, session)
    df = df[df['group'].notna()].copy()

    # Apply hour range — net window: start_hour <= exp.hour < end_hour
    start_hour, end_hour = session['hour_range']
    df = df[(df['exp.hour'] >= start_hour) & (df['exp.hour'] < end_hour)]

    if df.empty:
        raise fastapi.HTTPException(status_code=422, detail="No data remaining after filters")

    # Subject exclusions
    for s in session['subjects']:
        exc_hour = s.get('exc_hour')
        if exc_hour is not None:
            subj_id = str(s['subject'])
            mask = (df['subject.id'].astype(str) == subj_id) & (df['exp.hour'] >= exc_hour)
            df = df[~mask]

    if df.empty:
        raise fastapi.HTTPException(status_code=422, detail="No data remaining after exclusions")

    try:
        result = ancova_table(
            df,
            mass_variable=request.mass_variable,
            light_cycle_start=session['light_cycle_start'],
            dark_cycle_start=session['dark_cycle_start'],
        )
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"ANCOVA table calculation failed: {str(e)}")

    return result
```

- [ ] **Step 2: Run the full test suite**

```bash
python -m pytest calr/tests/ -q
```

Expected: all tests pass (the existing QC tests are not affected by the ancova change).

- [ ] **Step 3: Commit**

```bash
git add dataregistry/api/calr.py
git commit -m "refactor: run_ancova calls _enrich_df, removes inline group/kcal logic"
```

---

## Task 4: Refactor `run_power_calc`

**Files:**
- Modify: `dataregistry/api/calr.py` (lines ~1030–1086)

- [ ] **Step 1: Replace the body of `run_power_calc`**

Find and replace from `session, df = _load_session_and_standard_df(...)` to `return result` (currently lines ~1030–1086). The new body:

```python
    if request.time_of_day not in ('light', 'dark', 'total'):
        raise fastapi.HTTPException(status_code=422, detail="time_of_day must be 'light', 'dark', or 'total'")

    session, df = _load_session_and_standard_df(request.session_id, user.user_name if user else None)

    if request.variable not in df.columns:
        raise fastapi.HTTPException(status_code=422, detail=f"Variable '{request.variable}' not found in standard file")
    if request.mass_variable not in df.columns:
        raise fastapi.HTTPException(status_code=422, detail=f"Mass variable '{request.mass_variable}' not found in standard file")

    import pandas as pd
    df = _enrich_df(df, session)
    df = df[df['group'].notna()].copy()

    # Apply hour range — net window: start_hour <= exp.hour < end_hour
    start_hour, end_hour = session['hour_range']
    df = df[(df['exp.hour'] >= start_hour) & (df['exp.hour'] < end_hour)]

    # Subject exclusions
    for s in session['subjects']:
        exc_hour = s.get('exc_hour')
        if exc_hour is not None:
            subj_id = str(s['subject'])
            mask = (df['subject.id'].astype(str) == subj_id) & (df['exp.hour'] >= exc_hour)
            df = df[~mask]

    df = filter_by_time_of_day(
        df,
        request.time_of_day,
        session['light_cycle_start'],
        session['dark_cycle_start'],
    )

    if df.empty:
        raise fastapi.HTTPException(status_code=422, detail="No data remaining after filters")

    try:
        result = power_calc(df, request.variable, request.mass_variable, request.sample_sizes, request.alpha)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Power calculation failed: {str(e)}")

    return result
```

- [ ] **Step 2: Run the full test suite**

```bash
python -m pytest calr/tests/ -q
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add dataregistry/api/calr.py
git commit -m "refactor: run_power_calc calls _enrich_df, removes inline group/kcal logic"
```

---

## Task 5: Refactor `run_quality_control`

**Files:**
- Modify: `dataregistry/api/calr.py` (lines ~1105–1169)

**Important:** `quality_control()` in `calr/analysis.py` applies kcal conversion internally. After `_enrich_df` has already applied it, pass `group_diet_kcal=None` (the default) to avoid double-conversion. The zero-basing of acc columns (mirrors R's `fixFeed`/`setZero`) is QC-specific and stays after the hour-range filter.

- [ ] **Step 1: Replace the body of `run_quality_control`**

Find and replace from `session, df = _load_session_and_standard_df(...)` to `return result` (currently lines ~1105–1169). The new body:

```python
    session, df = _load_session_and_standard_df(request.session_id, user.user_name if user else None)

    for col in ('subject.mass', 'feed', 'ee'):
        if col not in df.columns:
            raise fastapi.HTTPException(status_code=422, detail=f"Required column '{col}' not found in standard file")

    import pandas as pd
    df = _enrich_df(df, session)
    df = df[df['group'].notna()].copy()

    # QC-specific hour range (independent slider from main xranges)
    session_start, session_end = session['hour_range']
    start_hour = request.min_hour if request.min_hour is not None else session_start
    end_hour = request.max_hour if request.max_hour is not None else session_end

    df = df[(df['exp.hour'] >= start_hour) & (df['exp.hour'] < end_hour)]

    if df.empty:
        raise fastapi.HTTPException(status_code=422, detail="No data remaining after filters")

    # Subject exclusions
    for s in session['subjects']:
        exc_hour = s.get('exc_hour')
        if exc_hour is not None:
            subj_id = str(s['subject'])
            mask = (df['subject.id'].astype(str) == subj_id) & (df['exp.hour'] >= exc_hour)
            df = df[~mask]

    if df.empty:
        raise fastapi.HTTPException(status_code=422, detail="No data remaining after exclusions")

    # QC-specific: zero-base acc columns within the analysis window.
    # Mirrors R's fixFeed()/setZero() — subtracts each subject's first value so
    # accumulators start at 0 within the window, not from experiment start.
    for acc_col in ('feed.acc', 'ee.acc', 'drink.acc', 'wheel.acc'):
        if acc_col in df.columns:
            df[acc_col] = df.groupby('subject.id')[acc_col].transform(
                lambda x: x - x.dropna().iloc[0] if x.dropna().size > 0 else x
            )

    # Pass group_diet_kcal=None: _enrich_df already applied kcal conversion.
    # quality_control() skips its internal conversion when the argument is falsy.
    try:
        result = quality_control(df, request.n_mass_measurements)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"QC analysis failed: {str(e)}")

    return result
```

- [ ] **Step 2: Run the full test suite**

```bash
python -m pytest calr/tests/ -q
```

Expected: all 22+ tests pass.

- [ ] **Step 3: Commit**

```bash
git add dataregistry/api/calr.py
git commit -m "refactor: run_quality_control calls _enrich_df, removes inline group/ee.acc logic"
```

---

## Task 6: Add `fetchEnrichedData` to `registryService.js`

**Files:**
- Modify: `calr-vue/src/services/registryService.js`

- [ ] **Step 1: Add the function after `fetchSessionConfig` (after line ~105)**

```javascript
export async function fetchEnrichedData(sessionId, token) {
  const response = await fetch(`${API_BASE}/sessions/${sessionId}/enriched`, {
    headers: token ? createHeaders(token) : {},
  })

  return parseTextResponse(response)
}
```

- [ ] **Step 2: Verify the file builds without syntax errors**

```bash
cd /home/dhite/code-repos/broad/calr-vue
node --input-type=module < /dev/null || npx vite build --mode development 2>&1 | head -20
```

If a build step isn't configured, just confirm the file can be parsed:

```bash
node -e "require('./src/services/registryService.js')" 2>&1 | head -5
```

(Expect no parse errors.)

- [ ] **Step 3: Commit**

```bash
git add src/services/registryService.js
git commit -m "feat: add fetchEnrichedData to registryService"
```

---

## Task 7: Update `AnalysisView.vue` and `AccountView.vue`

**Files:**
- Modify: `calr-vue/src/views/AnalysisView.vue` (around line 1046)
- Modify: `calr-vue/src/views/AccountView.vue` (around line 1606)

The `prepForAnalysis` call is replaced by:
1. Fetching enriched CSV from the new endpoint (replaces `fetchDataFile`)
2. Parsing it with `parseCsv`
3. Applying numeric column parsing with `preprocessDetail`
4. Building the session with `buildAnalysisSession`

The `processDetail` pipeline no longer runs — enrichment is already server-side.

- [ ] **Step 1: Update the import in `AnalysisView.vue`**

Find the line (around line 457):

```javascript
import { fetchDataFile, fetchPublicFiles, fetchSessionConfig, fetchSessionFile, fetchUserFiles, runAnalysis } from '../services/registryService'
```

Replace with:

```javascript
import { fetchEnrichedData, fetchPublicFiles, fetchSessionConfig, fetchSessionFile, fetchUserFiles, runAnalysis } from '../services/registryService'
```

- [ ] **Step 2: Update the import of `prep-for-analysis` in `AnalysisView.vue`**

Find (around line 461):

```javascript
import { buildAnalysisSession, prepForAnalysis } from '../utils/prep-for-analysis'
```

Replace with:

```javascript
import { buildAnalysisSession } from '../utils/prep-for-analysis'
import { preprocessDetail } from '../utils/process'
```

- [ ] **Step 3: Update `openExperimentForAnalysis` in `AnalysisView.vue`**

Find the block (around line 1046–1068):

```javascript
        clearProcessCaches()
        const [dataCsv, sessionCsv, sessionConfig] = await Promise.all([
          fetchDataFile(standard.id, this.store.auth.token, isPublic),
          fetchSessionFile(session.id, this.store.auth.token, isPublic),
          fetchSessionConfig(session.id, this.store.auth.token, isPublic),
        ])

        const parsedSessionRows = parseCsv(sessionCsv)
        const analysisData = prepForAnalysis(parseCsv(dataCsv), {
          numericalColumns,
          sessionRows: parsedSessionRows,
          sessionConfig,
        })

        this.store.experiment.current = file
        this.store.experiment.detailRows = analysisData.rows
        this.store.experiment.sessionRows = parsedSessionRows
        this.store.experiment.analysisData = analysisData
```

Replace with:

```javascript
        clearProcessCaches()
        const [enrichedCsv, sessionCsv, sessionConfig] = await Promise.all([
          fetchEnrichedData(session.id, this.store.auth.token),
          fetchSessionFile(session.id, this.store.auth.token, isPublic),
          fetchSessionConfig(session.id, this.store.auth.token, isPublic),
        ])

        const parsedSessionRows = parseCsv(sessionCsv)
        const analysisSession = buildAnalysisSession(parsedSessionRows, sessionConfig)
        const enrichedRows = preprocessDetail(parseCsv(enrichedCsv), numericalColumns)

        this.store.experiment.current = file
        this.store.experiment.detailRows = enrichedRows
        this.store.experiment.sessionRows = parsedSessionRows
        this.store.experiment.analysisData = { rows: enrichedRows, session: analysisSession }
```

- [ ] **Step 4: Apply the same changes to `AccountView.vue`**

Find the import (around line 741):

```javascript
import { prepForAnalysis } from '../utils/prep-for-analysis'
```

Replace with:

```javascript
import { buildAnalysisSession } from '../utils/prep-for-analysis'
import { preprocessDetail } from '../utils/process'
```

Find the import of `fetchDataFile` in `AccountView.vue` and add `fetchEnrichedData`:

```javascript
// Find the line importing fetchDataFile and add fetchEnrichedData to the import list
import { fetchEnrichedData, /* other imports */ } from '../services/registryService'
```

Find the equivalent `prepForAnalysis` call block (around line 1605–1623) and apply the same replacement as in Step 3 above (using the session's `session.id` from that context).

- [ ] **Step 5: Verify no JS build errors**

```bash
cd /home/dhite/code-repos/broad/calr-vue
npx vite build 2>&1 | tail -20
```

Expected: build completes without errors.

- [ ] **Step 6: Commit**

```bash
git add src/views/AnalysisView.vue src/views/AccountView.vue
git commit -m "feat: load enriched data from API instead of computing client-side"
```

---

## Task 8: Remove dead enrichment code from `process.js` and `prep-for-analysis.js`

**Files:**
- Modify: `calr-vue/src/utils/process.js`
- Modify: `calr-vue/src/utils/prep-for-analysis.js`

Before deleting anything, verify no remaining callers exist.

- [ ] **Step 1: Confirm no remaining callers of the functions being deleted**

```bash
cd /home/dhite/code-repos/broad/calr-vue
grep -rn "enrichDetailRows\|fillAccumulatorColumns\|convertFeedColumns\|applySessionFieldFallbacks\|attachSessionMetadata\|ensureEnviroLight\|prepForAnalysis" src/ --include="*.js" --include="*.vue"
```

Expected: only definitions in `process.js` and `prep-for-analysis.js`, no call sites. If any call sites remain outside those files, fix them first before deleting.

- [ ] **Step 2: Delete the following exported functions from `process.js`**

Functions to remove (find by `export function` name and delete the full function body including any preceding blank line):
- `enrichDetailRows` (around line 615)
- `fillAccumulatorColumns` (around line 332)
- `attachSessionMetadata` (around line 596)
- `ensureEnviroLight` (around line 481) — only if Step 1 confirms no remaining callers

Also remove these unexported private helpers that are only used by the deleted functions:
- `convertFeedColumns` (around line 298)
- `applySessionFieldFallbacks` (around line 140)

- [ ] **Step 3: Simplify `processDetail` in `process.js`**

Find `export function processDetail` (around line 735). It currently calls `enrichDetailRows` and `fillAccumulatorColumns`. Remove those two lines. The simplified function body:

```javascript
export function processDetail(rows, {
  numericalColumns = [],
  sessionRows = [],
  session = null,
  applySessionExclusions = true,
  hourRange = null,
} = {}) {
  const normalizedSession = session || preprocessSession(sessionRows)
  const cycleStarts = {
    lightCycleStart: normalizedSession.light_cycle_start ?? getSessionCycleStartsFromRows(sessionRows).lightCycleStart,
    darkCycleStart: normalizedSession.dark_cycle_start ?? getSessionCycleStartsFromRows(sessionRows).darkCycleStart,
  }

  let processedRows = ensureExpMinute(rows)

  const sessionPayload = toProcessingSessionShape(normalizedSession, cycleStarts, sessionRows)

  processedRows = preprocessDetail(processedRows, numericalColumns)

  if (applySessionExclusions) {
    processedRows = applyExclusions(processedRows, sessionPayload)
  }

  if (hourRange) {
    processedRows = cropDetailRows(processedRows, hourRange)
  }

  return processedRows
}
```

- [ ] **Step 4: Simplify `prep-for-analysis.js`**

The `buildAnalysisSession` export stays unchanged. The `prepForAnalysis` export is no longer called from any view. Remove it, or if you want to keep it for backward compatibility during rollout, leave it but note it's unused. Confirm with:

```bash
grep -rn "prepForAnalysis" src/ --include="*.js" --include="*.vue"
```

If no callers remain after Task 7, delete `prepForAnalysis` from `prep-for-analysis.js`.

- [ ] **Step 5: Verify build is clean**

```bash
cd /home/dhite/code-repos/broad/calr-vue
npx vite build 2>&1 | tail -20
```

Expected: no errors. If build fails referencing a deleted function, trace the call and fix.

- [ ] **Step 6: Run all Python tests one final time**

```bash
cd /home/dhite/code-repos/broad/data-registry-api
python -m pytest calr/tests/ -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/utils/process.js src/utils/prep-for-analysis.js
git commit -m "chore: remove client-side enrichment code — now handled server-side"
```

---

## Self-Review Checklist

- [x] **Spec coverage:**
  - `_enrich_df` all 8 enrichment steps — Task 1
  - New enriched endpoint — Task 2
  - `run_ancova` refactored — Task 3
  - `run_power_calc` refactored — Task 4
  - `run_quality_control` refactored (zero-basing stays) — Task 5
  - `fetchEnrichedData` in JS service — Task 6
  - Vue views updated — Task 7
  - Dead JS code removed — Task 8

- [x] **Double-conversion guard:** Task 5 explicitly passes no `group_diet_kcal` to `quality_control()` (defaults to None), which skips its internal conversion since `_enrich_df` already applied it.

- [x] **Auth:** New endpoint uses `get_calr_user_optional` + delegates access control to `_load_session_and_standard_df` (same as all other endpoints).

- [x] **Type consistency:** `_enrich_df(df, session)` signature matches every call site.

- [x] **No placeholders:** All code blocks are complete.

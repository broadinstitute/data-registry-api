"""
Unit tests for _enrich_df() in dataregistry/api/calr.py.

Verifies that each enrichment step matches the JS processDetail pipeline:
  1. exp.hour / hour derived from exp.minute
  2. enviro.light inferred from timestamp when all values blank
  3. light / dark / clockHour / day / exp.day derived
  4. Subject mass fallbacks applied from session subjects
  5. group / color / diet / groupIndex joined from session
  6. feed / feed.acc multiplied by diet_kcal per group
  7. ee.acc filled by per-subject cumulative sum when absent
  8. eb / eb.acc computed from feed - ee / feed.acc - ee.acc
"""

import io
import json
import os
import sys
from unittest.mock import patch
import pytest
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Prevent the module-level DB engine creation from making AWS calls
os.environ.setdefault('DATA_REGISTRY_DB_CONNECTION', 'sqlite://')

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
        df = _df()
        df.loc[df['subject.id'] == 'A1', 'subject.mass'] = 99.0  # distinct from session total_mass=25.0
        result = _enrich_df(df, _session())
        # Existing value (99.0) must be preserved; session's 25.0 must not overwrite it
        assert result.loc[result['subject.id'] == 'A1', 'subject.mass'].iloc[0] == 99.0


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
        # GroupA has diet_kcal=3.5, so feed is 0.5*3.5=1.75; ee is unchanged at 2.0
        expected_eb = 0.5 * 3.5 - 2.0
        assert pytest.approx(a1['eb']) == expected_eb

    def test_eb_acc_computed_as_feed_acc_minus_ee_acc(self):
        result = _enrich_df(_df(), _session())
        a1 = result[result['subject.id'] == 'A1'].iloc[0]
        assert pytest.approx(a1['eb.acc']) == a1['feed.acc'] - a1['ee.acc']


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

    def _standard_df(self):
        return pd.DataFrame({
            'subject.id': ['S1', 'S1'],
            'exp.minute': [15.0, 30.0],
            'feed': [0.5, 0.6],
            'feed.acc': [0.5, 1.1],
            'ee': [2.0, 2.1],
            'ee.acc': [2.0, 4.1],
            'subject.mass': [20.0, 19.9],
        })

    @patch('dataregistry.api.calr._load_session_and_standard_df')
    def test_returns_csv_with_enriched_columns(self, mock_load):
        mock_load.return_value = (self._session_dict(), self._standard_df())

        from dataregistry.server import app
        from fastapi.testclient import TestClient
        client = TestClient(app)

        # Adjust the URL prefix to match what main.py registers — check with:
        # grep include_router dataregistry/main.py
        response = client.get('/api/calr/sessions/test-session-id/enriched')

        assert response.status_code == 200
        assert 'text/csv' in response.headers['content-type']

        result_df = pd.read_csv(io.StringIO(response.text))
        for col in ('exp.hour', 'light', 'dark', 'day', 'group', 'eb'):
            assert col in result_df.columns, f"Expected column '{col}' in enriched output"

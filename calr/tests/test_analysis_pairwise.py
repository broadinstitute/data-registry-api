import pandas as pd
from scipy import stats

from calr.analysis import (
    ancova_table,
    apply_legacy_analysis_outliers,
    power_calc,
    prepare_analysis_hourly_rows,
    quality_control,
)


def _group_df(group_names=None):
    group_names = group_names or ['Group 1', 'Group 2', 'Group 3']
    rows = []
    for group_index, group in enumerate(group_names):
        offset = float(group_index * 2)
        for subject_index in range(5):
            subject = f'{group}-S{subject_index + 1}'
            mass = 20 + subject_index
            for hour in [1, 7, 19]:
                rows.append({
                    'group': group,
                    'subject.id': subject,
                    'exp.hour': hour,
                    'total_mass': mass,
                    'feed': 1.0 + offset + mass * 0.05,
                    'drink': 0.2 + offset,
                    'ee': 2.0 + offset + mass * 0.08,
                    'vo2': 3.0 + offset + mass * 0.1,
                    'vco2': 2.5 + offset + mass * 0.1,
                    'pedmeter': 10.0 + offset,
                    'allmeter': 20.0 + offset,
                    'rer': 0.8 + offset * 0.01,
                    'xytot': 100.0 + offset,
                    'body.temp': 36.0 + offset * 0.1,
                    'eb': -1.0 + offset,
                })
    return pd.DataFrame(rows)


def test_ancova_table_returns_pairwise_sections_against_reference_group():
    group_order = ['Group 1', 'Group 2', 'Group 3']
    result = ancova_table(
        _group_df(group_order),
        mass_variable='total_mass',
        light_cycle_start=6,
        dark_cycle_start=18,
        group_order=group_order,
        reference_group='Group 1',
    )

    assert result['reference_group'] == 'Group 1'
    assert [c['label'] for c in result['comparisons']] == [
        'Group 2 vs Group 1',
        'Group 3 vs Group 1',
    ]
    assert [s['label'] for s in result['ancova_pairwise']] == [
        'Group 2 vs Group 1',
        'Group 3 vs Group 1',
    ]
    assert [s['label'] for s in result['anova_pairwise']] == [
        'Group 2 vs Group 1',
        'Group 3 vs Group 1',
    ]
    assert result['ancova_pairwise'][0]['rows'][0]['full_day']['group'] is not None
    assert result['ancova_pairwise'][1]['rows'][0]['full_day']['group'] is not None
    assert result['anova_pairwise'][0]['rows'][0]['full_day']['group'] is not None
    assert result['anova_pairwise'][1]['rows'][0]['full_day']['group'] is not None


def test_ancova_table_pairwise_sections_scale_beyond_four_groups():
    group_order = ['Group 1', 'Group 2', 'Group 3', 'Group 4', 'Group 5']
    result = ancova_table(
        _group_df(group_order),
        mass_variable='total_mass',
        light_cycle_start=6,
        dark_cycle_start=18,
        group_order=group_order,
        reference_group='Group 1',
    )

    expected = [
        'Group 2 vs Group 1',
        'Group 3 vs Group 1',
        'Group 4 vs Group 1',
        'Group 5 vs Group 1',
    ]
    assert [c['label'] for c in result['comparisons']] == expected
    assert [s['label'] for s in result['ancova_pairwise']] == expected
    assert [s['label'] for s in result['anova_pairwise']] == expected


def test_prepare_analysis_hourly_rows_matches_calr_hourly_sums_and_means():
    df = pd.DataFrame([
        {
            'group': 'A', 'subject.id': 'S1', 'exp.minute': 0, 'exp.hour': 0,
            'Date.Time': '2024-01-01 07:00:00', 'total_mass': 20,
            'feed': 1.0, 'feed.acc': 10.0, 'drink': 2.0, 'drink.acc': 20.0,
            'ee': 3.0, 'ee.acc': 30.0, 'pedmeter': 4.0, 'allmeter': 5.0,
            'vo2': 100.0,
        },
        {
            'group': 'A', 'subject.id': 'S1', 'exp.minute': 30, 'exp.hour': 0,
            'Date.Time': '2024-01-01 07:30:00', 'total_mass': 20,
            'feed': 1.5, 'feed.acc': 11.5, 'drink': 2.5, 'drink.acc': 22.5,
            'ee': 5.0, 'ee.acc': 35.0, 'pedmeter': 6.0, 'allmeter': 7.0,
            'vo2': 120.0,
        },
    ])

    hourly = prepare_analysis_hourly_rows(df, light_cycle_start=6, dark_cycle_start=18)
    row = hourly.iloc[0]

    assert row['feed'] == 2.5
    assert row['drink'] == 4.5
    assert row['pedmeter'] == 2.0
    assert row['allmeter'] == 2.0
    assert row['ee'] == 4.0
    assert row['vo2'] == 110.0
    assert row['feed.acc'] == 1.5
    assert row['ee.acc'] == 2.5
    assert row['eb'] == -1.5


def test_apply_legacy_analysis_outliers_removes_coupled_respiration_values_and_rebuilds_accumulators():
    rows = []
    for minute in range(30):
        rows.append({
            'subject.id': 'S1',
            'exp.minute': minute,
            'feed': 1.0,
            'feed.acc': minute + 1.0,
            'drink': 2.0,
            'drink.acc': (minute + 1.0) * 2,
            'ee': 5.0,
            'ee.acc': (minute + 1.0) * 5,
            'vo2': 10.0,
            'vco2': 9.0,
            'rer': 0.9,
            'body.temp': 37.0,
        })
    rows.append({
        'subject.id': 'S1',
        'exp.minute': 30,
        'feed': 1.0,
        'feed.acc': 31.0,
        'drink': 2.0,
        'drink.acc': 62.0,
        'ee': 500.0,
        'ee.acc': 650.0,
        'vo2': 10.0,
        'vco2': 9.0,
        'rer': 0.9,
        'body.temp': 37.0,
    })

    result = apply_legacy_analysis_outliers(pd.DataFrame(rows)).sort_values('exp.minute')

    outlier_row = result.loc[result['exp.minute'] == 30].iloc[0]
    assert pd.isna(outlier_row['ee'])
    assert pd.isna(outlier_row['vo2'])
    assert pd.isna(outlier_row['vco2'])
    assert pd.isna(outlier_row['rer'])
    assert outlier_row['ee.acc'] == 150
    assert outlier_row['feed.acc'] == 31


def test_quality_control_prefers_session_mass_change_when_available():
    df = pd.DataFrame([
        {
            'subject.id': 'S1',
            'group': 'Group 1',
            'exp.minute': minute,
            'subject.mass': 20 + minute,
            'feed': 1.0,
            'feed.acc': minute + 1.0,
            'ee': 0.5,
            'ee.acc': (minute + 1.0) * 0.5,
        }
        for minute in range(4)
    ] + [
        {
            'subject.id': 'S2',
            'group': 'Group 1',
            'exp.minute': minute,
            'subject.mass': 30 + minute,
            'feed': 1.0,
            'feed.acc': minute + 1.0,
            'ee': 0.5,
            'ee.acc': (minute + 1.0) * 0.5,
        }
        for minute in range(4)
    ])

    result = quality_control(
        df,
        n_mass_measurements=1,
        mass_change_by_subject={'S1': 0.25},
    )
    by_subject = {row['subject_id']: row for row in result['subjects']}

    assert by_subject['S1']['mass_delta'] == 0.25
    assert by_subject['S2']['mass_delta'] == 3.0


def test_ancova_table_short_window_reports_full_day_only():
    result = ancova_table(
        _group_df(['Group 1', 'Group 2']),
        mass_variable='total_mass',
        light_cycle_start=6,
        dark_cycle_start=18,
        group_order=['Group 1', 'Group 2'],
        reference_group='Group 1',
        selected_hour_count=12,
    )

    assert 'full_day' in result['ancova'][0]
    assert 'light' not in result['ancova'][0]
    assert 'dark' not in result['ancova'][0]
    assert 'full_day' in result['anova'][0]
    assert 'light' not in result['anova'][0]
    assert 'dark' not in result['anova'][0]


def test_power_calc_anova_uses_legacy_eta_squared_as_pwr_f():
    df = pd.DataFrame([
        {'group': 'GFP', 'subject.id': 'G1', 'total_mass': 20, 'rer': 0.80},
        {'group': 'GFP', 'subject.id': 'G2', 'total_mass': 21, 'rer': 0.82},
        {'group': 'GFP', 'subject.id': 'G3', 'total_mass': 22, 'rer': 0.84},
        {'group': 'GFP', 'subject.id': 'G4', 'total_mass': 23, 'rer': 0.86},
        {'group': 'GFP', 'subject.id': 'G5', 'total_mass': 24, 'rer': 0.88},
        {'group': 'Irisin', 'subject.id': 'I1', 'total_mass': 20, 'rer': 0.805},
        {'group': 'Irisin', 'subject.id': 'I2', 'total_mass': 21, 'rer': 0.825},
        {'group': 'Irisin', 'subject.id': 'I3', 'total_mass': 22, 'rer': 0.845},
        {'group': 'Irisin', 'subject.id': 'I4', 'total_mass': 23, 'rer': 0.865},
        {'group': 'Irisin', 'subject.id': 'I5', 'total_mass': 24, 'rer': 0.885},
    ])

    result = power_calc(df, 'rer', 'total_mass', [24], alpha=0.05)
    eta2 = result['effect_size']['eta_squared']
    k = len(result['group_stats'])
    n = 24
    df1 = k - 1
    df2 = n * k - k
    f_crit = stats.f.ppf(1 - 0.05, df1, df2)
    legacy_expected = stats.ncf.sf(f_crit, df1, df2, nc=n * k * abs(eta2) ** 2)
    cohen_f_expected = stats.ncf.sf(
        f_crit,
        df1,
        df2,
        nc=n * k * (eta2 / (1 - eta2)),
    )

    assert result['method'] == 'anova'
    assert eta2 > 0
    assert result['power_curve'][-1]['power'] == round(float(legacy_expected), 4)
    assert result['power_curve'][-1]['power'] < round(float(cohen_f_expected), 4)

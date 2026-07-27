import pandas as pd

from calr.analysis import ancova_table


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

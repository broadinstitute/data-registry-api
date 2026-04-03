"""
CalR statistical analysis functions, ported from the R calr package.
"""
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats
from statsmodels.stats.anova import anova_lm


def _p_to_annotation(p: float) -> str:
    if p < 0.0001:
        return "***"
    if p < 0.001:
        return "**"
    if p < 0.01:
        return "*"
    return ""


def acute_ancova(df: pd.DataFrame, variable: str, mass_variable: str) -> dict:
    """
    Port of R acuteANCOVA(). Runs a GLM at each exp.hour to test for group
    differences in `variable` while accounting for `mass_variable` as a covariate.

    Expects df to have columns: exp.hour, group, `variable`, `mass_variable`.

    Returns a dict with per-hour arrays suitable for JSON serialisation:
        hours           - list of exp.hour values
        p_values        - GLM p-value for the group coefficient at each hour
        annotations     - significance stars ("*", "**", "***", or "")
        annotation_y    - max(group mean + SE) at each hour, for label placement
        groups          - { group_name: { means: [...], se: [...] } }
    """
    hours = sorted(df['exp.hour'].unique())

    p_values = []
    annotation_y = []
    group_stats: dict[str, dict[str, list]] = {
        g: {"means": [], "se": []} for g in df['group'].unique()
    }

    for hour in hours:
        hour_df = df[df['exp.hour'] == hour].copy()

        # Per-group mean and SE for this hour
        max_upper = -np.inf
        for group, gdf in hour_df.groupby('group'):
            mean = gdf[variable].mean()
            se = gdf[variable].std() / np.sqrt(len(gdf)) if len(gdf) > 1 else 0.0
            group_stats[group]["means"].append(round(float(mean), 6))
            group_stats[group]["se"].append(round(float(se), 6))
            max_upper = max(max_upper, mean + se)

        annotation_y.append(round(float(max_upper), 6))

        # GLM: variable ~ mass + C(group)
        # Need at least 2 groups and enough observations to fit
        n_groups = hour_df['group'].nunique()
        if len(hour_df) <= n_groups + 1 or n_groups < 2:
            p_values.append(None)
            continue

        try:
            formula = f'Q("{variable}") ~ Q("{mass_variable}") + C(group)'
            model = smf.ols(formula, data=hour_df).fit()
            # 3rd coefficient (index 2) is the first group dummy — matches R's coefficients[3,4]
            p_values.append(round(float(model.pvalues.iloc[2]), 6))
        except Exception:
            p_values.append(None)

    annotations = [
        _p_to_annotation(p) if p is not None else "" for p in p_values
    ]

    return {
        "hours": [float(h) for h in hours],
        "p_values": p_values,
        "annotations": annotations,
        "annotation_y": annotation_y,
        "groups": group_stats,
    }


def filter_by_time_of_day(
    df: pd.DataFrame,
    time_of_day: str,
    light_cycle_start: int,
    dark_cycle_start: int,
) -> pd.DataFrame:
    """
    Filter a CalR dataframe to light, dark, or total phase.
    Uses the hour-of-day (exp.hour % 24) to determine phase.
    """
    if time_of_day == "total":
        return df

    hour_of_day = df['exp.hour'] % 24

    if light_cycle_start < dark_cycle_start:
        in_light = (hour_of_day >= light_cycle_start) & (hour_of_day < dark_cycle_start)
    else:
        # light cycle wraps midnight
        in_light = (hour_of_day >= light_cycle_start) | (hour_of_day < dark_cycle_start)

    if time_of_day == "light":
        return df[in_light]
    else:  # dark
        return df[~in_light]


def quality_control(
    df: pd.DataFrame,
    n_mass_measurements: int = 5,
    group_diet_kcal: dict = None,
) -> dict:
    """
    Port of the CalR quality control analysis (revperAve / modified_df1 pipeline).

    Matches the R implementation exactly:
      1. Apply caloric density conversion to feed and feed.acc per group
         (group_diet_kcal maps group name → kcal per gram of food)
      2. Compute bin = 60 / modal_measurement_interval_minutes
         (converts cumulative EE from sum-of-rates to actual kcal)
      3. For each subject:
           - mass_delta: avg(last N mass rows) - avg(first N mass rows)
           - total_eb:   last feed.acc value - last ee.acc value / bin
             (mirrors R's l.eb.acc.x = last value of feed.acc - ee.acc/bin)

    Then fits per-group and overall linear regressions of mass_delta (x) vs
    total_eb (y).

    Expects df to have columns: subject.id, group, subject.mass, feed, feed.acc,
    ee, ee.acc, exp.minute — sorted by time within each subject.

    Returns:
        subjects            - per-subject [subject_id, group, mass_delta, total_eb]
        group_regressions   - per-group {slope, intercept, r_squared, n}
        overall_regression  - {slope, intercept, r_squared, n}
    """
    df = df.copy()

    # Step 1: caloric density conversion (mirrors R: feed *= cal_i, feed.acc *= cal_i)
    if group_diet_kcal:
        for group_name, kcal_per_g in group_diet_kcal.items():
            if kcal_per_g:
                mask = df['group'] == group_name
                df.loc[mask, 'feed'] = df.loc[mask, 'feed'] * kcal_per_g
                if 'feed.acc' in df.columns:
                    df.loc[mask, 'feed.acc'] = df.loc[mask, 'feed.acc'] * kcal_per_g

    # Step 2: bin = 60 / modal measurement interval in minutes
    # Mirrors R: binDf <- diff(my.table$minute)/60; bin <- 60/getmode(binDf)
    sort_col = 'exp.minute' if 'exp.minute' in df.columns else 'exp.hour'
    minute_diffs = df.groupby('subject.id')[sort_col].diff().dropna()
    if not minute_diffs.empty:
        modal_interval = float(minute_diffs.mode().iloc[0])
        # exp.minute is in minutes; exp.hour is in hours — normalise to minutes
        if sort_col == 'exp.hour':
            modal_interval *= 60
    else:
        modal_interval = 60.0  # assume hourly if no diff available
    bin_factor = 60.0 / modal_interval  # intervals per hour

    subject_rows = []

    for subject_id, sdf in df.groupby('subject.id'):
        sdf = sdf.sort_values(sort_col)
        group = sdf['group'].iloc[0]

        n = min(n_mass_measurements, len(sdf))
        first_mass = float(sdf['subject.mass'].iloc[:n].mean())
        last_mass = float(sdf['subject.mass'].iloc[-n:].mean())
        mass_delta = round(last_mass - first_mass, 4)

        # eb.acc = feed.acc - ee.acc/bin  (last cumulative value)
        # Mirrors R: my.table$ee.acc <- my.table$ee.acc/bin
        #            my.table$eb.acc  <- my.table$feed.acc - my.table$ee.acc
        #            l.eb.acc.x = tail(eb.acc, 1)
        feed_acc_last = float(sdf['feed.acc'].iloc[-1]) if 'feed.acc' in sdf.columns else float(sdf['feed'].sum())
        ee_acc_last = float(sdf['ee.acc'].iloc[-1]) if 'ee.acc' in sdf.columns else float(sdf['ee'].sum())
        total_eb = round(feed_acc_last - ee_acc_last / bin_factor, 4)

        subject_rows.append({
            'subject_id': str(subject_id),
            'group': group,
            'mass_delta': mass_delta,
            'total_eb': total_eb,
        })

    points_df = pd.DataFrame(subject_rows)

    def _regress(x, y):
        if len(x) < 2 or len(set(x)) < 2:
            return {'slope': None, 'intercept': None, 'r_squared': None, 'n': len(x)}
        result = stats.linregress(x, y)
        return {
            'slope': round(float(result.slope), 6),
            'intercept': round(float(result.intercept), 6),
            'r_squared': round(float(result.rvalue ** 2), 6),
            'n': len(x),
        }

    group_regressions = {
        group: _regress(gdf['mass_delta'].values, gdf['total_eb'].values)
        for group, gdf in points_df.groupby('group')
    }

    overall_regression = _regress(
        points_df['mass_delta'].values,
        points_df['total_eb'].values,
    )

    return {
        'subjects': subject_rows,
        'group_regressions': group_regressions,
        'overall_regression': overall_regression,
    }


# Variables that use ANCOVA (mass as covariate) rather than ANOVA for power calc.
# Mirrors R's ancovaList: c("Energy.Expenditure", "Total.Food")
ANCOVA_VARIABLES = {'ee', 'feed', 'feed.acc'}


def power_calc(
    df: pd.DataFrame,
    variable: str,
    mass_variable: str,
    sample_sizes: list[int],
    alpha: float = 0.05,
) -> dict:
    """
    Port of R AncovaReadyStats() + PowerCalc().

    Computes per-group summary statistics and a power curve across the given
    sample sizes. Method is auto-selected:
      - ANCOVA for 'ee', 'feed', 'feed.acc' (mass reduces residual variance)
      - ANOVA for all other variables

    Returns a dict with:
        method          - 'ancova' or 'anova'
        effect_size     - {'r_squared': float} or {'eta_squared': float}
        overall_sd      - pooled SD across all observations
        group_stats     - per-group n, mean, variance
        power_curve     - [{'n_per_group': int, 'power': float}, ...]
    """
    method = 'ancova' if variable in ANCOVA_VARIABLES else 'anova'
    k = df['group'].nunique()
    overall_sd = float(df[variable].std())

    # Per-group stats
    group_stats = {}
    for group, gdf in df.groupby('group'):
        group_stats[group] = {
            'n': len(gdf['subject.id'].unique()),
            'mean': round(float(gdf[variable].mean()), 6),
            'variance': round(float(gdf[variable].var()), 6),
        }

    group_means = [group_stats[g]['mean'] for g in sorted(group_stats)]

    # Effect size
    if method == 'ancova':
        model = smf.ols(f'Q("{variable}") ~ Q("{mass_variable}") + C(group)', data=df).fit()
        r_squared = float(model.rsquared)
        effect_size = {'r_squared': round(r_squared, 6)}
    else:
        from statsmodels.stats.anova import anova_lm
        model = smf.ols(f'Q("{variable}") ~ C(group)', data=df).fit()
        anova_table = anova_lm(model, typ=1)
        ss_group = float(anova_table['sum_sq'].iloc[0])
        ss_total = float(anova_table['sum_sq'].sum())
        eta2 = ss_group / ss_total if ss_total > 0 else 0.0
        effect_size = {'eta_squared': round(eta2, 6)}

    # Power curve
    grand_mean = np.mean(group_means)
    ss_means = sum((m - grand_mean) ** 2 for m in group_means)

    power_curve = []
    for n in sample_sizes:
        N = n * k
        df1 = k - 1
        if method == 'ancova':
            df2 = N - k - 1  # subtract 1 covariate df
            error_var = overall_sd ** 2 * (1 - r_squared)
        else:
            df2 = N - k
            f = np.sqrt(eta2 / (1 - eta2)) if eta2 < 1 else 1.0
            error_var = overall_sd ** 2 / (1 + (N * f ** 2) / df1) if df1 > 0 else overall_sd ** 2

        if df2 <= 0 or error_var <= 0:
            power_curve.append({'n_per_group': n, 'power': None})
            continue

        lambda_ = n * ss_means / error_var
        f_crit = stats.f.ppf(1 - alpha, df1, df2)
        power = float(1 - stats.ncf.cdf(f_crit, df1, df2, nc=lambda_))
        power_curve.append({'n_per_group': n, 'power': round(power, 4)})

    return {
        'method': method,
        'variable': variable,
        'effect_size': effect_size,
        'overall_sd': round(overall_sd, 6),
        'group_stats': group_stats,
        'power_curve': power_curve,
    }


# ---------------------------------------------------------------------------
# Summary ANCOVA / ANOVA table  (mirrors anovaTab() in calR's Input_tab.R)
# ---------------------------------------------------------------------------

# Variables analysed with mass as a covariate (ANCOVA / GLM section)
_ANCOVA_VARS = [
    ('feed',      'Food Consumed (kcal/period)'),
    ('drink',     'Water Consumed (ml/period)'),
    ('ee',        'Energy Expenditure (kcal/period)'),
    ('vo2',       'Oxygen Consumption (ml/hr)'),
    ('vco2',      'Carbon Dioxide Production (ml/hr)'),
]

# Variables analysed without a mass covariate (ANOVA section)
_ANOVA_VARS = [
    ('pedmeter',  'Pedestrian Locomotion (m)'),
    ('allmeter',  'Total Distance in Cage (m)'),
    ('rer',       'Respiratory Exchange Ratio'),
    ('xytot',     'Locomotor Activity (beam breaks)'),
    ('body.temp', 'Body Temperature (Celsius)'),
    ('eb',        'Energy Balance (kcal/period)'),   # computed: feed - ee
]


def _aggregate_subjects(df: pd.DataFrame, var_col: str, mass_col: str) -> pd.DataFrame:
    """Per-subject means of mass and var, mirroring calR's ddply(group, subject.id, mean)."""
    agg = (
        df.groupby(['group', 'subject.id'])
        .agg(mass=(mass_col, 'mean'), var=(var_col, 'mean'))
        .reset_index()
        .dropna(subset=['mass', 'var'])
    )
    return agg


def _fit_ancova_period(subj_df: pd.DataFrame):
    """
    Fit var ~ mass + C(group) + mass:C(group).
    If interaction p > 0.05, re-fit without interaction.
    Returns (p_mass, p_group, p_interaction) — p_interaction is None when dropped.
    Returns None when there is insufficient data.
    """
    n_groups = subj_df['group'].nunique()
    if n_groups < 2 or len(subj_df) <= n_groups + 2:
        return None

    try:
        m_full = smf.ols('var ~ mass + C(group) + mass:C(group)', data=subj_df).fit()
        at_full = anova_lm(m_full, typ=2)

        int_rows = [r for r in at_full.index if ':' in str(r)]
        if not int_rows:
            return None
        p_int = float(at_full.loc[int_rows[0], 'PR(>F)'])

        if p_int > 0.05:
            m_noint = smf.ols('var ~ mass + C(group)', data=subj_df).fit()
            at_noint = anova_lm(m_noint, typ=2)
            p_mass = float(at_noint.loc['mass', 'PR(>F)'])
            p_group = float(at_noint.loc['C(group)', 'PR(>F)'])
            return (round(p_mass, 4), round(p_group, 4), None)
        else:
            p_mass = float(at_full.loc['mass', 'PR(>F)'])
            grp_rows = [r for r in at_full.index if 'C(group)' in str(r) and ':' not in str(r)]
            p_group = float(at_full.loc[grp_rows[0], 'PR(>F)'])
            return (round(p_mass, 4), round(p_group, 4), round(p_int, 4))
    except Exception:
        return None


def _fit_anova_period(subj_df: pd.DataFrame):
    """
    Fit var ~ C(group).
    Returns p_group F-test p-value, or None when there is insufficient data.
    """
    n_groups = subj_df['group'].nunique()
    if n_groups < 2 or len(subj_df) <= n_groups:
        return None

    try:
        m = smf.ols('var ~ C(group)', data=subj_df).fit()
        at = anova_lm(m, typ=2)
        return round(float(at.loc['C(group)', 'PR(>F)']), 4)
    except Exception:
        return None


def ancova_table(
    df: pd.DataFrame,
    mass_variable: str = 'subject.mass',
    light_cycle_start: int = 6,
    dark_cycle_start: int = 18,
) -> dict:
    """
    Compute the summary ANCOVA/ANOVA table, mirroring anovaTab() from calR.

    For each variable in _ANCOVA_VARS, runs:
        var ~ mass + C(group) + mass:C(group)
    and if the interaction p-value > 0.05 drops the interaction term, mirroring the
    R code that tests ``a$coefficients[nrow(a$coefficients), 4] > 0.05``.

    For each variable in _ANOVA_VARS, runs:
        var ~ C(group)

    Both analyses are run for three time periods: full_day, light, dark.
    Energy balance (eb) is computed on the fly as feed − ee when absent from df.

    Returns
    -------
    {
      "mass_variable": str,
      "ancova": [
        {
          "variable": str,
          "label": str,
          "full_day": {"mass": float|null, "group": float|null, "interaction": float|null},
          "light":    {"mass": float|null, "group": float|null, "interaction": float|null},
          "dark":     {"mass": float|null, "group": float|null, "interaction": float|null}
        }, ...
      ],
      "anova": [
        {
          "variable": str,
          "label": str,
          "full_day": {"group": float|null},
          "light":    {"group": float|null},
          "dark":     {"group": float|null}
        }, ...
      ]
    }
    """
    # Compute energy balance if missing
    if 'eb' not in df.columns and 'feed' in df.columns and 'ee' in df.columns:
        df = df.copy()
        df['eb'] = df['feed'] - df['ee']

    # Phase subsets
    hour_of_day = df['exp.hour'] % 24
    if light_cycle_start < dark_cycle_start:
        in_light = (hour_of_day >= light_cycle_start) & (hour_of_day < dark_cycle_start)
    else:
        in_light = (hour_of_day >= light_cycle_start) | (hour_of_day < dark_cycle_start)

    phase_dfs = {
        'full_day': df,
        'light':    df[in_light],
        'dark':     df[~in_light],
    }

    ancova_rows = []
    for var_col, label in _ANCOVA_VARS:
        if var_col not in df.columns:
            continue
        row: dict = {'variable': var_col, 'label': label}
        for phase, phase_df in phase_dfs.items():
            subj = _aggregate_subjects(phase_df, var_col, mass_variable)
            result = _fit_ancova_period(subj)
            if result is None:
                row[phase] = {'mass': None, 'group': None, 'interaction': None}
            else:
                p_mass, p_group, p_int = result
                row[phase] = {'mass': p_mass, 'group': p_group, 'interaction': p_int}
        ancova_rows.append(row)

    anova_rows = []
    for var_col, label in _ANOVA_VARS:
        if var_col not in df.columns:
            continue
        row = {'variable': var_col, 'label': label}
        for phase, phase_df in phase_dfs.items():
            subj = _aggregate_subjects(phase_df, var_col, mass_variable)
            p_group = _fit_anova_period(subj)
            row[phase] = {'group': p_group}
        anova_rows.append(row)

    return {
        'mass_variable': mass_variable,
        'ancova': ancova_rows,
        'anova': anova_rows,
    }

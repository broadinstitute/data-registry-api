"""
CalR statistical analysis functions, ported from the R calr package.
"""
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats


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
) -> dict:
    """
    Port of the CalR quality control analysis.

    For each subject, computes:
      - mass_delta: average of last N body mass readings minus average of first N
      - total_eb:   total cumulative energy balance (total feed - total EE) over
                    the time window

    Then fits per-group and overall linear regressions of mass_delta (x) vs
    total_eb (y). A well-controlled experiment should show a strong positive
    correlation — subjects that lost mass should have a negative energy balance.

    Expects df to have columns: subject.id, group, subject.mass, feed, ee,
    ordered by time (exp.hour ascending).

    Returns:
        subjects            - per-subject [subject_id, group, mass_delta, total_eb]
        group_regressions   - per-group {slope, intercept, r_squared, n}
        overall_regression  - {slope, intercept, r_squared, n}
    """
    subject_rows = []

    for subject_id, sdf in df.groupby('subject.id'):
        sdf = sdf.sort_values('exp.hour')
        group = sdf['group'].iloc[0]

        n = min(n_mass_measurements, len(sdf))
        first_mass = float(sdf['subject.mass'].iloc[:n].mean())
        last_mass = float(sdf['subject.mass'].iloc[-n:].mean())
        mass_delta = round(last_mass - first_mass, 4)

        total_feed = float(sdf['feed'].sum())
        total_ee = float(sdf['ee'].sum())
        total_eb = round(total_feed - total_ee, 4)

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

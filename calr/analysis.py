"""
CalR statistical analysis functions, ported from the R calr package.
"""
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats
from statsmodels.stats.anova import anova_lm
from typing import Optional


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
    ee, ee.acc, exp.minute — sorted by time within each subject. ee.acc is
    expected to be the plain cumulative sum of ee (post-_enrich_df); this fn
    applies the /bin scaling itself.

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
    # Cascade: exp.minute → Date.Time → exp.hour (non-zero diffs only) → default 60 min
    def _modal_interval_minutes(df: pd.DataFrame) -> float:
        # exp.minute: reliable when present and has variation
        if 'exp.minute' in df.columns:
            diffs = df.groupby('subject.id')['exp.minute'].diff().dropna()
            nonzero = diffs[diffs > 0]
            if not nonzero.empty:
                return float(nonzero.mode().iloc[0])

        # Date.Time: parse timestamps and diff in minutes
        if 'Date.Time' in df.columns:
            try:
                times = pd.to_datetime(df['Date.Time'])
                diffs = times.groupby(df['subject.id']).diff().dropna()
                diffs_min = diffs.dt.total_seconds() / 60
                nonzero = diffs_min[diffs_min > 0]
                if not nonzero.empty:
                    return float(nonzero.mode().iloc[0])
            except Exception:
                pass

        # exp.hour: integer or decimal — only non-zero diffs, convert to minutes
        if 'exp.hour' in df.columns:
            diffs = df.groupby('subject.id')['exp.hour'].diff().dropna()
            nonzero = diffs[diffs > 0]
            if not nonzero.empty:
                return float(nonzero.mode().iloc[0]) * 60

        return 60.0  # fallback: assume hourly measurements

    modal_interval = _modal_interval_minutes(df)
    bin_factor = 60.0 / modal_interval  # intervals per hour

    # Determine sort column: prefer exp.minute (numeric, unambiguous), then Date.Time
    # (stable chronological order), then exp.hour.  When exp.hour is an integer, many
    # rows share the same value and an unstable sort scrambles them within the hour.
    if 'exp.minute' in df.columns:
        sort_col = 'exp.minute'
    elif 'Date.Time' in df.columns:
        sort_col = 'Date.Time'
    else:
        sort_col = 'exp.hour'

    subject_rows = []

    for subject_id, sdf in df.groupby('subject.id'):
        sdf = sdf.sort_values(sort_col, kind='stable')
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


def _shieh_ancova_power(
    mu: list, n_per_group: int, n_cov: int, r2: float, sd: float, alpha: float
) -> float:
    """
    Exact one-way ANCOVA power via Shieh's formula. Port of
    Superpower::pwr_ancova_shieh (type="exact"), which the legacy CalR R app
    uses for its power curves.

    Integrates the non-central F survival function over the t-distribution
    of the covariate's contribution (Simpson's rule, 2000 intervals), so it
    accounts for the randomness of the covariate adjustment — unlike the
    closed-form `n × ss_means / σ²(1−R²)` approximation, which fixes the
    covariate effect.
    """
    n_grp = len(mu)
    nvec = np.full(n_grp, n_per_group, dtype=float)
    var_e = sd ** 2 * (1.0 - r2)
    if var_e <= 0:
        return float('nan')

    # Sum-to-zero contrast matrix, (n_grp - 1) x n_grp, matching R's
    # t(contr.sum(n_grp)): rows are [I_{n_grp-1}, -1].
    cmat = np.zeros((n_grp - 1, n_grp))
    for i in range(n_grp - 1):
        cmat[i, i] = 1.0
        cmat[i, n_grp - 1] = -1.0

    cmu = cmat @ np.asarray(mu, dtype=float).reshape(-1, 1)
    num_df = cmat.shape[0]
    N_tot = float(nvec.sum())
    qmat = np.diag(N_tot / nvec)
    A = cmat @ qmat @ cmat.T
    quad = cmu.T @ np.linalg.inv(A) @ cmu
    l_gamma2 = float(np.asarray(quad).item()) / var_e

    den_df = N_tot - n_grp - n_cov
    if den_df <= 0:
        return float('nan')
    dfx = den_df + 1
    b = n_cov / dfx
    fcrit = stats.f.ppf(1.0 - alpha, num_df, den_df)

    numint = 2000
    dd = 1e-5
    # Simpson's 1/3 weights: 1, 4, 2, 4, 2, ..., 4, 1
    coevec = np.empty(numint + 1)
    coevec[0] = 1
    coevec[-1] = 1
    coevec[1:-1:2] = 4
    coevec[2:-1:2] = 2

    if n_cov == 1:
        tl = stats.t.ppf(dd, dfx)
        tu = stats.t.ppf(1 - dd, dfx)
        intl = (tu - tl) / numint
        tvec = tl + intl * np.arange(numint + 1)
        wtpdf = (intl / 3) * coevec * stats.t.pdf(tvec, dfx)
        ncp = N_tot * l_gamma2 / (1.0 + b * tvec ** 2)
        pow_ = float(np.sum(wtpdf * stats.ncf.sf(fcrit, num_df, den_df, ncp)))
    else:
        bl = dd
        bu = 1 - dd
        intl = (bu - bl) / numint
        bvec = bl + intl * np.arange(numint + 1)
        wbpdf = (intl / 3) * coevec * stats.beta.pdf(bvec, dfx / 2, n_cov / 2)
        ncp = N_tot * l_gamma2 * bvec
        pow_ = float(np.sum(wbpdf * stats.ncf.sf(fcrit, num_df, den_df, ncp)))

    # Clamp tiny numerical excursions
    return max(0.0, min(1.0, pow_))


def power_calc(
    df: pd.DataFrame,
    variable: str,
    mass_variable: str,
    sample_sizes: list[int],
    alpha: float = 0.05,
    group_diet_kcal: dict = None,
) -> dict:
    """
    Port of R AncovaReadyStats() + PowerCalc().

    Computes per-group summary statistics and a power curve across the given
    sample sizes. Method is auto-selected:
      - ANCOVA for 'ee', 'feed', 'feed.acc' (mass reduces residual variance)
      - ANOVA for all other variables

    group_diet_kcal: optional {group_name: kcal_per_g}. When provided, feed
    and feed.acc are scaled per group (mirrors R: feed *= cal_i) before
    fitting, so analysis is on kcal not grams.

    Returns a dict with:
        method          - 'ancova' or 'anova'
        effect_size     - {'r_squared': float} or {'eta_squared': float}
        overall_sd      - pooled SD across all observations
        group_stats     - per-group n, mean, variance
        power_curve     - [{'n_per_group': int, 'power': float}, ...]
    """
    if group_diet_kcal:
        df = df.copy()
        for group_name, kcal_per_g in group_diet_kcal.items():
            if not kcal_per_g:
                continue
            mask = df['group'] == group_name
            for col in ('feed', 'feed.acc'):
                if col in df.columns:
                    df.loc[mask, col] = (
                        pd.to_numeric(df.loc[mask, col], errors='coerce') * kcal_per_g
                    )

    method = 'ancova' if variable in ANCOVA_VARIABLES else 'anova'

    # Aggregate to per-subject means before fitting (mirrors R's PowerCalc, which
    # operates on subject-level summaries). Without this we'd be fitting OLS on
    # ~thousands of rows per subject, inflating degrees of freedom and producing
    # nonsensical power curves.
    subj = _aggregate_subjects(df, variable, mass_variable)

    k = subj['group'].nunique()
    overall_sd = float(subj['var'].std())

    # Per-group stats
    group_stats = {}
    for group, gdf in subj.groupby('group'):
        group_stats[group] = {
            'n': len(gdf['subject.id'].unique()),
            'mean': round(float(gdf['var'].mean()), 6),
            'variance': round(float(gdf['var'].var()), 6),
        }

    group_means = [group_stats[g]['mean'] for g in sorted(group_stats)]

    # Effect size — fit on per-subject means
    if method == 'ancova':
        model = smf.ols('var ~ mass + C(group)', data=subj).fit()
        r_squared = float(model.rsquared)
        effect_size = {'r_squared': round(r_squared, 6)}
    else:
        from statsmodels.stats.anova import anova_lm
        model = smf.ols('var ~ C(group)', data=subj).fit()
        anova_table = anova_lm(model, typ=1)
        ss_group = float(anova_table['sum_sq'].iloc[0])
        ss_total = float(anova_table['sum_sq'].sum())
        eta2 = ss_group / ss_total if ss_total > 0 else 0.0
        effect_size = {'eta_squared': round(eta2, 6)}

    # Power curve
    power_curve = []
    for n in sample_sizes:
        if method == 'ancova':
            # Use Shieh's exact formula (matches Superpower::power_oneway_ancova),
            # which integrates over the covariate's t-distribution rather than
            # treating the covariate adjustment as fixed.
            try:
                power = _shieh_ancova_power(
                    mu=group_means, n_per_group=int(n), n_cov=1,
                    r2=r_squared, sd=overall_sd, alpha=alpha,
                )
            except Exception:
                power = None
        else:
            # Closed-form Cohen's f² → non-central F approach for ANOVA.
            N = n * k
            df1 = k - 1
            df2 = N - k
            if df2 <= 0:
                power_curve.append({'n_per_group': n, 'power': None})
                continue
            f2 = eta2 / (1 - eta2) if 0 < eta2 < 1 else (1.0 if eta2 >= 1 else 0.0)
            lambda_ = N * f2
            f_crit = stats.f.ppf(1 - alpha, df1, df2)
            power = float(stats.ncf.sf(f_crit, df1, df2, nc=lambda_))
        if power is None:
            power_curve.append({'n_per_group': n, 'power': None})
        else:
            power_curve.append({'n_per_group': n, 'power': round(float(power), 4)})

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
    ('xyamb',     'Ambulatory Activity (beam breaks)'),
    ('body.temp', 'Body Temperature (Celsius)'),
    ('wheel',     'Wheel Running'),
    ('wheel.acc', 'Wheel Running Accumulated'),
    ('eb',        'Energy Balance (kcal/period)'),   # computed: feed - ee
]


def _analysis_light_flag(
    df: pd.DataFrame,
    light_cycle_start: int,
    dark_cycle_start: int,
) -> pd.Series:
    """Legacy CalR phase flag: use timestamp clock hour when available."""
    if 'clockHour' in df.columns:
        clock_hour = pd.to_numeric(df['clockHour'], errors='coerce')
    elif 'hour' in df.columns:
        hour_ts = pd.to_datetime(df['hour'], errors='coerce')
        clock_hour = hour_ts.dt.hour + hour_ts.dt.minute / 60.0
    elif 'Date.Time' in df.columns:
        date_time = pd.to_datetime(df['Date.Time'], errors='coerce')
        clock_hour = date_time.dt.hour + date_time.dt.minute / 60.0
    else:
        clock_hour = pd.to_numeric(df['exp.hour'], errors='coerce') % 24

    if light_cycle_start < dark_cycle_start:
        in_light = (clock_hour >= light_cycle_start) & (clock_hour < dark_cycle_start)
    else:
        # Matches revperAve(): wrapped light cycles are light outside dark..light.
        in_light = ~((clock_hour < light_cycle_start) & (clock_hour >= dark_cycle_start))
    return in_light.astype(float)


def _modal_interval_minutes(df: pd.DataFrame) -> float:
    """Best-effort measurement interval; mirrors the API enrichment fallback order."""
    if 'exp.minute' in df.columns:
        minute_df = pd.DataFrame({
            'subject.id': df['subject.id'].astype(str),
            'exp.minute': pd.to_numeric(df['exp.minute'], errors='coerce'),
        }).dropna(subset=['exp.minute']).sort_values(['subject.id', 'exp.minute'])
        diffs = minute_df.groupby('subject.id')['exp.minute'].diff().dropna()
        nonzero = diffs[diffs > 0]
        if not nonzero.empty:
            return float(nonzero.mode().iloc[0])

    if 'Date.Time' in df.columns:
        time_df = pd.DataFrame({
            'subject.id': df['subject.id'].astype(str),
            'Date.Time': pd.to_datetime(df['Date.Time'], errors='coerce'),
        }).dropna(subset=['Date.Time']).sort_values(['subject.id', 'Date.Time'])
        diffs = time_df.groupby('subject.id')['Date.Time'].diff().dropna()
        diffs_min = diffs.dt.total_seconds() / 60
        nonzero = diffs_min[diffs_min > 0]
        if not nonzero.empty:
            return float(nonzero.mode().iloc[0])

    if 'exp.hour' in df.columns:
        hour_df = pd.DataFrame({
            'subject.id': df['subject.id'].astype(str),
            'exp.hour': pd.to_numeric(df['exp.hour'], errors='coerce'),
        }).dropna(subset=['exp.hour']).sort_values(['subject.id', 'exp.hour'])
        diffs = hour_df.groupby('subject.id')['exp.hour'].diff().dropna()
        nonzero = diffs[diffs > 0]
        if not nonzero.empty:
            return float(nonzero.mode().iloc[0]) * 60

    return 60.0


def _zero_base_accumulators(df: pd.DataFrame) -> pd.DataFrame:
    """Port the analysis-relevant part of fixFeed(): start cumulative cols at 0."""
    df = df.copy()
    accum_cols = ['feed.acc', 'drink.acc', 'wheel.acc', 'ee.acc', 'pedmeter', 'allmeter']
    if 'xytot' in df.columns:
        xytot = pd.to_numeric(df['xytot'], errors='coerce').dropna().drop_duplicates()
        diffs = xytot.diff().dropna()
        if not diffs.empty:
            n_increasing = int((diffs > 0).sum())
            n_non_increasing = int((diffs <= 0).sum())
            if n_increasing and (n_non_increasing / n_increasing) <= 0.5:
                accum_cols.append('xytot')

    sort_cols = [c for c in ('subject.id', 'exp.minute', 'Date.Time', 'exp.hour') if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, kind='stable')

    for _, idx in df.groupby('subject.id', sort=False).groups.items():
        for col in accum_cols:
            if col not in df.columns:
                continue
            vals = pd.to_numeric(df.loc[idx, col], errors='coerce')
            first = vals.dropna().iloc[0] if vals.notna().any() else np.nan
            if pd.notna(first):
                df.loc[idx, col] = vals - first
    return df


def apply_legacy_analysis_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply CalR's default rmOutliers pass used by Analysis when requested.

    The legacy R path removes values outside abs(mean) +/- 3 SD for VO2, VCO2,
    EE, RER, and body temperature. If any respiratory channel is removed for a
    row, CalR removes the whole VO2/VCO2/EE/RER set for that row, then rebuilds
    cumulative columns from the remaining source values.
    """
    if df.empty:
        return df.copy()

    out = df.copy()
    rm_cols = [c for c in ['vo2', 'vco2', 'ee', 'rer', 'body.temp'] if c in out.columns]
    for col in rm_cols:
        values = pd.to_numeric(out[col], errors='coerce')
        mean = abs(values.mean(skipna=True))
        sd = values.std(skipna=True, ddof=1)
        if pd.isna(mean) or pd.isna(sd):
            continue
        out.loc[(values > mean + 3 * sd) | (values < mean - 3 * sd), col] = np.nan

    respiratory_cols = [c for c in ['vo2', 'vco2', 'ee', 'rer'] if c in out.columns]
    if respiratory_cols:
        removed = out[respiratory_cols].isna().any(axis=1)
        out.loc[removed, respiratory_cols] = np.nan

    sort_cols = [c for c in ['subject.id', 'exp.minute', 'Date.Time', 'exp.hour'] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, kind='stable')

    for _, idx in out.groupby('subject.id', sort=False).groups.items():
        for acc_col, source_col in [
            ('feed.acc', 'feed'),
            ('ee.acc', 'ee'),
            ('drink.acc', 'drink'),
            ('wheel.acc', 'wheel'),
        ]:
            if acc_col not in out.columns or source_col not in out.columns:
                continue
            source = pd.to_numeric(out.loc[idx, source_col], errors='coerce').fillna(0)
            out.loc[idx, acc_col] = source.cumsum().values

    return out


def prepare_analysis_hourly_rows(
    df: pd.DataFrame,
    light_cycle_start: int = 6,
    dark_cycle_start: int = 18,
) -> pd.DataFrame:
    """
    Build the per-subject hourly dataframe used by legacy CalR's anovaTab().

    Mirrors revmodCalDataSet(config=1, per="hour", grp=FALSE) after the API
    has already enriched group metadata and kcal/cutoff columns. This keeps
    Analysis parity isolated from the shared plot data path.
    """
    if df.empty:
        return df.copy()

    out = df.copy()
    out['light'] = _analysis_light_flag(out, light_cycle_start, dark_cycle_start)

    if 'hour' not in out.columns or out['hour'].isna().all():
        if 'Date.Time' in out.columns:
            out['hour'] = pd.to_datetime(out['Date.Time'], errors='coerce').dt.floor('h')
        else:
            out['hour'] = pd.to_numeric(out['exp.hour'], errors='coerce')
    if 'day' not in out.columns or out['day'].isna().all():
        if 'Date.Time' in out.columns:
            out['day'] = pd.to_datetime(out['Date.Time'], errors='coerce').dt.floor('D')
        elif 'exp.day' in out.columns:
            out['day'] = pd.to_numeric(out['exp.day'], errors='coerce')
        else:
            out['day'] = np.floor(pd.to_numeric(out['exp.hour'], errors='coerce') / 24)

    out = _zero_base_accumulators(out)
    bin_factor = 60.0 / _modal_interval_minutes(out)

    keys = ['subject.id', 'group', 'day', 'light', 'hour']
    for key in keys:
        if key not in out.columns:
            out[key] = np.nan

    numeric_cols = []
    for col in out.columns:
        if col in keys:
            continue
        converted = pd.to_numeric(out[col], errors='coerce')
        if converted.notna().any():
            out[col] = converted
            numeric_cols.append(col)

    special_cols = {'feed', 'feed.acc', 'drink', 'drink.acc', 'ee', 'ee.acc', 'pedmeter', 'allmeter'}
    average_cols = [c for c in numeric_cols if c not in special_cols]
    frames = []

    if average_cols:
        avg = (
            out[keys + average_cols]
            .assign(**{c: pd.to_numeric(out[c], errors='coerce') for c in average_cols})
            .groupby(keys, dropna=False)
            .mean(numeric_only=True)
            .reset_index()
        )
        frames.append(avg)
    else:
        frames.append(out[keys].drop_duplicates())

    sum_cols = [c for c in ['feed', 'drink', 'pedmeter', 'allmeter'] if c in out.columns]
    if sum_cols:
        sums = (
            out[keys + sum_cols]
            .assign(**{c: pd.to_numeric(out[c], errors='coerce') for c in sum_cols})
            .groupby(keys, dropna=False)
            .sum(min_count=1, numeric_only=True)
            .reset_index()
        )
        frames.append(sums)

    max_cols = [c for c in ['feed.acc', 'drink.acc', 'ee.acc'] if c in out.columns]
    if max_cols:
        maxes = (
            out[keys + max_cols]
            .assign(**{c: pd.to_numeric(out[c], errors='coerce') for c in max_cols})
            .groupby(keys, dropna=False)
            .max(numeric_only=True)
            .reset_index()
        )
        frames.append(maxes)

    if 'ee' in out.columns:
        ee = (
            out[keys + ['ee']]
            .assign(ee=pd.to_numeric(out['ee'], errors='coerce'))
            .groupby(keys, dropna=False)
            .mean(numeric_only=True)
            .reset_index()
        )
        frames.append(ee)

    hourly = frames[0]
    for frame in frames[1:]:
        hourly = hourly.merge(frame, on=keys, how='outer')

    if 'ee.acc' in hourly.columns and bin_factor:
        hourly['ee.acc'] = pd.to_numeric(hourly['ee.acc'], errors='coerce') / bin_factor
    if 'feed' in hourly.columns and 'ee' in hourly.columns:
        hourly['eb'] = pd.to_numeric(hourly['feed'], errors='coerce') - pd.to_numeric(hourly['ee'], errors='coerce')
    if 'feed.acc' in hourly.columns and 'ee.acc' in hourly.columns:
        hourly['eb.acc'] = pd.to_numeric(hourly['feed.acc'], errors='coerce') - pd.to_numeric(hourly['ee.acc'], errors='coerce')

    return hourly


def _aggregate_subjects(df: pd.DataFrame, var_col: str, mass_col: str) -> pd.DataFrame:
    """Per-subject means of mass and var, mirroring calR's ddply(group, subject.id, mean)."""
    agg = (
        df.groupby(['group', 'subject.id'])
        .agg(mass=(mass_col, 'mean'), var=(var_col, 'mean'))
        .reset_index()
        .dropna(subset=['mass', 'var'])
    )
    return agg


def _has_modelable_values(df: pd.DataFrame, var_col: str) -> bool:
    if var_col not in df.columns:
        return False
    values = pd.to_numeric(df[var_col], errors='coerce').dropna()
    return not values.empty and values.nunique() > 1


def _ordered_groups(subj_df: pd.DataFrame, group_order: Optional[list] = None) -> list:
    """Return present groups in configured order, then any remaining groups."""
    present = [g for g in (group_order or []) if g in set(subj_df['group'].dropna())]
    present_set = set(present)
    present.extend([g for g in subj_df['group'].dropna().unique() if g not in present_set])
    return present


def _comparison_key(group_name: str, reference_group: str) -> str:
    return f'{group_name} vs {reference_group}'


def _group_term_pvalues(model, groups: list, reference_group: str, interaction: bool = False) -> dict:
    """Extract coefficient p-values for each non-reference group."""
    out = {}
    for group_name in groups:
        if group_name == reference_group:
            continue
        marker = f'[T.{group_name}]'
        for name, p in model.pvalues.items():
            is_interaction = ':' in name
            if marker in name and is_interaction == interaction:
                out[_comparison_key(group_name, reference_group)] = round(float(p), 4)
                break
    return out


def _fit_ancova_period(
    subj_df: pd.DataFrame,
    group_order: Optional[list] = None,
    reference_group: Optional[str] = None,
    ordered_groups: bool = False,
):
    """
    Fit var ~ mass + C(group) + mass:C(group).
    If interaction p > 0.05, re-fit without interaction.
    Returns a dict containing the mass p-value and per-comparison group /
    interaction p-values. Interaction p-values are None when the model is
    re-fit without the interaction term.
    Returns None when there is insufficient data.

    Uses Wald T-test p-values from the model coefficients (matches the legacy
    R app's `summary(glm(...))$coefficients[..., 4]` extraction). With 3+ groups,
    each non-reference factor coefficient is a reference-vs-group comparison.
    """
    groups = _ordered_groups(subj_df, group_order)
    if reference_group in groups:
        groups = [reference_group] + [g for g in groups if g != reference_group]
    reference_group = groups[0] if groups else None
    n_groups = len(groups)
    if n_groups < 2 or len(subj_df) <= n_groups + 2:
        return None

    def _coef_p(model, predicate):
        for name, p in model.pvalues.items():
            if predicate(name):
                return float(p)
        return None

    subj_df = subj_df.copy()
    if ordered_groups:
        group_to_number = {group: i + 1 for i, group in enumerate(groups)}
        subj_df['group'] = subj_df['group'].map(group_to_number)
        trend_key = 'Group'
        try:
            m_full = smf.ols('var ~ mass + group + mass:group', data=subj_df).fit()
            interaction_p = _coef_p(m_full, lambda n: n == 'mass:group')
            if interaction_p is None:
                return None
            if interaction_p > 0.05:
                m_noint = smf.ols('var ~ mass + group', data=subj_df).fit()
                p_mass = _coef_p(m_noint, lambda n: n == 'mass')
                p_group = _coef_p(m_noint, lambda n: n == 'group')
                if p_mass is None or p_group is None:
                    return None
                return {
                    'mass': round(p_mass, 4),
                    'group': {trend_key: round(p_group, 4)},
                    'interaction': {trend_key: None},
                }
            p_mass = _coef_p(m_full, lambda n: n == 'mass')
            p_group = _coef_p(m_full, lambda n: n == 'group')
            if p_mass is None or p_group is None:
                return None
            return {
                'mass': round(p_mass, 4),
                'group': {trend_key: round(p_group, 4)},
                'interaction': {trend_key: round(interaction_p, 4)},
            }
        except Exception:
            return None

    subj_df['group'] = pd.Categorical(subj_df['group'], categories=groups, ordered=True)
    comparisons = [_comparison_key(g, reference_group) for g in groups if g != reference_group]

    try:
        m_full = smf.ols('var ~ mass + C(group) + mass:C(group)', data=subj_df).fit()
        interaction_p = _group_term_pvalues(m_full, groups, reference_group, interaction=True)
        if not interaction_p:
            return None

        if all(p > 0.05 for p in interaction_p.values()):
            m_noint = smf.ols('var ~ mass + C(group)', data=subj_df).fit()
            p_mass = _coef_p(m_noint, lambda n: n == 'mass')
            group_p = _group_term_pvalues(m_noint, groups, reference_group)
            if p_mass is None or not group_p:
                return None
            return {
                'mass': round(p_mass, 4),
                'group': group_p,
                'interaction': {comparison: None for comparison in comparisons},
            }
        else:
            p_mass = _coef_p(m_full, lambda n: n == 'mass')
            group_p = _group_term_pvalues(m_full, groups, reference_group)
            if p_mass is None or not group_p:
                return None
            return {
                'mass': round(p_mass, 4),
                'group': group_p,
                'interaction': interaction_p,
            }
    except Exception:
        return None


def _fit_anova_period(
    subj_df: pd.DataFrame,
    group_order: Optional[list] = None,
    reference_group: Optional[str] = None,
    ordered_groups: bool = False,
):
    """
    Fit var ~ C(group). Returns Wald T p-values for each non-reference group
    coefficient (matches legacy R's `summary(glm)$coefficients[-1, 4]`), or
    None when there is insufficient data.

    For binary group, this equals the F-test p-value from anova_lm.
    """
    groups = _ordered_groups(subj_df, group_order)
    if reference_group in groups:
        groups = [reference_group] + [g for g in groups if g != reference_group]
    reference_group = groups[0] if groups else None
    n_groups = len(groups)
    if n_groups < 2 or len(subj_df) <= n_groups:
        return None

    subj_df = subj_df.copy()
    if ordered_groups:
        group_to_number = {group: i + 1 for i, group in enumerate(groups)}
        subj_df['group'] = subj_df['group'].map(group_to_number)
        try:
            m = smf.ols('var ~ group', data=subj_df).fit()
            p_group = m.pvalues.get('group')
            if p_group is None:
                return None
            return {'Group': round(float(p_group), 4)}
        except Exception:
            return None

    subj_df['group'] = pd.Categorical(subj_df['group'], categories=groups, ordered=True)

    try:
        m = smf.ols('var ~ C(group)', data=subj_df).fit()
        group_p = _group_term_pvalues(m, groups, reference_group)
        return group_p or None
    except Exception:
        return None


def ancova_table(
    df: pd.DataFrame,
    mass_variable: str = 'subject.mass',
    light_cycle_start: int = 6,
    dark_cycle_start: int = 18,
    group_diet_kcal: dict = None,
    group_order: Optional[list] = None,
    reference_group: Optional[str] = None,
    selected_hour_count: Optional[float] = None,
    ordered_groups: bool = False,
    remove_outliers: bool = False,
) -> dict:
    """
    Compute the summary ANCOVA/ANOVA table, mirroring anovaTab() from calR.

    For each variable in _ANCOVA_VARS, runs:
        var ~ mass + C(group) + mass:C(group)
    and if the interaction p-value > 0.05 drops the interaction term, mirroring the
    R code that tests ``a$coefficients[nrow(a$coefficients), 4] > 0.05``.
    With 3+ groups, each non-reference group coefficient is returned as a
    comparison against `reference_group`.

    For each variable in _ANOVA_VARS, runs:
        var ~ C(group)

    Both analyses are run for three time periods: full_day, light, dark.
    Energy balance (eb) is computed on the fly as feed − ee when absent from df.

    Returns
    -------
    {
      "mass_variable": str,
      "reference_group": str,
      "comparisons": [{"key": str, "group": str, "reference": str, "label": str}],
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
      ],
      "ancova_pairwise": [
        {"comparison": str, "label": str, "group": str, "reference": str, "rows": [...]}
      ],
      "anova_pairwise": [
        {"comparison": str, "label": str, "group": str, "reference": str, "rows": [...]}
      ]
    }
    """
    # `df` is expected to be enriched before analysis, including CalR-style
    # feed kcal conversion. Build the legacy per-subject hourly table locally
    # so Analysis parity does not affect other plot endpoints.
    if remove_outliers:
        df = apply_legacy_analysis_outliers(df)

    df = prepare_analysis_hourly_rows(
        df,
        light_cycle_start=light_cycle_start,
        dark_cycle_start=dark_cycle_start,
    )

    groups = _ordered_groups(df, group_order)
    if reference_group in groups:
        groups = [reference_group] + [g for g in groups if g != reference_group]
    reference_group = groups[0] if groups else None
    if ordered_groups and len(groups) > 2:
        comparisons = [{
            'key': 'Group',
            'label': 'Group',
            'group': 'Group',
            'reference': reference_group,
        }]
    else:
        comparisons = [
            {
                'key': _comparison_key(group_name, reference_group),
                'label': _comparison_key(group_name, reference_group),
                'group': group_name,
                'reference': reference_group,
            }
            for group_name in groups
            if group_name != reference_group
        ]
    comparison_keys = [comparison['key'] for comparison in comparisons]

    # Phase subsets
    phase_dfs = {
        'full_day': df,
    }
    if selected_hour_count is None or selected_hour_count > 12:
        phase_dfs['light'] = df[df['light'] == 1]
        phase_dfs['dark'] = df[df['light'] == 0]

    ancova_rows = []
    ancova_pairwise_rows = {comparison: [] for comparison in comparison_keys}
    for var_col, label in _ANCOVA_VARS:
        if not _has_modelable_values(df, var_col):
            continue
        row: dict = {'variable': var_col, 'label': label}
        pair_rows = {
            comparison: {'variable': var_col, 'label': label}
            for comparison in comparison_keys
        }
        for phase, phase_df in phase_dfs.items():
            if not _has_modelable_values(phase_df, var_col):
                result = None
            else:
                subj = _aggregate_subjects(phase_df, var_col, mass_variable)
                result = _fit_ancova_period(
                    subj,
                    group_order=groups,
                    reference_group=reference_group,
                    ordered_groups=ordered_groups and len(groups) > 2,
                )
            if result is None:
                row[phase] = {'mass': None, 'group': None, 'interaction': None}
                for comparison in comparison_keys:
                    pair_rows[comparison][phase] = {'mass': None, 'group': None, 'interaction': None}
            else:
                first_comparison = comparison_keys[0] if comparison_keys else None
                row[phase] = {
                    'mass': result['mass'],
                    'group': result['group'].get(first_comparison) if first_comparison else None,
                    'interaction': result['interaction'].get(first_comparison) if first_comparison else None,
                }
                for comparison in comparison_keys:
                    pair_rows[comparison][phase] = {
                        'mass': result['mass'],
                        'group': result['group'].get(comparison),
                        'interaction': result['interaction'].get(comparison),
                    }
        ancova_rows.append(row)
        for comparison, pair_row in pair_rows.items():
            ancova_pairwise_rows[comparison].append(pair_row)

    anova_rows = []
    anova_pairwise_rows = {comparison: [] for comparison in comparison_keys}
    for var_col, label in _ANOVA_VARS:
        if not _has_modelable_values(df, var_col):
            continue
        row = {'variable': var_col, 'label': label}
        pair_rows = {
            comparison: {'variable': var_col, 'label': label}
            for comparison in comparison_keys
        }
        for phase, phase_df in phase_dfs.items():
            if not _has_modelable_values(phase_df, var_col):
                group_p = None
            else:
                subj = _aggregate_subjects(phase_df, var_col, mass_variable)
                group_p = _fit_anova_period(
                    subj,
                    group_order=groups,
                    reference_group=reference_group,
                    ordered_groups=ordered_groups and len(groups) > 2,
                )
            first_comparison = comparison_keys[0] if comparison_keys else None
            row[phase] = {'group': group_p.get(first_comparison) if group_p and first_comparison else None}
            for comparison in comparison_keys:
                pair_rows[comparison][phase] = {'group': group_p.get(comparison) if group_p else None}
        anova_rows.append(row)
        for comparison, pair_row in pair_rows.items():
            anova_pairwise_rows[comparison].append(pair_row)

    return {
        'mass_variable': mass_variable,
        'reference_group': reference_group,
        'comparisons': comparisons,
        'ancova': ancova_rows,
        'anova': anova_rows,
        'ancova_pairwise': [
            {
                'comparison': comparison['key'],
                'label': comparison['label'],
                'group': comparison['group'],
                'reference': comparison['reference'],
                'rows': ancova_pairwise_rows[comparison['key']],
            }
            for comparison in comparisons
        ],
        'anova_pairwise': [
            {
                'comparison': comparison['key'],
                'label': comparison['label'],
                'group': comparison['group'],
                'reference': comparison['reference'],
                'rows': anova_pairwise_rows[comparison['key']],
            }
            for comparison in comparisons
        ],
    }

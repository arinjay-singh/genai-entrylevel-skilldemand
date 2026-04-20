# Notebooks

This directory contains the analysis notebook that produces all tables, figures, and regression estimates reported in the paper.

## Files

| File | Description |
|---|---|
| `econ_skill_analysis.ipynb` | Main analysis notebook: loads the aggregated data, constructs the balanced panel, runs the difference-in-differences regressions, and generates figures |

## Notebook Structure

The notebook is organized to follow the paper's empirical flow:

1. **Data loading** — Reads the four annual skill-frequency CSVs from `../data/`
2. **Panel construction** — Concatenates yearly data and builds a balanced skill × occupation × year panel (279 × 19 × 4 = 21,204 observations)
3. **Treatment assignment** — Classifies the 279 skills into 162 high AI-substitutable (treatment) and 117 low AI-substitutable (control) groups
4. **Descriptive statistics** — Produces Table 1 and the sample composition table (Table A1)
5. **Visual diagnostics** — Generates Figure 1 (parallel trends) and Figure 2 (AI exposure by occupation)
6. **Main DiD regressions** — Estimates the five specifications in Table 2 (base, occupation FE, year FE, two-way FE, Post = 2024)
7. **Event study** — Estimates year-specific interaction terms to produce Table 3
8. **Per-occupation regressions** — Runs the 19 occupation-level DiD regressions for Table A2, and produces the forest plot (Figure 3)
9. **Diagnostics** — VIF calculations for the main specification

## Outputs Produced

Running the notebook top to bottom reproduces:

- All tables in the paper (Tables 1–3, A1, A2)
- All figures in the paper (Figures 1–3)
- All regression coefficients, standard errors, and p-values cited in the text

## Requirements

```
python >= 3.9
pandas
numpy
statsmodels
matplotlib
```

Install with:

```bash
pip install pandas numpy statsmodels matplotlib jupyter
```

## Running

From the repository root:

```bash
jupyter notebook notebooks/econ_skill_analysis.ipynb
```

All cells execute top to bottom without manual intervention. The data files in `../data/` must be present.

## Key Specification

The main two-way fixed effects regression is estimated using `statsmodels`:

```python
import statsmodels.formula.api as smf

model = smf.ols(
    'pct ~ treated + treated_post + C(occupation_group) + C(year)',
    data=panel
).fit(
    cov_type='cluster',
    cov_kwds={'groups': panel['occupation_group']}
)
```

Standard errors are clustered at the occupation level throughout. See the paper's Section 4 for the full identification strategy.

## Notes

- The notebook uses `pct` (skill frequency as a percentage, 0–100) as the dependent variable. No log or other transformation is applied.
- The `treated_post` interaction term is constructed manually as `treated × post` rather than via a formula interaction, to make the coefficient of interest (β₃) easier to read in regression output.
- The per-occupation regressions run with the same formula as the main specification but subset to one occupation at a time, yielding 19 separate β₃ estimates.
- If cell execution fails at the panel construction step, verify that all four CSVs in `../data/` have the expected schema (see `../data/README.md`).

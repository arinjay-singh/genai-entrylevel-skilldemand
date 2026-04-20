# Generative Artificial Intelligence and Entry-Level Skill Demand

**An Empirical Analysis of ChatGPT's Impact on the Labor Market**

Arinjay Singh · Northeastern University · ECON 4692 Senior Economics Seminar · Spring 2026

---

## Abstract

This paper examines whether ChatGPT's November 2022 release shifted employer demand for AI-substitutable skills in entry-level job postings. Using a skill-occupation-year panel of 21,204 observations constructed from the Web Data Commons JobPosting Corpus, I estimate a difference-in-differences specification comparing 162 high AI-substitutable skills to 117 low AI-substitutable skills across 19 occupation groups from 2021 to 2024. The pooled DiD estimate is a precisely estimated null, but an event study reveals a statistically significant decline in 2024 with no pre-trend in 2022 and no response in 2023. The pattern is consistent with a diffusion lag in which employers revised skill requirements only after enterprise GenAI adoption matured.

## Key Findings

| Specification | β₃ | Std. Err. | p-value |
|---|---|---|---|
| Pooled DiD (Post = 2023–2024) | **−0.009** | 0.119 | 0.940 |
| DiD with Post = 2024 only | **−0.216** | 0.114 | 0.059 |
| Event study: Treated × 2022 (pre-trend) | −0.109 | 0.108 | 0.312 |
| Event study: Treated × 2023 | +0.094 | 0.205 | 0.647 |
| **Event study: Treated × 2024** | **−0.221** | **0.112** | **0.048** |

- The **pooled aggregate effect is null** (β₃ = −0.009, p = 0.940). GenAI's effect on entry-level skill demand was not immediately observable in the year following ChatGPT's release.
- The **event study reveals the effect concentrates in 2024** (β₃ = −0.221, p = 0.048), the first year in which a statistically significant decline in AI-substitutable skill demand is detectable.
- The 2022 pre-treatment coefficient is statistically indistinguishable from zero, **providing formal support for the parallel trends assumption**.
- **13 of 19 occupation groups** show directionally negative per-occupation estimates, with the largest declines in Finance & Accounting (−1.182), Marketing & Communications (−0.962), and Compliance & Risk (−0.756). No individual occupation estimate is statistically significant.
- The findings support a **qualified displacement hypothesis**: GenAI's effect on entry-level skill demand is real but delayed, consistent with the historical record of gradual labor market adjustment following technological shocks (Autor, Levy, and Murnane 2003; Acemoglu and Restrepo 2019).

## Contribution

This paper makes three contributions to the literature on AI and labor markets:

1. **Skill-level rather than occupation-level analysis.** Existing work (Eloundou et al. 2023; Felten, Raj, and Seamans 2021) measures AI exposure at the occupation level, which cannot tell curriculum designers which specific competencies to emphasize. This paper decomposes GenAI's impact at the skill level, observing employer demand directly in job postings.

2. **Focus on entry-level labor markets.** Entry-level hiring is the setting where skill signaling matters most, because employers cannot rely on professional history to screen new graduates. This is the population most directly relevant to higher-education curriculum decisions.

3. **A difference-in-differences identification strategy.** Prior AI-exposure studies are cross-sectional and cannot test whether employer demand actually shifted following a specific technological shock. The four-year panel spanning ChatGPT's November 2022 release enables causal identification of the demand response.

## Methodology

**Main specification:**

```
skill_pct_iot = β₀ + β₁·Treated_io + β₃·(Treated_io × Post_t) + α_o + λ_t + ε_iot
```

where `skill_pct_iot` is the frequency of skill *i* in occupation *o* postings during year *t*, `Treated_io` equals 1 for the 162 high AI-substitutable skills, and `Post_t` equals 1 for 2023–2024. Occupation fixed effects α_o absorb time-invariant occupation-level confounders; year fixed effects λ_t absorb macro shocks common to all skills. Standard errors are clustered at the occupation level.

**Event study specification:**

```
skill_pct_iot = β₀ + β₁·Treated_io + Σ_t β_t·(Treated_io × Year_t) + α_o + λ_t + ε_iot
```

with 2021 as the reference year, used to formally test parallel trends and trace the dynamic pattern of treatment effects.

**Robustness checks:** (1) event study, (2) per-occupation DiD regressions across 19 occupation groups, (3) alternative post-period definition restricting Post = 1 to 2024 only.

## Data

The analysis uses the **Web Data Commons JobPosting Corpus**, a public dataset of structured job postings extracted from the Common Crawl via schema.org/JobPosting markup. After filtering to U.S. full-time entry-level roles requiring a bachelor's degree, I draw a stratified reservoir sample of approximately 4,750 postings per year (18,927 postings total, 2021–2024).

Skill extraction was performed by an automated pipeline using Anthropic's Claude Opus 4.6 applied to six text fields per posting (description, qualifications, skills, experience requirements, education requirements, responsibilities). The 279 extracted skills are classified into:

- **Treatment group:** 162 high AI-substitutable skills (coding, data analysis, content writing)
- **Control group:** 117 low AI-substitutable skills (patient care, welding, leadership)

Classification is informed by the GPT-exposure framework in Eloundou et al. (2023). The resulting panel contains 279 × 19 × 4 = 21,204 skill-occupation-year observations.

The CSVs in `/data` are pre-aggregated to the skill × occupation × year level. Posting-level raw data is not included.

## Repository Structure

```
.
├── paper/
│   └── GenAI_Skill_Demand.pdf
├── data/
│   ├── 2021_skill_frequencies_by_occupation.csv
│   ├── 2022_skill_frequencies_by_occupation.csv
│   ├── 2023_skill_frequencies_by_occupation.csv
│   └── 2024_skill_frequencies_by_occupation.csv
├── notebooks/
│   └── econ_skill_analysis.ipynb
└── README.md
```

## Reproducing the Analysis

Requirements:

```
python >= 3.9
pandas
numpy
statsmodels
matplotlib
```

Run:

```bash
pip install pandas numpy statsmodels matplotlib
jupyter notebook notebooks/econ_skill_analysis.ipynb
```

All cells execute top to bottom. The notebook reproduces Tables 1–5 and Figures 1–3 in the paper.

## References

- Acemoglu, D., and P. Restrepo. 2019. "Automation and New Tasks: How Technology Displaces and Reinstates Labor." *Journal of Economic Perspectives*, 33(2): 3–30.
- Autor, D. H., F. Levy, and R. J. Murnane. 2003. "The Skill Content of Recent Technological Change: An Empirical Exploration." *The Quarterly Journal of Economics*, 118(4): 1279–1333.
- Eloundou, T., S. Manning, P. Mishkin, and D. Rock. 2023. "GPTs are GPTs: An Early Look at the Labor Market Impact Potential of Large Language Models." *Science*, 381(6659): eadi9837.
- Felten, E. W., M. Raj, and R. Seamans. 2021. "A Method to Link Advances in Artificial Intelligence to Occupational Abilities." *AEA Papers and Proceedings*, 111: 54–57.

## Citation

```
Singh, A. 2026. "Generative Artificial Intelligence and Entry-Level Skill Demand:
An Empirical Analysis of ChatGPT's Impact on the Labor Market."
Senior Seminar Paper, Department of Economics, Northeastern University.
```

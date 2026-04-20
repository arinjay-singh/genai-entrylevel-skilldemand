# Paper

This directory contains the final research paper and related materials for *Generative Artificial Intelligence and Entry-Level Skill Demand: An Empirical Analysis of ChatGPT's Impact on the Labor Market*.

## Files

| File | Description |
|---|---|
| `GenAI_Skill_Demand.pdf` | Final paper (Part V submission for ECON 4692) |

## Paper Structure

The paper contains six sections and two appendix tables, following the structure required by the Senior Economics Seminar:

1. **Introduction** — Motivation, research question, and contribution
2. **Literature Review** — Acemoglu and Restrepo (2019), Autor, Levy, and Murnane (2003), Felten, Raj, and Seamans (2021), Eloundou et al. (2023)
3. **Data** — Web Data Commons JobPosting Corpus, sample construction, descriptive statistics
4. **Econometric Model** — Difference-in-differences specification, identification strategy, robustness checks
5. **Results** — Main DiD estimates, event study, per-occupation heterogeneity, policy implications
6. **Conclusion** — Summary, limitations, future research directions

## Tables and Figures

**Main text:**

- Table 1 — Descriptive Statistics
- Table 2 — DiD Estimates: Effect of ChatGPT on AI-Substitutable Skill Demand
- Table 3 — Event Study Estimates: Dynamic Treatment Effects by Year
- Figure 1 — Annual Mean Skill Frequency of Treatment and Control Groups
- Figure 2 — AI Exposure by Occupation Group using Skill-level Granularity
- Figure 3 — Per-Occupation DiD Estimates with 95% Confidence Intervals

**Appendix:**

- Table A1 — Sample Composition by Occupation Group and Year
- Table A2 — Per-Occupation DiD Estimates (Post = 2024)

## Key Result

The paper's headline finding is a diffusion-lag pattern: the pooled 2023–2024 DiD estimate is a precisely estimated null (β₃ = −0.009, p = 0.940), but the event study reveals a statistically significant decline in 2024 (β₃ = −0.221, p = 0.048) with no pre-trend in 2022 and no response in 2023. See the repository-level README for a full summary of findings.

## Course Context

The paper was written for **ECON 4692 Senior Economics Seminar** (Spring 2026) at Northeastern University, taught by Professor Shuo Zhang. The course requires a five-part research project culminating in a 10-page empirical paper on an economic topic of the student's choosing. Part V is the final integrated paper, due April 20, 2026.

## Reproducing Tables and Figures

All tables and figures in the paper are reproducible from the analysis notebook:

```bash
jupyter notebook ../notebooks/econ_skill_analysis.ipynb
```

See `../README.md` for full instructions.

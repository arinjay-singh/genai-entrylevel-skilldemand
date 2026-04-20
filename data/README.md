# Data

This directory contains the aggregated skill-frequency data used in the analysis. Each CSV represents one annual cross-section of entry-level U.S. job postings, aggregated to the skill × occupation group level.

## Files

| File | Year | Rows |
|---|---|---|
| `2021_skill_frequencies_by_occupation.csv` | 2021 | ~5,300 |
| `2022_skill_frequencies_by_occupation.csv` | 2022 | ~5,300 |
| `2023_skill_frequencies_by_occupation.csv` | 2023 | ~5,300 |
| `2024_skill_frequencies_by_occupation.csv` | 2024 | ~5,300 |

Each row is a (skill, occupation_group, year) observation.

## Schema

| Column | Type | Description |
|---|---|---|
| `occupation_group` | string | One of 19 occupation groups (e.g., Finance & Accounting, Software & Web Development, Nursing & Clinical Care) |
| `skill` | string | One of 279 standardized skill labels |
| `year` | int | Posting year (2021, 2022, 2023, or 2024) |
| `count` | int | Number of postings in the occupation-year cell that mention the skill |
| `pct` | float | Skill frequency: `count / n_postings × 100`, expressed as a percentage |
| `n_postings` | int | Total number of postings in the occupation-year cell (repeated across all skill rows in that cell) |

## Source

The underlying raw data is the **Web Data Commons (WDC) JobPosting Corpus**, a public dataset of structured job postings extracted from the Common Crawl via schema.org/JobPosting markup. See https://webdatacommons.org/structureddata/ for details on the source corpus.

## Sample Construction

The raw WDC corpus was filtered using three sequential criteria:

1. U.S. postings only
2. Full-time positions
3. Entry-level roles requiring a bachelor's degree with at least one junior-level indicator

From each filtered year, a stratified reservoir sample of approximately 4,750 postings was drawn, yielding 18,927 postings in total across 2021–2024.

## Skill Extraction

Skill mentions were identified by an automated pipeline using Anthropic's Claude Opus 4.6 applied to six text fields per posting:

- Description
- Qualifications
- Skills
- Experience requirements
- Education requirements
- Responsibilities

Each posting was also classified into one of 19 occupation groups using a priority-ordered regex classifier applied to job titles.

## Treatment Classification

The 279 skills are split into two groups, informed by the GPT-exposure framework in Eloundou et al. (2023):

- **Treatment (high AI-substitutable):** 162 skills that an LLM can perform at entry level (e.g., coding, data analysis, content writing)
- **Control (low AI-substitutable):** 117 skills requiring physical presence, human interaction, or licensed judgment (e.g., patient care, welding, leadership)

The treatment/control assignment is applied programmatically in the analysis notebook, not stored in these CSVs.

## Aggregation Note

**These files are not posting-level data.** They are pre-aggregated to the skill × occupation × year level. Each occupation-year cell in the panel contains 279 skill rows (one per skill), and the `n_postings` column holds the cell-level posting count repeated across all 279 rows. To get posting counts per cell, take the `.first()` of `n_postings` within each `(occupation_group, year)` group, not `.sum()` or `.size()`.

## Usage

```python
import pandas as pd

df_2021 = pd.read_csv('data/2021_skill_frequencies_by_occupation.csv')
df_2022 = pd.read_csv('data/2022_skill_frequencies_by_occupation.csv')
df_2023 = pd.read_csv('data/2023_skill_frequencies_by_occupation.csv')
df_2024 = pd.read_csv('data/2024_skill_frequencies_by_occupation.csv')

panel = pd.concat([df_2021, df_2022, df_2023, df_2024], ignore_index=True)
```

See `notebooks/econ_skill_analysis.ipynb` for the full pipeline that constructs the balanced panel and runs the regressions.

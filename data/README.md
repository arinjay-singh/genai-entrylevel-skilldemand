# Data

This directory contains the aggregated skill-frequency data used in the analysis, along with the pipeline script that produced it. Each CSV represents one annual cross-section of entry-level U.S. job postings, aggregated to the skill × occupation group level.

## Files

| File | Description |
|---|---|
| `wdc_jobposting_pipeline.py` | Pipeline script that filters, samples, and aggregates the Web Data Commons JobPosting Corpus into the skill-frequency CSVs |
| `2021_skill_frequencies_by_occupation.csv` | 2021 skill × occupation aggregated data |
| `2022_skill_frequencies_by_occupation.csv` | 2022 skill × occupation aggregated data |
| `2023_skill_frequencies_by_occupation.csv` | 2023 skill × occupation aggregated data |
| `2024_skill_frequencies_by_occupation.csv` | 2024 skill × occupation aggregated data |

Each row in the CSVs is a (skill, occupation_group, year) observation.

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

## Pipeline

`wdc_jobposting_pipeline.py` performs the full data preparation:

1. **Ingests** raw WDC JobPosting records from the Common Crawl extraction
2. **Filters** to U.S. postings, full-time positions, and entry-level roles requiring a bachelor's degree with at least one junior-level indicator
3. **Samples** approximately 4,750 postings per year using a stratified reservoir approach (18,927 postings total across 2021–2024)
4. **Classifies** each posting into one of 19 occupation groups using a priority-ordered regex classifier applied to job titles
5. **Extracts** skill mentions from six posting text fields (description, qualifications, skills, experience requirements, education requirements, responsibilities) using Anthropic's Claude Opus 4.6
6. **Aggregates** posting-level data into the skill × occupation × year frequency tables written to the CSVs in this directory

The pipeline emits one CSV per year. Running it end-to-end requires:

- Access to the WDC JobPosting Corpus for the target years
- An Anthropic API key (for the skill extraction step)
- `python >= 3.9` with `pandas`, `anthropic`, and standard scientific libraries

Note that posting-level raw data is not included in this repository due to size and licensing considerations. Only the aggregated CSVs are redistributed.

## Treatment Classification

The 279 skills are split into two groups, informed by the GPT-exposure framework in Eloundou et al. (2023):

- **Treatment (high AI-substitutable):** 162 skills that an LLM can perform at entry level (e.g., coding, data analysis, content writing)
- **Control (low AI-substitutable):** 117 skills requiring physical presence, human interaction, or licensed judgment (e.g., patient care, welding, leadership)

The treatment/control assignment is applied programmatically in the analysis notebook, not stored in these CSVs.

## Aggregation Note

**These CSVs are not posting-level data.** They are pre-aggregated to the skill × occupation × year level. Each occupation-year cell in the panel contains 279 skill rows (one per skill), and the `n_postings` column holds the cell-level posting count repeated across all 279 rows. To get posting counts per cell, take the `.first()` of `n_postings` within each `(occupation_group, year)` group, not `.sum()` or `.size()`.

## Usage

```python
import pandas as pd

df_2021 = pd.read_csv('data/2021_skill_frequencies_by_occupation.csv')
df_2022 = pd.read_csv('data/2022_skill_frequencies_by_occupation.csv')
df_2023 = pd.read_csv('data/2023_skill_frequencies_by_occupation.csv')
df_2024 = pd.read_csv('data/2024_skill_frequencies_by_occupation.csv')

panel = pd.concat([df_2021, df_2022, df_2023, df_2024], ignore_index=True)
```

See `notebooks/econ_skill_analysis.ipynb` for the balanced panel construction and regressions, and `wdc_jobposting_pipeline.py` in this directory for the upstream data preparation.

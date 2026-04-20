"""
WDC JobPosting Pipeline
=======================
Parses Web Data Commons N-Quads JobPosting extractions from gzipped files,
filters for US job postings, and outputs clean CSV and JSON.

Usage:
    # Process a single year
    python wdc_jobposting_pipeline.py --input-dir ./2022/ --output-dir ./output/2022/

    # Process all years at once
    python wdc_jobposting_pipeline.py --input-dir ./data/ --output-dir ./output/ --recursive

    # Limit output for testing
    python wdc_jobposting_pipeline.py --input-dir ./2022/ --output-dir ./output/ --limit 1000

    # Include non-US postings too (just tag them)
    python wdc_jobposting_pipeline.py --input-dir ./2022/ --output-dir ./output/ --all-countries

Expected input directory structure:
    ./2022/
        part_0.gz
        part_1.gz
    ./2023/
        part_0.gz
        part_1.gz
    ...

Output:
    - us_job_postings.csv       (flat CSV, one row per posting)
    - us_job_postings.jsonl     (JSON Lines, one JSON object per line)
    - pipeline_stats.json       (summary statistics)
"""

import argparse
import csv
import gzip
import json
import logging
import math
import os
import random
import re
import sys
import time
from collections import defaultdict, Counter
from glob import glob
from html import unescape
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Patterns that indicate a US-based posting
US_COUNTRY_PATTERNS = {
    "US", "USA", "United States", "United States of America",
    "us", "usa", "united states", "united states of america",
    "U.S.", "U.S.A.", "U.S.A",
}

# US state abbreviations and names for fallback detection
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
}

US_STATE_NAMES = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming", "District of Columbia",
}

# Fields we want to extract for each job posting
OUTPUT_FIELDS = [
    "title",
    "company_name",
    "date_posted",
    "valid_through",
    "employment_type",
    "country",
    "region",
    "city",
    "postal_code",
    "street_address",
    "description",
    "experience_requirements",
    "education_requirements",
    "qualifications",
    "skills",
    "responsibilities",
    "industry",
    "occupation",
    "work_hours",
    "job_benefits",
    "base_salary",
    "salary_currency",
    "salary_min",
    "salary_max",
    "salary_unit",
    "identifier",
    "url",
    "source_url",
    "source_domain",
    "crawl_year",
    "industry_group",
]

# Regex for tokenizing N-Quad lines
# Matches: <URI>, _:blankNode, "literal"^^<type>, "literal"@lang, "literal"
TOKEN_RE = re.compile(
    r'<[^>]+>'               # URI in angle brackets
    r'|_:\S+'                # blank node
    r'|"(?:[^"\\]|\\.)*"'   # quoted literal (handles escaped quotes)
    r'(?:\^\^<[^>]+>)?'     # optional ^^<datatype>
    r'(?:@[a-zA-Z-]+)?'     # optional @language
)

# ---------------------------------------------------------------------------
# Industry Classification
# ---------------------------------------------------------------------------

# Broad industry groups for stratified sampling.
# Each group has keywords checked against: industry field, title, source domain,
# and (optionally) description. Order matters — first match wins.
INDUSTRY_TAXONOMY = {
    "Technology & IT": {
        "industry_kw": [
            "technology", "software", "information technology", "computer",
            "internet", "saas", "cloud", "cybersecurity", "data",
            "artificial intelligence", "machine learning", "telecom",
        ],
        "title_kw": [
            "software engineer", "developer", "devops", "sre", "data scientist",
            "data engineer", "ml engineer", "frontend", "backend", "fullstack",
            "full-stack", "full stack", "cloud architect", "cybersecurity",
            "it manager", "it director", "sysadmin", "systems administrator",
            "network engineer", "qa engineer", "test engineer", "scrum master",
            "product manager", "ux designer", "ui designer", "web developer",
        ],
        "domain_kw": [
            "tech", "software", "cloud", "cyber", "digital", "computing",
            "dice.com", "stackoverflow", "github", "hired.com",
        ],
    },
    "Finance & Insurance": {
        "industry_kw": [
            "financial", "finance", "banking", "insurance", "investment",
            "accounting", "capital markets", "private equity", "venture capital",
            "fintech", "asset management", "wealth management",
        ],
        "title_kw": [
            "financial analyst", "accountant", "auditor", "actuary",
            "investment banker", "portfolio manager", "underwriter",
            "loan officer", "credit analyst", "tax ", "cpa", "bookkeeper",
            "controller", "treasurer", "compliance officer", "risk analyst",
        ],
        "domain_kw": [
            "bank", "financ", "capital", "invest", "fidelity", "jpmorgan",
            "goldman", "schwab", "blackrock", "vanguard", "insurance",
        ],
    },
    "Healthcare & Pharma": {
        "industry_kw": [
            "health", "hospital", "medical", "pharmaceutical", "biotech",
            "clinical", "nursing", "dental", "mental health", "veterinary",
        ],
        "title_kw": [
            "nurse", "rn ", " rn", "lpn", "physician", "doctor", "surgeon",
            "therapist", "pharmacist", "medical assistant", "radiolog",
            "technologist", "phlebotom", "dental", "optometr", "psycholog",
            "psychiatr", "clinical", "emt", "paramedic", "caregiver", "cna",
        ],
        "domain_kw": [
            "health", "hospital", "medical", "nurse", "clinical", "pharma",
            "care", "dental", "wellness",
        ],
    },
    "Marketing & Media": {
        "industry_kw": [
            "marketing", "advertising", "media", "public relations",
            "publishing", "entertainment", "creative", "design agency",
            "digital marketing", "content", "journalism",
        ],
        "title_kw": [
            "marketing manager", "marketing director", "brand manager",
            "content ", "copywriter", "seo ", "social media", "graphic design",
            "creative director", "art director", "media buyer", "pr ",
            "public relations", "journalist", "editor", "advertising",
        ],
        "domain_kw": [
            "marketing", "media", "creative", "advertis", "design",
            "mediabistro", "creativepool",
        ],
    },
    "Legal": {
        "industry_kw": [
            "legal", "law firm", "law practice", "legal services",
        ],
        "title_kw": [
            "attorney", "lawyer", "paralegal", "legal assistant",
            "legal counsel", "associate attorney", "law clerk", "litigat",
            "corporate counsel", "compliance",
        ],
        "domain_kw": [
            "law", "legal", "attorney",
        ],
    },
    "Education": {
        "industry_kw": [
            "education", "higher education", "school", "university",
            "academic", "e-learning", "training",
        ],
        "title_kw": [
            "teacher", "professor", "instructor", "tutor", "principal",
            "dean", "academic", "curriculum", "school counselor",
            "librarian", "teaching assistant",
        ],
        "domain_kw": [
            "edu", "school", "university", "college", "academy",
        ],
    },
    "Manufacturing": {
        "industry_kw": [
            "manufactur", "industrial", "automotive", "aerospace",
            "chemical", "semiconductor", "electronics", "machinery",
            "metals", "plastics", "paper", "textile", "food processing",
        ],
        "title_kw": [
            "manufacturing engineer", "plant manager", "production",
            "machinist", "welder", "assembler", "quality inspector",
            "industrial engineer", "maintenance technician", "cnc",
            "tool and die", "machine operator", "forklift",
        ],
        "domain_kw": [
            "manufactur", "industrial", "automotive", "factory",
        ],
    },
}

# Pre-compile for speed: lowercase all keywords
_INDUSTRY_COMPILED = {}
for group, kw_dict in INDUSTRY_TAXONOMY.items():
    _INDUSTRY_COMPILED[group] = {
        "industry_kw": [k.lower() for k in kw_dict["industry_kw"]],
        "title_kw": [k.lower() for k in kw_dict["title_kw"]],
        "domain_kw": [k.lower() for k in kw_dict["domain_kw"]],
    }


def classify_industry(job: dict) -> str:
    """Classify a job posting into a broad industry group.
    
    Uses multiple signals in priority order:
    1. The 'industry' field from schema.org (highest signal, but often empty)
    2. Job title keywords
    3. Source domain keywords
    Falls back to 'Other' if no match.
    """
    industry_raw = (job.get("industry") or "").lower()
    title_raw = (job.get("title") or "").lower()
    domain_raw = (job.get("source_domain") or "").lower()
    
    best_match = None
    best_score = 0

    for group, kws in _INDUSTRY_COMPILED.items():
        score = 0
        
        # Industry field match (strongest signal)
        if industry_raw:
            for kw in kws["industry_kw"]:
                if kw in industry_raw:
                    score += 10
                    break
        
        # Title match (strong signal)
        if title_raw:
            for kw in kws["title_kw"]:
                if kw in title_raw:
                    score += 5
                    break
        
        # Domain match (moderate signal)
        if domain_raw:
            for kw in kws["domain_kw"]:
                if kw in domain_raw:
                    score += 3
                    break
        
        if score > best_score:
            best_score = score
            best_match = group

    return best_match if best_match else "Other"


# ---------------------------------------------------------------------------
# Reservoir Sampling
# ---------------------------------------------------------------------------

class ReservoirSampler:
    """Simple reservoir sampling (Algorithm R) for streaming data.
    
    Maintains a fixed-size reservoir that, after processing N items,
    contains a uniform random sample of size min(k, N).
    """
    
    def __init__(self, k: int, seed: int = 42):
        self.k = k
        self.reservoir = []
        self.n = 0  # total items seen
        self.rng = random.Random(seed)
    
    def add(self, item):
        self.n += 1
        if len(self.reservoir) < self.k:
            self.reservoir.append(item)
        else:
            j = self.rng.randint(0, self.n - 1)
            if j < self.k:
                self.reservoir[j] = item
    
    def get_sample(self) -> list:
        return list(self.reservoir)


class StratifiedReservoirSampler:
    """Stratified reservoir sampling across industry groups.
    
    Maintains a per-stratum count of items seen, and stores ALL items
    up to a generous per-stratum cap. At the end, draws the final
    sample with either proportional or equal allocation.
    
    For memory efficiency with large datasets, each stratum uses its
    own reservoir sampler with a generous buffer (10x the target per
    stratum) so we don't need to hold everything in memory.
    """
    
    def __init__(self, total_k: int, allocation: str = "proportional", seed: int = 42):
        self.total_k = total_k
        self.allocation = allocation  # "proportional" or "equal"
        self.seed = seed
        self.rng = random.Random(seed)
        # Per-stratum reservoir with generous buffer
        # (we'll downsample at the end)
        buffer_per_stratum = max(total_k, 50_000)
        self.strata_samplers: dict[str, ReservoirSampler] = defaultdict(
            lambda: ReservoirSampler(k=buffer_per_stratum, seed=self.rng.randint(0, 2**31))
        )
        self.strata_counts: Counter = Counter()  # true counts per stratum
    
    def add(self, item, stratum: str):
        self.strata_counts[stratum] += 1
        self.strata_samplers[stratum].add(item)
    
    def get_sample(self) -> list:
        """Draw the final stratified sample."""
        total_seen = sum(self.strata_counts.values())
        strata = sorted(self.strata_counts.keys())
        
        if total_seen == 0:
            return []
        
        # Compute allocation per stratum
        allocations = {}
        if self.allocation == "equal":
            # Phase 1: equal split
            per_stratum = self.total_k // len(strata)
            remainder = self.total_k % len(strata)
            for i, s in enumerate(strata):
                allocations[s] = per_stratum + (1 if i < remainder else 0)
            
            # Phase 2: cap at available pool size, redistribute surplus
            # Iterate until no surplus remains or all strata are maxed out
            for _ in range(len(strata)):
                surplus = 0
                capped = set()
                for s in strata:
                    available = len(self.strata_samplers[s].get_sample())
                    if allocations[s] > available:
                        surplus += allocations[s] - available
                        allocations[s] = available
                        capped.add(s)
                
                if surplus == 0:
                    break
                
                # Redistribute surplus to uncapped strata that still have room
                eligible = [s for s in strata if s not in capped]
                if not eligible:
                    break
                
                extra_each = surplus // len(eligible)
                extra_rem = surplus % len(eligible)
                for i, s in enumerate(eligible):
                    available = len(self.strata_samplers[s].get_sample())
                    bonus = extra_each + (1 if i < extra_rem else 0)
                    allocations[s] = min(allocations[s] + bonus, available)
                    
        else:  # proportional
            for s in strata:
                raw = (self.strata_counts[s] / total_seen) * self.total_k
                allocations[s] = int(raw)
            # Distribute rounding remainder to largest strata
            shortfall = self.total_k - sum(allocations.values())
            for s in sorted(strata, key=lambda x: -self.strata_counts[x]):
                if shortfall <= 0:
                    break
                allocations[s] += 1
                shortfall -= 1
        
        # Draw from each stratum's reservoir
        result = []
        for s in strata:
            pool = self.strata_samplers[s].get_sample()
            n_draw = min(allocations[s], len(pool))
            drawn = self.rng.sample(pool, n_draw) if n_draw < len(pool) else pool
            # Tag each item with its assigned industry
            for item in drawn:
                item["_industry_group"] = s
            result.extend(drawn)
        
        self.rng.shuffle(result)
        return result
    
    def get_strata_summary(self) -> dict:
        """Return summary of stratum populations and allocations."""
        total_seen = sum(self.strata_counts.values())
        strata = sorted(self.strata_counts.keys())
        summary = {}
        for s in strata:
            count = self.strata_counts[s]
            summary[s] = {
                "total_seen": count,
                "pct_of_total": round(100 * count / max(total_seen, 1), 1),
            }
        summary["_total"] = total_seen
        return summary


# ---------------------------------------------------------------------------
# Full-Time Filter
# ---------------------------------------------------------------------------

_FULL_TIME_SIGNALS = [
    "full_time", "full-time", "full time", "fulltime",
    "ft", "permanent", "regular",
]

_NOT_FULL_TIME_SIGNALS = [
    "part_time", "part-time", "part time", "parttime",
    "contract", "temporary", "temp ", "freelance", "per diem",
    "seasonal", "internship", "volunteer", "prn",
]

# Separate list for employment_type field only (structured values, no false positives)
_NOT_FULL_TIME_EMP_TYPE = _NOT_FULL_TIME_SIGNALS + ["intern"]


def is_full_time(job: dict) -> bool:
    """Determine if a job posting is for a full-time position.
    
    Checks employment_type field first (strongest signal), then falls
    back to title and work_hours. Postings with no employment type info
    are included by default to avoid discarding the majority of postings
    that simply omit the field.
    """
    emp_type = (job.get("employment_type") or "").lower()
    title = (job.get("title") or "").lower()
    work_hours = (job.get("work_hours") or "").lower()

    # If employment_type explicitly says not full-time, reject
    if emp_type and any(sig in emp_type for sig in _NOT_FULL_TIME_EMP_TYPE):
        return False

    # If employment_type explicitly says full-time, accept
    if emp_type and any(sig in emp_type for sig in _FULL_TIME_SIGNALS):
        return True

    # No employment_type — check title/work_hours for disqualifiers
    combined = f"{title} {work_hours}"
    if any(sig in combined for sig in _NOT_FULL_TIME_SIGNALS):
        return False

    # No signal either way — include by default (most postings omit this field)
    return True


# ---------------------------------------------------------------------------
# Entry-Level / New-Grad Filter
# ---------------------------------------------------------------------------

# Bachelor's degree signals (REQUIRED gate)
_BACHELORS_SIGNALS = [
    "bachelor", "b.s.", "b.a.", "b.s ", "b.a ", "bs ", "ba ",
    "undergraduate degree", "college degree", "4-year degree",
    "four-year degree", "4 year degree", "university degree",
]

# Positive entry-level signals (need at least 1 alongside bachelor's gate)
_ENTRY_TITLE_POS = [
    "junior", "jr.", "jr ", "entry-level", "entry level", "associate",
    "intern ", "internship", "trainee", "new grad", "new graduate",
    "early career", "level i", "level ii", "level 1", "level 2",
    " i ", " ii ",  # Roman numerals surrounded by spaces
    "apprentice", "rotational", "rotation program", "graduate program",
    "analyst",  # common entry-level finance/consulting title
    "coordinator",  # common entry-level ops/marketing title
]

_ENTRY_EXP_POS = [
    "0-1 year", "0-2 year", "0 - 1 year", "0 - 2 year",
    "1-2 year", "1 - 2 year", "0 to 1 year", "0 to 2 year",
    "1 to 2 year", "less than 1 year", "less than 2 year",
    "no experience", "none required", "not required",
    "entry level", "entry-level", "new grad", "new graduate",
    "recent graduate", "minimal experience", "little experience",
    "0+ year", "1+ year",
]

_ENTRY_EDU_POS = [
    "new graduate", "recent graduate", "new grad", "recent grad",
    "currently enrolled", "graduating", "class of 20",
    "no experience required",
]

# Disqualifying signals (override everything)
_ENTRY_TITLE_NEG = [
    "senior", "sr.", "sr ", "staff", "lead ", "leader", "principal",
    "director", "vp ", "vice president", "head of", "chief",
    "manager", "managing", " iii", " iv", " v ",
    "level iii", "level iv", "level v", "level 3", "level 4", "level 5",
    "architect",  # usually senior
    "fellow",     # usually senior in tech
]

_ENTRY_EXP_NEG = [
    "5+ year", "5-7 year", "5 to 7 year", "5 - 7 year",
    "7+ year", "7-10 year", "8+ year", "10+ year", "15+ year",
    "5 years", "6 years", "7 years", "8 years", "10 years",
    "extensive experience", "significant experience",
    "seasoned", "proven track record",
]

_ENTRY_EDU_NEG = [
    "master's required", "masters required", "master's degree required",
    "phd required", "doctorate required", "md required", "jd required",
    "mba required", "m.d. required", "j.d. required",
]


def is_entry_level(job: dict) -> bool:
    """Determine if a job posting targets bachelor's-level candidates with
    little to no experience.

    Requires BOTH:
      1. A bachelor's degree signal (gate)
      2. At least one positive entry-level signal from title, experience,
         or education fields

    Returns False if any disqualifying signal is present, regardless of
    positive matches.
    """
    title = (job.get("title") or "").lower()
    exp = (job.get("experience_requirements") or "").lower()
    edu = (job.get("education_requirements") or "").lower()
    qual = (job.get("qualifications") or "").lower()
    desc = (job.get("description") or "").lower()

    # Combine education-adjacent fields for bachelor's check
    edu_combined = f"{edu} {qual}"
    # Also check first ~1500 chars of description for education mentions
    # (many postings only list requirements in the description body)
    edu_combined_with_desc = f"{edu_combined} {desc[:1500]}"

    # ---- Gate: must mention bachelor's degree ----
    has_bachelors = any(sig in edu_combined_with_desc for sig in _BACHELORS_SIGNALS)
    if not has_bachelors:
        return False

    # ---- Disqualifiers: reject if any match ----
    if any(sig in title for sig in _ENTRY_TITLE_NEG):
        return False
    if any(sig in f"{exp} {qual}" for sig in _ENTRY_EXP_NEG):
        return False
    if any(sig in edu_combined for sig in _ENTRY_EDU_NEG):
        return False

    # ---- Positive signals: need at least 1 ----
    positive = 0

    if any(sig in title for sig in _ENTRY_TITLE_POS):
        positive += 1
    if any(sig in f"{exp} {qual}" for sig in _ENTRY_EXP_POS):
        positive += 1
    if any(sig in edu_combined_with_desc for sig in _ENTRY_EDU_POS):
        positive += 1

    return positive >= 1


# ---------------------------------------------------------------------------
# Parsing Functions
# ---------------------------------------------------------------------------

def tokenize_nquad(line: str) -> list[str] | None:
    """Tokenize a single N-Quad line into [subject, predicate, object, graph]."""
    tokens = TOKEN_RE.findall(line)
    if len(tokens) >= 4:
        return tokens[:4]
    return None


def extract_field_name(predicate: str) -> str:
    """Extract the field name from a schema.org predicate URI.
    
    '<http://schema.org/datePosted>' -> 'datePosted'
    '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' -> 'type'
    """
    uri = predicate.strip("<>")
    # Handle fragment identifiers (#type)
    if "#" in uri:
        return uri.split("#")[-1]
    return uri.split("/")[-1]


def clean_literal(value: str) -> str:
    """Clean a literal value: remove quotes, type annotations, unescape HTML."""
    if not value.startswith('"'):
        return value.strip("<>")
    
    # Extract content between first and last quote
    match = re.match(r'"(.*)"', value, re.DOTALL)
    if not match:
        return value
    
    text = match.group(1)
    
    # Unescape common escape sequences
    text = text.replace('\\"', '"')
    text = text.replace("\\n", "\n")
    text = text.replace("\\r", "\r")
    text = text.replace("\\t", "\t")
    
    # Unescape HTML entities
    text = unescape(text)
    
    return text


def clean_description(text: str) -> str:
    """Clean HTML from description fields for downstream NLP."""
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Remove HTML entities that survived
    text = re.sub(r"&\w+;", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# Core Pipeline
# ---------------------------------------------------------------------------

class WDCJobPostingParser:
    """Streaming parser for WDC N-Quads JobPosting data."""

    def __init__(self, us_only: bool = True):
        self.us_only = us_only
        self.stats = {
            "lines_processed": 0,
            "nodes_seen": 0,
            "job_postings_total": 0,
            "job_postings_us": 0,
            "job_postings_non_us": 0,
            "job_postings_no_country": 0,
            "files_processed": 0,
            "errors": 0,
        }

    def _is_us_posting(self, job: dict) -> bool:
        """Determine if a job posting is US-based."""
        country = job.get("country", "").strip()
        region = job.get("region", "").strip()
        street = job.get("street_address", "").strip()
        source = job.get("source_url", "").strip()

        # Check country field directly
        if country in US_COUNTRY_PATTERNS:
            return True

        # If a non-US country is specified, reject
        if country:
            return False

        # No country specified — use heuristics
        # Check if region matches a US state name (full names are unambiguous)
        if region in US_STATE_NAMES:
            return True

        # State abbreviations are ambiguous (e.g. SC = South Carolina or Santa Catarina)
        # Only match if other signals suggest US
        if region in US_STATES:
            # Check for non-US signals in street address or source domain
            non_us_signals = ["Brasil", "Brazil", "México", "Mexico", "Canada",
                              "India", ".br/", ".mx/", ".ca/", ".in/", ".de/",
                              ".fr/", ".uk/", ".au/", ".jp/", ".nl/"]
            combined = f"{street} {source}".lower()
            if any(sig.lower() in combined for sig in non_us_signals):
                return False
            
            # Check for US signals in source domain
            us_tlds = [".com/", ".org/", ".net/", ".us/", ".gov/", ".edu/"]
            if any(tld in source.lower() for tld in us_tlds):
                return True

        return False

    def _resolve_job_posting(self, job_node_id: str, nodes: dict) -> dict:
        """Resolve a JobPosting node and its linked nodes into a flat dict."""
        data = nodes.get(job_node_id, {})
        job = {}

        # Direct scalar fields
        field_mapping = {
            "title": "title",
            "datePosted": "date_posted",
            "validThrough": "valid_through",
            "employmentType": "employment_type",
            "description": "description",
            "experienceRequirements": "experience_requirements",
            "educationRequirements": "education_requirements",
            "qualifications": "qualifications",
            "skills": "skills",
            "responsibilities": "responsibilities",
            "industry": "industry",
            "occupationalCategory": "occupation",
            "workHours": "work_hours",
            "jobBenefits": "job_benefits",
            "url": "url",
            "identifier": "identifier",
        }

        for src_field, dst_field in field_mapping.items():
            if src_field in data:
                val = data[src_field]
                # Skip blank node references for scalar fields
                if not val.startswith("_:"):
                    job[dst_field] = clean_literal(val)

        # Resolve hiringOrganization -> company_name
        org_ref = data.get("hiringOrganization", "")
        if org_ref.startswith("_:") and org_ref in nodes:
            org = nodes[org_ref]
            job["company_name"] = clean_literal(org.get("name", ""))
        elif org_ref and not org_ref.startswith("_:"):
            job["company_name"] = clean_literal(org_ref)

        # Resolve jobLocation -> address fields
        loc_ref = data.get("jobLocation", "")
        if loc_ref.startswith("_:") and loc_ref in nodes:
            loc = nodes[loc_ref]
            addr_ref = loc.get("address", "")
            
            # Sometimes address is directly on the Place node
            if addr_ref.startswith("_:") and addr_ref in nodes:
                addr = nodes[addr_ref]
            else:
                addr = loc  # fallback: address fields on Place itself
            
            job["country"] = clean_literal(addr.get("addressCountry", ""))
            job["region"] = clean_literal(addr.get("addressRegion", ""))
            job["city"] = clean_literal(addr.get("addressLocality", ""))
            job["postal_code"] = clean_literal(addr.get("postalCode", ""))
            job["street_address"] = clean_literal(addr.get("streetAddress", ""))

        # Resolve baseSalary -> salary fields
        salary_ref = data.get("baseSalary", "")
        if salary_ref.startswith("_:") and salary_ref in nodes:
            salary = nodes[salary_ref]
            job["salary_currency"] = clean_literal(salary.get("currency", ""))
            
            # Salary value might be nested in another QuantitativeValue node
            val_ref = salary.get("value", "")
            if val_ref.startswith("_:") and val_ref in nodes:
                qv = nodes[val_ref]
                job["salary_min"] = clean_literal(qv.get("minValue", ""))
                job["salary_max"] = clean_literal(qv.get("maxValue", ""))
                job["salary_unit"] = clean_literal(qv.get("unitText", ""))
                if "value" in qv and not qv["value"].startswith("_:"):
                    job["base_salary"] = clean_literal(qv["value"])
            elif val_ref and not val_ref.startswith("_:"):
                job["base_salary"] = clean_literal(val_ref)
        elif salary_ref and not salary_ref.startswith("_:"):
            job["base_salary"] = clean_literal(salary_ref)

        # Also check estimatedSalary
        est_ref = data.get("estimatedSalary", "")
        if est_ref.startswith("_:") and est_ref in nodes and "base_salary" not in job:
            est = nodes[est_ref]
            job["salary_currency"] = job.get("salary_currency") or clean_literal(est.get("currency", ""))
            val_ref = est.get("value", "")
            if val_ref.startswith("_:") and val_ref in nodes:
                qv = nodes[val_ref]
                job["salary_min"] = clean_literal(qv.get("minValue", ""))
                job["salary_max"] = clean_literal(qv.get("maxValue", ""))
                job["salary_unit"] = clean_literal(qv.get("unitText", ""))

        # Source URL (the page this was scraped from)
        job["source_url"] = data.get("_source", "")

        # Clean the description
        if "description" in job:
            job["description"] = clean_description(job["description"])

        return job

    def process_page_group(self, nodes: dict, job_node_ids: list, crawl_year: str) -> list[dict]:
        """Process a group of nodes from the same source page into job records."""
        results = []

        for nid in job_node_ids:
            self.stats["job_postings_total"] += 1
            job = self._resolve_job_posting(nid, nodes)
            job["crawl_year"] = crawl_year

            # Extract domain from source URL
            src = job.get("source_url", "")
            if src:
                match = re.match(r"https?://([^/]+)", src)
                job["source_domain"] = match.group(1) if match else ""

            # Filter by country
            is_us = self._is_us_posting(job)
            if is_us:
                self.stats["job_postings_us"] += 1
            elif not job.get("country"):
                self.stats["job_postings_no_country"] += 1
            else:
                self.stats["job_postings_non_us"] += 1

            if self.us_only and not is_us:
                continue

            results.append(job)

        return results

    def stream_gz_file(self, filepath: str, crawl_year: str, limit: int = 0):
        """
        Stream-parse a gzipped N-Quads file, yielding US job postings.
        
        Groups lines by source page (4th element), then resolves
        JobPosting nodes with their linked Organization/Place/etc nodes.
        
        Yields dicts, one per US job posting.
        """
        logging.info(f"Processing: {filepath}")
        self.stats["files_processed"] += 1

        current_source = None
        nodes = defaultdict(dict)      # node_id -> {field: value}
        job_node_ids = []              # node IDs that are JobPostings
        yielded = 0

        with gzip.open(filepath, "rt", encoding="utf-8", errors="replace") as f:
            for line_num, line in enumerate(f, 1):
                if limit and yielded >= limit:
                    return

                self.stats["lines_processed"] += 1

                if line_num % 5_000_000 == 0:
                    logging.info(
                        f"  ...{line_num:,} lines | "
                        f"{self.stats['job_postings_us']:,} US postings so far"
                    )

                # Tokenize
                tokens = tokenize_nquad(line)
                if not tokens:
                    continue

                subject, predicate, obj, source = tokens
                source_url = source.strip("<>")

                # When we hit a new source page, flush the previous group
                if source_url != current_source and current_source is not None:
                    if job_node_ids:
                        for job in self.process_page_group(nodes, job_node_ids, crawl_year):
                            yielded += 1
                            yield job
                            if limit and yielded >= limit:
                                return

                    # Reset for new page group
                    nodes.clear()
                    job_node_ids.clear()

                current_source = source_url

                # Parse this triple
                field = extract_field_name(predicate)

                if field == "type":
                    type_name = obj.strip("<>").split("/")[-1]
                    nodes[subject]["_type"] = type_name
                    nodes[subject]["_source"] = source_url
                    if type_name == "JobPosting":
                        job_node_ids.append(subject)
                        self.stats["nodes_seen"] += 1
                else:
                    nodes[subject][field] = obj
                    if "_source" not in nodes[subject]:
                        nodes[subject]["_source"] = source_url

            # Flush the last group
            if job_node_ids:
                for job in self.process_page_group(nodes, job_node_ids, crawl_year):
                    yielded += 1
                    yield job


def detect_crawl_year(filepath: str) -> str:
    """Try to detect crawl year from the directory path."""
    path = str(filepath)
    match = re.search(r"(20\d{2})", path)
    return match.group(1) if match else "unknown"


def find_gz_files(input_dir: str, recursive: bool = False) -> list[tuple[str, str]]:
    """Find all part_*.gz files. Returns list of (filepath, crawl_year)."""
    results = []
    
    if recursive:
        for gz in sorted(glob(os.path.join(input_dir, "**", "part_*.gz"), recursive=True)):
            year = detect_crawl_year(gz)
            results.append((gz, year))
    else:
        for gz in sorted(glob(os.path.join(input_dir, "part_*.gz"))):
            year = detect_crawl_year(gz)
            results.append((gz, year))

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Parse WDC JobPosting N-Quads into clean CSV/JSONL for US postings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Full extraction (all US postings)
    python wdc_jobposting_pipeline.py --input-dir ./2022/ --output-dir ./output/2022/

    # Random sample of 10,000 US postings
    python wdc_jobposting_pipeline.py --input-dir ./2022/ --output-dir ./output/2022/ --sample 10000

    # Stratified sample: 10,000 US postings, proportional to industry distribution
    python wdc_jobposting_pipeline.py --input-dir ./2022/ --output-dir ./output/2022/ --sample 10000 --stratify proportional

    # Stratified sample: equal number from each industry
    python wdc_jobposting_pipeline.py --input-dir ./2022/ --output-dir ./output/2022/ --sample 10000 --stratify equal

    # Process all years recursively with sampling
    python wdc_jobposting_pipeline.py --input-dir ./data/ --output-dir ./output/ --recursive --sample 10000

    # Reproducible sampling with custom seed
    python wdc_jobposting_pipeline.py --input-dir ./2022/ --output-dir ./output/ --sample 10000 --seed 123
        """,
    )
    parser.add_argument(
        "--input-dir", required=True,
        help="Directory containing part_*.gz files (or parent dir with --recursive)",
    )
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory to write output CSV, JSONL, and stats",
    )
    parser.add_argument(
        "--recursive", action="store_true",
        help="Search for part_*.gz files recursively (for processing multiple years)",
    )
    parser.add_argument(
        "--all-countries", action="store_true",
        help="Include all countries, not just US (adds is_us flag to output)",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max US postings to extract per file (0 = no limit, useful for testing)",
    )
    parser.add_argument(
        "--sample", type=int, default=0,
        help="Randomly sample N postings from all files (uses reservoir sampling). "
             "0 = no sampling, output everything.",
    )
    parser.add_argument(
        "--stratify", choices=["proportional", "equal"], default=None,
        help="Stratify sampling by industry group. 'proportional' preserves the natural "
             "industry distribution; 'equal' draws the same number from each industry. "
             "Requires --sample.",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducible sampling (default: 42)",
    )
    parser.add_argument(
        "--entry-level", action="store_true",
        help="Filter for entry-level / new-grad postings only. Requires a bachelor's "
             "degree signal plus at least one positive entry-level indicator (title, "
             "experience, or education). Applied before sampling.",
    )
    parser.add_argument(
        "--full-time", action="store_true",
        help="Filter for full-time positions only. Rejects postings explicitly marked "
             "as part-time, contract, temporary, etc. Postings with no employment type "
             "are included by default. Applied before sampling.",
    )
    parser.add_argument(
        "--format", choices=["csv", "jsonl", "both"], default="both",
        help="Output format (default: both CSV and JSONL)",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    args = parser.parse_args()

    # Validate args
    if args.stratify and not args.sample:
        parser.error("--stratify requires --sample N")

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Find input files
    gz_files = find_gz_files(args.input_dir, recursive=args.recursive)
    if not gz_files:
        logging.error(f"No part_*.gz files found in {args.input_dir}")
        sys.exit(1)

    logging.info(f"Found {len(gz_files)} file(s) to process:")
    for fp, year in gz_files:
        logging.info(f"  {fp} (year: {year})")

    if args.sample:
        if args.stratify:
            logging.info(
                f"Stratified sampling: {args.sample:,} postings, "
                f"allocation={args.stratify}, seed={args.seed}"
            )
        else:
            logging.info(f"Reservoir sampling: {args.sample:,} postings, seed={args.seed}")

    if args.entry_level:
        logging.info("Entry-level filter: ON (bachelor's + 1 positive signal required)")
    if args.full_time:
        logging.info("Full-time filter: ON (rejecting part-time, contract, temporary, etc.)")

    # Setup output
    os.makedirs(args.output_dir, exist_ok=True)
    
    prefix = "us_job"
    if args.all_countries:
        prefix = "all_job"
    if args.entry_level:
        prefix = prefix.replace("job", "entry_level")
    if args.full_time:
        prefix += "_ft"
    if args.sample:
        prefix += f"_sample_{args.sample}"
    
    csv_path = os.path.join(args.output_dir, f"{prefix}_postings.csv")
    jsonl_path = os.path.join(args.output_dir, f"{prefix}_postings.jsonl")
    stats_path = os.path.join(args.output_dir, "pipeline_stats.json")

    output_fields = list(OUTPUT_FIELDS)
    if args.all_countries:
        output_fields.append("is_us")

    # Initialize parser
    pipeline = WDCJobPostingParser(us_only=not args.all_countries)
    if args.entry_level:
        pipeline.stats["entry_level_passed"] = 0
        pipeline.stats["entry_level_rejected"] = 0
    if args.full_time:
        pipeline.stats["full_time_passed"] = 0
        pipeline.stats["full_time_rejected"] = 0

    # Initialize sampler (if sampling)
    sampler = None
    stratified_sampler = None
    if args.sample:
        if args.stratify:
            stratified_sampler = StratifiedReservoirSampler(
                total_k=args.sample, allocation=args.stratify, seed=args.seed,
            )
        else:
            sampler = ReservoirSampler(k=args.sample, seed=args.seed)

    # ---------------------------------------------------------------
    # Pass 1: Stream all files, either writing directly or sampling
    # ---------------------------------------------------------------
    start_time = time.time()
    total_seen = 0
    total_written = 0

    # Only open output files for direct writing (non-sampled mode)
    csv_file = None
    csv_writer = None
    jsonl_file = None

    if not args.sample:
        if args.format in ("csv", "both"):
            csv_file = open(csv_path, "w", newline="", encoding="utf-8")
            csv_writer = csv.DictWriter(
                csv_file, fieldnames=output_fields, extrasaction="ignore",
                quoting=csv.QUOTE_ALL, escapechar="\\",
            )
            csv_writer.writeheader()

        if args.format in ("jsonl", "both"):
            jsonl_file = open(jsonl_path, "w", encoding="utf-8")

    try:
        for filepath, crawl_year in gz_files:
            for job in pipeline.stream_gz_file(filepath, crawl_year, limit=args.limit):
                # Add is_us flag if including all countries
                if args.all_countries:
                    job["is_us"] = pipeline._is_us_posting(job)

                # Always classify industry (useful for output + stratification)
                job["industry_group"] = classify_industry(job)

                # Full-time filter (applied before entry-level and sampling)
                if args.full_time:
                    if is_full_time(job):
                        pipeline.stats["full_time_passed"] += 1
                    else:
                        pipeline.stats["full_time_rejected"] += 1
                        continue

                # Entry-level filter (applied before sampling)
                if args.entry_level:
                    if is_entry_level(job):
                        pipeline.stats["entry_level_passed"] += 1
                    else:
                        pipeline.stats["entry_level_rejected"] += 1
                        continue

                total_seen += 1

                if args.sample:
                    # Feed into sampler
                    if stratified_sampler:
                        stratified_sampler.add(job, stratum=job["industry_group"])
                    else:
                        sampler.add(job)
                else:
                    # Write directly
                    if csv_writer:
                        csv_writer.writerow(job)
                    if jsonl_file:
                        jsonl_file.write(json.dumps(job, ensure_ascii=False) + "\n")
                    total_written += 1

                if total_seen % 10_000 == 0:
                    logging.info(f"  Processed {total_seen:,} postings...")

    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Saving progress...")

    finally:
        if csv_file:
            csv_file.close()
        if jsonl_file:
            jsonl_file.close()

    # ---------------------------------------------------------------
    # Pass 2 (sampling only): Write the sampled postings to output
    # ---------------------------------------------------------------
    if args.sample:
        if stratified_sampler:
            sample = stratified_sampler.get_sample()
            strata_summary = stratified_sampler.get_strata_summary()
        else:
            sample = sampler.get_sample()
            # Classify for summary even in non-stratified mode
            strata_summary = Counter(j.get("industry_group", "Other") for j in sample)
            strata_summary = {
                k: {"sampled": v} for k, v in sorted(strata_summary.items())
            }

        total_written = len(sample)
        logging.info(f"Sample drawn: {total_written:,} postings from {total_seen:,} total")

        # Write sampled output
        if args.format in ("csv", "both"):
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f, fieldnames=output_fields, extrasaction="ignore",
                    quoting=csv.QUOTE_ALL, escapechar="\\",
                )
                writer.writeheader()
                for job in sample:
                    writer.writerow(job)

        if args.format in ("jsonl", "both"):
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for job in sample:
                    f.write(json.dumps(job, ensure_ascii=False) + "\n")

    elapsed = time.time() - start_time

    # ---------------------------------------------------------------
    # Save stats
    # ---------------------------------------------------------------
    pipeline.stats["total_seen"] = total_seen
    pipeline.stats["total_written"] = total_written
    pipeline.stats["elapsed_seconds"] = round(elapsed, 1)
    pipeline.stats["postings_per_second"] = round(total_seen / max(elapsed, 0.1), 1)

    if args.sample:
        pipeline.stats["sampling"] = {
            "method": f"stratified_{args.stratify}" if args.stratify else "reservoir",
            "target_n": args.sample,
            "actual_n": total_written,
            "seed": args.seed,
            "total_population": total_seen,
        }
        if stratified_sampler:
            pipeline.stats["sampling"]["strata"] = stratified_sampler.get_strata_summary()

    # Industry distribution in output
    if args.sample:
        dist = Counter(j.get("industry_group", "Other") for j in sample)
    else:
        # Not tracked in non-sample mode (would require a second pass)
        dist = None

    if dist:
        pipeline.stats["industry_distribution"] = {
            k: {"count": v, "pct": round(100 * v / max(total_written, 1), 1)}
            for k, v in sorted(dist.items(), key=lambda x: -x[1])
        }

    with open(stats_path, "w") as f:
        json.dump(pipeline.stats, f, indent=2)

    # Print summary
    logging.info("=" * 60)
    logging.info("PIPELINE COMPLETE")
    logging.info("=" * 60)
    logging.info(f"  Files processed:       {pipeline.stats['files_processed']}")
    logging.info(f"  Lines processed:       {pipeline.stats['lines_processed']:,}")
    logging.info(f"  Total JobPostings:     {pipeline.stats['job_postings_total']:,}")
    logging.info(f"  US JobPostings:        {pipeline.stats['job_postings_us']:,}")
    logging.info(f"  Non-US JobPostings:    {pipeline.stats['job_postings_non_us']:,}")
    logging.info(f"  No country specified:  {pipeline.stats['job_postings_no_country']:,}")
    if args.sample:
        logging.info(f"  Population (passed filter): {total_seen:,}")
        logging.info(f"  Sampled:               {total_written:,}")
    else:
        logging.info(f"  Written to output:     {total_written:,}")
    if args.entry_level:
        passed = pipeline.stats["entry_level_passed"]
        rejected = pipeline.stats["entry_level_rejected"]
        total_el = passed + rejected
        pct = 100 * passed / max(total_el, 1)
        logging.info(f"  Entry-level filter:    {passed:,} / {total_el:,} passed ({pct:.1f}%)")
    if args.full_time:
        passed = pipeline.stats["full_time_passed"]
        rejected = pipeline.stats["full_time_rejected"]
        total_ft = passed + rejected
        pct = 100 * passed / max(total_ft, 1)
        logging.info(f"  Full-time filter:      {passed:,} / {total_ft:,} passed ({pct:.1f}%)")
    logging.info(f"  Time elapsed:          {elapsed:.1f}s")
    logging.info(f"  Speed:                 {pipeline.stats['postings_per_second']:.0f} postings/sec")
    logging.info(f"  Output: {args.output_dir}")

    if dist:
        logging.info("")
        logging.info("Industry distribution in output:")
        for group, count in sorted(dist.items(), key=lambda x: -x[1]):
            pct = 100 * count / max(total_written, 1)
            bar = "█" * int(pct / 2)
            logging.info(f"  {group:30s} {count:6,} ({pct:5.1f}%) {bar}")


if __name__ == "__main__":
    main()

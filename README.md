# IRS 501(c)(3) Data Pipeline

A repeatable Python + BigQuery data pipeline that ingests IRS EO BMF
organisational data and 990 e-file XML data (including Schedule M noncash
contributions) for all US 501(c)(3) organisations, producing a flat
prospecting table of organisations that accept in-kind donations.

## Quick start

```bash
# 1. Clone and install
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env with your GCP project ID, bucket name, etc.

# 3. Create BigQuery tables
python setup_bigquery.py

# 4. Run the full pipeline
python -m scripts.run_full_pipeline
```

## Data sources

| Source | What it provides | Refresh cadence |
|--------|-----------------|-----------------|
| [IRS EO BMF](https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf) | Organisation identity, address, NTEE code, status, assets, income | Monthly (2nd Monday) |
| [IRS 990 E-Files on AWS S3](https://docs.opendata.aws/irs-990/readme.html) | Full 990 XML returns — financials, contact info, schedules | Rolling (new filings added continuously) |
| [Master Concordance File](https://nonprofit-open-data-collective.github.io/irs-efile-master-concordance-file/) | XPath-to-variable mapping for consistent XML parsing | Periodically |

## GCP setup (one-time)

1. **Create a GCP project** at https://console.cloud.google.com/
2. **Enable APIs**: BigQuery API, Cloud Storage API
3. **Create a service account** with roles:
   - `BigQuery Data Editor`
   - `BigQuery Job User`
   - `Storage Object Admin`
4. **Download the JSON key** and set `GOOGLE_APPLICATION_CREDENTIALS` in `.env`
5. **Create a GCS bucket** for staging (e.g., `my-irs-staging-bucket`)
6. Run `python setup_bigquery.py` to create the dataset and tables

## Pipeline architecture

```
IRS EO BMF CSVs ─┐
                  ├─→ download_bmf.py ─→ organizations.jsonl ─┐
                  │                                            │
S3 Index CSVs ────┤                                            ├─→ GCS ─→ BigQuery
                  ├─→ download_index.py ─→ filtered_index.csv  │
                  │                                            │
S3 XML files  ────┤─→ download_xml.py ──→ *.xml ──┐           │
                  │                                │           │
Concordance   ────┘─→ concordance.py               ├─→ parse_990.py ──→ filings.jsonl ────┤
                                                   │                ──→ schedule_m.jsonl ──┘
                                                   │
                                        views.py ──→ vw_inkind_prospecting (BigQuery view)
```

## Running the pipeline

### Full pipeline

```bash
python -m scripts.run_full_pipeline

# Options:
#   --force          Re-download and re-parse everything
#   --xml-limit N    Only download N XML files (for testing)
#   --skip-bigquery  Skip upload to BigQuery (local files only)
```

### BMF-only refresh (monthly)

```bash
python -m scripts.run_bmf_only
```

### Incremental 990 update

```bash
python -m scripts.run_990_incremental
```

## BigQuery tables

### `organizations`

Source: IRS EO BMF CSVs (filtered to SUBSECTION=03 for 501(c)(3))

| Column | Type | Description |
|--------|------|-------------|
| `ein` | STRING | Employer Identification Number (primary key) |
| `name` | STRING | Organisation name |
| `sort_name` | STRING | Secondary / DBA name |
| `street` | STRING | Street address |
| `city` | STRING | City |
| `state` | STRING | 2-letter state code |
| `zip` | STRING | ZIP code |
| `ntee_code` | STRING | NTEE category (e.g., P20 = Human Services) |
| `foundation_code` | INTEGER | Foundation type code |
| `ruling_date` | STRING | Date granted tax-exempt status (YYYYMM) |
| `asset_amount` | INTEGER | End-of-year asset amount |
| `income_amount` | INTEGER | Gross receipts |
| `revenue_amount` | INTEGER | Form 990 revenue |
| ... | | *(26 columns total — see `setup_bigquery.py`)* |

### `filings`

Source: Parsed from 990 XML e-files (Header + Part I + Part II Signature)

| Column | Type | Description |
|--------|------|-------------|
| `object_id` | STRING | Unique filing identifier (primary key) |
| `ein` | STRING | FK to organisations |
| `tax_year` | INTEGER | Tax year |
| `org_name` | STRING | Name as filed |
| `org_phone` | STRING | Organisation telephone |
| `website` | STRING | Organisation website |
| `principal_officer_name` | STRING | Principal officer |
| `signing_officer_name` | STRING | Signing officer |
| `signing_officer_title` | STRING | e.g., "Executive Director" |
| `signing_officer_phone` | STRING | Signing officer phone |
| `mission` | STRING | Mission statement |
| `num_employees` | INTEGER | Total employees |
| `num_volunteers` | INTEGER | Total volunteers |
| `total_revenue_cy` | INTEGER | Total revenue (current year) |
| `total_expenses_cy` | INTEGER | Total expenses (current year) |
| `total_assets_eoy` | INTEGER | Total assets (end of year) |
| `noncash_contributions_total` | INTEGER | Total noncash (Part VIII) |
| `has_schedule_m` | BOOLEAN | Filed Schedule M? |
| ... | | *(41 columns total — see `setup_bigquery.py`)* |

### `schedule_m`

Source: Parsed from 990 XML Schedule M

Each of the 28 property types (Lines 1-28) has four columns:

| Suffix | Type | Meaning |
|--------|------|---------|
| `_x` | BOOLEAN | Was this property type received? |
| `_count` | INTEGER | Number of contributions |
| `_amount` | INTEGER | Dollar amount reported |
| `_method` | STRING | Valuation method |

Property types include: `food_inventory`, `clothing_household`,
`cars_vehicles`, `books_publications`, `drugs_medical`,
`securities_publicly_traded`, `real_estate_*`, `art_*`,
`collectibles`, and more (28 categories).

Lines 25-28 (`other_1` through `other_4`) also have a `_desc` column
for the free-text category description.

Summary fields: `num_forms_8283`, `gift_acceptance_policy`,
`uses_third_parties`, `hold_3_years_required`.

### `vw_inkind_prospecting` (view)

A flat join of all three tables, designed for non-technical staff.
Includes convenience columns:

| Column | Description |
|--------|-------------|
| `ntee_major_group` | Human-readable NTEE category name |
| `accepts_food` | TRUE if org received food donations |
| `accepts_clothing` | TRUE if org received clothing/household goods |
| `accepts_vehicles` | TRUE if org received vehicle donations |
| `accepts_books` | TRUE if org received books/publications |
| `accepts_drugs_medical` | TRUE if org received drugs/medical supplies |
| `total_noncash_amount` | Sum of all property-type amounts |
| `noncash_category_count` | Number of distinct property types received |

### Example queries

```sql
-- Find food banks that accept food donations, sorted by amount
SELECT org_name, city, state, org_phone, website,
       food_inventory_amount, total_revenue_cy
FROM `project.irs_501c3.vw_inkind_prospecting`
WHERE accepts_food IS TRUE
ORDER BY food_inventory_amount DESC
LIMIT 100;

-- Find orgs accepting clothing by state
SELECT state, COUNT(*) AS org_count,
       SUM(clothing_household_amount) AS total_clothing_value
FROM `project.irs_501c3.vw_inkind_prospecting`
WHERE accepts_clothing IS TRUE
GROUP BY state
ORDER BY org_count DESC;

-- Orgs with the most diverse in-kind donation types
SELECT org_name, city, state, noncash_category_count,
       total_noncash_amount, website
FROM `project.irs_501c3.vw_inkind_prospecting`
WHERE noncash_category_count >= 3
ORDER BY noncash_category_count DESC, total_noncash_amount DESC
LIMIT 50;
```

## Testing

```bash
pip install pytest
pytest tests/ -v
```

## Project structure

```
pod/
  README.md                     # This file
  requirements.txt              # Python dependencies
  .env.example                  # Environment config template
  setup_bigquery.py             # Create BigQuery dataset + tables
  pipeline/
    __init__.py
    config.py                   # Constants, URLs, field mappings
    download_bmf.py             # Download + filter EO BMF CSVs
    download_index.py           # Download 990 e-file index from S3
    download_xml.py             # Parallel XML download from S3
    concordance.py              # Master Concordance File loader
    parse_990.py                # XML parser → JSONL
    load_bigquery.py            # GCS upload + BigQuery load
    views.py                    # Prospecting view creation
  scripts/
    run_full_pipeline.py        # Full end-to-end pipeline
    run_bmf_only.py             # BMF-only refresh
    run_990_incremental.py      # Incremental 990 update
  tests/
    test_parse_990.py           # XML parser unit tests
```

## Data limitations

- **Schedule M** is only filed by full Form 990 filers (not 990-EZ or 990-PF).
  This covers organisations with >$200K revenue or >$500K assets.
- **Email addresses** are not collected on any IRS form. For email enrichment,
  use a third-party service (Candid/GuideStar, Hunter.io) or scrape org
  websites as a separate step.
- **E-file coverage**: ~60-65% of 990 filers filed electronically before 2022.
  E-filing became mandatory in 2022, so recent years have near-complete coverage.
- The IRS data has a filing lag of 6-18 months. The most recent complete tax
  year available is typically 1-2 years behind the current date.

## License

Public domain data from the IRS. Pipeline code is provided as-is.

"""Configuration constants for the IRS 501(c)(3) data pipeline."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# GCP / BigQuery / GCS
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_LOCATION = os.getenv("GCP_LOCATION", "US")
BQ_DATASET = os.getenv("BQ_DATASET", "irs_501c3")
GCS_BUCKET = os.getenv("GCS_BUCKET", "")

# ---------------------------------------------------------------------------
# Local data directories
# ---------------------------------------------------------------------------
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
BMF_DIR = DATA_DIR / "bmf"
INDEX_DIR = DATA_DIR / "index"
XML_DIR = DATA_DIR / "xml"
PARSED_DIR = DATA_DIR / "parsed"
CONCORDANCE_DIR = DATA_DIR / "concordance"

for d in [BMF_DIR, INDEX_DIR, XML_DIR, PARSED_DIR, CONCORDANCE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Pipeline settings
# ---------------------------------------------------------------------------
XML_DOWNLOAD_CONCURRENCY = int(os.getenv("XML_DOWNLOAD_CONCURRENCY", "50"))
TAX_YEARS = [
    int(y.strip())
    for y in os.getenv("TAX_YEARS", "2022,2023,2024").split(",")
]

# ---------------------------------------------------------------------------
# IRS EO BMF – CSV download URLs
# ---------------------------------------------------------------------------
BMF_BASE_URL = "https://www.irs.gov/pub/irs-soi"

# All 50 states + DC + PR + international
BMF_STATE_CODES = [
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl",
    "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me",
    "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh",
    "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri",
    "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi",
    "wy", "pr",
]

BMF_URLS = {
    code: f"{BMF_BASE_URL}/eo_{code}.csv" for code in BMF_STATE_CODES
}

# ---------------------------------------------------------------------------
# IRS 990 E-File on AWS S3
# ---------------------------------------------------------------------------
S3_BASE_URL = "https://s3.amazonaws.com/irs-form-990"


def s3_index_url(year: int) -> str:
    """Return the CSV index URL for a given filing year."""
    return f"{S3_BASE_URL}/index_{year}.csv"


def s3_xml_url(object_id: str) -> str:
    """Return the XML filing URL for a given object ID."""
    return f"{S3_BASE_URL}/{object_id}_public.xml"


# ---------------------------------------------------------------------------
# Master Concordance File
# ---------------------------------------------------------------------------
CONCORDANCE_CSV_URL = (
    "https://raw.githubusercontent.com/Nonprofit-Open-Data-Collective/"
    "irs-efile-master-concordance-file/master/concordance.csv"
)

# ---------------------------------------------------------------------------
# EO BMF column names (as they appear in the IRS CSV files)
# ---------------------------------------------------------------------------
BMF_COLUMNS = [
    "EIN", "NAME", "ICO", "STREET", "CITY", "STATE", "ZIP",
    "GROUP", "SUBSECTION", "AFFILIATION", "CLASSIFICATION",
    "RULING", "DEDUCTIBILITY", "FOUNDATION", "ACTIVITY",
    "ORGANIZATION", "STATUS", "TAX_PERIOD", "ASSET_CD",
    "INCOME_CD", "FILING_REQ_CD", "PF_FILING_REQ_CD",
    "ACCT_PD", "ASSET_AMT", "INCOME_AMT", "REVENUE_AMT",
    "NTEE_CD", "SORT_NAME",
]

# Mapping from BMF CSV columns to our BigQuery column names
BMF_COLUMN_MAP = {
    "EIN": "ein",
    "NAME": "name",
    "SORT_NAME": "sort_name",
    "STREET": "street",
    "CITY": "city",
    "STATE": "state",
    "ZIP": "zip",
    "SUBSECTION": "subsection",
    "CLASSIFICATION": "classification",
    "NTEE_CD": "ntee_code",
    "FOUNDATION": "foundation_code",
    "AFFILIATION": "affiliation",
    "ORGANIZATION": "organization_type",
    "STATUS": "status",
    "RULING": "ruling_date",
    "DEDUCTIBILITY": "deductibility",
    "ASSET_CD": "asset_code",
    "ASSET_AMT": "asset_amount",
    "INCOME_CD": "income_code",
    "INCOME_AMT": "income_amount",
    "REVENUE_AMT": "revenue_amount",
    "TAX_PERIOD": "tax_period",
    "FILING_REQ_CD": "filing_requirement",
    "ACTIVITY": "activity_codes",
    "GROUP": "group_number",
    "ACCT_PD": "accounting_period",
}

# ---------------------------------------------------------------------------
# BigQuery table names
# ---------------------------------------------------------------------------
BQ_TABLE_ORGANIZATIONS = "organizations"
BQ_TABLE_FILINGS = "filings"
BQ_TABLE_SCHEDULE_M = "schedule_m"
BQ_VIEW_PROSPECTING = "vw_inkind_prospecting"

# ---------------------------------------------------------------------------
# Schedule M property type definitions (lines 1-28)
# Each tuple: (line_number, field_prefix, description)
# ---------------------------------------------------------------------------
SCHEDULE_M_PROPERTY_TYPES = [
    (1, "art_works", "Art - Works of art"),
    (2, "art_historical", "Art - Historical treasures"),
    (3, "art_fractional", "Art - Fractional interests"),
    (4, "books_publications", "Books and publications"),
    (5, "clothing_household", "Clothing and household goods"),
    (6, "cars_vehicles", "Cars and other vehicles"),
    (7, "boats_planes", "Boats and planes"),
    (8, "intellectual_property", "Intellectual property"),
    (9, "securities_publicly_traded", "Securities - Publicly traded"),
    (10, "securities_closely_held", "Securities - Closely held stock"),
    (11, "securities_partnership", "Securities - Partnership, LLC, or trust"),
    (12, "securities_misc", "Securities - Miscellaneous"),
    (13, "conservation_historic", "Qualified conservation - Historic structures"),
    (14, "conservation_other", "Qualified conservation - Other"),
    (15, "real_estate_residential", "Real estate - Residential"),
    (16, "real_estate_commercial", "Real estate - Commercial"),
    (17, "real_estate_other", "Real estate - Other"),
    (18, "collectibles", "Collectibles"),
    (19, "food_inventory", "Food inventory"),
    (20, "drugs_medical", "Drugs and medical supplies"),
    (21, "taxidermy", "Taxidermy"),
    (22, "historical_artifacts", "Historical artifacts"),
    (23, "scientific_specimens", "Scientific specimens"),
    (24, "archaeological_artifacts", "Archeological artifacts"),
    (25, "other_1", "Other (1)"),
    (26, "other_2", "Other (2)"),
    (27, "other_3", "Other (3)"),
    (28, "other_4", "Other (4)"),
]

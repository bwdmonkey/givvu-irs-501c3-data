#!/usr/bin/env python3
"""Create or replace the BigQuery prospecting view.

The ``vw_inkind_prospecting`` view joins the organisations, filings, and
schedule_m tables into a single flat table designed for non-technical staff
to explore organisations that accept in-kind donations.
"""

from __future__ import annotations

import logging

from google.cloud import bigquery

from pipeline.config import (
    BQ_DATASET,
    BQ_TABLE_FILINGS,
    BQ_TABLE_ORGANIZATIONS,
    BQ_TABLE_SCHEDULE_M,
    BQ_VIEW_PROSPECTING,
    GCP_PROJECT_ID,
    SCHEDULE_M_PROPERTY_TYPES,
)

logger = logging.getLogger(__name__)


def _property_type_cols() -> str:
    """Generate SQL column references for all Schedule M property types."""
    lines: list[str] = []
    for _line, prefix, _desc in SCHEDULE_M_PROPERTY_TYPES:
        lines.append(f"    m.{prefix}_x")
        lines.append(f"    m.{prefix}_count")
        lines.append(f"    m.{prefix}_amount")
        lines.append(f"    m.{prefix}_method")
        if prefix.startswith("other_"):
            lines.append(f"    m.{prefix}_desc")
    return ",\n".join(lines)


def _noncash_amount_sum() -> str:
    """Generate a COALESCE+sum expression for total noncash amounts."""
    parts = []
    for _line, prefix, _desc in SCHEDULE_M_PROPERTY_TYPES:
        parts.append(f"COALESCE(m.{prefix}_amount, 0)")
    return " + ".join(parts)


def _noncash_category_count() -> str:
    """Generate expression counting how many property types have a check."""
    parts = []
    for _line, prefix, _desc in SCHEDULE_M_PROPERTY_TYPES:
        parts.append(f"CASE WHEN m.{prefix}_x IS TRUE THEN 1 ELSE 0 END")
    return " + ".join(parts)


def build_view_sql() -> str:
    """Return the CREATE OR REPLACE VIEW SQL statement."""
    dataset = f"`{GCP_PROJECT_ID}.{BQ_DATASET}`"
    orgs = f"{dataset}.{BQ_TABLE_ORGANIZATIONS}"
    filings = f"{dataset}.{BQ_TABLE_FILINGS}"
    sched_m = f"{dataset}.{BQ_TABLE_SCHEDULE_M}"

    property_cols = _property_type_cols()
    noncash_sum = _noncash_amount_sum()
    noncash_count = _noncash_category_count()

    sql = f"""CREATE OR REPLACE VIEW {dataset}.{BQ_VIEW_PROSPECTING} AS
WITH latest_filing AS (
    -- Pick the most recent filing per EIN
    SELECT *
    FROM (
        SELECT
            f.*,
            ROW_NUMBER() OVER (
                PARTITION BY f.ein ORDER BY f.tax_year DESC, f.object_id DESC
            ) AS _rn
        FROM {filings} AS f
    )
    WHERE _rn = 1
)
SELECT
    -- Organisation identity
    o.ein,
    o.name                          AS org_name,
    o.sort_name,
    o.street,
    o.city,
    o.state,
    o.zip,
    o.ntee_code,
    CASE SUBSTR(o.ntee_code, 1, 1)
        WHEN 'A' THEN 'Arts, Culture & Humanities'
        WHEN 'B' THEN 'Education'
        WHEN 'C' THEN 'Environment and Animals'
        WHEN 'D' THEN 'Animal-Related'
        WHEN 'E' THEN 'Health'
        WHEN 'F' THEN 'Mental Health & Crisis'
        WHEN 'G' THEN 'Diseases, Disorders & Medical'
        WHEN 'H' THEN 'Medical Research'
        WHEN 'I' THEN 'Crime & Legal-Related'
        WHEN 'J' THEN 'Employment & Job-Related'
        WHEN 'K' THEN 'Food, Agriculture & Nutrition'
        WHEN 'L' THEN 'Housing & Shelter'
        WHEN 'M' THEN 'Public Safety & Disaster'
        WHEN 'N' THEN 'Recreation & Sports'
        WHEN 'O' THEN 'Youth Development'
        WHEN 'P' THEN 'Human Services'
        WHEN 'Q' THEN 'International'
        WHEN 'R' THEN 'Civil Rights & Advocacy'
        WHEN 'S' THEN 'Community Improvement'
        WHEN 'T' THEN 'Philanthropy & Voluntarism'
        WHEN 'U' THEN 'Science & Technology'
        WHEN 'V' THEN 'Social Science'
        WHEN 'W' THEN 'Public & Societal Benefit'
        WHEN 'X' THEN 'Religion Related'
        WHEN 'Y' THEN 'Mutual & Membership Benefit'
        WHEN 'Z' THEN 'Unknown / Unclassified'
        ELSE 'Unknown / Unclassified'
    END                             AS ntee_major_group,
    o.foundation_code,
    o.ruling_date,
    o.asset_code,

    -- Contact info
    lf.org_phone,
    lf.website,
    lf.principal_officer_name,
    lf.signing_officer_name,
    lf.signing_officer_title,
    lf.signing_officer_phone,

    -- Filing metadata
    lf.tax_year,
    lf.year_formation,
    lf.mission,
    lf.form_type,

    -- Staffing
    lf.num_employees,
    lf.num_volunteers,
    lf.num_voting_members,

    -- Financials
    lf.total_revenue_cy,
    lf.total_expenses_cy,
    lf.contributions_grants_cy,
    lf.program_service_revenue_cy,
    lf.salaries_cy,
    lf.total_assets_eoy,
    lf.total_liabilities_eoy,
    lf.net_assets_eoy,
    lf.noncash_contributions_total,
    lf.has_schedule_m,

    -- Schedule M: all property-type columns
{property_cols},
    m.num_forms_8283,
    m.gift_acceptance_policy,
    m.uses_third_parties,

    -- Computed convenience columns
    COALESCE(m.food_inventory_x, FALSE)             AS accepts_food,
    COALESCE(m.clothing_household_x, FALSE)         AS accepts_clothing,
    COALESCE(m.cars_vehicles_x, FALSE)              AS accepts_vehicles,
    COALESCE(m.books_publications_x, FALSE)         AS accepts_books,
    COALESCE(m.drugs_medical_x, FALSE)              AS accepts_drugs_medical,
    COALESCE(m.securities_publicly_traded_x, FALSE) AS accepts_securities,
    ({noncash_sum})                                 AS total_noncash_amount,
    ({noncash_count})                               AS noncash_category_count

FROM {orgs} AS o
LEFT JOIN latest_filing AS lf ON o.ein = lf.ein
LEFT JOIN {sched_m} AS m ON lf.object_id = m.object_id
"""
    return sql


def create_prospecting_view() -> None:
    """Execute CREATE OR REPLACE VIEW for the prospecting view."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    sql = build_view_sql()
    logger.info("Creating view %s.%s ...", BQ_DATASET, BQ_VIEW_PROSPECTING)
    query_job = client.query(sql)
    query_job.result()
    logger.info("View %s created successfully.", BQ_VIEW_PROSPECTING)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    # Print the SQL for review
    print(build_view_sql())
    print("\n--- Executing ---")
    create_prospecting_view()

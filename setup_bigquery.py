#!/usr/bin/env python3
"""Create the BigQuery dataset and tables for the IRS 501(c)(3) pipeline.

Run this once to bootstrap the BigQuery schema. Subsequent pipeline runs
will load data into the tables created here.

Usage:
    python setup_bigquery.py
"""

from google.cloud import bigquery

from pipeline.config import (
    BQ_DATASET,
    BQ_TABLE_FILINGS,
    BQ_TABLE_ORGANIZATIONS,
    BQ_TABLE_SCHEDULE_M,
    GCP_LOCATION,
    GCP_PROJECT_ID,
    SCHEDULE_M_PROPERTY_TYPES,
)


def get_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT_ID)


# ── Table schemas ─────────────────────────────────────────────────────────


def organizations_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("ein", "STRING", mode="REQUIRED",
                             description="Employer Identification Number"),
        bigquery.SchemaField("name", "STRING", description="Primary organization name"),
        bigquery.SchemaField("sort_name", "STRING", description="Secondary / DBA name"),
        bigquery.SchemaField("street", "STRING", description="Street address"),
        bigquery.SchemaField("city", "STRING", description="City"),
        bigquery.SchemaField("state", "STRING", description="2-letter state code"),
        bigquery.SchemaField("zip", "STRING", description="ZIP code"),
        bigquery.SchemaField("subsection", "INTEGER", description="IRC subsection (03=501c3)"),
        bigquery.SchemaField("classification", "STRING", description="Classification code(s)"),
        bigquery.SchemaField("ntee_code", "STRING", description="NTEE category code"),
        bigquery.SchemaField("foundation_code", "INTEGER", description="Foundation type code"),
        bigquery.SchemaField("affiliation", "INTEGER", description="Affiliation code"),
        bigquery.SchemaField("organization_type", "INTEGER", description="Corp/Trust/Assoc/Other"),
        bigquery.SchemaField("status", "INTEGER", description="EO status code"),
        bigquery.SchemaField("ruling_date", "STRING", description="Date granted tax-exempt status (YYYYMM)"),
        bigquery.SchemaField("deductibility", "INTEGER", description="Deductibility code"),
        bigquery.SchemaField("asset_code", "INTEGER", description="Asset range code (0-9)"),
        bigquery.SchemaField("asset_amount", "INTEGER", description="End-of-year asset amount"),
        bigquery.SchemaField("income_code", "INTEGER", description="Income range code (0-9)"),
        bigquery.SchemaField("income_amount", "INTEGER", description="Gross receipts amount"),
        bigquery.SchemaField("revenue_amount", "INTEGER", description="Form 990 revenue amount"),
        bigquery.SchemaField("tax_period", "INTEGER", description="Tax period (YYYYMM)"),
        bigquery.SchemaField("filing_requirement", "STRING", description="Filing requirement code"),
        bigquery.SchemaField("activity_codes", "STRING", description="Activity codes"),
        bigquery.SchemaField("group_number", "STRING", description="Group exemption number"),
        bigquery.SchemaField("accounting_period", "INTEGER", description="Accounting period end month"),
    ]


def filings_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("object_id", "STRING", mode="REQUIRED",
                             description="Unique filing identifier"),
        bigquery.SchemaField("ein", "STRING", mode="REQUIRED",
                             description="Employer Identification Number"),
        bigquery.SchemaField("tax_year", "INTEGER", description="Tax year"),
        bigquery.SchemaField("tax_period_begin", "DATE", description="Tax period start"),
        bigquery.SchemaField("tax_period_end", "DATE", description="Tax period end"),
        bigquery.SchemaField("form_type", "STRING", description="Form type (990)"),
        bigquery.SchemaField("org_name", "STRING", description="Name as filed"),
        bigquery.SchemaField("org_city", "STRING", description="City as filed"),
        bigquery.SchemaField("org_state", "STRING", description="State as filed"),
        bigquery.SchemaField("org_zip", "STRING", description="ZIP as filed"),
        bigquery.SchemaField("org_phone", "STRING", description="Organization phone"),
        bigquery.SchemaField("website", "STRING", description="Organization website"),
        bigquery.SchemaField("principal_officer_name", "STRING",
                             description="Name of principal officer"),
        bigquery.SchemaField("signing_officer_name", "STRING",
                             description="Name of signing officer"),
        bigquery.SchemaField("signing_officer_title", "STRING",
                             description="Title of signing officer"),
        bigquery.SchemaField("signing_officer_phone", "STRING",
                             description="Signing officer phone number"),
        bigquery.SchemaField("year_formation", "INTEGER", description="Year formed"),
        bigquery.SchemaField("mission", "STRING", description="Mission statement"),
        bigquery.SchemaField("num_voting_members", "INTEGER",
                             description="Voting members of governing body"),
        bigquery.SchemaField("num_voting_members_independent", "INTEGER",
                             description="Independent voting members"),
        bigquery.SchemaField("num_employees", "INTEGER", description="Total employees"),
        bigquery.SchemaField("num_volunteers", "INTEGER", description="Total volunteers"),
        bigquery.SchemaField("contributions_grants_cy", "INTEGER",
                             description="Contributions + grants, current year"),
        bigquery.SchemaField("program_service_revenue_cy", "INTEGER",
                             description="Program service revenue, CY"),
        bigquery.SchemaField("investment_income_cy", "INTEGER",
                             description="Investment income, CY"),
        bigquery.SchemaField("other_revenue_cy", "INTEGER",
                             description="Other revenue, CY"),
        bigquery.SchemaField("total_revenue_cy", "INTEGER",
                             description="Total revenue, CY"),
        bigquery.SchemaField("total_revenue_py", "INTEGER",
                             description="Total revenue, prior year"),
        bigquery.SchemaField("grants_similar_cy", "INTEGER",
                             description="Grants and similar amounts paid, CY"),
        bigquery.SchemaField("salaries_cy", "INTEGER",
                             description="Salaries, other compensation, CY"),
        bigquery.SchemaField("total_expenses_cy", "INTEGER",
                             description="Total expenses, CY"),
        bigquery.SchemaField("total_expenses_py", "INTEGER",
                             description="Total expenses, prior year"),
        bigquery.SchemaField("revenue_less_expenses_cy", "INTEGER",
                             description="Excess/deficit, CY"),
        bigquery.SchemaField("total_assets_boy", "INTEGER",
                             description="Total assets, BOY"),
        bigquery.SchemaField("total_assets_eoy", "INTEGER",
                             description="Total assets, EOY"),
        bigquery.SchemaField("total_liabilities_boy", "INTEGER",
                             description="Total liabilities, BOY"),
        bigquery.SchemaField("total_liabilities_eoy", "INTEGER",
                             description="Total liabilities, EOY"),
        bigquery.SchemaField("net_assets_boy", "INTEGER",
                             description="Net assets/fund balances, BOY"),
        bigquery.SchemaField("net_assets_eoy", "INTEGER",
                             description="Net assets/fund balances, EOY"),
        bigquery.SchemaField("noncash_contributions_total", "INTEGER",
                             description="Total noncash contributions (Part VIII Line 1g)"),
        bigquery.SchemaField("has_schedule_m", "BOOLEAN",
                             description="Filed Schedule M (Part IV Line 29/30)"),
    ]


def schedule_m_schema() -> list[bigquery.SchemaField]:
    fields: list[bigquery.SchemaField] = [
        bigquery.SchemaField("object_id", "STRING", mode="REQUIRED",
                             description="Unique filing identifier"),
        bigquery.SchemaField("ein", "STRING", mode="REQUIRED",
                             description="Employer Identification Number"),
        bigquery.SchemaField("tax_year", "INTEGER", description="Tax year"),
    ]

    for _line, prefix, desc in SCHEDULE_M_PROPERTY_TYPES:
        fields.append(bigquery.SchemaField(
            f"{prefix}_x", "BOOLEAN", description=f"{desc} - received"))
        fields.append(bigquery.SchemaField(
            f"{prefix}_count", "INTEGER", description=f"{desc} - count"))
        fields.append(bigquery.SchemaField(
            f"{prefix}_amount", "INTEGER", description=f"{desc} - amount"))
        fields.append(bigquery.SchemaField(
            f"{prefix}_method", "STRING", description=f"{desc} - valuation method"))
        # Lines 25-28 have a description/free-text field
        if prefix.startswith("other_"):
            fields.append(bigquery.SchemaField(
                f"{prefix}_desc", "STRING", description=f"{desc} - description"))

    fields.extend([
        bigquery.SchemaField("num_forms_8283", "INTEGER",
                             description="Number of Forms 8283 received"),
        bigquery.SchemaField("hold_3_years_required", "BOOLEAN",
                             description="Must hold property 3+ years"),
        bigquery.SchemaField("gift_acceptance_policy", "BOOLEAN",
                             description="Has gift acceptance policy"),
        bigquery.SchemaField("uses_third_parties", "BOOLEAN",
                             description="Uses third parties for noncash"),
    ])

    return fields


# ── Create / update ───────────────────────────────────────────────────────


def create_dataset(client: bigquery.Client) -> bigquery.Dataset:
    dataset_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = GCP_LOCATION
    dataset.description = "IRS 501(c)(3) organizations, 990 filings, and Schedule M noncash contributions"
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset.dataset_id} ready.")
    return dataset


def create_table(
    client: bigquery.Client,
    table_name: str,
    schema: list[bigquery.SchemaField],
    description: str = "",
    clustering_fields: list[str] | None = None,
) -> bigquery.Table:
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    table = bigquery.Table(table_ref, schema=schema)
    table.description = description
    if clustering_fields:
        table.clustering_fields = clustering_fields
    table = client.create_table(table, exists_ok=True)
    print(f"Table {table.table_id} ready ({len(schema)} columns).")
    return table


def main() -> None:
    client = get_client()
    create_dataset(client)

    create_table(
        client,
        BQ_TABLE_ORGANIZATIONS,
        organizations_schema(),
        description="IRS EO BMF – 501(c)(3) organizations",
        clustering_fields=["state", "ntee_code"],
    )

    create_table(
        client,
        BQ_TABLE_FILINGS,
        filings_schema(),
        description="990 e-file header + Part I financial summary",
        clustering_fields=["ein", "tax_year"],
    )

    create_table(
        client,
        BQ_TABLE_SCHEDULE_M,
        schedule_m_schema(),
        description="Schedule M – Noncash contributions by property type",
        clustering_fields=["ein", "tax_year"],
    )

    print("\nAll tables created. Run the pipeline to load data.")


if __name__ == "__main__":
    main()

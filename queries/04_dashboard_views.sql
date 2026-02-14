-- ============================================================================
-- 04 DASHBOARD VIEWS — Materialized tables optimized for Looker Studio
--
-- Looker Studio works best with flat tables. These queries create
-- materialized views that pre-compute the aggregations so dashboards
-- load instantly instead of scanning the full dataset each time.
--
-- Run each CREATE statement once. Re-run to refresh after a pipeline update.
-- ============================================================================


-- ──────────────────────────────────────────────────────────────────────────
-- VIEW 1: Prospecting list (one row per org, flat, dashboard-ready)
-- This is the main table Looker Studio will connect to.
-- ──────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting` AS
SELECT
  -- Identity
  ein,
  org_name,
  city,
  state,
  zip,
  ntee_code,
  ntee_major_group,
  ruling_date,

  -- Contact
  org_phone,
  website,
  principal_officer_name,
  signing_officer_name,
  signing_officer_title,
  signing_officer_phone,

  -- Filing
  tax_year,
  mission,

  -- Staffing
  num_employees,
  num_volunteers,

  -- Financials
  total_revenue_cy,
  total_expenses_cy,
  total_assets_eoy,
  contributions_grants_cy,
  noncash_contributions_total,

  -- Revenue tier (for filtering/charting)
  CASE
    WHEN total_revenue_cy IS NULL       THEN 'Unknown'
    WHEN total_revenue_cy < 100000      THEN '1. Under $100K'
    WHEN total_revenue_cy < 1000000     THEN '2. $100K–$1M'
    WHEN total_revenue_cy < 10000000    THEN '3. $1M–$10M'
    WHEN total_revenue_cy < 100000000   THEN '4. $10M–$100M'
    ELSE                                     '5. $100M+'
  END AS revenue_tier,

  -- In-kind donation flags (booleans for Looker filters)
  COALESCE(accepts_food, FALSE)             AS accepts_food,
  COALESCE(accepts_clothing, FALSE)         AS accepts_clothing,
  COALESCE(accepts_vehicles, FALSE)         AS accepts_vehicles,
  COALESCE(accepts_books, FALSE)            AS accepts_books,
  COALESCE(accepts_drugs_medical, FALSE)    AS accepts_drugs_medical,
  COALESCE(accepts_securities, FALSE)       AS accepts_securities,
  COALESCE(has_schedule_m, FALSE)           AS has_schedule_m,

  -- In-kind amounts
  COALESCE(food_inventory_amount, 0)             AS food_amount,
  COALESCE(clothing_household_amount, 0)         AS clothing_amount,
  COALESCE(cars_vehicles_amount, 0)              AS vehicles_amount,
  COALESCE(books_publications_amount, 0)         AS books_amount,
  COALESCE(drugs_medical_amount, 0)              AS drugs_medical_amount,
  COALESCE(securities_publicly_traded_amount, 0) AS securities_amount,
  COALESCE(real_estate_residential_amount, 0)
    + COALESCE(real_estate_commercial_amount, 0)
    + COALESCE(real_estate_other_amount, 0)      AS real_estate_amount,
  COALESCE(total_noncash_amount, 0)              AS total_noncash_amount,
  COALESCE(noncash_category_count, 0)            AS noncash_category_count,

  -- Other donation descriptions
  other_1_desc,
  COALESCE(other_1_amount, 0) AS other_1_amount,
  other_2_desc,
  COALESCE(other_2_amount, 0) AS other_2_amount,

  -- Policy
  gift_acceptance_policy,
  uses_third_parties

FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
-- Use schedule_m join presence instead of has_schedule_m flag (which
-- depends on the filings table being reloaded after parser fixes).
WHERE ein IN (
  SELECT ein FROM `irs-dataset-487317.irs_501c3_data_bq.schedule_m`
);


-- ──────────────────────────────────────────────────────────────────────────
-- VIEW 2: State-level summary (for the map chart)
-- ──────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `irs-dataset-487317.irs_501c3_data_bq.dashboard_by_state` AS
SELECT
  state,
  COUNT(*)                                       AS org_count,
  SUM(total_noncash_amount)                      AS total_noncash,
  ROUND(AVG(total_noncash_amount))               AS avg_noncash,
  COUNTIF(accepts_food)                          AS food_orgs,
  COUNTIF(accepts_clothing)                      AS clothing_orgs,
  COUNTIF(accepts_vehicles)                      AS vehicle_orgs,
  COUNTIF(accepts_drugs_medical)                 AS medical_orgs,
  SUM(COALESCE(food_amount, 0))                  AS food_total,
  SUM(COALESCE(clothing_amount, 0))              AS clothing_total
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
GROUP BY state;


-- ──────────────────────────────────────────────────────────────────────────
-- VIEW 3: NTEE sector summary (for the bar chart)
-- ──────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `irs-dataset-487317.irs_501c3_data_bq.dashboard_by_sector` AS
SELECT
  ntee_major_group,
  COUNT(*)                          AS org_count,
  SUM(total_noncash_amount)         AS total_noncash,
  ROUND(AVG(total_noncash_amount))  AS avg_noncash,
  COUNTIF(accepts_food)             AS food_orgs,
  COUNTIF(accepts_clothing)         AS clothing_orgs,
  COUNTIF(accepts_drugs_medical)    AS medical_orgs
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
GROUP BY ntee_major_group;


-- ──────────────────────────────────────────────────────────────────────────
-- VIEW 4: Donation type breakdown (for the pie/donut chart)
-- ──────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `irs-dataset-487317.irs_501c3_data_bq.dashboard_donation_types` AS
SELECT category, org_count, total_amount FROM (
  SELECT 'Food'                AS category, COUNTIF(accepts_food) AS org_count, SUM(food_amount) AS total_amount FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
  UNION ALL
  SELECT 'Clothing & Household', COUNTIF(accepts_clothing), SUM(clothing_amount) FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
  UNION ALL
  SELECT 'Vehicles', COUNTIF(accepts_vehicles), SUM(vehicles_amount) FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
  UNION ALL
  SELECT 'Books & Publications', COUNTIF(accepts_books), SUM(books_amount) FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
  UNION ALL
  SELECT 'Drugs & Medical', COUNTIF(accepts_drugs_medical), SUM(drugs_medical_amount) FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
  UNION ALL
  SELECT 'Securities', COUNTIF(accepts_securities), SUM(securities_amount) FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
  UNION ALL
  SELECT 'Real Estate', COUNTIF(real_estate_amount > 0), SUM(real_estate_amount) FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
)
WHERE org_count > 0
ORDER BY total_amount DESC;

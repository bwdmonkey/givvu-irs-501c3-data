-- ============================================================================
-- 05 ADVANCED PROSPECTING — Targeted queries for sales/outreach teams
-- All queries run against the dashboard_prospecting table for speed.
-- ============================================================================


-- ──────────────────────────────────────────────────────────────────────────
-- ELECTRONICS & TECHNOLOGY RECIPIENTS
-- ──────────────────────────────────────────────────────────────────────────

-- Orgs that specifically accept electronics, computers, laptops, tech
SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  other_1_desc, other_1_amount,
  other_2_desc, other_2_amount,
  total_noncash_amount, total_revenue_cy, num_employees
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE REGEXP_CONTAINS(
  LOWER(CONCAT(COALESCE(other_1_desc,''), ' ', COALESCE(other_2_desc,''))),
  r'electron|computer|laptop|tablet|phone|tech|device|hardware|software|applia|it equip'
)
ORDER BY COALESCE(other_1_amount,0) + COALESCE(other_2_amount,0) DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- FURNITURE & HOUSEHOLD ITEMS RECIPIENTS
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  clothing_household_amount,
  other_1_desc, other_1_amount,
  other_2_desc, other_2_amount,
  total_noncash_amount
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE accepts_clothing IS TRUE
   OR REGEXP_CONTAINS(
        LOWER(CONCAT(COALESCE(other_1_desc,''), ' ', COALESCE(other_2_desc,''))),
        r'furnitur|household|mattress|bedding|linen|applian'
      )
ORDER BY COALESCE(clothing_household_amount,0) + COALESCE(other_1_amount,0) DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- LARGE FOOD BANKS — $1M+ in food donations
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  food_inventory_amount,
  drugs_medical_amount,
  total_noncash_amount,
  total_revenue_cy,
  num_employees, num_volunteers
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE accepts_food IS TRUE
  AND food_inventory_amount >= 1000000
ORDER BY food_inventory_amount DESC;


-- ──────────────────────────────────────────────────────────────────────────
-- MEDICAL / HEALTH ORGS accepting drugs, medical supplies, or equipment
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  drugs_medical_amount,
  other_1_desc, other_1_amount,
  total_noncash_amount, total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE accepts_drugs_medical IS TRUE
   OR REGEXP_CONTAINS(
        LOWER(CONCAT(COALESCE(other_1_desc,''), ' ', COALESCE(other_2_desc,''))),
        r'medical|pharma|drug|health|dental|surgical|hospital|clinic'
      )
ORDER BY COALESCE(drugs_medical_amount,0) + COALESCE(other_1_amount,0) DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- SCHOOL SUPPLIES & EDUCATION ORGS
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  books_publications_amount,
  other_1_desc, other_1_amount,
  other_2_desc, other_2_amount,
  total_noncash_amount, total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE accepts_books IS TRUE
   OR ntee_major_group = 'Education'
   OR REGEXP_CONTAINS(
        LOWER(CONCAT(COALESCE(other_1_desc,''), ' ', COALESCE(other_2_desc,''))),
        r'school suppl|education|textbook|learning|curriculum'
      )
ORDER BY COALESCE(books_publications_amount,0) + COALESCE(other_1_amount,0) DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- VEHICLE DONATION PROGRAMS
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  cars_vehicles_amount,
  boats_planes_amount,
  total_noncash_amount, total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE accepts_vehicles IS TRUE
ORDER BY cars_vehicles_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- ORGS WITH GIFT ACCEPTANCE POLICIES (more likely to be organized buyers)
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  noncash_category_count,
  total_noncash_amount,
  accepts_food, accepts_clothing, accepts_vehicles,
  gift_acceptance_policy,
  uses_third_parties,
  total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE gift_acceptance_policy IS TRUE
  AND total_noncash_amount > 100000
ORDER BY total_noncash_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- ORGS USING THIRD PARTIES (partnerships, resellers, liquidators)
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  total_noncash_amount,
  noncash_category_count,
  accepts_food, accepts_clothing, accepts_vehicles,
  total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE uses_third_parties IS TRUE
ORDER BY total_noncash_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- GEOGRAPHIC: Metro area targeting (city-level)
-- ──────────────────────────────────────────────────────────────────────────

-- Change the cities to target your metro area
SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  total_noncash_amount,
  accepts_food, accepts_clothing, accepts_vehicles, accepts_drugs_medical,
  other_1_desc, other_1_amount,
  total_revenue_cy, num_employees
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE UPPER(city) IN ('NEW YORK', 'BROOKLYN', 'BRONX', 'QUEENS', 'MANHATTAN')
  AND total_noncash_amount > 0
ORDER BY total_noncash_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- FASTEST-GROWING DONATION PROGRAMS (high volume + many categories)
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  noncash_category_count,
  total_noncash_amount,
  total_revenue_cy,
  ROUND(SAFE_DIVIDE(total_noncash_amount, total_revenue_cy) * 100, 1) AS noncash_pct_of_revenue,
  num_employees
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE total_noncash_amount > 50000
  AND total_revenue_cy > 0
ORDER BY SAFE_DIVIDE(total_noncash_amount, total_revenue_cy) DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- CRYPTO / CRYPTOCURRENCY DONATION RECIPIENTS
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  other_1_desc, other_1_amount,
  other_2_desc, other_2_amount,
  total_noncash_amount, total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE REGEXP_CONTAINS(
  LOWER(CONCAT(COALESCE(other_1_desc,''), ' ', COALESCE(other_2_desc,''))),
  r'crypto|bitcoin|digital currency|digital asset|virtual currency'
)
ORDER BY COALESCE(other_1_amount,0) + COALESCE(other_2_amount,0) DESC
LIMIT 50;


-- ──────────────────────────────────────────────────────────────────────────
-- TOYS & CHILDREN'S ITEMS
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  other_1_desc, other_1_amount,
  other_2_desc, other_2_amount,
  total_noncash_amount, total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE REGEXP_CONTAINS(
  LOWER(CONCAT(COALESCE(other_1_desc,''), ' ', COALESCE(other_2_desc,''))),
  r'toy|children|kid|youth|baby|infant|diaper'
)
ORDER BY COALESCE(other_1_amount,0) + COALESCE(other_2_amount,0) DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- BUILDING MATERIALS & CONSTRUCTION
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  other_1_desc, other_1_amount,
  real_estate_amount,
  total_noncash_amount, total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE REGEXP_CONTAINS(
  LOWER(CONCAT(COALESCE(other_1_desc,''), ' ', COALESCE(other_2_desc,''))),
  r'building material|construction|lumber|habitat|renovation|repair'
)
   OR (ntee_major_group = 'Housing & Shelter' AND total_noncash_amount > 100000)
ORDER BY total_noncash_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- EXPORT-READY: Full prospecting list with all contact + donation info
-- (Download as CSV from BigQuery for import into CRM)
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  ein,
  org_name,
  city,
  state,
  zip,
  org_phone,
  website,
  signing_officer_name,
  signing_officer_title,
  signing_officer_phone,
  principal_officer_name,
  ntee_major_group,
  revenue_tier,
  -- Financials
  total_revenue_cy,
  total_expenses_cy,
  total_assets_eoy,
  contributions_grants_cy,
  noncash_contributions_total,
  -- Staffing
  num_employees,
  num_volunteers,
  -- Noncash summary
  total_noncash_amount,
  noncash_category_count,
  -- Donation type flags
  accepts_food,
  food_inventory_amount,
  accepts_clothing,
  clothing_household_amount,
  accepts_vehicles,
  cars_vehicles_amount,
  accepts_books,
  books_publications_amount,
  accepts_drugs_medical,
  drugs_medical_amount,
  accepts_securities,
  securities_publicly_traded_amount,
  -- Other types
  other_1_desc,
  other_1_amount,
  other_2_desc,
  other_2_amount,
  -- Policies
  gift_acceptance_policy,
  uses_third_parties,
  -- Context
  mission
FROM `irs-dataset-487317.irs_501c3_data_bq.dashboard_prospecting`
WHERE total_noncash_amount > 0
ORDER BY total_noncash_amount DESC;

-- ============================================================================
-- 02 PROSPECTING — Find organizations that accept in-kind donations
-- All queries use the flat vw_inkind_prospecting view (no joins needed)
-- ============================================================================

-- ──────────────────────────────────────────────────────────────────────────
-- FOOD DONATIONS
-- ──────────────────────────────────────────────────────────────────────────

-- Food banks / pantries sorted by donation volume
SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  food_inventory_amount,
  food_inventory_count,
  total_revenue_cy,
  num_employees
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE accepts_food IS TRUE
ORDER BY food_inventory_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- CLOTHING & HOUSEHOLD GOODS
-- ──────────────────────────────────────────────────────────────────────────

-- Orgs accepting clothing donations, by state
SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  clothing_household_amount,
  clothing_household_count,
  total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE accepts_clothing IS TRUE
ORDER BY clothing_household_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- MULTI-CATEGORY (orgs that accept 3+ types of in-kind donations)
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  noncash_category_count,
  total_noncash_amount,
  accepts_food, accepts_clothing, accepts_vehicles, accepts_books, accepts_drugs_medical,
  total_revenue_cy, num_employees
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE noncash_category_count >= 3
ORDER BY total_noncash_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- DRUGS & MEDICAL SUPPLY RECIPIENTS
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  drugs_medical_amount,
  drugs_medical_count,
  total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE accepts_drugs_medical IS TRUE
ORDER BY drugs_medical_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- GEOGRAPHIC TARGETING — filter by state
-- ──────────────────────────────────────────────────────────────────────────

-- Change 'CA' to any state you want to target
SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_major_group,
  total_noncash_amount,
  noncash_category_count,
  accepts_food, accepts_clothing, accepts_vehicles, accepts_drugs_medical,
  total_revenue_cy, num_employees
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE state = 'CA'
  AND total_noncash_amount > 0
ORDER BY total_noncash_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- NTEE CATEGORY TARGETING — Human Services orgs accepting donations
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  ntee_code, ntee_major_group,
  total_noncash_amount,
  accepts_food, accepts_clothing, accepts_drugs_medical,
  total_revenue_cy, num_employees
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE ntee_major_group = 'Human Services'
  AND total_noncash_amount > 0
ORDER BY total_noncash_amount DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────────────────────
-- "OTHER" DONATIONS — free-text descriptions (electronics, etc.)
-- ──────────────────────────────────────────────────────────────────────────

-- Search for specific donation types in the "other" description fields
SELECT
  org_name, city, state, org_phone, website,
  signing_officer_name, signing_officer_title,
  other_1_desc, other_1_amount,
  other_2_desc, other_2_amount,
  other_3_desc, other_3_amount,
  total_revenue_cy
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE LOWER(COALESCE(other_1_desc, '') || ' ' ||
            COALESCE(other_2_desc, '') || ' ' ||
            COALESCE(other_3_desc, ''))
      LIKE '%electron%'  -- <-- change to: furniture, supplies, equipment, etc.
ORDER BY COALESCE(other_1_amount, 0) + COALESCE(other_2_amount, 0) DESC
LIMIT 100;

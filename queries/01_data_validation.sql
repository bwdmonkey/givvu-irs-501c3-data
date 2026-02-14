-- ============================================================================
-- 01 DATA VALIDATION — Spot-check the pipeline output
-- Run each query individually in BigQuery console (highlight + Run)
-- ============================================================================

-- 1. Row counts across all tables
SELECT
  'organizations' AS table_name,
  COUNT(*)        AS row_count
FROM `irs-dataset-487317.irs_501c3_data_bq.organizations`
UNION ALL
SELECT 'filings', COUNT(*)
FROM `irs-dataset-487317.irs_501c3_data_bq.filings`
UNION ALL
SELECT 'schedule_m', COUNT(*)
FROM `irs-dataset-487317.irs_501c3_data_bq.schedule_m`;


-- 2. Schedule M category distribution — how many orgs receive each type
SELECT
  COUNTIF(food_inventory_x IS TRUE)             AS food,
  COUNTIF(clothing_household_x IS TRUE)         AS clothing,
  COUNTIF(cars_vehicles_x IS TRUE)              AS vehicles,
  COUNTIF(books_publications_x IS TRUE)         AS books,
  COUNTIF(drugs_medical_x IS TRUE)              AS drugs_medical,
  COUNTIF(securities_publicly_traded_x IS TRUE) AS securities_public,
  COUNTIF(securities_closely_held_x IS TRUE)    AS securities_closely_held,
  COUNTIF(real_estate_residential_x IS TRUE)    AS real_estate_res,
  COUNTIF(real_estate_commercial_x IS TRUE)     AS real_estate_com,
  COUNTIF(real_estate_other_x IS TRUE)          AS real_estate_other,
  COUNTIF(collectibles_x IS TRUE)               AS collectibles,
  COUNTIF(intellectual_property_x IS TRUE)      AS intellectual_prop,
  COUNTIF(boats_planes_x IS TRUE)               AS boats_planes,
  COUNTIF(art_works_x IS TRUE)                  AS art_works,
  COUNTIF(other_1_x IS TRUE)                    AS other_type_1,
  COUNTIF(other_2_x IS TRUE)                    AS other_type_2
FROM `irs-dataset-487317.irs_501c3_data_bq.schedule_m`;


-- 3. Top 10 states by number of orgs filing Schedule M
SELECT
  o.state,
  COUNT(*)                    AS orgs_with_schedule_m,
  SUM(COALESCE(m.food_inventory_amount, 0)
    + COALESCE(m.clothing_household_amount, 0)
    + COALESCE(m.drugs_medical_amount, 0)
    + COALESCE(m.securities_publicly_traded_amount, 0))  AS total_noncash_selected
FROM `irs-dataset-487317.irs_501c3_data_bq.schedule_m` m
JOIN `irs-dataset-487317.irs_501c3_data_bq.organizations` o ON m.ein = o.ein
GROUP BY o.state
ORDER BY orgs_with_schedule_m DESC
LIMIT 10;


-- 4. Spot-check: sample filings with contact info populated
SELECT
  ein, org_name, org_city, org_state,
  org_phone, website,
  principal_officer_name, signing_officer_name, signing_officer_title,
  total_revenue_cy, num_employees
FROM `irs-dataset-487317.irs_501c3_data_bq.filings`
WHERE signing_officer_name IS NOT NULL
  AND website IS NOT NULL
  AND org_phone IS NOT NULL
ORDER BY total_revenue_cy DESC
LIMIT 10;


-- 5. Spot-check: a single org end-to-end (pick any EIN you know)
-- Replace the EIN below with one from query 4 to trace it across all 3 tables
SELECT
  o.ein, o.name, o.city, o.state, o.ntee_code,
  f.org_phone, f.website, f.signing_officer_name, f.signing_officer_title,
  f.total_revenue_cy, f.total_expenses_cy, f.num_employees,
  f.has_schedule_m,
  m.food_inventory_x, m.food_inventory_amount,
  m.clothing_household_x, m.clothing_household_amount,
  m.securities_publicly_traded_x, m.securities_publicly_traded_amount,
  m.gift_acceptance_policy
FROM `irs-dataset-487317.irs_501c3_data_bq.organizations` o
LEFT JOIN `irs-dataset-487317.irs_501c3_data_bq.filings` f ON o.ein = f.ein
LEFT JOIN `irs-dataset-487317.irs_501c3_data_bq.schedule_m` m ON f.object_id = m.object_id
WHERE o.ein = '760664093'  -- <-- change this EIN
LIMIT 5;


-- 6. Coverage check: what % of orgs have filings? have Schedule M?
SELECT
  COUNT(DISTINCT o.ein)                                               AS total_orgs,
  COUNT(DISTINCT f.ein)                                               AS orgs_with_filings,
  COUNT(DISTINCT m.ein)                                               AS orgs_with_schedule_m,
  ROUND(100.0 * COUNT(DISTINCT f.ein) / COUNT(DISTINCT o.ein), 1)    AS pct_with_filings,
  ROUND(100.0 * COUNT(DISTINCT m.ein) / COUNT(DISTINCT o.ein), 1)    AS pct_with_schedule_m
FROM `irs-dataset-487317.irs_501c3_data_bq.organizations` o
LEFT JOIN `irs-dataset-487317.irs_501c3_data_bq.filings` f ON o.ein = f.ein
LEFT JOIN `irs-dataset-487317.irs_501c3_data_bq.schedule_m` m ON f.object_id = m.object_id;

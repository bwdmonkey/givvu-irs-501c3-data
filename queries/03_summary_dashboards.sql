-- ============================================================================
-- 03 SUMMARY / DASHBOARD — Aggregate stats for reporting
-- ============================================================================

-- 1. National summary: in-kind donation landscape
SELECT
  COUNT(*)                                    AS total_schedule_m_filers,
  COUNTIF(accepts_food)                       AS accept_food,
  COUNTIF(accepts_clothing)                   AS accept_clothing,
  COUNTIF(accepts_vehicles)                   AS accept_vehicles,
  COUNTIF(accepts_books)                      AS accept_books,
  COUNTIF(accepts_drugs_medical)              AS accept_drugs_medical,
  COUNTIF(accepts_securities)                 AS accept_securities,
  SUM(total_noncash_amount)                   AS total_noncash_dollars,
  ROUND(AVG(total_noncash_amount))            AS avg_noncash_per_org,
  ROUND(APPROX_QUANTILES(total_noncash_amount, 100)[OFFSET(50)]) AS median_noncash
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE total_noncash_amount > 0;


-- 2. By state: which states have the most in-kind donation activity
SELECT
  state,
  COUNT(*)                          AS orgs_with_noncash,
  SUM(total_noncash_amount)         AS total_noncash,
  ROUND(AVG(total_noncash_amount))  AS avg_noncash,
  COUNTIF(accepts_food)             AS food_orgs,
  COUNTIF(accepts_clothing)         AS clothing_orgs
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE total_noncash_amount > 0
GROUP BY state
ORDER BY total_noncash DESC;


-- 3. By NTEE category: which sectors accept the most in-kind
SELECT
  ntee_major_group,
  COUNT(*)                          AS orgs,
  SUM(total_noncash_amount)         AS total_noncash,
  ROUND(AVG(total_noncash_amount))  AS avg_noncash,
  COUNTIF(accepts_food)             AS food,
  COUNTIF(accepts_clothing)         AS clothing,
  COUNTIF(accepts_drugs_medical)    AS drugs
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE total_noncash_amount > 0
GROUP BY ntee_major_group
ORDER BY total_noncash DESC;


-- 4. Size distribution: noncash amounts by org revenue tier
SELECT
  CASE
    WHEN total_revenue_cy IS NULL       THEN 'Unknown'
    WHEN total_revenue_cy < 100000      THEN 'Under $100K'
    WHEN total_revenue_cy < 1000000     THEN '$100K - $1M'
    WHEN total_revenue_cy < 10000000    THEN '$1M - $10M'
    WHEN total_revenue_cy < 100000000   THEN '$10M - $100M'
    ELSE '$100M+'
  END AS revenue_tier,
  COUNT(*)                          AS orgs,
  SUM(total_noncash_amount)         AS total_noncash,
  ROUND(AVG(total_noncash_amount))  AS avg_noncash
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE total_noncash_amount > 0
GROUP BY revenue_tier
ORDER BY MIN(COALESCE(total_revenue_cy, 0));


-- 5. Top "other" donation descriptions (what kinds of stuff orgs receive)
SELECT
  UPPER(TRIM(desc)) AS donation_type,
  COUNT(*)          AS org_count,
  SUM(amount)       AS total_amount
FROM (
  SELECT other_1_desc AS desc, other_1_amount AS amount
  FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
  WHERE other_1_desc IS NOT NULL
  UNION ALL
  SELECT other_2_desc, other_2_amount
  FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
  WHERE other_2_desc IS NOT NULL
  UNION ALL
  SELECT other_3_desc, other_3_amount
  FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
  WHERE other_3_desc IS NOT NULL
)
GROUP BY donation_type
ORDER BY org_count DESC
LIMIT 50;


-- 6. Gift acceptance policies: how many orgs have formal policies?
SELECT
  COUNTIF(gift_acceptance_policy IS TRUE)  AS has_policy,
  COUNTIF(gift_acceptance_policy IS FALSE) AS no_policy,
  COUNTIF(gift_acceptance_policy IS NULL)  AS unknown,
  COUNTIF(uses_third_parties IS TRUE)      AS uses_third_parties
FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE total_noncash_amount > 0;

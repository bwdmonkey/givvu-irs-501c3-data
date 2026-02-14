-- ============================================================================
-- 06 ADVANCED PROSPECTING — Ideal Customer Profile for Givvu physical donation management
--
-- Uses vw_inkind_prospecting (unfiltered) instead of dashboard_prospecting, so we
-- include all eligible 501(c)(3) orgs that accept in-kind donations, not only
-- those already present in the schedule_m table. See 04_dashboard_views.sql
-- for how dashboard_prospecting is built from this view with a schedule_m filter.
--
-- Target ICP: Orgs that accept physical donations Givvu can support:
--   clothing, electronics, household items, furniture, food, drugs/medical, etc.
-- ============================================================================


-- ──────────────────────────────────────────────────────────────────────────
-- SINGLE QUERY: Full prospect data for physical-donation ICP
-- Returns org info, finance info, donation info, and contact info in one row.
-- Filter: accepts food, clothing, household/furniture, electronics, or has
-- other_1/other_2 descriptions matching those categories.
-- ──────────────────────────────────────────────────────────────────────────

SELECT
  -- ── Org identity (see 04_dashboard_views.sql for column semantics)
  ein,
  org_name,
  sort_name,
  street,
  city,
  state,
  zip,
  ntee_code,
  ntee_major_group,
  foundation_code,
  ruling_date,
  asset_code,

  -- ── Contact
  org_phone,
  website,
  principal_officer_name,
  signing_officer_name,
  signing_officer_title,
  signing_officer_phone,

  -- ── Filing
  tax_year,
  year_formation,
  mission,
  form_type,

  -- ── Staffing
  num_employees,
  num_volunteers,
  num_voting_members,

  -- ── Financials
  total_revenue_cy,
  total_expenses_cy,
  total_assets_eoy,
  total_liabilities_eoy,
  net_assets_eoy,
  contributions_grants_cy,
  program_service_revenue_cy,
  salaries_cy,
  noncash_contributions_total,
  has_schedule_m,

  -- Revenue tier (same logic as 04_dashboard_views.sql)
  CASE
    WHEN total_revenue_cy IS NULL       THEN 'Unknown'
    WHEN total_revenue_cy < 100000      THEN '1. Under $100K'
    WHEN total_revenue_cy < 1000000     THEN '2. $100K–$1M'
    WHEN total_revenue_cy < 10000000    THEN '3. $1M–$10M'
    WHEN total_revenue_cy < 100000000   THEN '4. $10M–$100M'
    ELSE                                     '5. $100M+'
  END AS revenue_tier,

  -- ── Donation / in-kind summary
  total_noncash_amount,
  noncash_category_count,
  accepts_food,
  food_inventory_amount,
  accepts_clothing,
  clothing_household_amount,
  accepts_vehicles,
  cars_vehicles_amount,
  boats_planes_amount,
  accepts_books,
  books_publications_amount,
  accepts_drugs_medical,
  drugs_medical_amount,
  accepts_securities,
  securities_publicly_traded_amount,
  COALESCE(real_estate_residential_amount, 0)
    + COALESCE(real_estate_commercial_amount, 0)
    + COALESCE(real_estate_other_amount, 0)   AS real_estate_amount,
  other_1_desc,
  other_1_amount,
  other_2_desc,
  other_2_amount,
  other_3_desc,
  other_3_amount,
  other_4_desc,
  other_4_amount,

  -- ── Policies
  gift_acceptance_policy,
  uses_third_parties,
  num_forms_8283

FROM `irs-dataset-487317.irs_501c3_data_bq.vw_inkind_prospecting`
WHERE total_noncash_amount > 0
  AND (
    -- Explicit Schedule M categories for physical donations
    accepts_food IS TRUE
    OR accepts_clothing IS TRUE
    OR accepts_drugs_medical IS TRUE
    OR accepts_books IS TRUE
    -- Electronics, household, furniture, food-related in "other" descriptions
    OR REGEXP_CONTAINS(
         LOWER(CONCAT(
           COALESCE(other_1_desc, ''), ' ',
           COALESCE(other_2_desc, ''), ' ',
           COALESCE(other_3_desc, ''), ' ',
           COALESCE(other_4_desc, '')
         )),
         r'electron|computer|laptop|tablet|tech|device|hardware|software|it equip|appliance'
       )
    OR REGEXP_CONTAINS(
         LOWER(CONCAT(
           COALESCE(other_1_desc, ''), ' ',
           COALESCE(other_2_desc, ''), ' ',
           COALESCE(other_3_desc, ''), ' ',
           COALESCE(other_4_desc, '')
         )),
         r'furnitur|household|mattress|bedding|linen|applian'
       )
    OR REGEXP_CONTAINS(
         LOWER(CONCAT(
           COALESCE(other_1_desc, ''), ' ',
           COALESCE(other_2_desc, ''), ' ',
           COALESCE(other_3_desc, ''), ' ',
           COALESCE(other_4_desc, '')
         )),
         r'food|grocer|meal|nutrition|pantry'
       )
    OR REGEXP_CONTAINS(
         LOWER(CONCAT(
           COALESCE(other_1_desc, ''), ' ',
           COALESCE(other_2_desc, ''), ' ',
           COALESCE(other_3_desc, ''), ' ',
           COALESCE(other_4_desc, '')
         )),
         r'cloth|clothier|textile|apparel'
       )
    OR REGEXP_CONTAINS(
         LOWER(CONCAT(
           COALESCE(other_1_desc, ''), ' ',
           COALESCE(other_2_desc, ''), ' ',
           COALESCE(other_3_desc, ''), ' ',
           COALESCE(other_4_desc, '')
         )),
         r'medical|pharma|drug|health|dental|surgical|hospital|clinic|supply'
       )
    OR REGEXP_CONTAINS(
         LOWER(CONCAT(
           COALESCE(other_1_desc, ''), ' ',
           COALESCE(other_2_desc, ''), ' ',
           COALESCE(other_3_desc, ''), ' ',
           COALESCE(other_4_desc, '')
         )),
         r'toy|children|kid|youth|baby|infant|diaper'
       )
    OR REGEXP_CONTAINS(
         LOWER(CONCAT(
           COALESCE(other_1_desc, ''), ' ',
           COALESCE(other_2_desc, ''), ' ',
           COALESCE(other_3_desc, ''), ' ',
           COALESCE(other_4_desc, '')
         )),
         r'building material|construction|lumber|habitat|renovation|repair'
       )
  )
ORDER BY total_noncash_amount DESC;

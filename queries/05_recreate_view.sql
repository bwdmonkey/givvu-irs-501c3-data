CREATE OR REPLACE VIEW `irs-dataset-487317.irs_501c3_data_bq`.vw_inkind_prospecting AS
WITH latest_filing AS (
    -- Pick the most recent filing per EIN
    SELECT *
    FROM (
        SELECT
            f.*,
            ROW_NUMBER() OVER (
                PARTITION BY f.ein ORDER BY f.tax_year DESC, f.object_id DESC
            ) AS _rn
        FROM `irs-dataset-487317.irs_501c3_data_bq`.filings AS f
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
    m.art_works_x,
    m.art_works_count,
    m.art_works_amount,
    m.art_works_method,
    m.art_historical_x,
    m.art_historical_count,
    m.art_historical_amount,
    m.art_historical_method,
    m.art_fractional_x,
    m.art_fractional_count,
    m.art_fractional_amount,
    m.art_fractional_method,
    m.books_publications_x,
    m.books_publications_count,
    m.books_publications_amount,
    m.books_publications_method,
    m.clothing_household_x,
    m.clothing_household_count,
    m.clothing_household_amount,
    m.clothing_household_method,
    m.cars_vehicles_x,
    m.cars_vehicles_count,
    m.cars_vehicles_amount,
    m.cars_vehicles_method,
    m.boats_planes_x,
    m.boats_planes_count,
    m.boats_planes_amount,
    m.boats_planes_method,
    m.intellectual_property_x,
    m.intellectual_property_count,
    m.intellectual_property_amount,
    m.intellectual_property_method,
    m.securities_publicly_traded_x,
    m.securities_publicly_traded_count,
    m.securities_publicly_traded_amount,
    m.securities_publicly_traded_method,
    m.securities_closely_held_x,
    m.securities_closely_held_count,
    m.securities_closely_held_amount,
    m.securities_closely_held_method,
    m.securities_partnership_x,
    m.securities_partnership_count,
    m.securities_partnership_amount,
    m.securities_partnership_method,
    m.securities_misc_x,
    m.securities_misc_count,
    m.securities_misc_amount,
    m.securities_misc_method,
    m.conservation_historic_x,
    m.conservation_historic_count,
    m.conservation_historic_amount,
    m.conservation_historic_method,
    m.conservation_other_x,
    m.conservation_other_count,
    m.conservation_other_amount,
    m.conservation_other_method,
    m.real_estate_residential_x,
    m.real_estate_residential_count,
    m.real_estate_residential_amount,
    m.real_estate_residential_method,
    m.real_estate_commercial_x,
    m.real_estate_commercial_count,
    m.real_estate_commercial_amount,
    m.real_estate_commercial_method,
    m.real_estate_other_x,
    m.real_estate_other_count,
    m.real_estate_other_amount,
    m.real_estate_other_method,
    m.collectibles_x,
    m.collectibles_count,
    m.collectibles_amount,
    m.collectibles_method,
    m.food_inventory_x,
    m.food_inventory_count,
    m.food_inventory_amount,
    m.food_inventory_method,
    m.drugs_medical_x,
    m.drugs_medical_count,
    m.drugs_medical_amount,
    m.drugs_medical_method,
    m.taxidermy_x,
    m.taxidermy_count,
    m.taxidermy_amount,
    m.taxidermy_method,
    m.historical_artifacts_x,
    m.historical_artifacts_count,
    m.historical_artifacts_amount,
    m.historical_artifacts_method,
    m.scientific_specimens_x,
    m.scientific_specimens_count,
    m.scientific_specimens_amount,
    m.scientific_specimens_method,
    m.archaeological_artifacts_x,
    m.archaeological_artifacts_count,
    m.archaeological_artifacts_amount,
    m.archaeological_artifacts_method,
    m.other_1_x,
    m.other_1_count,
    m.other_1_amount,
    m.other_1_method,
    m.other_1_desc,
    m.other_2_x,
    m.other_2_count,
    m.other_2_amount,
    m.other_2_method,
    m.other_2_desc,
    m.other_3_x,
    m.other_3_count,
    m.other_3_amount,
    m.other_3_method,
    m.other_3_desc,
    m.other_4_x,
    m.other_4_count,
    m.other_4_amount,
    m.other_4_method,
    m.other_4_desc,
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
    (COALESCE(m.art_works_amount, 0) + COALESCE(m.art_historical_amount, 0) + COALESCE(m.art_fractional_amount, 0) + COALESCE(m.books_publications_amount, 0) + COALESCE(m.clothing_household_amount, 0) + COALESCE(m.cars_vehicles_amount, 0) + COALESCE(m.boats_planes_amount, 0) + COALESCE(m.intellectual_property_amount, 0) + COALESCE(m.securities_publicly_traded_amount, 0) + COALESCE(m.securities_closely_held_amount, 0) + COALESCE(m.securities_partnership_amount, 0) + COALESCE(m.securities_misc_amount, 0) + COALESCE(m.conservation_historic_amount, 0) + COALESCE(m.conservation_other_amount, 0) + COALESCE(m.real_estate_residential_amount, 0) + COALESCE(m.real_estate_commercial_amount, 0) + COALESCE(m.real_estate_other_amount, 0) + COALESCE(m.collectibles_amount, 0) + COALESCE(m.food_inventory_amount, 0) + COALESCE(m.drugs_medical_amount, 0) + COALESCE(m.taxidermy_amount, 0) + COALESCE(m.historical_artifacts_amount, 0) + COALESCE(m.scientific_specimens_amount, 0) + COALESCE(m.archaeological_artifacts_amount, 0) + COALESCE(m.other_1_amount, 0) + COALESCE(m.other_2_amount, 0) + COALESCE(m.other_3_amount, 0) + COALESCE(m.other_4_amount, 0))                                 AS total_noncash_amount,
    (CASE WHEN m.art_works_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.art_historical_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.art_fractional_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.books_publications_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.clothing_household_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.cars_vehicles_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.boats_planes_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.intellectual_property_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.securities_publicly_traded_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.securities_closely_held_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.securities_partnership_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.securities_misc_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.conservation_historic_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.conservation_other_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.real_estate_residential_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.real_estate_commercial_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.real_estate_other_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.collectibles_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.food_inventory_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.drugs_medical_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.taxidermy_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.historical_artifacts_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.scientific_specimens_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.archaeological_artifacts_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.other_1_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.other_2_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.other_3_x IS TRUE THEN 1 ELSE 0 END + CASE WHEN m.other_4_x IS TRUE THEN 1 ELSE 0 END)                               AS noncash_category_count

FROM `irs-dataset-487317.irs_501c3_data_bq`.organizations AS o
LEFT JOIN latest_filing AS lf ON o.ein = lf.ein
LEFT JOIN `irs-dataset-487317.irs_501c3_data_bq`.schedule_m AS m ON lf.object_id = m.object_id


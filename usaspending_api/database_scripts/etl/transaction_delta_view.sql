-- Needs to be present in the Postgres DB if data needs to be retrieved for Elasticsearch

CREATE OR REPLACE VIEW transaction_delta_view AS
SELECT
  UTM.transaction_id,
  FPDS.detached_award_proc_unique,
  FABS.afa_generated_unique,

  CASE
    WHEN FPDS.detached_award_proc_unique IS NOT NULL THEN 'CONT_TX_' || UPPER(FPDS.detached_award_proc_unique)
    WHEN FABS.afa_generated_unique IS NOT NULL THEN 'ASST_TX_' || UPPER(FABS.afa_generated_unique)
    ELSE NULL  -- if this happens: Activate Batsignal
  END AS generated_unique_transaction_id,

  CASE
    WHEN UTM.type IN ('02', '03', '04', '05', '06', '10', '07', '08', '09', '11') AND UTM.fain IS NOT NULL THEN UTM.fain
    WHEN UTM.piid IS NOT NULL THEN UTM.piid  -- contracts. Did it this way to easily handle IDV contracts
    ELSE UTM.uri
  END AS display_award_id,

  TM.update_date,
  UTM.modification_number,
  AWD.generated_unique_award_id,
  UTM.award_id,
  UTM.piid,
  UTM.fain,
  UTM.uri,
  UTM.transaction_description AS award_description,

  UTM.product_or_service_code,
  UTM.product_or_service_description,
  UTM.naics_code,
  UTM.naics_description,
  AWD.type_description,
  UTM.award_category,
  UTM.recipient_unique_id,
  CONCAT(UTM.recipient_hash, '-', case when UTM.parent_recipient_unique_id is null then 'R' else 'C' end) as recipient_hash,
  UTM.parent_recipient_unique_id,
  UTM.recipient_name,

  AWD.date_signed,
  UTM.action_date,
  DATE(UTM.action_date + interval '3 months') AS fiscal_date,
  extract(month from UTM.action_date::date + interval '3 months') AS fiscal_month,
  extract(quarter from UTM.action_date::date + interval '3 months') AS fiscal_quarter,
  extract(year from UTM.action_date::date + interval '3 months') AS fiscal_year,
  AWD.period_of_performance_start_date,
  AWD.period_of_performance_current_end_date,
  FPDS.ordering_period_end_date,
  UTM.fiscal_year AS transaction_fiscal_year,
  AWD.fiscal_year AS award_fiscal_year,
  AWD.total_obligation AS award_amount,
  UTM.federal_action_obligation AS transaction_amount,
  UTM.face_value_loan_guarantee,
  UTM.original_loan_subsidy_cost,
  UTM.generated_pragmatic_obligation,

  UTM.awarding_agency_id,
  UTM.funding_agency_id,
  UTM.awarding_toptier_agency_name,
  UTM.funding_toptier_agency_name,
  UTM.awarding_subtier_agency_name,
  UTM.funding_subtier_agency_name,
  TAA.toptier_code AS awarding_toptier_agency_code,
  TFA.toptier_code AS funding_toptier_agency_code,
  SAA.subtier_code AS awarding_subtier_agency_code,
  SFA.subtier_code AS funding_subtier_agency_code,
  UTM.awarding_toptier_agency_abbreviation,
  UTM.funding_toptier_agency_abbreviation,
  UTM.awarding_subtier_agency_abbreviation,
  UTM.funding_subtier_agency_abbreviation,

  CFDA.id AS cfda_id,
  UTM.cfda_number,
  UTM.cfda_title,
  '' AS cfda_popular_name,
  UTM.type_of_contract_pricing,
  UTM.type_set_aside,
  UTM.extent_competed,
  UTM.pulled_from,
  UTM.type,

  UTM.pop_country_code,
  UTM.pop_country_name,
  UTM.pop_state_code,
  COALESCE(FPDS.place_of_perfor_state_desc, FABS.place_of_perform_state_nam) AS pop_state_name,
  LPAD(
    CAST(
      CAST(
        (REGEXP_MATCH(UTM.pop_county_code, '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint
      ) AS text
  ), 3, '0') AS pop_county_code,
  UTM.pop_county_name,
  UTM.pop_zip5,
  LPAD(
    CAST(
      CAST(
        (REGEXP_MATCH(UTM.pop_congressional_code, '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint
      ) AS text
  ), 2, '0') AS pop_congressional_code,
  UTM.pop_city_name,

  UTM.recipient_location_country_code,
  UTM.recipient_location_country_name,
  UTM.recipient_location_state_code,
  COALESCE(FPDS.legal_entity_state_descrip, FABS.legal_entity_state_name) AS recipient_location_state_name,
  LPAD(
    CAST(
      CAST(
        (REGEXP_MATCH(UTM.recipient_location_county_code, '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint
      ) AS text
  ), 3, '0') AS recipient_location_county_code,
  UTM.recipient_location_county_name,
  UTM.recipient_location_zip5,
  LPAD(
    CAST(
      CAST(
        (REGEXP_MATCH(UTM.recipient_location_congressional_code, '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint
      ) AS text
  ), 2, '0') AS recipient_location_congressional_code,
  UTM.recipient_location_city_name,
  UTM.treasury_account_identifiers,
  FED_ACCT.federal_accounts,

  UTM.business_categories


FROM universal_transaction_matview UTM
JOIN transaction_normalized TM ON (UTM.transaction_id = TM.id)
LEFT JOIN transaction_fpds FPDS ON (UTM.transaction_id = FPDS.transaction_id)
LEFT JOIN transaction_fabs FABS ON (UTM.transaction_id = FABS.transaction_id)
LEFT OUTER JOIN awards AWD ON (UTM.award_id = AWD.id)
-- these joins should be removed once moving away from proof of concept with ES Advanced Search
-- and all changes should be made to universal_transaction_matview
LEFT OUTER JOIN agency AA ON (TM.awarding_agency_id = AA.id)
LEFT OUTER JOIN agency FA ON (TM.funding_agency_id = FA.id)
LEFT OUTER JOIN toptier_agency TAA ON (AA.toptier_agency_id = TAA.toptier_agency_id)
LEFT OUTER JOIN subtier_agency SAA ON (AA.subtier_agency_id = SAA.subtier_agency_id)
LEFT OUTER JOIN toptier_agency TFA ON (FA.toptier_agency_id = TFA.toptier_agency_id)
LEFT OUTER JOIN subtier_agency SFA ON (FA.subtier_agency_id = SFA.subtier_agency_id)
LEFT OUTER JOIN references_cfda CFDA ON (FABS.cfda_number = CFDA.program_number)
LEFT OUTER JOIN (
  SELECT
    faba.award_id,
    JSONB_AGG(
      DISTINCT JSONB_BUILD_OBJECT(
        'id', fa.id,
        'account_title', fa.account_title,
        'federal_account_code', fa.federal_account_code
      )
    ) federal_accounts
  FROM
    federal_account fa
    INNER JOIN treasury_appropriation_account taa ON fa.id = taa.federal_account_id
    INNER JOIN financial_accounts_by_awards faba ON taa.treasury_account_identifier = faba.treasury_account_id
  WHERE
    faba.award_id IS NOT NULL
  GROUP BY
    faba.award_id
) FED_ACCT ON (FED_ACCT.award_id = UTM.award_id);

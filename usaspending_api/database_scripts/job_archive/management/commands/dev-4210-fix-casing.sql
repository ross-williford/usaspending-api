
-- Jira Ticket Number: DEV-4210
-- Expected CLI: psql -v ON_ERROR_STOP=1 -c '\timing' -f dev-4210-fix-casing.sql $DATABASE_URL

DO $$ BEGIN RAISE NOTICE 'Replacing FKs from financial_accounts_by_awards'; END $$;
WITH  CTE AS 
(
SELECT *, ROW_NUMBER() 
    OVER (
        PARTITION BY program_activity_code, budget_year, responsible_agency_id, allocation_transfer_agency_id, main_account_code, upper(program_activity_name) 
    ORDER BY program_activity_name desc) AS RN
FROM ref_program_activity
),
CORRECT_IDS AS (
SELECT CTE.id AS incorrect_id, pa.id AS correct_id 
FROM CTE 
INNER JOIN ref_program_activity pa 
    ON pa.program_activity_name = upper(CTE.program_activity_name) AND 
    pa.program_activity_code = CTE.program_activity_code AND 
    pa.budget_year = CTE.budget_year AND 
    pa.responsible_agency_id = CTE.responsible_agency_id AND 
    pa.main_account_code = CTE.main_account_code AND 
    (CTE.allocation_transfer_agency_id = pa.allocation_transfer_agency_id OR pa.allocation_transfer_agency_id IS NULL) 
WHERE CTE.RN <> 1
)
UPDATE financial_accounts_by_awards faba SET program_activity_id = (select correct_id from CORRECT_IDS where faba.program_activity_id = CORRECT_IDS.incorrect_id) where program_activity_id in (select incorrect_id from CORRECT_IDS);

DO $$ BEGIN RAISE NOTICE 'Replacing FKs from financial_accounts_by_program_activity_object_class'; END $$;

WITH  CTE AS 
(
SELECT *, ROW_NUMBER() 
    OVER (
        PARTITION BY program_activity_code, budget_year, responsible_agency_id, allocation_transfer_agency_id, main_account_code, upper(program_activity_name) 
    ORDER BY program_activity_name desc) AS RN
FROM ref_program_activity
),
CORRECT_IDS AS (
SELECT CTE.id AS incorrect_id, pa.id AS correct_id 
FROM CTE 
INNER JOIN ref_program_activity pa 
    ON pa.program_activity_name = upper(CTE.program_activity_name) AND 
    pa.program_activity_code = CTE.program_activity_code AND 
    pa.budget_year = CTE.budget_year AND 
    pa.responsible_agency_id = CTE.responsible_agency_id AND 
    pa.main_account_code = CTE.main_account_code AND 
    (CTE.allocation_transfer_agency_id = pa.allocation_transfer_agency_id OR pa.allocation_transfer_agency_id IS NULL) 
WHERE CTE.RN <> 1
)
UPDATE financial_accounts_by_program_activity_object_class faba2 SET program_activity_id = (select correct_id from CORRECT_IDS where faba2.program_activity_id = CORRECT_IDS.incorrect_id) where program_activity_id in (select incorrect_id from CORRECT_IDS);

DO $$ BEGIN RAISE NOTICE 'Replacing FKs from tas_program_activity_object_class_quarterly'; END $$;

WITH  CTE AS 
(
SELECT *, ROW_NUMBER() 
    OVER (
        PARTITION BY program_activity_code, budget_year, responsible_agency_id, allocation_transfer_agency_id, main_account_code, upper(program_activity_name) 
    ORDER BY program_activity_name desc) AS RN
FROM ref_program_activity
),
CORRECT_IDS AS (
SELECT CTE.id AS incorrect_id, pa.id AS correct_id 
FROM CTE 
INNER JOIN ref_program_activity pa 
    ON pa.program_activity_name = upper(CTE.program_activity_name) AND 
    pa.program_activity_code = CTE.program_activity_code AND 
    pa.budget_year = CTE.budget_year AND 
    pa.responsible_agency_id = CTE.responsible_agency_id AND 
    pa.main_account_code = CTE.main_account_code AND 
    (CTE.allocation_transfer_agency_id = pa.allocation_transfer_agency_id OR pa.allocation_transfer_agency_id IS NULL) 
WHERE CTE.RN <> 1
)
UPDATE tas_program_activity_object_class_quarterly tas SET program_activity_id = (select correct_id from CORRECT_IDS where tas.program_activity_id = CORRECT_IDS.incorrect_id) where program_activity_id in (select incorrect_id from CORRECT_IDS);

DO $$ BEGIN RAISE NOTICE 'Deleting duplicates from ref_program_activity'; END $$;

WITH  CTE AS 
(
SELECT *, ROW_NUMBER() 
    OVER (
        PARTITION BY program_activity_code, budget_year, responsible_agency_id, allocation_transfer_agency_id, main_account_code, upper(program_activity_name) 
    ORDER BY program_activity_name desc) AS RN
FROM ref_program_activity
),
CORRECT_IDS AS (
SELECT CTE.id AS incorrect_id, pa.id AS correct_id 
FROM CTE 
INNER JOIN ref_program_activity pa 
    ON pa.program_activity_name = upper(CTE.program_activity_name) AND 
    pa.program_activity_code = CTE.program_activity_code AND 
    pa.budget_year = CTE.budget_year AND 
    pa.responsible_agency_id = CTE.responsible_agency_id AND 
    pa.main_account_code = CTE.main_account_code AND 
    (CTE.allocation_transfer_agency_id = pa.allocation_transfer_agency_id OR pa.allocation_transfer_agency_id IS NULL) 
WHERE CTE.RN <> 1
)
DELETE FROM ref_program_activity where id in (SELECT incorrect_id from CORRECT_IDS);

DO $$ BEGIN RAISE NOTICE 'Completed deletions'; 
END $$;
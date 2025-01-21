INSERT INTO 
	public.klus_dwh_fact_passport_blacklist (
		entry_dt,
		passport_num
	)
SELECT
	stg.entry_dt,
	stg.passport_num
FROM
	public.klus_stg_blacklist stg
WHERE NOT EXISTS (
    SELECT 1
    FROM public.klus_dwh_fact_passport_blacklist dwh
    WHERE dwh.entry_dt = stg.entry_dt
    AND dwh.passport_num = stg.passport_num
);

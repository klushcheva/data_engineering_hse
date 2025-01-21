INSERT INTO 
	public.klus_dwh_dim_clients(
		client_id,
		last_name,
		first_name,
		patronymic,
		date_of_birth,
		passport_num,
		passport_valid_to,
		phone,
		create_dt,
		update_dt 
	)
SELECT 
	stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.create_dt,
	NULL 
FROM public.klus_stg_clients stg
LEFT JOIN
	public.klus_dwh_dim_clients tgt
ON 
	stg.client_id = tgt.client_id
WHERE
	tgt.client_id IS NULL;

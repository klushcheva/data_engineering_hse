INSERT INTO 
	public.klus_dwh_dim_terminals(
		terminal_id,
		terminal_type,
		terminal_city,
		terminal_address,
		create_dt,
		update_dt 
	)
SELECT 
	stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	stg.create_dt,
	NULL
FROM public.klus_stg_terminals stg
LEFT JOIN
	public.klus_dwh_dim_terminals tgt
ON 
	stg.terminal_id = tgt.terminal_id
WHERE
	tgt.terminal_id IS NULL;
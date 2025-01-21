UPDATE
	public.klus_dwh_dim_terminals AS tgt
SET
	terminal_type = stg.terminal_type,
	terminal_city = stg.terminal_city,
	terminal_address = stg.terminal_address,
	update_dt = stg.create_dt
FROM
	public.klus_stg_terminals stg
WHERE
	tgt.terminal_id = stg.terminal_id
	AND tgt.create_dt < stg.create_dt;


INSERT INTO public.klus_stg_terminals(
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    create_dt
    )
VALUES(%s, %s, %s, %s, %s)
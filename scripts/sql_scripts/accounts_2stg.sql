INSERT INTO public.klus_stg_accounts(
    account_num,
    valid_to,
    client,
    create_dt,
    update_dt
    )
VALUES(%s, %s, %s, %s, %s)
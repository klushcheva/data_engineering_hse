INSERT INTO public.klus_stg_clients(
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
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
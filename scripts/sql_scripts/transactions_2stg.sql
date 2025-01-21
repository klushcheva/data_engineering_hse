INSERT INTO public.klus_stg_transactions(
    trans_id,
    trans_date,
    amt,
    card_num,
    oper_type,
    oper_result,
    terminal
    )
VALUES(%s, %s, %s, %s, %s, %s, %s)

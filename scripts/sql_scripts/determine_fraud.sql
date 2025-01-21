-- 1. Совершение операции при заблокированном или просроченном паспорте
INSERT INTO 
    public.klus_rep_fraud (
        event_dt,
        passport_num,
        fio,
        phone,
        event_type,
        report_dt
    )
SELECT
    t.trans_date AS event_dt,
    c.passport_num AS passport,
    CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) AS fio,
    c.phone AS phone,
    1 AS event_type,
    t.trans_date::DATE AS report_dt
FROM
    public.klus_stg_transactions t
LEFT JOIN
    public.klus_dwh_dim_cards cr
ON
    t.card_num = cr.card_num
LEFT JOIN
    public.klus_dwh_dim_accounts acc
ON
    cr.account_num = acc.account_num
LEFT JOIN
    public.klus_dwh_dim_clients c
ON
    acc.client = c.client_id
LEFT JOIN
    public.klus_dwh_fact_passport_blacklist b
ON
    c.passport_num = b.passport_num
WHERE
    1 = 1 AND
    b.passport_num IS NOT NULL
    OR c.passport_valid_to < t.trans_date
;


-- 2. Совершение операции при недействующем договоре
INSERT INTO
    public.klus_rep_fraud (
        event_dt,
        passport_num,
        fio,
        phone,
        event_type,
        report_dt
    )
SELECT
    t.trans_date AS event_dt,
    c.passport_num AS passport,
    CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) AS fio,
    c.phone AS phone,
    2 AS event_type,
    t.trans_date::DATE AS report_dt
FROM
    public.klus_stg_transactions t
LEFT JOIN
    public.klus_dwh_dim_cards cr
ON
    t.card_num = cr.card_num
LEFT JOIN
    public.klus_dwh_dim_accounts acc
ON
    cr.account_num = acc.account_num
LEFT JOIN
    public.klus_dwh_dim_clients c
ON
    acc.client = c.client_id
WHERE
    1=1 AND
    acc.valid_to < t.trans_date
;

-- 3. Совершение операций в разных городах в течение одного часа
INSERT INTO
    public.klus_rep_fraud (
        event_dt,
        passport_num,
        fio,
        phone,
        event_type,
        report_dt
    )
SELECT
    t1.trans_date AS event_dt,
    c.passport_num AS passport,
    CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) AS fio,
    c.phone AS phone,
    3 AS event_type,
    t1.trans_date::DATE AS report_dt
FROM
    public.klus_stg_transactions t1
INNER JOIN
    public.klus_dwh_dim_cards cr ON t1.card_num = cr.card_num
INNER JOIN
    public.klus_dwh_dim_accounts acc ON cr.account_num = acc.account_num
INNER JOIN
    public.klus_dwh_dim_clients c ON acc.client = c.client_id
INNER JOIN
    public.klus_dwh_dim_terminals ter1 ON t1.terminal = ter1.terminal_id
INNER JOIN
    public.klus_stg_transactions t2 ON
        t1.trans_id <> t2.trans_id AND
        t1.card_num = t2.card_num
INNER JOIN
    public.klus_dwh_dim_terminals ter2 ON t2.terminal = ter2.terminal_id
WHERE
    t1.trans_date BETWEEN t2.trans_date - INTERVAL '1 hour' AND t2.trans_date + INTERVAL '1 hour' AND
    ter1.terminal_city <> ter2.terminal_city
;

-- type 4. Попытка подбора суммы
-- В течение 20 минут происходит более 3 операций с последовательными суммами,
-- где каждая последующая меньше предыдущей, при этом отклонены все, кроме последней.
-- Последняя операция (успешная) в такой цепочке считается мошеннической.
WITH t1 AS (
    SELECT
        client_id,
        trans_date AS event_dt,
        row_number() OVER (PARTITION BY client_id ORDER BY trans_date ASC) AS rn,
        EXTRACT(EPOCH FROM (trans_date - LAG(trans_date, 3) OVER (PARTITION BY client_id ORDER BY trans_date ASC))) AS time_diff,
        LAG(amt) OVER (PARTITION BY client_id ORDER BY trans_date ASC) AS amt_1,
        LAG(amt, 2) OVER (PARTITION BY client_id ORDER BY trans_date ASC) AS amt_2,
        LAG(amt, 3) OVER (PARTITION BY client_id ORDER BY trans_date ASC) AS amt_3,
        LAG(oper_result) OVER (PARTITION BY client_id ORDER BY trans_date ASC) AS status_1,
        LAG(oper_result, 2) OVER (PARTITION BY client_id ORDER BY trans_date ASC) AS status_2,
        LAG(oper_result, 3) OVER (PARTITION BY client_id ORDER BY trans_date ASC) AS status_3,
        amt,
        oper_result,
        passport_num,
        CONCAT(last_name, ' ', first_name, ' ', patronymic) AS fio,
        phone,
        4 AS event_type
    FROM
        public.klus_dwh_fact_transactions kdft
        LEFT JOIN public.klus_dwh_dim_cards kgdc ON TRIM(kdft.card_num) = TRIM(kgdc.card_num)
        LEFT JOIN public.klus_dwh_dim_accounts kdda ON kgdc.account_num = kdda.account_num
        LEFT JOIN public.klus_dwh_dim_clients kddc2 ON kdda.client = kddc2.client_id
        LEFT JOIN public.klus_dwh_dim_terminals kddt ON kdft.terminal = kddt.terminal_id
),
t2 AS (
    SELECT
        *
    FROM t1
    WHERE
        oper_result = 'SUCCESS'
        AND status_1 = 'REJECT'
        AND status_2 = 'REJECT'
        AND status_3 = 'REJECT'
        AND time_diff <= 1200
        AND amt_3 > amt_2
        AND amt_2 > amt_1
        AND amt_1 > amt
)
INSERT INTO
	public.klus_rep_fraud(
		event_dt,
		passport_num,
		fio,
		phone,
		event_type,
		report_dt
	)
SELECT
    event_dt,
    passport_num,
    fio,
    phone,
    event_type,
    TO_DATE(%s, 'YYYY-MM-DD') AS report_dt
FROM
    t2
WHERE NOT EXISTS (
        SELECT 1
        FROM public.klus_rep_fraud t
        WHERE
            t.event_dt = t2.event_dt
            AND t.passport_num = t2.passport_num
            AND t.fio = t2.fio
            AND t.phone = t2.phone
            AND t.event_type = 4
    );


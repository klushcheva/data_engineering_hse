-- 1. Совершение операции при заблокированном или просроченном паспорте
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
    kdft.trans_date AS event_dt,
    kddc2.passport_num AS passport_num,
    CONCAT(kddc2.last_name, ' ', kddc2.first_name, ' ', kddc2.patronymic) AS fio,
    kddc2.phone AS phone,
    1 AS event_type,
    TO_DATE(%s, 'YYYY-MM-DD') AS report_dt
FROM
    public.klus_dwh_fact_transactions kdft
LEFT JOIN
    public.klus_dwh_dim_cards kddc ON TRIM(kdft.card_num) = TRIM(kddc.card_num)
LEFT JOIN
    public.klus_dwh_dim_accounts kdda ON kddc.account_num = kdda.account_num
LEFT JOIN
    public.klus_dwh_dim_clients kddc2 ON kdda.client = kddc2.client_id
WHERE
    (
        kddc2.passport_num IN (SELECT passport_num FROM public.klus_dwh_fact_passport_blacklist)
        OR kddc2.passport_valid_to < DATE(kdft.trans_date)
    )
    AND NOT EXISTS (
        SELECT 1
        FROM public.klus_rep_fraud t
        WHERE
            t.event_dt = kdft.trans_date
            AND t.passport_num = kddc2.passport_num
            AND t.fio = CONCAT(kddc2.last_name, ' ', kddc2.first_name, ' ', kddc2.patronymic)
            AND t.phone = kddc2.phone
            AND t.event_type = 1
    )
GROUP BY
    kdft.trans_date, kddc2.passport_num, fio, kddc2.phone;


-- 2. Совершение операции при недействующем договоре
INSERT into public.klus_rep_fraud(
	event_dt,
	passport_num,
	fio,
	phone,
	event_type,
	report_dt
	)
    SELECT DISTINCT
        kdft.trans_date AS event_dt,
	    kddc2.passport_num AS passport,
	    CONCAT(kddc2.last_name, ' ', kddc2.first_name, ' ', kddc2.patronymic) AS fio,
	    kddc2.phone AS phone,
        2 as event_type,
        TO_DATE(%s, 'YYYY-MM-DD') as report_dt
    FROM
	    public.klus_dwh_fact_transactions kdft
	LEFT JOIN
	    public.klus_dwh_dim_cards kddc ON TRIM(kdft.card_num) = TRIM(kddc.card_num)
	LEFT JOIN
	    public.klus_dwh_dim_accounts kdda ON kddc.account_num = kdda.account_num
	LEFT JOIN
	    public.klus_dwh_dim_clients kddc2 ON kdda.client = kddc2.client_id
	WHERE
	    kdda.valid_to < DATE(kdft.trans_date)
	    AND NOT EXISTS (
        SELECT 1
        FROM public.klus_rep_fraud t
        WHERE
            t.event_dt = kdft.trans_date
            AND t.passport_num = kddc2.passport_num
            AND t.fio = CONCAT(kddc2.last_name, ' ', kddc2.first_name, ' ', kddc2.patronymic)
            AND t.phone = kddc2.phone
            AND t.event_type = 2
    );
;

-- 3. Совершение операций в разных городах в течение одного часа
WITH ranked_transactions AS (
    SELECT
        event_dt,
        card_num,
        terminal_city,
        passport_num,
        fio,
        phone,
        ROW_NUMBER() OVER (PARTITION BY card_num ORDER BY event_dt) AS rn
    FROM
        (SELECT kdft.trans_date AS event_dt,
        		TRIM(kgdc.card_num) AS card_num,
        		kddt.terminal_city AS terminal_city,
     			kddc2.passport_num AS passport_num,
     			CONCAT(kddc2.last_name, ' ', kddc2.first_name, ' ', kddc2.patronymic) AS fio,
     			kddc2.phone AS phone
        		from public.klus_dwh_fact_transactions kdft
			left join public.klus_dwh_dim_cards kgdc ON TRIM(kdft.card_num) = TRIM(kgdc.card_num)
			left join public.klus_dwh_dim_accounts kdda ON kgdc.account_num = kdda.account_num
			left join public.klus_dwh_dim_clients kddc2 ON kdda.client = kddc2.client_id
			left join public.klus_dwh_dim_terminals kddt ON kdft.terminal = kddt.terminal_id)
 AS inner_query)
INSERT INTO
	public.klus_rep_fraud(
		event_dt,
		passport_num,
		fio,
		phone,
		event_type,
		report_dt
	)
SELECT DISTINCT
    t1.event_dt,
    t1.passport_num,
    t1.fio,
    t1.phone,
	3 as event_type,
	to_date(%s, 'YYYY-MM-DD') as report_dt
FROM
    ranked_transactions t1
JOIN
    ranked_transactions t2 ON t1.card_num = t2.card_num
WHERE
    t1.terminal_city <> t2.terminal_city
    AND t1.rn <> t2.rn
    AND ABS(EXTRACT(EPOCH FROM (t1.event_dt - t2.event_dt))) <= 3600
    AND NOT EXISTS (
        SELECT 1
        FROM public.klus_rep_fraud t3
        WHERE
            t3.event_dt = t1.event_dt
            AND t3.passport_num = t1.passport_num
            AND t3.fio = t1.fio
            AND t3.phone = t1.phone
            AND t3.event_type = 3
    );
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


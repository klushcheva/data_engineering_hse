-- staging
create table public.klus_stg_transactions(
	trans_id varchar PRIMARY KEY,
	trans_date timestamp,
	amt decimal,
	card_num varchar,
	oper_type varchar,
	oper_result varchar,
	terminal varchar
	);

create table public.klus_stg_terminals(
	terminal_id varchar PRIMARY KEY,
	terminal_type varchar,
	terminal_city varchar,
	terminal_address varchar,
	create_dt date,
	update_dt date
	);

create table public.klus_stg_blacklist (
	entry_dt date,
	passport_num varchar PRIMARY KEY
	);

create table public.klus_stg_cards(
	card_num varchar PRIMARY KEY,
	account_num varchar,
	create_dt date,
	update_dt date
	);

create table public.klus_stg_accounts(
	account_num varchar PRIMARY KEY,
	valid_to date,
	client varchar,
	create_dt date,
	update_dt date
	);

create table public.klus_stg_clients(
	client_id varchar PRIMARY KEY,
	last_name varchar,
	first_name varchar,
	patronymic varchar,
	date_of_birth date,
	passport_num varchar,
	passport_valid_to date,
	phone varchar,
	create_dt date,
	update_dt date
	);

-- dwh
create table public.klus_dwh_fact_transactions(
	trans_id varchar PRIMARY KEY,
	trans_date timestamp,
	card_num varchar,
	oper_type varchar,
	amt decimal,
	oper_result varchar,
	terminal varchar
	);

create table public.klus_dwh_fact_passport_blacklist(
	entry_dt date,
	passport_num varchar PRIMARY KEY
	);

-- DWH
create table public.klus_dwh_dim_terminals(
	terminal_id varchar PRIMARY KEY,
	terminal_type varchar,
	terminal_city varchar,
	terminal_address varchar,
	create_dt date,
	update_dt date
	);

create table public.klus_dwh_dim_cards(
	card_num varchar PRIMARY KEY,
	account_num varchar,
	create_dt date,
	update_dt date
	);

create table public.klus_dwh_dim_accounts(
	account_num varchar PRIMARY KEY,
	valid_to date,
	client varchar,
	create_dt date,
	update_dt date
	);

create table public.klus_dwh_dim_clients(
	client_id varchar PRIMARY KEY,
	last_name varchar,
	first_name varchar,
	patronymic varchar,
	date_of_birth date,
	passport_num varchar,
	passport_valid_to date,
	phone varchar,
	create_dt date,
	update_dt date
	);

-- report
create table public.klus_rep_fraud(
	event_dt timestamp,
	passport_num varchar,
	fio varchar,
	phone varchar,
	event_type int,
	report_dt date
	);
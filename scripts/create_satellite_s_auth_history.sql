drop table if exists STV202502277__DWH.s_auth_history;
create table STV202502277__DWH.s_auth_history
(
	hk_l_user_group_activity bigint not null constraint fk_s_auth_history_l_user_group_activity references STV202502277__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar(6),
	event_dt timestamp,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

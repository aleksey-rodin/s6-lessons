create table STV202502277__DWH.l_user_group_activity
(
	hk_l_user_group_activity bigint primary key,
	hk_user_id int not null constraint fk_l_user_group_activity_users references STV202502277__DWH.h_users(hk_user_id),
	hk_group_id int not null constraint fk_l_user_group_activity_groups references STV202502277__DWH.h_groups(hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
segmented by hk_l_user_group_activity all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

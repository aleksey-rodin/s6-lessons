create table STV202502277__STAGING.group_log
(
	group_id int references STV202502277__STAGING.groups(id),
	user_id int references STV202502277__STAGING.users(id),
	user_id_from int references STV202502277__STAGING.users(id),
	event varchar(6) check (event = 'add' or event = 'leave' or event = 'create'), -- check???
	datetime timestamp
)
order by group_id, datetime
segmented by hash(group_id, user_id) all nodes
partition by datetime::date
group by calendar_hierarchy_day(datetime::date, 3, 2)
;

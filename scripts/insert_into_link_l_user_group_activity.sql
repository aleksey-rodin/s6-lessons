insert into STV202502277__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct
	hash(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity,
	hu.hk_user_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from STV202502277__STAGING.group_log as gl
left join STV202502277__DWH.h_users hu on hu.user_id = gl.user_id
left join STV202502277__DWH.h_groups hg on hg.group_id = gl.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from STV202502277__DWH.l_user_group_activity)
;

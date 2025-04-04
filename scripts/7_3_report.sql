with user_group_log as (
	select
    	hg.hk_group_id as hk_group_id,
        count(distinct hs.hk_user_id) as cnt_added_users
    from STV202502277__DWH.h_groups hg
    inner join STV202502277__DWH.l_user_group_activity luga on luga.hk_group_id = hg.hk_group_id
    inner join STV202502277__DWH.s_auth_history sah on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity and sah.event = 'add'
    inner join STV202502277__DWH.h_users hs on hs.hk_user_id = luga.hk_user_id
    group by hg.hk_group_id
)
,user_group_messages as (
	select
    	hg.hk_group_id as hk_group_id,
        count(distinct sdi.message_from) as cnt_users_in_group_with_messages
    from STV202502277__DWH.h_groups hg
    inner join STV202502277__DWH.l_groups_dialogs lgd on lgd.hk_group_id = hg.hk_group_id
    inner join STV202502277__DWH.h_dialogs hd on lgd.hk_message_id = hd.hk_message_id
    inner join STV202502277__DWH.s_dialog_info sdi on sdi.hk_message_id = hd.hk_message_id
    group by hg.hk_group_id
    having count(distinct sdi.message_from) > 0
)

select
	hg.hk_group_id as hk_group_id,
	ugl.cnt_added_users as cnt_added_users,
	ugm.cnt_users_in_group_with_messages as cnt_users_in_group_with_messages,
	ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion
from (
	select hk_group_id
	from STV202502277__DWH.h_groups
	order by registration_dt asc
	limit 10
) hg
left join user_group_log as ugl on ugl.hk_group_id = hg.hk_group_id
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc
;

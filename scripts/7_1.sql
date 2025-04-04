with user_group_messages as (
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

select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10
;

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

select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10
;

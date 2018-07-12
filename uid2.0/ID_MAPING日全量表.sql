insert overwrite table dws_integ.dws_wdtb_uid_id_maping_all_d partition(prov_id,day_id)
select tt1.mdn,
       tt1.mdn_md5,
       tt1.id,
       tt1.id_type,
       tt1.id_md5,
       tt1.id_date,
       tt1.prov_id,
       tt1.day_id
  from (select t2.mdn,
               t2.mdn_md5,
               t2.id,
               t2.id_type,
               t2.id_md5,
               t2.id_date,
               t2.prov_id,
               t2.day_id
          from (select t1.mdn,
                       t1.mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id,
                       t1.day_id
                  from (select mdn,
                               mdn_md5,
                               id,
                               id_type,
                               id_md5,
                               id_date,
                               prov_id,
                               day_id,
                               row_number() over(partition by id order by id_date desc) rn
                          from dws_integ.dws_wdtb_uid_id_maping_update_d) t1
                 where t1.rn = 1) t2
          left join (select t2.id_date
                      from (select t1.id, count(distinct t1.mdn) mdn_cnt
                              from (select mdn,
                                           mdn_md5,
                                           id,
                                           id_type,
                                           id_md5,
                                           id_date,
                                           prov_id,
                                           day_id,
                                           row_number() over(partition by id order by id_date desc) rn
                                      from dws_integ.dws_wdtb_uid_id_maping_update_d) t1
                             where t1.rn = 1) t2
                     where t2.mdn_cnt > 1) t3
            on t2.id = t3.id
         where t3.id is null
         group by t2.mdn,
                  t2.mdn_md5,
                  t2.id,
                  t2.id_type,
                  t2.id_md5,
                  t2.id_date,
                  t2.prov_id,
                  t2.day_id) tt1;
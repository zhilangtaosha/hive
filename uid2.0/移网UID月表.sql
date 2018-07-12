add jar ${baseDirForScriptSelf}/md.jar;
create temporary function md5 as 'com.udf.Md5';
insert overwrite table dws_integ.dws_wdtb_uid_id_maping_update_d partition (id_source, prov_id, day_id)
select tt1.mdn,
       tt1.mdn_md5,
       tt1.id,
       tt1.id_type,
       tt1.id_md5,
       tt1.id_date,
       tt1.id_source,
       tt1.prov_id,
       '${DAY_ID}' day_id
  from (select t4.mdn,
               t4.mdn_md5,
               t4.id,
               t4.id_type,
               t4.id_md5,
               t4.id_date,
               '2' id_source,
               t4.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t3.imsi    id,
                       t1.id_type,
                       t1.imsi    id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               imsi id,
                               'IMSI' id_type,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(imsi) = 32
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 inner join (select imsi, imsi_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by imsi, imsi_md5) t3
                    on t1.imsi = t3.imsi_md5
                 group by t2.mdn,
                          t1.mdn,
                          t3.imsi,
                          t1.id_type,
                          t1.imsi,
                          t1.id_date,
                          t1.prov_id) t4 --分组去重选出最新的id_date对应的imsi
          left join (select t5.id
                      from (select t4.id, count(distinct t4.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t3.imsi    id,
                                           t1.id_type,
                                           t1.imsi    id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   imsi,
                                                   'IMSI' id_type,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(imsi) = 32
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     inner join (select imsi, imsi_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by imsi, imsi_md5) t3
                                        on t1.imsi = t3.imsi_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t3.imsi,
                                              t1.id_type,
                                              t1.imsi,
                                              t1.id_date,
                                              t1.prov_id) t4) t5
                     where t5.mdn_cnt > 1) t6 --过滤掉id_date相同时，一个imsi对应多个mdn的数据
            on t4.id = t6.id
         where t6.id is null
        union all
        select t4.mdn,
               t4.mdn_md5,
               t4.id,
               t4.id_type,
               t4.id_md5,
               t4.id_date,
               '2' id_source,
               t4.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t3.meid    id,
                       t1.id_type,
                       t1.meid    id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               meid,
                               'MEID' id_type,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(meid) = 32
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 inner join (select meid, meid_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by meid, meid_md5) t3
                    on t1.meid = t3.meid_md5
                 group by t2.mdn,
                          t1.mdn,
                          t3.meid,
                          t1.id_type,
                          t1.meid,
                          t1.id_date,
                          t1.prov_id) t4 --分组去重选出最新的id_date对应的meid
          left join (select t5.id
                      from (select t4.id, count(distinct t4.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t3.meid    id,
                                           t1.id_type,
                                           t1.meid    id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   meid,
                                                   'MEID' id_type,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(meid) = 32
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     inner join (select meid, meid_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by meid, meid_md5) t3
                                        on t1.meid = t3.meid_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t3.meid,
                                              t1.id_type,
                                              t1.meid,
                                              t1.id_date,
                                              t1.prov_id) t4) t5
                     where t5.mdn_cnt > 1) t6 --过滤掉id_date相同时，一个meid对应多个mdn的数据
            on t4.id = t6.id
         where t6.id is null
        union all
        select t4.mdn,
               t4.mdn_md5,
               t4.id,
               t4.id_type,
               t4.id_md5,
               t4.id_date,
               '2' id_source,
               t4.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t3.meid    id,
                       t1.id_type,
                       t1.imei    id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               imei,
                               'IMEI' id_type,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(imei) = 32
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 inner join (select meid, meid_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by meid, meid_md5) t3
                    on t1.imei = t3.meid_md5
                 group by t2.mdn,
                          t1.mdn,
                          t3.meid,
                          t1.id_type,
                          t1.imei,
                          t1.id_date,
                          t1.prov_id) t4 --分组去重选出最新的id_date对应的imei
          left join (select t5.id
                      from (select t4.id, count(distinct t4.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t3.meid    id,
                                           t1.id_type,
                                           t1.imei    id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   imei,
                                                   'IMEI' id_type,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(imei) = 32
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     inner join (select meid, meid_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by meid, meid_md5) t3
                                        on t1.imei = t3.meid_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t3.meid,
                                              t1.id_type,
                                              t1.imei,
                                              t1.id_date,
                                              t1.prov_id) t4) t5
                     where t5.mdn_cnt > 1) t6 --过滤掉id_date相同时，一个imei对应多个mdn的数据
            on t4.id = t6.id
         where t6.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               qq id,
                               'QQ' id_type,
                               md5(qq) id_md5,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(qq) > 0
                           and qq <> 'null'
                           and qq <> 'NULL'
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          t1.id_md5,
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的qq
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           t1.id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   qq id,
                                                   'QQ' id_type,
                                                   md5(qq) id_md5,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(qq) > 0
                                               and qq <> 'null'
                                               and qq <> 'NULL'
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              t1.id_md5,
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个qq对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               weibo id,
                               'WEIBO' id_type,
                               md5(weibo) id_md5,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(weibo) > 0
                           and weibo <> 'null'
                           and weibo <> 'NULL'
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          t1.id_md5,
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的weibo
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           t1.id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   weibo id,
                                                   'WEIBO' id_type,
                                                   md5(weibo) id_md5,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(weibo) > 0
                                               and weibo <> 'null'
                                               and weibo <> 'NULL'
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              t1.id_md5,
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个weibo对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               e_mail id,
                               'Email' id_type,
                               md5(e_mail) id_md5,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(e_mail) > 0
                           and e_mail <> 'null'
                           and e_mail <> 'NULL'
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          t1.id_md5,
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的e_mail
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           t1.id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   e_mail id,
                                                   'Email' id_type,
                                                   md5(e_mail) id_md5,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(e_mail) > 0
                                               and e_mail <> 'null'
                                               and e_mail <> 'NULL'
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              t1.id_md5,
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个e_mail对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               '' id,
                               'IMSI' id_type,
                               sdkimsi id_md5,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(sdkimsi) = 32
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          t1.id_md5,
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的sdkimsi
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           t1.id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   '' id,
                                                   'IMSI' id_type,
                                                   sdkimsi id_md5,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(sdkimsi) = 32
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              t1.id_md5,
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个sdkimsi对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               '' id,
                               'UDID' id_type,
                               sdkudid id_md5,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(sdkudid) = 32
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          t1.id_md5,
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的sdkudid
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           t1.id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   '' id,
                                                   'UDID' id_type,
                                                   sdkudid id_md5,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(sdkudid) = 32
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              t1.id_md5,
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个sdkudid对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               taobao_id id,
                               'Taobao_ID' id_type,
                               md5(taobao_id) id_md5,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(taobao_id) > 0
                           and taobao_id <> 'null'
                           and taobao_id <> 'NULL'
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          t1.id_md5,
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的taobao_id
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           t1.id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   taobao_id id,
                                                   'Taobao_ID' id_type,
                                                   md5(taobao_id) id_md5,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(taobao_id) > 0
                                               and taobao_id <> 'null'
                                               and taobao_id <> 'NULL'
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              t1.id_md5,
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个taobao_id对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               jd_id id,
                               'JD_ID' id_type,
                               md5(jd_id) id_md5,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(jd_id) > 0
                           and jd_id <> 'null'
                           and jd_id <> 'NULL'
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          t1.id_md5,
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的jd_id
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           t1.id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   jd_id id,
                                                   'JD_ID' id_type,
                                                   md5(jd_id) id_md5,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(jd_id) > 0
                                               and jd_id <> 'null'
                                               and jd_id <> 'NULL'
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              t1.id_md5,
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个jd_id对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               idfa id,
                               'IDFA' id_type,
                               md5(idfa) id_md5,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and (idfa regexp
                                '^[0-9A-Fa-f]{8}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{12}$' and
                                idfa <> '00000000-0000-0000-000000000000')
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          t1.id_md5,
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的idfa
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           t1.id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   idfa id,
                                                   'IDFA' id_type,
                                                   md5(idfa) id_md5,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and (idfa regexp
                                                    '^[0-9A-Fa-f]{8}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{12}$' and
                                                    idfa <>
                                                    '00000000-0000-0000-000000000000')
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              t1.id_md5,
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个idfa对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               androidid id,
                               'AndroidID' id_type,
                               md5(androidid) id_md5,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and (androidid regexp
                                '^[0-9A-Fa-f]{16}$' and androidid regexp
                                '[^0]{16}$')
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          t1.id_md5,
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的androidid
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           t1.id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   androidid id,
                                                   'AndroidID' id_type,
                                                   md5(androidid) id_md5,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and (androidid regexp '^[0-9A-Fa-f]{16}$' and
                                                    androidid regexp
                                                    '[^0]{16}$')
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              t1.id_md5,
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个androidid对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn     mdn,
                       t1.mdn     mdn_md5,
                       t1.id,
                       t1.id_type,
                       t1.id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               weixinid id,
                               'WEIXINID' id_type,
                               md5(weixinid) id_md5,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                         where length(mdn) = 32
                           and length(weixinid) > 0
                           and weixinid <> 'null'
                           and weixinid <> 'NULL'
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          t1.id_md5,
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的weixinid
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn     mdn,
                                           t1.mdn     mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           t1.id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   weixinid id,
                                                   'WEIXINID' id_type,
                                                   md5(weixinid) id_md5,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
                                             where length(mdn) = 32
                                               and length(weixinid) > 0
                                               and weixinid <> 'null'
                                               and weixinid <> 'NULL'
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              t1.id_md5,
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个weixinid对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               '2' id_source,
               t3.prov_id,
               '${DAY_ID}' day_id
          from (select t2.mdn mdn,
                       t1.mdn mdn_md5,
                       t1.id,
                       t1.id_type,
                       md5(t1.id) id_md5,
                       t1.id_date,
                       t1.prov_id
                  from (select mdn,
                               id,
                               'MAC' id_type,
                               '${LAST_DAY}' id_date,
                               prov_id
                          from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m lateral view explode(split(regexp_replace(if (substr(regexp_replace(mac, '-', ':'), 1, 1) = '\;', substr(regexp_replace(mac, '-', ':'), 2), regexp_replace(mac, '-', ':')), '\;', ','), ',')) a as id
                         where length(mdn) = 32
                           and id regexp '^([0-9a-fA-F]{2})(([/\s:-][0-9a-fA-F]{2}){5})$'
                           and prov_id = '${PROV_ID}'
                           and month_id = '${MONTH_ID}') t1
                 inner join (select mdn, mdn_md5
                              from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                             where prov_id = '${PROV_ID}'
                               and month_id = '${MONTH_ID}'
                             group by mdn, mdn_md5) t2
                    on t1.mdn = t2.mdn_md5
                 group by t2.mdn,
                          t1.mdn,
                          t1.id,
                          t1.id_type,
                          md5(t1.id),
                          t1.id_date,
                          t1.prov_id) t3 --分组去重选出最新的id_date对应的mac
          left join (select t4.id
                      from (select t3.id, count(distinct t3.mdn) mdn_cnt
                              from (select t2.mdn mdn,
                                           t1.mdn mdn_md5,
                                           t1.id,
                                           t1.id_type,
                                           md5(t1.id) id_md5,
                                           t1.id_date,
                                           t1.prov_id
                                      from (select mdn,
                                                   id,
                                                   'MAC' id_type,
                                                   '${LAST_DAY}' id_date,
                                                   prov_id
                                              from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m lateral view explode(split(regexp_replace(if (substr(regexp_replace(mac, '-', ':'), 1, 1) = '\;', substr(regexp_replace(mac, '-', ':'), 2), regexp_replace(mac, '-', ':')), '\;', ','), ',')) a as id
                                             where length(mdn) = 32
                                               and id regexp
                                             '^([0-9a-fA-F]{2})(([/\s:-][0-9a-fA-F]{2}){5})$'
                                               and prov_id = '${PROV_ID}'
                                               and month_id = '${MONTH_ID}') t1
                                     inner join (select mdn, mdn_md5
                                                  from dwi_integ. dwi_evt_blog_dpi_mbl_contrast_m
                                                 where prov_id = '${PROV_ID}'
                                                   and month_id =
                                                       '${MONTH_ID}'
                                                 group by mdn, mdn_md5) t2
                                        on t1.mdn = t2.mdn_md5
                                     group by t2.mdn,
                                              t1.mdn,
                                              t1.id,
                                              t1.id_type,
                                              md5(t1.id),
                                              t1.id_date,
                                              t1.prov_id) t3) t4
                     where t4.mdn_cnt > 1) t5 --过滤掉id_date相同时，一个mac对应多个mdn的数据
            on t3.id = t5.id
         where t5.id is null) tt1;

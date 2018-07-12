add jar ${baseDirForScriptSelf}/md.jar;
create temporary function md5 as 'com.udf.Md5';
add jar ${baseDirForScriptSelf}/meidcode.jar;
create temporary function check as 'udf.MeidCode';
insert overwrite table dws_integ.dws_wdtb_uid_id_maping_update_d partition(id_source, prov_id, day_id)
select tt1.mdn,
       tt1.mdn_md5,
       tt1.id,
       tt1.id_type,
       tt1.id_md5,
       tt1.id_date,
       '4' id_source,
       tt1.prov_id,
       '${DAY_ID}' day_id
  from (select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'MEID' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       substr(meid_cure, 1, 14) id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and dpi_type = '3'
                                   and (length(meid_cure) = 16 and
                                        substr(meid_cure, 1, 2) = '00' or
                                        length(meid_cure) = 15 and
                                        substr(meid_cure, 1, 1) = '0' or
                                        check(meid_cure, 3))
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          substr(meid_cure, 1, 14),
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --取出dpi_type=3时，分组去重选出最新的id_date对应的meid_cure
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   substr(meid_cure, 1, 14) id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and dpi_type = '3'
                                               and (length(meid_cure) = 16 and
                                                    substr(meid_cure, 1, 2) = '00' or
                                                    length(meid_cure) = 15 and
                                                    substr(meid_cure, 1, 1) = '0' or
                                                    check(meid_cure, 3))
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      substr(meid_cure, 1, 14),
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，dpi_type=3,一个meid_cure对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'IMEI' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       substr(meid_cure, 1, 14) id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and dpi_type = '4'
                                   and check(meid_cure, 4)
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          substr(meid_cure, 1, 14),
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --取出dpi_type=4时，分组去重选出最新的id_date对应的meid_cure
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   substr(meid_cure, 1, 14) id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and dpi_type = '4'
                                               and check(meid_cure, 4)
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      substr(meid_cure, 1, 14),
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，dpi_type=4,一个meid_cure对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'IMEI' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       substr(imei_url, 1, 14) id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and check(imei_url, 4)
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          substr(imei_url, 1, 14),
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的imei_url
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   substr(imei_url, 1, 14) id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and check(imei_url, 4)
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      substr(imei_url, 1, 14),
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个imei_url对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'IMSI' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       imsi id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and (length(imsi) = 15 and
                                        substr(imsi, 1, 3) = 460 and imsi
                                        regexp '^[0-9]{15}$')
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          imsi,
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的imsi
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   imsi id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and (length(imsi) = 15 and
                                                    substr(imsi, 1, 3) = 460 and imsi
                                                    regexp '^[0-9]{15}$')
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      imsi,
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个imsi对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'MEID' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       substr(meid, 1, 14) id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and (length(meid) = 16 and
                                        substr(meid, 1, 2) = '00' or
                                        length(meid) = 15 and
                                        substr(meid, 1, 1) = '0' or
                                        check(meid, 3))
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          substr(meid, 1, 14),
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的meid
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   substr(meid, 1, 14) id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and (length(meid) = 16 and
                                                    substr(meid, 1, 2) = '00' or
                                                    length(meid) = 15 and
                                                    substr(meid, 1, 1) = '0' or
                                                    check(meid, 3))
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      substr(meid, 1, 14),
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个meid对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'TDID' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       tdid id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and length(tdid) > 0
                                   and tdid <> 'null'
                                   and tdid <> 'NULL'
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          tdid,
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的tdid
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   tdid id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and length(tdid) > 0
                                               and tdid <> 'null'
                                               and tdid <> 'NULL'
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      tdid,
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个tdid对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'AndroidID' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       android_id id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and (android_id regexp
                                        '^[0-9A-Fa-f]{16}$' and android_id
                                        regexp '[^0]{16}$')
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          android_id,
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的android_id
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   android_id id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and (android_id regexp
                                                    '^[0-9A-Fa-f]{16}$' and
                                                    android_id regexp
                                                    '[^0]{16}$')
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      android_id,
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个android_id对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'IDFA' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       idfa id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and (idfa regexp
                                        '^[0-9A-Fa-f]{8}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{12}$' and
                                        idfa <>
                                        '00000000-0000-0000-000000000000')
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          idfa,
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的idfa
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   idfa id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and (idfa regexp
                                                    '^[0-9A-Fa-f]{8}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{12}$' and
                                                    idfa <>
                                                    '00000000-0000-0000-000000000000')
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      idfa,
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个idfa对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'MAC' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d lateral view explode(split(regexp_replace(if (substr(regexp_replace(mac, '-', ':'), 1, 1) = '\;', substr(regexp_replace(mac, '-', ':'), 2), regexp_replace(mac, '-', ':')), '\;', ','), ',')) a as id
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and id regexp
                                 '^([0-9a-fA-F]{2})(([/\s:-][0-9a-fA-F]{2}){5})$'
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          id,
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的mac
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d lateral view explode(split(regexp_replace(if (substr(regexp_replace(mac, '-', ':'), 1, 1) = '\;', substr(regexp_replace(mac, '-', ':'), 2), regexp_replace(mac, '-', ':')), '\;', ','), ',')) a as id
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and id regexp
                                             '^([0-9a-fA-F]{2})(([/\s:-][0-9a-fA-F]{2}){5})$'
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      id,
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个mac对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'DEVICETOKEN' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       devicetoken id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and (devicetoken regexp
                                        '[0-9a-f]{64}$' and
                                        devicetoken <>
                                        '0000000000000000000000000000000000000000000000000000000000000000')
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          devicetoken,
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的devicetoken
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   devicetoken id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and (devicetoken regexp
                                                    '[0-9a-f]{64}$' and
                                                    devicetoken <>
                                                    '0000000000000000000000000000000000000000000000000000000000000000')
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      devicetoken,
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个devicetoken对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               t3.id_type,
               t3.id_md5,
               t3.id_date,
               t3.prov_id
          from (select t2.mdn,
                       md5(t2.mdn) mdn_md5,
                       t2.id,
                       'BAIDU_ID' id_type,
                       md5(t2.id) id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.id,
                               t1.id_date,
                               t1.prov_id,
                               row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                          from (select mdn,
                                       id,
                                       substr(start_time, 1, 8) id_date,
                                       prov_id
                                  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d lateral view explode(split(if (substr(baidu_id, 1, 1) = ',', substr(baidu_id, 2), baidu_id), ',')) a as id
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and length(id) > 0
                                   and id <> 'null'
                                   and id <> 'NULL'
                                   and prov_id = '${PROV_ID}'
                                   and day_id = '${DAY_ID}'
                                 group by mdn,
                                          id,
                                          substr(start_time, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的baidu_id
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id_date order by t1.id_date desc) rn
                                      from (select mdn,
                                                   id,
                                                   substr(start_time, 1, 8) id_date,
                                                   prov_id
                                              from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d lateral view explode(split(if (substr(baidu_id, 1, 1) = ',', substr(baidu_id, 2), baidu_id), ',')) a as id
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and length(id) > 0
                                               and id <> 'null'
                                               and id <> 'NULL'
                                               and prov_id = '${PROV_ID}'
                                               and day_id = '${DAY_ID}'
                                             group by mdn,
                                                      id,
                                                      substr(start_time, 1, 8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null) tt1; --过滤掉id_date相同时，一个baidu_id对应多个mdn的数据

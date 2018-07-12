add jar ${baseDirForScriptSelf}/md.jar;
create temporary function md5 as 'com.udf.Md5';
add jar ${baseDirForScriptSelf}/meidcode.jar;
create temporary function check as 'udf.MeidCode';
insert overwrite table dws_integ.dws_wdtb_uid_info_mbl_match_m partition(id_source,prov_id,day_id)
select tt1.mdn,
       tt1.mdn_md5,
       tt1.id,
       tt1.id_type,
       tt1.id_md5,
       tt1.id_date,
       tt1.id_source,
       tt1.prov_id,
       tt1.day_id
  from (select t3.mdn,
               t3.mdn_md5,
               t3.id,
               'IMSI' id_type,
               t3.id_md5,
               t3.id_date,
               '0' id_source,
               t3.prov_id,
               '${DAY_id}' day_id
          from (select t2.mdn,
                       t2.mdn_md5,
                       t2.id,
                       t2.id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.mdn_md5,
                               t1.id,
                               t1.id_md5,
                               t1.id_date,
                               prov_id,
                               row_number() over(partition by t1.id order by t1.id_date desc) rn
                          from (select mdn,
                                       mdn_md5,
                                       imsi id,
                                       imsi_md5 id_md5,
                                       substr(imsi_start_date, 1, 8) id_date,
                                       prov_id
                                  from dws_integ.dws_wdtb_uid_info_mbl_match_m
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and (length(imsi) = 15 and
                                        substr(imsi, 1, 3) = 460 and imsi
                                        regexp '^[0-9]{15}$')
								   and prov_id='${PROV_ID}'
								   and month_id='${MONTH_ID}'
                                 group by mdn,
                                          mdn_md5,
                                          imsi,
                                          imsi_md5,
                                          substr(imsi_start_date, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的imsi
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id order by t1.id_date desc) rn
                                      from (select mdn,
                                                   mdn_md5,
                                                   imsi id,
                                                   imsi_md5 id_md5,
                                                   substr(imsi_start_date, 1, 8) id_date,
                                                   prov_id
                                              from dws_integ.dws_wdtb_uid_info_mbl_match_m
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and (length(imsi) = 15 and
                                                    substr(imsi, 1, 3) = 460 and imsi
                                                    regexp '^[0-9]{15}$')
											   and prov_id='${PROV_ID}'
											   and month_id='${MONTH_ID}'
                                             group by mdn,
                                                      mdn_md5,
                                                      imsi,
                                                      imsi_md5,
                                                      substr(imsi_start_date,
                                                             1,
                                                             8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个imsi对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               'IMEI' id_type,
               t3.id_md5,
               t3.id_date,
               '0' id_source,
               t3.prov_id,
               '${DAY_id}' day_id
          from (select t2.mdn,
                       t2.mdn_md5,
                       t2.id,
                       t2.id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.mdn_md5,
                               t1.id,
                               md5(t1.id) id_md5,
                               t1.id_date,
                               prov_id,
                               row_number() over(partition by t1.id order by t1.id_date desc) rn
                          from (select mdn,
                                       mdn_md5,
                                       substr(imei, 1, 14) id,
                                       substr(imei_start_date, 1, 8) id_date,
                                       prov_id
                                  from dws_integ.dws_wdtb_uid_info_mbl_match_m
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and check(imei, 4)
								   and prov_id='${PROV_ID}'
								   and month_id='${MONTH_ID}'
                                 group by mdn,
                                          mdn_md5,
                                          substr(imei, 1, 14),
                                          substr(imei_start_date, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的imei
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id order by t1.id_date desc) rn
                                      from (select mdn,
                                                   mdn_md5,
                                                   substr(imei, 1, 14) id,
                                                   substr(imei_start_date, 1, 8) id_date,
                                                   prov_id
                                              from dws_integ.dws_wdtb_uid_info_mbl_match_m
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and check(imei, 4)
											   and prov_id='${PROV_ID}'
											   and month_id='${MONTH_ID}'
                                             group by mdn,
                                                      mdn_md5,
                                                      substr(imei, 1, 14),
                                                      substr(imei_start_date,
                                                             1,
                                                             8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个imei对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               'MEID' id_type,
               t3.id_md5,
               t3.id_date,
               '0' id_source,
               t3.prov_id,
               '${DAY_id}' day_id
          from (select t2.mdn,
                       t2.mdn_md5,
                       t2.id,
                       t2.id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.mdn_md5,
                               t1.id,
                               md5(t1.id) id_md5,
                               t1.id_date,
                               prov_id,
                               row_number() over(partition by t1.id order by t1.id_date desc) rn
                          from (select mdn,
                                       mdn_md5,
                                       substr(meid, 1, 14) id,
                                       substr(meid_start_date, 1, 8) id_date,
                                       prov_id
                                  from dws_integ.dws_wdtb_uid_info_mbl_match_m
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and (length(meid) = 16 and
                                        substr(meid, 1, 2) = '00' or
                                        length(meid) = 15 and
                                        substr(meid, 1, 1) = '0' or
                                        check(meid, 3))
								   and prov_id='${PROV_ID}'
								   and month_id='${MONTH_ID}'
                                 group by mdn,
                                          mdn_md5,
                                          substr(meid, 1, 14),
                                          substr(meid_start_date, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的meid
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id order by t1.id_date desc) rn
                                      from (select mdn,
                                                   mdn_md5,
                                                   substr(meid, 1, 14) id,
                                                   substr(meid_start_date, 1, 8) id_date,
                                                   prov_id
                                              from dws_integ.dws_wdtb_uid_info_mbl_match_m
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and (length(meid) = 16 and
                                                    substr(meid, 1, 2) = '00' or
                                                    length(meid) = 15 and
                                                    substr(meid, 1, 1) = '0' or
                                                    check(meid, 3))
											   and prov_id='${PROV_ID}'
											   and month_id='${MONTH_ID}'
                                             group by mdn,
                                                      mdn_md5,
                                                      substr(meid, 1, 14),
                                                      substr(meid_start_date,
                                                             1,
                                                             8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个meid对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               'ESN' id_type,
               t3.id_md5,
               t3.id_date,
               '0' id_source,
               t3.prov_id,
               '${DAY_id}' day_id
          from (select t2.mdn,
                       t2.mdn_md5,
                       t2.id,
                       t2.id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.mdn_md5,
                               t1.id,
                               t1.id_md5,
                               t1.id_date,
                               prov_id,
                               row_number() over(partition by t1.id order by t1.id_date desc) rn
                          from (select mdn,
                                       mdn_md5,
                                       esn_code id,
                                       esn_code_md5 id_md5,
                                       substr(esn_code_start_date, 1, 8) id_date,
                                       prov_id
                                  from dws_integ.dws_wdtb_uid_info_mbl_match_m
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and length(esn_code) > 0
                                   and esn_code <> 'null'
                                   and esn_code <> 'NULL'
								   and prov_id='${PROV_ID}'
								   and month_id='${MONTH_ID}'
                                 group by mdn,
                                          mdn_md5,
                                          esn_code,
                                          esn_code_md5,
                                          substr(esn_code_start_date, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的esn_code
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id order by t1.id_date desc) rn
                                      from (select mdn,
                                                   mdn_md5,
                                                   esn_code id,
                                                   esn_code_md5 id_md5,
                                                   substr(esn_code_start_date,
                                                          1,
                                                          8) id_date,
                                                   prov_id
                                              from dws_integ.dws_wdtb_uid_info_mbl_match_m
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and length(esn_code) > 0
                                               and esn_code <> 'null'
                                               and esn_code <> 'NULL'
											   and prov_id='${PROV_ID}'
											   and month_id='${MONTH_ID}'
                                             group by mdn,
                                                      mdn_md5,
                                                      esn_code,
                                                      esn_code_md5,
                                                      substr(esn_code_start_date,
                                                             1,
                                                             8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null --过滤掉id_date相同时，一个esn_code对应多个mdn的数据
        union all
        select t3.mdn,
               t3.mdn_md5,
               t3.id,
               'ICCID' id_type,
               t3.id_md5,
               t3.id_date,
               '0' id_source,
               t3.prov_id,
               '${DAY_id}' day_id
          from (select t2.mdn,
                       t2.mdn_md5,
                       t2.id,
                       t2.id_md5,
                       t2.id_date,
                       t2.prov_id
                  from (select t1.mdn,
                               t1.mdn_md5,
                               t1.id,
                               md5(t1.iccid) id_md5,
                               t1.id_date,
                               prov_id,
                               row_number() over(partition by t1.id order by t1.id_date desc) rn
                          from (select mdn,
                                       mdn_md5,
                                       iccid id,
                                       substr(iccid_start_date, 1, 8) id_date,
                                       prov_id
                                  from dws_integ.dws_wdtb_uid_info_mbl_match_m
                                 where length(mdn) > 0
                                   and mdn <> 'null'
                                   and mdn <> 'NULL'
                                   and length(iccid) > 0
                                   and iccid <> 'null'
                                   and iccid <> 'NULL'
								   and prov_id='${PROV_ID}'
								   and month_id='${MONTH_ID}'
                                 group by mdn,
                                          mdn_md5,
                                          iccid,
                                          substr(iccid_start_date, 1, 8),
                                          prov_id) t1) t2
                 where t2.rn = 1) t3 --分组去重选出最新的id_date对应的iccid                                                                                                           
          left join (select t3.id
                      from (select t2.id, count(distinct t2.mdn) mdn_cnt
                              from (select t1.mdn,
                                           t1.id,
                                           row_number() over(partition by t1.id order by t1.id_date desc) rn
                                      from (select mdn,
                                                   mdn_md5,
                                                   iccid id,
                                                   substr(iccid_start_date,
                                                          1,
                                                          8) id_date,
                                                   prov_id
                                              from dws_integ.dws_wdtb_uid_info_mbl_match_m
                                             where length(mdn) > 0
                                               and mdn <> 'null'
                                               and mdn <> 'NULL'
                                               and length(iccid) > 0
                                               and iccid <> 'null'
                                               and iccid <> 'NULL'
											   and prov_id='${PROV_ID}'
											   and month_id='${MONTH_ID}'
                                             group by mdn,
                                                      mdn_md5,
                                                      iccid,
                                                      substr(iccid_start_date,
                                                             1,
                                                             8),
                                                      prov_id) t1) t2
                             where t2.rn = 1) t3
                     where t3.mdn_cnt > 1) t4
            on t3.id = t4.id
         where t4.id is null) tt1; --过滤掉id_date相同时，一个iccid对应多个mdn的数据
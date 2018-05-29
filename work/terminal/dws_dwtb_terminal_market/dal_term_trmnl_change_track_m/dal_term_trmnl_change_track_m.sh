#!/bin/bash
#***********************************************************************************
# **  文件名称: dal_term_trmnl_change_track_m.sh
# **  创建日期: 20180503
# **  编写人员: yy
# **  输入信息: 
# **  输出信息: 
# **
# **  功能描述: 
# **  处理过程:
# **  Copyright(c) 2016 TianYi Cloud Technologies (China), Inc.
# **  All Rights Reserved.
#***********************************************************************************

#***********************************************************************************
#==修改日期==|===修改人=====|======================================================|
# 2018-5-3 yangyong
#***********************************************************************************
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
source ~/.bash_profile
source ${baseDirForScriptSelf}/common.fun
echo "${baseDirForScriptSelf}/common.fun"
ScriptName=$0
#日志输出路径
#LOGPATH=${baseDirForScriptSelf}
###############配置区############################################################################
#脚本名称
LOGNAME="dal_term_trmnl_change_track_m"

#用户名、队列名
USERNAME="dwi_m"
QUEUENAME="root.test.test15"

##############SQL变量############################################################################
#ods省份编码
#PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
PROVS=811
DATES=20170301
#开起并发参数,一般单独执行的sql,不建议开起并发参数
#concurrency=8
#################################################################################################
#报错发送信息,联系邮箱#邮件组
ARREMAIL=

###############脚本参数判断######################################################################
#不输入参数,月份默认上个月,省份默认为配置省份
#输入参数为月份（例如201608）,月份默认上个月,省份默认为配置省份
#输入参数为dates,月份、省份为配置月份、省份
#################################################################################################
QUEUE1=$(echo $1|awk -F '.' '{print $1}')
QUEUE2=$(echo $2|awk -F '.' '{print $1}')
if [ $# -eq 1 ] && [ "$1"x != "dates"x ]  && [ "$QUEUE1"x != "queue"x ];then
             DATES=($1)
elif [ $# -eq 1 ] && [ "$QUEUE1"x = "queue"x ];then
             QUEUENAME=$(echo $1 |awk -F 'queue\\.' '{print $2}')
    #默认上个月
    DATES=($(date -d "$(date +%Y%m)01 -1 month" +%Y%m))
    #默认昨天
    #DATES=($(date +"%Y%m%d" -d "-1day"))
    
elif [ $# -eq 2 ];then
             DATES=($1)
    if [ "$QUEUE2"x = "queue"x ];then
             QUEUENAME=$(echo $2 |awk -F 'queue\\.' '{print $2}')
        else PROVS=($2)
    fi
elif [ $# -eq 3 ];then
             DATES=($1)
             PROVS=($2)
             QUEUENAME=$(echo $3 |awk -F 'queue\\.' '{print $2}')
else
    #默认上个月
    DATES=($(date -d "$(date +%Y%m)01 -1 month" +%Y%m))
    #默认昨天
    #DATES=($(date +"%Y%m%d" -d "-1day"))
fi
echo ${QUEUENAME}
PROVS=(${PROVS//,/ })
DATES=(${DATES//,/ })
ARREMAIL=(${ARREMAIL//,/ })
echo ${DATES[*]}
echo ${PROVS[*]}

#############HIVE参数区###########################################################################
#常用jar包 路径 /home/st001/soft/
#md5 输出结果md5大写
#add jar /home/st001/soft/BoncHiveUDF.jar;
#CREATE TEMPORARY FUNCTION MD5Encode AS 'com.bonc.hive.MyMD5';
#常规参数
COMMON_VAR="use ${USERNAME};
set mapreduce.job.queuename=${QUEUENAME};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec; 
set mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=256000000;
set mapred.min.split.size.per.rack=256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;"
#合并小文件参数
MERGE_VAR="use ${USERNAME};
set mapreduce.job.queuename=${QUEUENAME};
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles= true;
set hive.merge.size.per.task=134217728;
set hive.merge.smallfiles.avgsize=150000000;
set mapred.max.split.size=134217728;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack = 100000000;
set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec; 
set hive.exec.compress.output = true;
set hive.hadoop.supports.splittable.combineinputformat=true;"

###############函数区################################################################################
###############时间配置函数##########################################################################
function CONFIGURE(){
    DAY_ID=$1
    MONTH_ID=${DAY_ID:0:6}
    PRE_MONTH_ID=$(date -d "${MONTH_ID}01  -1 month" +%Y%m)
    PRE2_MONTH_ID=$(date -d "${MONTH_ID}01  -2 month" +%Y%m)
    NEXT_MONTH_ID=$(date -d "${MONTH_ID}01  1 month" +%Y%m)
	MONTH_LAST_DAY=$(date -d  "`date -d "${MONTH_ID}01 1 month" +%Y%m%d`  -1  day" +%Y-%m-%d)
}
#####################################################################################################
#程序执行开始时间
start_dt=`date "+%Y-%m-%d %H:%M:%S"`
i=0
#执行时间段,使用下面注释的for循环
#for (( DAY_ID=20170501; DAY_ID<=20170531; DAY_ID++ ))
for DAY_ID in ${DATES[@]};
do
    CONFIGURE ${DAY_ID}
    for PROV_ID in ${PROVS[@]};
    do
    let i+=1
#################################FOR循环开始##########################################################
###############################以下为SQL编辑区########################################################

#正常执行hql
SQL="INSERT OVERWRITE TABLE dal_term.dal_term_trmnl_change_track_m partition(month_id)
select t3.add_trmnl_cnt,
       t3.add_trmnl_user_cnt,
       (case
         when t3.prov_name in ('广东', '广西', '福建', '海南') then
          '华南'
         when t3.prov_name in ('江苏', '浙江', '上海', '台湾') then
          '华东'
         when t3.prov_name in ('河南', '湖北', '安徽', '湖南', '江西') then
          '华中'
         when t3.prov_name in ('西藏', '云南', '四川', '贵州', '重庆') then
          '西南'
         when t3.prov_name in
              ('内蒙古', '山西', '河北', '北京', '天津', '山东') then
          '华北'
         when t3.prov_name in ('新疆', '甘肃', '青海', '宁夏', '陕西') then
          '西北'
         when t3.prov_name in ('黑龙江', '吉林', '辽宁') then
          '东北'
       end) redion_name,
       t3.prov_name,
       t3.city_id city_name,
       (case
         when t4.listed_price > 0 and t4.listed_price < 600 then
          '(0,600)'
         when t4.listed_price >= 600 and t4.listed_price < 1000 then
          '[600,1000)'
         when t4.listed_price >= 1000 and t4.listed_price < 1500 then
          '[1000,1500)'
         when t4.listed_price >= 1500 and t4.listed_price < 2500 then
          '[1500,2500)'
         when t4.listed_price >= 2500 and t4.listed_price < 4000 then
          '[2500,4000)'
         when t4.listed_price >= 4000 then
          '[4000,∞)'
         else
          ''
       end),
       t4.model_point function_orientation,
       t4.ios,
       t4.standby_type,
       t4.dm_net_frequency,
       t4.model_type trmnl_type,
       t4.core_num,
       t4.dm_net_type,
       t3.trmnl_brand,
       t3.trmnl_model,
       t3.pre_trmnl_brand_1 pre1_trmnl_brand,
       t3.pre_trmnl_model_1 pre1_trmnl_mdl,
       (case
         when t5.listed_price > 0 and t5.listed_price < 600 then
          '(0,600)'
         when t5.listed_price >= 600 and t5.listed_price < 1000 then
          '[600,1000)'
         when t5.listed_price >= 1000 and t5.listed_price < 1500 then
          '[1000,1500)'
         when t5.listed_price >= 1500 and t5.listed_price < 2500 then
          '[1500,2500)'
         when t5.listed_price >= 2500 and t5.listed_price < 4000 then
          '[2500,4000)'
         when t5.listed_price >= 4000 then
          '[4000,∞)'
         else
          ''
       end) pre1_listed_price,
       t5.model_point pre1_function_orientation,
       t5.ios pre1_ios,
       t5.standby_type pre1_standby_type,
       t5.dm_net_frequency pre1_dm_net_frequency,
       t5.model_type pre1_trmnl_type,
       t5.core_num pre1_core_num,
       t5.dm_net_type pre1_dm_net_type,
       t3.change_innet_trmnl,
       t3.change_innet_trmnl_cnt,
       t3.change_outnet_trmnl,
       t3.change_outnet_trmnl_cnt,
       t4.looks_type,
       t4.product_weight,
       t4.monitor_num,
       t4.monitor_size,
       t4.camera_pixels,
       t4.s_cammera_pixels,
       t4.ram,
       t4.rom,
       t4.flash_ram,
       t4.battery_capacity,
       t4.chip_platform,
       t3.unuse_trmnl_dur,
       t3.unuse_trmnl_cnt,
       t3.month_id
  from (select t1.month_id,
               t1.prov_name,
               t1.city_id,
               t1.trmnl_brand,
               t1.trmnl_model,
               t1.pre_trmnl_brand_1,
               t1.pre_trmnl_model_1,
               t1.unuse_trmnl_dur,
               count(distinct(case
                                when from_unixtime(unix_timestamp(t2.regst_date,
                                                                  'yyyy-MM-dd HH:mm:ss'),
                                                   'yyyyMM') = '${MONTH_ID}' then
                                 t2.meid
                              end)) add_trmnl_cnt,
               count(distinct(case
                                when from_unixtime(unix_timestamp(t2.regst_date,
                                                                  'yyyy-MM-dd HH:mm:ss'),
                                                   'yyyyMM') = '${MONTH_ID}' then
                                 t2.mdn
                              end)) add_trmnl_user_cnt,
               count(distinct(case
                                when from_unixtime(unix_timestamp(t2.regst_date,
                                                                  'yyyy-MM-dd HH:mm:ss'),
                                                   'yyyyMM') = '${MONTH_ID}' and
                                     datediff('${MONTH_LAST_DAY}',
                                              substr(t2.regst_date, 1, 10)) >= 30 then
                                 t2.mdn
                              end)) change_innet_trmnl_cnt,
               count(distinct(case
                                when from_unixtime(unix_timestamp(t2.regst_date,
                                                                  'yyyy-MM-dd HH:mm:ss'),
                                                   'yyyyMM') in
                                     ('${PRE_MONTH_ID}', '${PRE2_MONTH_ID}') and
                                     t2.trmnl_model <> t1.pre_trmnl_model_1 then
                                 t2.mdn
                              end)) change_outnet_trmnl_cnt,
               (case
                 when from_unixtime(unix_timestamp(t2.regst_date,
                                                   'yyyy-MM-dd HH:mm:ss'),
                                    'yyyyMM') = '${MONTH_ID}' and
                      datediff('${MONTH_LAST_DAY}',
                               substr(t2.regst_date, 1, 10)) >= 30 then
                  t2.trmnl_model
               end) change_innet_trmnl,
               (case
                 when from_unixtime(unix_timestamp(t2.regst_date,
                                                   'yyyy-MM-dd HH:mm:ss'),
                                    'yyyyMM') in
                      ('${PRE_MONTH_ID}', '${PRE2_MONTH_ID}') and
                      t2.trmnl_model <> t1.pre_trmnl_model_1 then
                  t1.pre_trmnl_model_1
               end) change_outnet_trmnl,
               count(case
                       when t1.unuse_trmnl_dur is not null and
                            t1.unuse_trmnl_dur <> '0' then
                        1
                     end) unuse_trmnl_cnt
          from (select mdn,
                       prov_name,
                       city_id,
                       change_cnt,
                       change_dur,
                       change_cycle,
                       pre_trmnl_brand_1,
                       pre_trmnl_model_1,
                       pre_trmnl_use_date_1,
                       pre_use_date_1,
                       pre_trmnl_brand_2,
                       pre_trmnl_model_2,
                       pre_trmnl_use_date_2,
                       pre_use_date_2,
                       loyal_brand,
                       pre_trmnl_brand_3,
                       pre_trmnl_model_3,
                       pre_trmnl_use_date_3,
                       pre_use_date_3,
                       trmnl_price,
                       pre_trmnl_price_1,
                       pre_trmnl_price_2,
                       pre_trmnl_price_3,
                       avg_price,
                       trmnl_brand,
                       trmnl_model,
                       use_date,
                       unuse_trmnl_dur,
                       prov_id,
                       month_id
                  from dws_m.dws_wdtb_trmnl_regst_user_info_msk_m
                 where month_id = '${MONTH_ID}') t1
          left join (select meid,
                           mdn,
                           imsi,
                           prov_name,
                           trmnl_brand,
                           trmnl_model,
                           version,
                           regst_date,
                           is_first_regst,
                           plug_card_cnt,
                           innet_used_trmnl,
                           prov_id,
                           month_id
                      from dws_m.dws_wdtb_trmnl_first_regst_stack_msk_m
                     where month_id = '${MONTH_ID}') t2
            on t1.mdn = t2.mdn
         group by t1.month_id,
                  t1.prov_name,
                  t1.city_id,
                  t1.trmnl_brand,
                  t1.trmnl_model,
                  t1.pre_trmnl_brand_1,
                  t1.pre_trmnl_model_1,
                  t1.unuse_trmnl_dur,
                  (case
                    when from_unixtime(unix_timestamp(t2.regst_date,
                                                      'yyyy-MM-dd HH:mm:ss'),
                                       'yyyyMM') = '${MONTH_ID}' and
                         datediff('${MONTH_LAST_DAY}',
                                  substr(t2.regst_date, 1, 10)) >= 30 then
                     t2.trmnl_model
                  end),
                  (case
                    when from_unixtime(unix_timestamp(t2.regst_date,
                                                      'yyyy-MM-dd HH:mm:ss'),
                                       'yyyyMM') in
                         ('${PRE_MONTH_ID}', '${PRE2_MONTH_ID}') and
                         t2.trmnl_model <> t1.pre_trmnl_model_1 then
                     t1.pre_trmnl_model_1
                  end)) t3
  left join dim.dim_devinfo t4
    on t3.trmnl_model = t4.dm_model
  left join dim.dim_devinfo t5
    on t3.pre_trmnl_model_1 = t5.dm_model;
"

RunScript "${SQL}"
#and accs_nbr rlike '^1[0-9]{10}$'

#执行mr
#SQL="hadoop jar sort.jar /apps/hive/warehouse/st001.db/ztj_test3/ /apps/hive/warehouse/st001.db/ztj_test2/ root.bigdata.motl.mt1"
#RunMr "${SQL}"

#计算单表条数
#value=0
#SQL="
#select count(*) from odsbstl.d_area_code where crm_code='811';
#"
#value "${SQL}"
##可以根据返回值进行表的条数,空,波动性判断
#if [ ${value} -eq 0 ] ; then
#SendMessage "bss_d_mask${DAY_ID}账期,表记录数为0"
#fi

#合并小文件方法
#Mergefile "${USERNAME}" "dm_ind_req_zhjt_4guser_m" "where month_id = '${MONTH_ID}'"





##################################FOR循环结束#########################################################
    echo "================================================================================"
    done
done
wait
######################################################################################################
##################################开始合并小文件#######################################################
#i=0
#for DAY_ID in ${DATES[@]};
#do
#    CONFIGURE ${DAY_ID}
#    for PROV_ID in ${PROVS[@]};
#    do
#    let i+=1
#    echo "===========================mergesql $i=================================================="
################################以下为SQL编辑区########################################################
##Mergefile函数参数解释:需要合并的分区日期、需要合并的分区省份、表名、where条件{只需修改后面两个参数}
##Mergefile "${USERNAME}" "MBL_DPI_TYPE_APP_LABEL_D" "where day_id = '${DAY_ID}'
#SendMessage "Mergefile函数参数解释:需要合并的分区日期"
###################################FOR循环结束#########################################################
#    echo "=================================================================================="
#    done
#done

#程序执行结束时间
end_dt=`date "+%Y-%m-%d %H:%M:%S"`
time1=$(($(date +%s -d "$end_dt") - $(date +%s -d "$start_dt")))
######################################################################################################
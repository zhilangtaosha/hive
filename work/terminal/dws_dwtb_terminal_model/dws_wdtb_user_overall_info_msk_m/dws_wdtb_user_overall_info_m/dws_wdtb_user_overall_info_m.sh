#!/bin/bash
#***********************************************************************************
# **  文件名称: CR_USER_CREDIT_INFO .sh
# **  创建日期: 2017年08月24日
# **  编写人员: qiaoyin
# **  输入信息: 
# **      MT001.INTEG_USER_ORDER_M,
# **      ODSBSTL.D_OFFER_SPEC f,
# **      ODSBSTL.D_CERT_TYPE g,
# **      ODSBSTL.D_INDUSTY_CD h
# **  输出信息: MT001.INTEG_USER_INFO_M
# **
# **  功能描述: 关联ods日表，校验身份证信息，并且，将日表的出账标识最后一天带到月表中
# **  处理过程:
# **            1 处理过程1
# **            2 处理过程2
# **  Copyright(c) 2016 TianYi Cloud Technologies (China), Inc.
# **  All Rights Reserved.
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
LOGNAME="dws_wdtb_user_overall_info_m.sh"

#用户名、队列名
USERNAME="dws_integ"
#QUEUENAME="root.bigdata.motl.mt1"
QUEUENAME="root.test.test15"

##############SQL变量############################################################################
#ods省份编码
#PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
PROVS=815
#DATES=201701,201702
#开起并发参数,一般单独执行的sql,不建议开起并发参数
#concurrency=8
#################################################################################################
#报错发送信息,联系邮箱#邮件组
ARREMAIL=qiaoy@chinatelecom.cn

###############脚本参数判断######################################################################
#不输入参数,月份默认上个月,省份默认为配置省份
#输入参数为月份（例如201608）,月份默认上个月,省份默认为配置省份
#输入参数为dates,月份、省份为配置月份、省份
#################################################################################################
if [ $# -eq 1 ];then
    if [ "$1"x != "dates"x ];then
            DATES=($1)
    fi
elif [ $# -eq 2 ];then
        DATES=($1)
        PROVS=($2)
else
    #默认上上个月
    DATES=($(date -d "$(date +%Y%m)01 -2 month" +%Y%m))
    #默认昨天
    #DATES=($(date +"%Y%m%d" -d "-1day"))
fi

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
set hive.hadoop.supports.splittable.combineinputformat=true;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=256000000;
set mapred.min.split.size.per.rack=256000000;"
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
set hive.exec.compress.output = true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;"

###############函数区################################################################################
###############时间配置函数##########################################################################
function CONFIGURE(){
    DAY_ID=$1
    MONTH_ID=${DAY_ID:0:6}
    PRE1_MONTH_ID=$(date -d "${MONTH_ID}01  -1 month" +%Y%m)
	PRE2_MONTH_ID=$(date -d "${MONTH_ID}01  -2 month" +%Y%m)
	PRE3_MONTH_ID=$(date -d "${MONTH_ID}01  -3 month" +%Y%m)
    NEXT_MONTH_ID=$(date -d "${MONTH_ID}01  1 month" +%Y%m)
	TWO_MONTH_BEFORE=$(date -d "${MONTH_ID}01 -2 month" +%Y%m)
	MONTH_LAST_DAY=$(date -d  "`date -d "${MONTH_ID}01 1 month" +%Y%m%d`  -1  day" +%Y%m%d)
	SEVEN_MONTH_BEFORE=$(date -d "${MONTH_ID}01 -6 month" +%Y%m)
	THREE_YEAR_BEFORE=$(date -d "${MONTH_ID}01 -36 month" +%Y%m)
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

#执行mr
# SQL="hadoop jar sort.jar /apps/hive/warehouse/st001.db/ztj_test3/ /apps/hive/warehouse/st001.db/ztj_test2/ mt1"
#RunMr "${SQL}"

#计算单表条数
# SQL="
# select count(*) id,2 name,10 name2 from odsbstl.d_area_code where crm_code='811';
# "
# value "${SQL}"
#echo value0= ${value[0]} value1= ${value[1]} value2= ${value[2]}
#可以根据返回值进行表的条数，空，波动性判断
# if [ ${value[0]} != 0 ] ; then
# SendMessage "test bss_d_mask${DAY_ID}账期,表记录数为0" 
# echo "aa"
# fi
# echo 'bbbbbbbb'

#
SQL="add jar ${baseDirForScriptSelf}/BoncHiveUDF.jar;
CREATE TEMPORARY FUNCTION MD5Encode AS 'com.bonc.hive.MyMD5';
add jar ${baseDirForScriptSelf}/BluePaperHiveUDF.jar;
CREATE TEMPORARY FUNCTION Star AS 'com.bonc.hive.Star';
INSERT OVERWRITE TABLE dws_integ.dws_wdtb_user_overall_info_m PARTITION (PROV_ID, MONTH_ID)
SELECT T1.ACCS_NBR,
       T1.PROV_NAME,
       T1.CITY_NAME,
       T6.MEID,
       T6.IMSI,
       T6.REGST_DATE,
       T6.TRMNL_BRAND,
       T6.TRMNL_MODEL,
       T6.MODEL_TYPE,
       T6.LISTED_PRICE,
       T1.GENDER,
       T1.AGE,
       T1.PROD_INST_STATUS,
       T1.OPEN_DATE,
       T1.ONLINE_DUR,
       T4.OFFER_ID,
       T4.OFFER_NAME,
       T4.MAIN_OFFER_FLOW,
       T4.MAIN_OFFER_FEE,
       T4.OFFER_CALL_DUR,
       T4.IS_MIX,
       T4.IS_LEASE,
       T5.FEE,
       T5.BILL_FEE,
       T2.FLOW_WLAN,
       T2.NET_DUR_WLAN,
       T2.FLOW_2G,
       T2.NET_DUR_2G,
       T2.FLOW_3G,
       T2.NET_DUR_3G,
       T2.FLOW_4G,
       T2.NET_DUR_4G,
       T2.FLOW,
       T2.NET_DUR,
       T2.CALL_DUR_DAYS,
       T2.CALL_CNT,
       T2.BILLING_DUR,
       '${MONTH_ID}' CRM_OFFER_FEE_MONTH,
       STAR(T1.CERT_NBR) STAR, -- 星座
       T1.PAY_MODE,
       T7.CONPOT, -- 消费水平
       T1.PROD_INST_ID,
       t1.industry_type, -- industry
       '' profession, -- profession
       '' user_educat, -- user_educat
       t1.birth_date, -- birthday
       t1.birth_place, -- birth_place
       case
         when t1.star_level = '1700' then
          '7星'
         when t1.star_level = '1600' then
          '6星'
         when t1.star_level = '1500' then
          '5星'
         when t1.star_level = '1400' then
          '4星'
         when t1.star_level = '1300' then
          '3星'
         when t1.star_level = '1200' then
          '2星'
         when t1.star_level = '1100' then
          '1星'
         when t1.star_level = '2100' then
          '符合评级资格未评上星级'
         when t1.star_level = '2200' then
          '不符合评级资格'
         when t1.star_level = '9999' then
          '其他'
         else
          ''
       end star_level, -- star_level
       t9.score, -- score
       t8.frend_circle_cnt,
       t10.mdr_cnt, -- mdr_cnt
       (case
         when from_unixtime(unix_timestamp(t6.regst_date,
                                           'yyyy-MM-dd HH:mm:ss'),
                            'yyyyMM') = '${MONTH_ID}' then
          t12.month_activ_days
         else
          t14.month_activ_days
       end) month_activ_days, -- month_activ_days
       (case
         when from_unixtime(unix_timestamp(t6.regst_date,
                                           'yyyy-MM-dd HH:mm:ss'),
                            'yyyyMM') = '${MONTH_ID}' then
          t13.nextmonth_activ_days
         else
          t14.nextmonth_activ_days
       end) nextmonth_activ_days, -- nextmonth_activ_days
       t11.month_avg_pay_cnt, -- month_avg_pay_cnt
       t11.once_avg_pay_money, -- once_avg_pay_money
       T1.PROV_ID,
       T1.MONTH_ID
  FROM (SELECT ACCS_NBR,
               PROD_INST_STATUS,
               ONLINE_DUR,
               PROD_INST_ID,
               AGE,
               GENDER,
               PROV_NAME,
               CITY_NAME,
               OPEN_DATE,
               birth_date,
               birth_place,
               star_level,
               industry_type,
               MONTH_ID,
               CERT_NBR,
               PAY_MODE,
               PROV_ID
          FROM (SELECT *,
                       ROW_NUMBER() OVER(PARTITION BY ACCS_NBR ORDER BY OPEN_DATE DESC) RN_TIME
                  FROM DWI_INTEG.DWI_SEV_USER_MAIN_INFO_M
                 WHERE MONTH_ID = '${MONTH_ID}'
                   AND PROD_INST_TYPE = '3') I
         WHERE I.RN_TIME = 1) T1
  LEFT JOIN (SELECT *
               FROM DWI_I_MID.DWI_EVT_BILL_OTH_FLOW_NET_CALL_M_MID
              WHERE MONTH_ID = '${MONTH_ID}') T2
    ON T1.ACCS_NBR = T2.MDN
   AND T1.PROV_ID = T2.PROV_ID
  LEFT JOIN (SELECT ACCS_NBR,
                    PROD_INST_ID,
                    IS_MIX,
                    PROV_ID,
                    MONTH_ID,
                    OFFER_ID,
                    OFFER_NAME,
                    MAIN_OFFER_FLOW,
                    MAIN_OFFER_FEE,
                    IS_LEASE,
                    OFFER_CALL_DUR
               FROM DWI_INTEG.DWI_SEV_PROD_OFFER_INFO_M
              WHERE MONTH_ID = '${MONTH_ID}') T4
    ON T1.ACCS_NBR = T4.ACCS_NBR
   AND T1.PROD_INST_ID = T4.PROD_INST_ID
   AND T1.PROV_ID = T4.PROV_ID
  LEFT JOIN (SELECT *
               FROM (SELECT ACCS_NBR,
                            PROV_ID,
                            ROW_NUMBER() OVER(PARTITION BY ACCS_NBR ORDER BY ACCS_NBR DESC) NUM,
                            FEE,
                            BILL_FEE
                       FROM dwi_integ.dwi_sev_user_fee_m
                      WHERE MONTH_ID = '${MONTH_ID}') T
              WHERE T.NUM = 1) T5
    ON T1.ACCS_NBR = T5.ACCS_NBR
   AND T1.PROV_ID = T5.PROV_ID
  LEFT JOIN (SELECT P1.*, P2.MODEL_TYPE, P2.LISTED_PRICE, P2.DM_MODEL
               FROM (SELECT *
                       FROM DWI_INTEG.dwi_sev_user_last_regst_m
                      WHERE MONTH_ID = '${MONTH_ID}') P1
               LEFT JOIN (SELECT MODEL_TYPE, LISTED_PRICE, DM_MODEL
                           FROM DIM.DIM_DEVINFO) P2
                 ON P1.TRMNL_MODEL = P2.DM_MODEL) T6
    ON T1.ACCS_NBR = T6.MDN
   AND T1.PROV_ID = T6.PROV_ID
  LEFT JOIN (SELECT MDN, CONPOT
               FROM dws_m.dws_dtfs_usertag_msk_m
              WHERE MONTH_id = '${MONTH_ID}') T7
    ON UPPER(MD5ENCODE(T1.ACCS_NBR)) = T7.MDN
  left join (select temp1.mdn, count(distinct temp1.other_party) frend_circle_cnt
  from (select mdn,
               other_party,
               avg(call_cnt) avg_call_cnt,
               avg(call_dur) avg_call_dur
          from dwi_integ.dwi_evt_bill_oth_cdr_m
         where MONTH_id in ('${MONTH_ID}',
                                 '${PRE1_MONTH_ID}',
                                 '${PRE2_MONTH_ID}',
                                 '${PRE3_MONTH_ID}')
           and prov_id = '812'
         group by mdn, other_party) temp1
 where temp1.avg_call_cnt > 30
   and temp1.avg_call_dur > 5400
 group by temp1.mdn) t8
    on T1.ACCS_NBR = t8.mdn
  left join (select mdn, score
               from dws_m.dws_wdtb_mdn_value_msk_m
              where MONTH_id = '${MONTH_ID}') t9
    on UPPER(MD5ENCODE(T1.ACCS_NBR)) = T9.MDN
  left join (select mdn, sum(sms_cnt) mdr_cnt
               from dwi_integ.dwi_evt_bill_oth_mdr_m
              where MONTH_id = '${MONTH_ID}'
              group by mdn) t10
    on T1.ACCS_NBR = t10.MDN
  left join (select accs_nbr,
                    avg(pay_cnt) month_avg_pay_cnt,
                    sum(pay_fee) / sum(pay_cnt) once_avg_pay_money
               from dwi_integ.dwi_act_acct_user_fee_m
              where MONTH_id in
                    ('${MONTH_ID}', '${PRE1_MONTH_ID}', '${PRE2_MONTH_ID}')
              group by accs_nbr) t11
    on T1.ACCS_NBR = t11.accs_nbr
  left join (select temp1.mdn,
                    count(distinct temp1.start_day) month_activ_days
               from (select mdn, substr(start_time, 1, 8) start_day
                       from dwi_m.dwi_evt_bill_oth_cdr_msk_d
                      where month_id = '${MONTH_ID}'
                      group by mdn, substr(start_time, 1, 8)
                     union all
                     select mdn, substr(start_time, 1, 8) start_day
                       from dwi_m.dwi_evt_bill_oth_ddr_msk_d
                      where month_id = '${MONTH_ID}'
                      group by mdn, substr(start_time, 1, 8)
                     union all
                     select mdn, substr(cdr_time, 1, 8) start_day
                       from dwi_m.dwi_evt_bill_oth_mdr_msk_d
                      where month_id = '${MONTH_ID}'
                      group by mdn, substr(cdr_time, 1, 8)) temp1
              group by temp1.mdn) t12
    on UPPER(MD5ENCODE(T1.ACCS_NBR)) = t12.MDN
   and from_unixtime(unix_timestamp(t6.regst_date, 'yyyy-MM-dd HH:mm:ss'),
                     'yyyyMM') = '${MONTH_ID}'
  left join (select temp1.mdn,
                    count(distinct temp1.start_day) nextmonth_activ_days
               from (select mdn, substr(start_time, 1, 8) start_day
                       from dwi_m.dwi_evt_bill_oth_cdr_msk_d
                      where month_id = '${NEXT_MONTH_ID}'
                      group by mdn, substr(start_time, 1, 8)
                     union all
                     select mdn, substr(start_time, 1, 8) start_day
                       from dwi_m.dwi_evt_bill_oth_ddr_msk_d
                      where month_id = '${NEXT_MONTH_ID}'
                      group by mdn, substr(start_time, 1, 8)
                     union all
                     select mdn, substr(cdr_time, 1, 8) start_day
                       from dwi_m.dwi_evt_bill_oth_mdr_msk_d
                      where month_id = '${NEXT_MONTH_ID}'
                      group by mdn, substr(cdr_time, 1, 8)) temp1
              group by temp1.mdn) t13
    on UPPER(MD5ENCODE(T1.ACCS_NBR)) = t13.MDN
   and from_unixtime(unix_timestamp(t6.regst_date, 'yyyy-MM-dd HH:mm:ss'),
                     'yyyyMM') = '${MONTH_ID}'
  left join (select temp2.mdn,
                    temp2. mdn month_activ_days,
                    temp2.mdn nextmonth_activ_days
               from dws_integ.dws_wdtb_user_overall_info_m temp2
              where temp2.month_id = '${PRE1_MONTH_ID}') t14
    on T1.ACCS_NBR = t14.mdn;"

echo "${SQL}"
RunScript "${SQL}"

#合并小文件方法
#Mergefile "${USERNAME}" "dm_ind_req_zhjt_4guser_m" "where month_id = '${MONTH_ID}'"





##################################FOR循环结束#########################################################
    echo "================================================================================"
    done
done
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
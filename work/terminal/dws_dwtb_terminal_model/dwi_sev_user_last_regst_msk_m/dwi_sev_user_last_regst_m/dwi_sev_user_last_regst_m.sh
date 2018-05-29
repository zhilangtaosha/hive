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
LOGNAME="dwi_sev_user_last_regst_m.sh"

#用户名、队列名
USERNAME="dwi_m"
QUEUENAME="root.bigdata.motl.mt1"

##############SQL变量############################################################################
#ods省份编码
PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
#PROVS=815
DATES=201701,201702
#开起并发参数,一般单独执行的sql,不建议开起并发参数
concurrency=8
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
insert overwrite table dwi_integ.dwi_sev_user_last_regst_m partition (prov_id, month_id )
select a.meid,
       a.mdn,
       a.imsi,
       a.prov_name,
       a.city_name,
       a.trmnl_brand,
       a.trmnl_model,
       a.version,
       a.regst_date,
       b.change_cnt,
       b.change_cycle,
       b.change_dur,
       b.last_brand_1,
       b.last_mdl_1,
       b.last_rgst_dur_1,
       b.last_rgst_dt_1,
       b.last_brand_2,
       b.last_mdl_2,
       b.last_rgst_dur_2,
       b.last_rgst_dt_2,
       b.loyal_brand,
       b.pre_trmnl_brand_3 last_brand_3,
       b.pre_trmnl_model_3 last_mdl_3,
       b.pre_trmnl_use_date_3 last_rgst_dur_3,
       b.pre_use_date_3 last_rgst_dt_3,
       b.trmnl_price,
       b.pre_trmnl_price_1,
       b.pre_trmnl_price_2,
       b.pre_trmnl_price_3,
       b.avg_price,
       (b.last_rgst_dur_1 + b.last_rgst_dur_2) / 2, -- 用户换机频率
       case
         when b.last_rgst_dur_1 is null and b.last_rgst_dur_2 is null or
              b.last_rgst_dur_1 is null and b.pre_trmnl_use_date_3 is null or
              b.last_rgst_dur_2 is null and b.pre_trmnl_use_date_3 is null then
          '不明'
         when nvl(b.last_rgst_dur_1, 0) > nvl(b.last_rgst_dur_2, 0) and
              nvl(b.last_rgst_dur_1, 0) > nvl(b.pre_trmnl_use_date_3, 0) then
          last_brand_1
         when nvl(b.last_rgst_dur_2, 0) > nvl(b.pre_trmnl_use_date_3, 0) and
              nvl(b.last_rgst_dur_2, 0) > nvl(b.last_rgst_dur_1, 0) then
          last_brand_2
         when nvl(b.pre_trmnl_use_date_3, 0) > nvl(b.last_rgst_dur_2, 0) and
              nvl(b.pre_trmnl_use_date_3, 0) > nvl(b.last_rgst_dur_1, 0) then
          pre_trmnl_brand_3
       end, -- 品牌偏好
       case
         when b.last_rgst_dur_1 is null and b.last_rgst_dur_2 is null or
              b.last_rgst_dur_1 is null and b.pre_trmnl_use_date_3 is null or
              b.last_rgst_dur_2 is null and b.pre_trmnl_use_date_3 is null then
          '不明'
         when nvl(b.last_rgst_dur_1, 0) > nvl(b.last_rgst_dur_2, 0) and
              nvl(b.last_rgst_dur_1, 0) > nvl(b.pre_trmnl_use_date_3, 0) then
          last_mdl_1
         when nvl(b.last_rgst_dur_2, 0) > nvl(b.pre_trmnl_use_date_3, 0) and
              nvl(b.last_rgst_dur_2, 0) > nvl(b.last_rgst_dur_1, 0) then
          last_mdl_2
         when nvl(b.pre_trmnl_use_date_3, 0) > nvl(b.last_rgst_dur_2, 0) and
              nvl(b.pre_trmnl_use_date_3, 0) > nvl(b.last_rgst_dur_1, 0) then
          pre_trmnl_model_3
       end, -- 机型偏好
       (case when c.monitor_size is null or d.monitor_size is null then '不明' else (c.monitor_size + d.monitor_size) / 2 end), -- 屏幕尺寸偏好
       (case
         when a.meid_asc = 1 then
          '否'
         else
          '是'
       end), -- 在网二手机用户（是否）
       '${PROV_ID}',
       '${MONTH_ID}'
  from (select meid,
               mdn,
               imsi,
               prov_name,
               city_name,
               trmnl_brand,
               trmnl_model,
               version,
               regst_date,
               meid_asc
          from dwi_integ.dwi_res_serv_trmnl_regst_sort_m
         where month_id = '${MONTH_ID}'
           and prov_id = '${PROV_ID}'
           and mdn_desc = 1
         group by meid,
                  mdn,
                  imsi,
                  prov_name,
                  city_name,
                  trmnl_brand,
                  trmnl_model,
                  version,
                  regst_date,
                  meid_asc) a
  left join (select mdn,
                    change_cnt           as change_cnt,
                    change_cycle,
                    change_dur,
                    pre_trmnl_brand_1    as last_brand_1,
                    pre_trmnl_model_1    as last_mdl_1,
                    pre_trmnl_use_date_1 as last_rgst_dur_1,
                    pre_use_date_1       as last_rgst_dt_1,
                    pre_trmnl_brand_2    as last_brand_2,
                    pre_trmnl_model_2    as last_mdl_2,
                    pre_trmnl_use_date_2 as last_rgst_dur_2,
                    pre_use_date_2       as last_rgst_dt_2,
                    loyal_brand,
                    pre_trmnl_brand_3,
                    pre_trmnl_model_3,
                    pre_trmnl_use_date_3,
                    pre_use_date_3,
                    trmnl_price,
                    pre_trmnl_price_1,
                    pre_trmnl_price_2,
                    pre_trmnl_price_3,
                    avg_price
               from dwi_integ.dwi_res_serv_trmnl_user_info_m
              where prov_id = '${PROV_ID}'
                and month_id = '${MONTH_ID}'
              group by mdn,
                       change_cnt,
                       change_cycle,
                       change_dur,
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
                       avg_price) b
    on a.mdn = b.mdn
  left join termbstl.devinfo c
    on a.trmnl_model = c.dm_model
  left join termbstl.devinfo d
    on b.last_mdl_1 = d.dm_model;"

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
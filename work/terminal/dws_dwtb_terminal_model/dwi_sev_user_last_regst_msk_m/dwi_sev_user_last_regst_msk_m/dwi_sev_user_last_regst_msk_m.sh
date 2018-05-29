#!/bin/bash
#***********************************************************************************
# **  文件名称: dwi_sev_user_last_regst_msk_m .sh
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
LOGNAME="dwi_sev_user_last_regst_msk_m.sh"

#用户名、队列名
USERNAME="dws_integ"
QUEUENAME="root.bigdata.motl.mt1"

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
insert overwrite table dwi_m.dwi_sev_user_last_regst_msk_m
 partition(prov_id, month_id)
select md5encode(meid) as meid,
       md5encode(mdn) as mdn,
       md5encode(imsi) as imsi,
       prov_name,
       city_name,
       trmnl_brand,
       trmnl_model,
       version,
       regst_date,
       change_cnt,
       change_dur,
       change_cycle,
       pre_brand_1,
       pre_model_1,
       pre_use_dur_1,
       pre_regst_date_1,
       pre_brand_2,
       pre_model_2,
       pre_use_dur_2,
       pre_regst_date_2,
       loyal_brand,
       pre_brand_3,
       pre_model_3,
       pre_use_dur_3,
       pre_regst_date_3,
       trmnl_price,
       pre_trmnl_price_1,
       pre_trmnl_price_2,
       pre_trmnl_price_3,
       avg_price,
       (case when mobile_change_fre/30>0 and mobile_change_fre/30<6 then '(0,6]'
	   when mobile_change_fre/30>6 and mobile_change_fre/30<12 then '(6,12]'
	   when mobile_change_fre/30>12 and mobile_change_fre/30<18 then '(12,18]'
	   when mobile_change_fre/30>18 and mobile_change_fre/30<24 then '(18,24)'
	   when mobile_change_fre/30>24 then '(24,∞]' else '' end) mobile_change_fre,
       brang_preference,
       type_preference,
	   -- (0，1]；(1，2.5]；(2.5，4.5]；（4.5，5.5]；（5.5，6.5]；(6.5,∞)
	   (case when screen_size>0 and screen_size<=1 then '(0,1]'
	   when screen_size>1 and screen_size<=2.5 then '(1,2.5]'
	   when screen_size>2.5 and screen_size<=4.5 then '(2.5,4.5]'
	   when screen_size>4.5 and screen_size<=5.5 then '(4.5，5.5]'
	   when screen_size>5.5 and screen_size<=6.5 then '(5.5，6.5]'
	   when screen_size>6.5 then '(6.5,∞)'
	   else '' end) screen_size,
       if_first_register,
       prov_id,
       month_id
  from dwi_integ.dwi_sev_user_last_regst_m
 where month_id = '${MONTH_ID}';
"

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
#!/bin/bash
#***********************************************************************************
# **  文件名称: dwi_res_netk_base_station_list_msk_m.sh
# **  创建日期: 2018年6月19日
# **  编写人员: hgd
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
#***********************************************************************************
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
source ~/.bash_profile
source ${baseDirForScriptSelf}/common.fun
echo "${baseDirForScriptSelf}/common.fun"
ScriptName=$0
#日志输出路径
#LOGPATH=${baseDirForScriptSelf}
#DACPDIR="/data11/dacp/data_m/logs"
###############配置区############################################################################
#脚本名称
LOGNAME="dwi_res_netk_base_station_list_msk_m"

#用户名、队列名
USERNAME="dwi_m"
QUEUENAME="root.test.test15"

##############SQL变量############################################################################
#ods省份编码
#PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
PROVS=1
DATES=20170301
#开起并发参数,一般单独执行的sql,不建议开起并发参数
#concurrency=8
#################################################################################################
#报错发送信息,联系邮箱#邮件组
ARREMAIL=zhangtj@bigdata.com

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
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles= true;
set hive.merge.size.per.task=134217728;
set hive.merge.smallfiles.avgsize=150000000;
"
#合并小文件参数
MERGE_VAR="use ${USERNAME};
set mapreduce.job.queuename=${QUEUENAME};
set hive.exec.dynamic.partition.mode=nonstrict;
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
    NEXT_MONTH_ID=$(date -d "${MONTH_ID}01  1 month" +%Y%m)
	FRIST_DAY_ID=$(date -d "${MONTH_ID}01" +%Y%m%d)
	LAST_ONE_DAY=$(date -d "`date -d "${MONTH_ID}01 1 month" +%Y%m%d` -1 day" +%Y%m%d)
	LAST_TWO_DAY=$(date -d "`date -d "${MONTH_ID}01 1 month" +%Y%m%d` -2 day" +%Y%m%d)
	LAST_THREE_DAY=$(date -d "`date -d "${MONTH_ID}01 1 month" +%Y%m%d` -3 day" +%Y%m%d)
	LAST_FOUR_DAY=$(date -d "`date -d "${MONTH_ID}01 1 month" +%Y%m%d` -4 day" +%Y%m%d)
	LAST_FIVE_DAY=$(date -d "`date -d "${MONTH_ID}01 1 month" +%Y%m%d` -5 day" +%Y%m%d)
	LAST_SIX_DAY=$(date -d "`date -d "${MONTH_ID}01 1 month" +%Y%m%d` -6 day" +%Y%m%d)
	LAST_SEVEN_DAY=$(date -d "`date -d "${MONTH_ID}01 1 month" +%Y%m%d` -7 day" +%Y%m%d)	
}
#####################################################################################################
#程序执行开始时间
start_dt=`date "+%Y-%m-%d %H:%M:%S"`
i=0
#执行时间段,使用下面注释的for循环
#for (( DAY_ID=20170501; DAY_ID<=20170531; DAY_ID=`date -d "${DAY_ID} +1 day" "+%Y%m%d"` ))
for DAY_ID in ${DATES[@]};
do
    CONFIGURE ${DAY_ID}
    for PROV_ID in ${PROVS[@]};
    do
    let i+=1
	
 
#################################FOR循环开始##########################################################
###############################以下为SQL编辑区########################################################

#正常执行hql
SQL="
msck repair table bsabstl.bsa3g_station_d;

insert overwrite table dwi_m.dwi_res_netk_base_station_list_msk_m partition(month_id)
select t1.bsid,
       t1.pilotsectorname,
       t1.sid,
       t1.nid,
       t1.exbsid,
       '3G' site_type,
       t1.txpn,
       t1.ant_lat,
       t1.ant_long,
       t1.ant_alt,
       t1.survey_acc,
       t1.sec_lat,
       t1.sec_long,
       t1.sec_alt,
       t1.ant_orient,
       t1.ant_opening,
       t1.mar,
       t1.ms_avg_height,
       t1.terrain_sd,
       t1.rtdc,
       t1.rtdc_acc,
       t1.fwd,
       t1.fwd_acc,
       t1.p_reptr,
       t1.pn_inc,
       t1.format_type,
       t1.msc_no,
       '' reserve,
       '${MONTH_ID}' month_id
  from (select *,
               row_number() over(partition by bsid order by day_id desc, ant_long desc, ant_lat desc) rn
          from bsabstl.bsa3g_station_d
         where substr(day_id,1,6) = '${MONTH_ID}'
		   and length(bsid) > 0
           and length(ant_lat) > 0
           and length(ant_long) > 0
           and bsid <> 'null'
           and bsid <> 'NULL'
           and ant_lat <> 'null'
           and ant_lat <> 'NULL'
           and ant_long <> 'null'
           and ant_long <> 'NULL') t1
 where t1.rn = 1;"


RunScript "${SQL}"

#合并小文件方法
Mergefile "${USERNAME}" "dwi_res_netk_base_station_list_msk_m" "where month_id = '${MONTH_ID}'"

#执行mr
#SQL="hadoop jar sort.jar /apps/hive/warehouse/st001.db/ztj_test3/ /apps/hive/warehouse/st001.db/ztj_test2/ mt1"
#RunMr "${SQL}"

#计算单表条数
#value=0
#SQL="
#select count(*) from odsbstl.d_area_code where crm_code='811';
#"
#value "${SQL}"
#可以根据返回值进行表的条数，空，波动性判断
#if [ ${value} -eq 0 ] ; then
#SendMessage "bss_d_mask${DAY_ID}账期,表记录数为0"
#fi

#取分区最大最小值例子
#par_max "dwi_integ.dwi_sev_user_normal_info_bss_d" "prov_id=811"
#echo par_max=$par_max
#par_min "dwi_integ.dwi_sev_user_normal_info_bss_d" "prov_id=811"
#echo par_min=$par_min


##################################FOR循环结束#########################################################
    echo "================================================================================"
    done
done
wait
######################################################################################################
##################################开始合并小文件#######################################################

#程序执行结束时间
end_dt=`date "+%Y-%m-%d %H:%M:%S"`
time1=$(($(date +%s -d "$end_dt") - $(date +%s -d "$start_dt")))
######################################################################################################
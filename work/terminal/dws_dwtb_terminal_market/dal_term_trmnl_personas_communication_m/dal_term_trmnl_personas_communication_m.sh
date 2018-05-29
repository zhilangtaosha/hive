#!/bin/bash
#***********************************************************************************
# **  文件名称: dal_term_trmnl_personas_communication_m.sh
# **  创建日期: 2018年04月09日
# **  编写人员: qinlin
# **  输入信息: 
# **  输出信息: 
# **
# **  功能描述: 
# **  处理过程:
# **  
#***********************************************************************************
##################################建表语句##########################################

#***********************************************************************************
#==修改日期==|===修改人=====|======================================================|
# 2018-4-9 qinlin.修改时间
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
LOGNAME="dal_term_trmnl_personas_communication_m.sh"

#用户名、队列名
USERNAME="dal_term"
QUEUENAME="root.bigdata.motl.mt1"

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
ARREMAIL=chenkai@bigdata.com

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
#add jar /home/st001/soft/MdnMd5.jar;
#CREATE TEMPORARY FUNCTION MD5Encode AS 'com.bonc.hive.MyMD5';
#常规参数
COMMON_VAR="use ${USERNAME};
set mapreduce.job.queuename=${QUEUENAME};
set mapred.max.map.failures.percent=10;
set mapred.max.reduce.failures.percent=10;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles= true;
set hive.merge.size.per.task = 134217728;
set hive.merge.smallfiles.avgsize = 110000000;
SET mapred.max.split.size = 134217728;
SET mapred.min.split.size.per.node = 100000000;
SET mapred.min.split.size.per.rack = 100000000;
set hive.exec.compress.output = true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.created.files=655350;
set hive.hadoop.supports.splittable.combineinputformat=true;
SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;"

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
	PRE_MONTH_DAYID=${MONTH_ID}01
    NEXT_MONTH_ID=$(date -d "${MONTH_ID}01  1 month" +%Y%m%d)
	TWO_MONTH_BEFORE=$(date -d "${MONTH_ID}01 -2 month" +%Y%m)
	MONTH_LAST_DAY=$(date -d  "`date -d "${MONTH_ID}01 1 month" +%Y%m%d`  -1  day" +%Y%m%d)
   
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
SQL=" 
INSERT OVERWRITE table dal_term.dal_term_trmnl_personas_communication_m PARTITION
      (MONTH_ID) 
   select 
   case
         when a.prov_name in('广东','广西','福建','海南') then '华南'
         when a.prov_name in('江苏','浙江','上海','台湾')then '华东'
         when a.prov_name in('河南','湖北','安徽','湖南','江西')then '华中'
         when a.prov_name in('西藏','云南','四川','贵州','重庆')then '西南'
         when a.prov_name in('内蒙古','山西','河北','北京','天津','山东')then '华北'
         when a.prov_name in('新疆','甘肃','青海','宁夏','陕西')then '西北'
         when a.prov_name in('黑龙江','吉林','辽宁')then '东北' END as  redion_name,
   a.prov_name,
   a.city_name,
   a.trmnl_brand,
   a.trmnl_model trmnl_mdl,
   case
         when d.listed_price > 0 and d.listed_price < 600 then '(0,600)' --价格区间
         when d.listed_price >= 600 and d.listed_price < 1000 then '[600,1000)'
         when d.listed_price >= 1000 and d.listed_price < 1500 then '[1000,1500)'
         when d.listed_price >= 1500 and d.listed_price < 2500 then '[1500,2500)'
         when d.listed_price >= 2500 and d.listed_price < 4000 then '[2500,4000)'
         when d.listed_price >= 4000 then '[4000,∞)' else '' END as listed_price,
   d.model_point,
   d.ios,
   d.standby_type,
   d.dm_net_frequency,
   a.trmnl_type,
   d.core_num,
   d.dm_net_type as standard_type,
   case
         when c.cdrtime > 0 and c.cdrtime <= 3 then '0-3' --通话时段
         when c.cdrtime >= 4 and c.cdrtime <= 6 then '4-6'
         when c.cdrtime >= 7 and c.cdrtime <= 9 then '7-9'
         when c.cdrtime >= 10 and c.cdrtime <= 12 then '10-12'
         when c.cdrtime >= 13 and c.cdrtime <= 15 then '13-15'
		 when c.cdrtime >= 16 and c.cdrtime <= 18 then '16-18'
		 when c.cdrtime >= 19 and c.cdrtime <= 21 then '19-21'
		 when c.cdrtime >= 22 and c.cdrtime <= 24 then '22-24'
         else '0' END as cdr_hour_level,
   case
         when b.ddrtime > 0 and b.ddrtime <= 3 then '0-3'  --数据时段
         when b.ddrtime >= 4 and b.ddrtime <= 6 then '4-6'
         when b.ddrtime >= 7 and b.ddrtime <= 9 then '7-9'
         when b.ddrtime >= 10 and b.ddrtime <= 12 then '10-12'
         when b.ddrtime >= 13 and b.ddrtime <= 15 then '13-15'
		 when b.ddrtime >= 16 and b.ddrtime <= 18 then '16-18'
		 when b.ddrtime >= 19 and b.ddrtime <= 21 then '19-21'
		 when b.ddrtime >= 22 and b.ddrtime <= 24 then '22-24'
         else '0' END as data_hour_level,
   count(distinct (case when c.cdrtime<>0 then a.mdn end)) user_cnt,
   count (distinct (case when b.ddrtime<>0 then a.mdn end)) user_internet_cnt,
   a.month_id  
   from 
(select mdn,prov_name,city_name,trmnl_brand,trmnl_model,trmnl_type,month_id from dws_m.dws_wdtb_user_overall_info_msk_m where month_id='${MONTH_ID}')a 
left join
(select mdn,ceil((UNIX_TIMESTAMP(end_time, 'yyyyMMddHHmmss')-UNIX_TIMESTAMP(start_time, 'yyyyMMddHHmmss')) /3600) ddrtime from dwi_m.dwi_evt_bill_oth_ddr_msk_d where month_id='${MONTH_ID}')b on a.mdn=b.mdn 
left join
(select mdn,ceil((UNIX_TIMESTAMP(end_time, 'yyyyMMddHHmmss')-UNIX_TIMESTAMP(start_time, 'yyyyMMddHHmmss')) /3600) cdrtime from dwi_m.dwi_evt_bill_oth_cdr_msk_d where month_id='${MONTH_ID}')c on a.mdn=c.mdn 
left join 
(select  dm_model,listed_price,model_point,ios,standby_type,dm_net_frequency,core_num,dm_net_type  from termbstl.devinfo )d on a.trmnl_model=d.dm_model  group by a.prov_name,a.city_name,a.trmnl_brand,a.trmnl_model,d.listed_price,d.model_point,d.ios,d.standby_type,d.dm_net_frequency,a.trmnl_type,d.core_num,d.dm_net_type,c.cdrtime,b.ddrtime,a.month_id;"

RunScript "${SQL}" 

#执行mr
# SQL="hadoop jar sort.jar /apps/hive/warehouse/st001.db/ztj_test3/ /apps/hive/warehouse/st001.db/ztj_test2/ root.bigdata.motl.mt1"
# RunMr "${SQL}"

#计算单表条数
# value=0
# SQL="
# select count(*) from odsbstl.d_area_code where crm_code='811';
# "
# value "${SQL}"
#可以根据返回值进行表的条数，空，波动性判断
# if [ ${value} -eq 0 ] ; then
# SendMessage "bss_d_mask${DAY_ID}账期,表记录数为0"
# fi

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

#!/bin/bash
#***********************************************************************************
# **  文件名称: dal_term_trmnl_personas_consume_m.sh
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
LOGNAME="dal_term_trmnl_personas_consume_m.sh"

#用户名、队列名
USERNAME="dws_i_mid"
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
INSERT OVERWRITE table dal_term.dal_term_trmnl_personas_consume_m PARTITION
      (MONTH_ID) 
	  select 
	  a.month_id month,
	  count(distinct a.mdn) innet_trmnl_user_cnt,
	  count(distinct a.meid) innet_trmnl_cnt,
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
	  d.dm_net_type standard_type,
	  case
	     when a.trmnl_use_date>0 and a.trmnl_use_date<=1 then '(0,1]'
		 when a.trmnl_use_date>1 and a.trmnl_use_date<=3 then '(1,3]'
		 when a.trmnl_use_date>3 and a.trmnl_use_date<=6 then '(3,6]'
		 when a.trmnl_use_date>6 and a.trmnl_use_date<=12 then '(6,12]'
		 when a.trmnl_use_date>12 and a.trmnl_use_date<=18 then '(12,18]'
		 when a.trmnl_use_date>18 and a.trmnl_use_date<=24 then '(18,24]'
		 when a.trmnl_use_date>24  then '(24,∞)' else '' END as trmnl_use_date,
	  case 
         when c.avgflow>0 and c.avgflow<=50 then '(0,50M]'
	     when c.avgflow>50 and c.avgflow<=100 then '(50M,100M]'
	     when c.avgflow>100 and c.avgflow<=300 then '(100M,300M]'
	     when c.avgflow>300 and c.avgflow<=600 then '(300M,600M]'
	     when c.avgflow>600 and c.avgflow<=1024 then '(600M,1G]'
	     when c.avgflow>1024 and c.avgflow<=2048 then '(1G,2G]'
	     when c.avgflow>2048 and c.avgflow<=4096 then '(2G,4G]'
	     when c.avgflow>4096 and c.avgflow<=8192 then '(4G,8G]'
	     when c.avgflow>8192 then '(8G,∞)' else '0' END as  flow_avg_level,
	  case 
         when c.avgcnt>0 and c.avgcnt<=5 then '(0条,5条]'
	     when c.avgcnt>5 and c.avgcnt<=10 then '(5条,10条]'
	     when c.avgcnt>10 and c.avgcnt<=30 then '(10条,30条]'
	     when c.avgcnt>30 and c.avgcnt<=50 then '(30条，50条]'
	     when c.avgcnt>50 and c.avgcnt<=100 then '(50条，100条]'
	     when c.avgcnt>100 and c.avgcnt<=200 then '(100条，200条]'
	     when c.avgcnt>200 and c.avgcnt<=500 then '(200条，500条]'
	     when c.avgcnt>500 then '(500条，∞)' else '0' END as mdr_cnt_avg_level,
	  case 
         when c.avgdur>0 and c.avgdur<=20 then '(0,20]'
	     when c.avgdur>20 and c.avgdur<=50 then '(20,50]'
	     when c.avgdur>50 and c.avgdur<=100 then '(50,100]'
	     when c.avgdur>100 and c.avgdur<=200 then '(100,200]'
	     when c.avgdur>200 and c.avgdur<=400 then '(200,400]'
	     when c.avgdur>400 and c.avgdur<=600 then '(400,600]'
	     when c.avgdur>600 and c.avgdur<=800 then '(600,800]'
	     when c.avgdur>800 and c.avgdur<=1000 then '(800,1000]'
	     when c.avgdur>1000 then '(1000,∞)' else '0' END as cdr_dur_avg_level,
	  a.frend_circle_cnt as frend_circle_cnt,
	  b.mobile_change_fre mobile_change_fre,
	  a.conpot fee_level,
	  case 
         when c.avg_fee_level>0 and c.avg_fee_level<=50 then '(0,50]'
	     when c.avg_fee_level>50 and c.avg_fee_level<=100 then '(50,100]'
	     when c.avg_fee_level>100 and c.avg_fee_level<=150 then '(100,150]'
	     when c.avg_fee_level>150 and c.avg_fee_level<=200 then '(150,200]'
		 when c.avg_fee_level>200 and c.avg_fee_level<=300 then '(200,300]'
		 when c.avg_fee_level>300 and c.avg_fee_level<=500 then '(300,500]'
		 when c.avg_fee_level>500 and c.avg_fee_level<=1000 then '(500,1000]'
	     when c.avg_fee_level>1000 then '(1000,∞)' else '0' END as avg_fee_level,
	    a.score as score_level,
	  a.month_id 
	  from 
(select mdn,meid,prov_name,city_name,trmnl_brand,trmnl_model,trmnl_type,trmnl_price,ceil((UNIX_TIMESTAMP(concat('${MONTH_ID}','01000000'), 'yyyyMMddHHmmss')-UNIX_TIMESTAMP(regst_date, 'yyyy-MM-dd HH:mm:ss')) /2592000) trmnl_use_date,frend_circle_cnt,conpot,score,month_id from dws_m.dws_wdtb_user_overall_info_msk_m where month_id=${MONTH_ID})a 
left join 
(select mdn,mobile_change_fre from dwi_m.dwi_sev_user_last_regst_msk_m where month_id=${MONTH_ID})b on a.mdn=b.mdn 
left join 
(select mdn,round(avg(call_dur),2) avgdur,round(avg(flow)/1024*1024,2) avgflow,round(avg(mdr_cnt),2) avgcnt,round(avg(fee),2) avg_fee_level from dws_m.dws_wdtb_user_overall_info_msk_m where month_id in('${TWO_MONTH_BEFORE}','${PRE_MONTH_ID}','${MONTH_ID}') group by mdn)c on a.mdn=c.mdn 
left join 
(select  dm_model,listed_price,model_point,ios,standby_type,dm_net_frequency,core_num,dm_net_type  from termbstl.devinfo )d  on a.trmnl_model=d.dm_model group by a.prov_name,a.city_name,a.trmnl_brand, a.trmnl_model,d.listed_price,d.model_point,d.ios,d.standby_type,d.dm_net_frequency,a.trmnl_type,d.core_num,d.dm_net_type,a.trmnl_use_date, c.avgflow,c.avgcnt,c.avgdur,a.frend_circle_cnt,b.mobile_change_fre,a.conpot,c.avg_fee_level,a.score,a.month_id;"
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

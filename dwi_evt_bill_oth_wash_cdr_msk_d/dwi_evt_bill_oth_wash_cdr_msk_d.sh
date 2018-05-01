#!/bin/bash
#***********************************************************************************
# **  文件名称: dal_term_devinfo_m.sh
# **  创建日期: 2017年8月8日
# **  编写人员: zhangtiejian
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
# 2016-8-8 wangshuai.修改时间
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
LOGNAME="dwi_evt_bill_oth_wash_cdr_msk_d"

#用户名、队列名
USERNAME="dwi_m"
# 队列名 mt1  -   mt8   
#QUEUENAME="root.bigdata.motl.mt1"
QUEUENAME="root.test.test15"

##############SQL变量############################################################################
#ods省份编码
#PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
PROVS=851
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
SQL="insert overwrite table dwi_m.dwi_evt_bill_oth_wash_cdr_msk_d partition (prov_id,day_id)
select t1.mdn,
       t1.other_party,
       t1.call_type,
       t1.biz_type,
       t1.roam_type,
       t1.imsi,
       t1.third_party,
       t1.start_time,
       t1.end_time,
       t1.call_dur,
       t1.home_area_code    calling_home_code, --计费号码归属地
       t1.called_home_code,
       t1.visit_area_code   calling_visit_area, --计费号码到访地
       t1.called_visit_code,
       t1.third_home_code,
       t1.third_visit_code,
       t1.corporation_id,
       t1.prov_id,
       t1.day_id
  from (select temp1.mdn,
               temp1.other_party,
               temp1.call_type,
               temp1.biz_type,
               temp1.roam_type,
               temp1.imsi,
               temp1.third_party,
               temp1.start_time,
               temp1.end_time,
               temp1.call_dur,
               temp2.city_id        home_area_code,
               temp3.city_id        called_home_code,
               temp4.city_id        visit_area_code,
               temp5.city_id        called_visit_code,
               temp6.city_id        third_home_code,
               temp7.city_id        third_visit_code,
               temp1.corporation_id,
               temp1.prov_id,
               temp1.day_id
          from (select mdn,
                       other_party,
                       call_type,
                       biz_type,
                       roam_type,
                       imsi,
                       third_party,
                       start_time,
                       end_time,
                       call_dur,
                       home_area_code,
                       called_home_code,
                       visit_area_code,
                       called_visit_code,
                       third_home_code,
                       third_visit_code,
                       corporation_id,
                       prov_id,
                       day_id
                  from dwi_m.dwi_evt_bill_oth_cdr_msk_d
                 where prov_id = '${PROV_ID}'
                   and day_id = '${DAY_ID}') temp1
          left join dim.dim_city temp2
            on temp1.home_area_code = temp2.area_code
          left join dim.dim_city temp3
            on temp1.called_home_code = temp3.area_code
          left join dim.dim_city temp4
            on temp1.visit_area_code = temp4.area_code
          left join dim.dim_city temp5
            on temp1.called_visit_code = temp5.area_code
          left join dim.dim_city temp6
            on temp1.third_home_code = temp6.area_code
          left join dim.dim_city temp7
            on temp1.third_visit_code = temp7.area_code) t1
 group by t1.prov_id,
          t1.day_id,
          t1.mdn,
          t1.other_party,
          t1.call_type,
          t1.biz_type,
          t1.roam_type,
          t1.imsi,
          t1.third_party,
          t1.start_time,
          t1.end_time,
          t1.call_dur,
          t1.home_area_code,
          t1.called_home_code,
          t1.visit_area_code,
          t1.called_visit_code,
          t1.third_home_code,
          t1.third_visit_code,
          t1.corporation_id;"

RunScript "${SQL}"


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
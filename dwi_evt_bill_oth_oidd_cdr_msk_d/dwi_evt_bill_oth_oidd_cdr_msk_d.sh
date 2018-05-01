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
LOGNAME="dwi_evt_bill_oth_oidd_cdr_msk_d"

#用户名、队列名
USERNAME="dwi_m"
# 队列名 mt1  -   mt8   
QUEUENAME="root.bigdata.motl.mt1"

##############SQL变量############################################################################
#ods省份编码
PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
#PROVS=1
DATES=20170301
#开起并发参数,一般单独执行的sql,不建议开起并发参数
concurrency=8
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
SQL="insert overwrite table dwi_m.dwi_evt_bill_oth_oidd_cdr_msk_d partition (prov_id,day_id)
select t7.mdn,
       t7.other_party,
       t7.third_party,
       t7.call_type,
       t7.start_time,
       t7.end_time,
       t7.call_dur,
       t7.calling_visit_area,
       t7.called_visit_code,
       t7.third_visit_code,
       t7.imsi,
       t7.prov_id,
       t7.day_id
  from (select t4.mdn,
               t4.opposite_num other_party,
               '' third_party,
               (case
                 when t4.call_sms_mm_type in ('0', '2', '3') then
                  '01'
                 when t4.call_sms_mm_type = '1' then
                  '02'
               end) call_type,
               t4.occur_time start_time,
               t5.occur_time end_time,
               unix_timestamp(t5.occur_time, 'yyyyMMddHHmmss') -
               unix_timestamp(t4.occur_time,'yyyyMMddHHmmss') call_dur,
               t4.city_id calling_visit_area,
               '' called_visit_code,
               '' third_visit_code,
               t4.imsi,
               '${PROV_ID}' prov_id,
               t4.receive_day day_id
          from (select t2.*, temp2.city_id
                  from (select mdn,
                               opposite_num,
                               occur_time,
                               call_sms_mm_type,
                               city_code calling_visit_area,
                               '' called_visit_code,
                               msg_type,
                               imsi,
                               row_number() over(partition by mdn, opposite_num, call_sms_mm_type order by occur_time) rn,
                               prov_id,
                               receive_day
                          from oiddbstl.bt_regetl_oidd temp1
                         where notice_id = '01'
                           and temp1.prov_id in
                               (select lpad(oidd_id, 2, '0') oidd_id
                                  from dim.dim_prov_match
                                 where prov_id = '${PROV_ID}')
                           and receive_day = '${DAY_ID}') t2
                  left join dim.dim_city temp2
                    on concat('0', t2.calling_visit_area) = temp2.area_code
                 where t2.msg_type = '1') t4
         inner join (select t3.*
                      from (select mdn,
                                   opposite_num,
                                   occur_time,
                                   call_sms_mm_type,
                                   city_code calling_visit_area,
                                   '' called_visit_code,
                                   msg_type,
                                   imsi,
                                   row_number() over(partition by mdn, opposite_num, call_sms_mm_type order by occur_time) rn,
                                   prov_id,
                                   receive_day
                              from oiddbstl.bt_regetl_oidd temp1
                             where notice_id = '01'
                               and temp1.prov_id in
                                   (select lpad(oidd_id, 2, '0') oidd_id
                                      from dim.dim_prov_match
                                     where prov_id = '${PROV_ID}')
                               and receive_day = '${DAY_ID}') t3
                     where t3.msg_type = '3') t5
            on t4.mdn = t5.mdn
           and t4.opposite_num = t5.opposite_num
           and t4.rn = t5.rn - 1) t7
 group by t7.mdn,
          t7.other_party,
          t7.third_party,
          t7.call_type,
          t7.start_time,
          t7.end_time,
          t7.call_dur,
          t7.calling_visit_area,
          t7.called_visit_code,
          t7.third_visit_code,
          t7.imsi,
          t7.prov_id,
          t7.day_id;
"

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
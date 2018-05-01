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
LOGNAME="dwi_evt_bill_oth_wcdr_mdr_msk_d"

#用户名、队列名
USERNAME="dws_m"
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
SQL="add jar ${baseDirForScriptSelf}/md.jar;
CREATE TEMPORARY FUNCTION md5 AS 'com.udf.Md5';
insert overwrite table dws_m.dws_wdtb_mdr_mix_msk_d partition (prov_id,month_id,day_id)
select t2.biz_type,
       t2.Mdn,
       t2.city_id,
       t2.corporation_id,
       t2.other_party,
       t2.other_city_id,
       t2.other_corporation,
       t2.call_type,
       t2.start_time,
       t2.Imsi,
       t2.calling_home_code,
       t2.mdr_type,
       t2.Biz_id,
       t2.biz_name,
       t2.Source_type,
       t2.Prov_id,
       t2.Month_id,
       t2.Day_id
  from (select t1.biz_type,
               t1.mdn,
               t1.city_id,
               t1.corporation_id,
               t1.other_party,
               t1.other_city_id,
               t1.other_corporation,
               t1.call_type,
               t1.start_time,
               t1.imsi,
               t1.calling_home_code,
               t1.mdr_type,
               t1.biz_id,
               t1.biz_name,
               t1.source_type,
               t1.priority_level,
               row_number() over(partition by t1.mdn, t1.other_party, t1.start_time order by t1.priority_level) rn,
               t1.prov_id,
               substr(day_id, 1, 6) month_id,
               t1.day_id
          from (select biz_type,
                       md5(mdn) mdn,
                       temp2.city_id,
                       temp2.corporation_name corporation_id,
                       md5(other_party) other_party,
                       temp3.city_id other_city_id,
                       temp3.corporation_name other_corporation,
                       call_type,
                       start_date start_time,
                       md5(imsi) imsi,
                       '' calling_home_code,
                       '个人' mdr_type,
                       '' biz_id,
                       '' biz_name,
                       'MDR' source_type,
                       '2' priority_level,
                       prov_id,
                       day_id
                  from (select mdn,
                               other_party,
                               call_type,
                               biz_type,
                               imsi,
                               start_date,
                               prov_id,
                               day_id
                          from dwi_integ.dwi_evt_bill_oth_wash_mdr_d
                         where prov_id = '${PROV_ID}'
                           and day_id = '${DAY_ID}') temp1
                  left join (select tt1.mobile_num,
                                   tt1.corporation_name,
                                   tt1.city_name,
                                   tt2.city_id
                              from dwi_integ.dwi_evt_bill_mbl_num_att_d tt1
                              left join dim.dim_city tt2
                                on tt1.city_name = tt2.city_name) temp2
                    on substr(temp1.mdn, 1, 7) = temp2.mobile_num
                  left join (select tt1.mobile_num,
                                   tt1.corporation_name,
                                   tt1.city_name,
                                   tt2.city_id
                              from dwi_integ.dwi_evt_bill_mbl_num_att_d tt1
                              left join dim.dim_city tt2
                                on tt1.city_name = tt2.city_name) temp3
                    on substr(temp1.other_party, 1, 7) = temp3.mobile_num
                union all
                select '11' biz_type,
                       mdn,
                       '' city_id,
                       '' corporation_id,
                       other_party,
                       '' other_city_id,
                       '' other_corporation,
                       '' call_type,
                       start_time,
                       imsi imsi,
                       calling_visit_code calling_home_code,
                       '个人' mdr_type,
                       '' biz_id,
                       '' biz_name,
                       'WCDR' source_type,
                       '4' priority_level,
                       prov_id,
                       day_id
                  from dwi_m.dwi_evt_bill_oth_wcdr_mdr_msk_d
                 where prov_id = '${PROV_ID}'
                   and day_id = '${DAY_ID}'
                union all
                select '11' biz_type,
                       mdn,
                       '' city_id,
                       '' corporation_id,
                       other_party,
                       '' other_city_id,
                       '' other_corporation,
                       '' call_type,
                       start_time,
                       imsi,
                       calling_visit_code calling_home_code,
                       '个人' mdr_type,
                       '' biz_id,
                       '' biz_name,
                       'OIDD' source_type,
                       '1' priority_level,
                       prov_id,
                       day_id
                  from dwi_m.dwi_evt_bill_oth_oidd_mdr_msk_d
                 where prov_id = '${PROV_ID}'
                   and day_id = '${DAY_ID}'
                union all
                select '11' biz_type,
                       mdn,
                       Biz_area_code city_id,
                       '' corporation_id,
                       other_party,
                       '' other_city_id,
                       '' other_corporation,
                       call_type,
                       start_date start_time,
                       '' imsi,
                       Biz_area_code calling_home_code,
                       '企业' mdr_type,
                       biz_id,
                       Biz_cust_name biz_name,
                       'INDSMS' source_type,
                       '3' priority_level,
                       prov_id,
                       day_id
                          from dwi_m.dwi_evt_bill_oth_duty_mdr_msk_d
                         where prov_id = '${PROV_ID}'
                           and day_id = '${DAY_ID}') t1) t2
 where t2.rn = 1;"

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

#!/bin/bash
#***********************************************************************************
# **  文件名称: dwi_res_serv_trmnl_user_info_m.sh
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
LOGNAME="dwi_res_serv_trmnl_user_info_m.sh"

#用户名、队列名
USERNAME="dwi_integ"
# 队列名 mt1  -   mt8   
#QUEUENAME="root.bigdata.motl.mt1"
QUEUENAME="root.test.test15"

##############SQL变量############################################################################
#ods省份编码
PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
#PROVS=812
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
SQL="add jar $baseDirForScriptSelf/md.jar;
CREATE TEMPORARY FUNCTION md5 AS 'com.udf.Md5';
insert overwrite table dwi_integ.dwi_res_serv_trmnl_user_info_m partition
  (prov_id, month_id)
  select t.mdn,
         t.prov_name,
         t.city_name city_id,
         t.change_cnt,
         t.USER_CHANGE_SUMDUR change_dur,
         t.CHANGE_CYCLE,
         t.PRE1_TRMNL_BRAND pre_trmnl_brand_1,
         t.PRE1_TRMNL_MODEL pre_trmnl_model_1,
         t.PRE1_TRMNL_USE_DATE pre_trmnl_use_date_1,
         t.pre1_use_date pre_use_date_1,
         t.PRE2_TRMNL_BRAND pre_trmnl_brand_2,
         t.PRE2_TRMNL_MODEL pre_trmnl_model_2,
         t.PRE2_TRMNL_USE_DATE pre_trmnl_use_date_2,
         t.pre2_use_date pre_use_date_2,
         t.loyal_brand,
         t.pre3_trmnl_brand pre_trmnl_brand_3,
         t.pre3_trmnl_model pre_trmnl_model_3,
         t.pre3_trmnl_use_date pre_trmnl_use_date_3,
         t.pre3_use_date pre_use_date_3,
         t.trmnl_price,
         t.pre1_trmnl_price pre_trmnl_price_1,
         t.pre2_trmnl_price pre_trmnl_price_2,
         t.pre3_trmnl_price pre_trmnl_price_3,
         t.avg_price,
         t.trmnl_brand,
         t.trmnl_model,
         t.use_date,
         (case
           when t.unuse_trmnl_dur > 0 and t.unuse_trmnl_dur <= 6 then
            '(0,6]'
           when t.unuse_trmnl_dur > 6 and t.unuse_trmnl_dur <= 12 then
            '(6,12]'
           when t.unuse_trmnl_dur > 12 and t.unuse_trmnl_dur <= 18 then
            '(12,18]'
           when t.unuse_trmnl_dur > 18 and t.unuse_trmnl_dur <= 24 then
            '(18,24]'
           when t.unuse_trmnl_dur > 24 and t.unuse_trmnl_dur <= 36 then
            '(24,36]'
           when t.unuse_trmnl_dur > 36 then
            '(36,∞)'
           else
            '0'
         end) unuse_trmnl_dur,
         t.prov_id,
         t.month_id
    from (select t1.mdn,
                 t1.prov_name,
                 t1.city_name,
                 t1.change_cnt,
                 round(t1.CHANGE_CYCLE, 2) CHANGE_CYCLE,
                 t1.CHANGE_CYCLE * t1.change_cnt USER_CHANGE_SUMDUR,
                 t1.PRE1_TRMNL_BRAND,
                 t1.PRE1_TRMNL_MODEL,
                 t1.PRE1_TRMNL_USE_DATE,
                 t1.pre1_use_date,
                 t1.PRE2_TRMNL_BRAND,
                 t1.PRE2_TRMNL_MODEL,
                 t1.PRE2_TRMNL_USE_DATE,
                 t1.pre2_use_date,
                 t2.loyal_brand,
                 t1.pre3_trmnl_brand,
                 t1.pre3_trmnl_model,
                 t1.pre3_trmnl_use_date,
                 t1.pre3_use_date,
                 t1.trmnl_price,
                 t1.pre1_trmnl_price,
                 t1.pre2_trmnl_price,
                 t1.pre3_trmnl_price,
                 t1.avg_price,
                 t1.trmnl_brand,
                 t1.trmnl_model,
                 (case
                   when tt1.meid is null and tt2.meid is null and
                        tt3.meid is null then
                    ceil((t1.PRE1_TRMNL_USE_DATE + t1.PRE2_TRMNL_USE_DATE +
                         t1.PRE3_TRMNL_USE_DATE) / 30)
                   when tt1.meid is null and tt2.meid is null and
                        tt3.meid is not null then
                    ceil((t1.PRE1_TRMNL_USE_DATE + t1.PRE2_TRMNL_USE_DATE) / 30)
                   when tt1.meid is null and tt2.meid is not null and
                        tt3.meid is null then
                    ceil((t1.PRE1_TRMNL_USE_DATE + t1.PRE3_TRMNL_USE_DATE) / 30)
                   when tt1.meid is not null and tt2.meid is null and
                        tt3.meid is null then
                    ceil((t1.PRE2_TRMNL_USE_DATE + t1.PRE3_TRMNL_USE_DATE) / 30)
                   when tt1.meid is null and tt2.meid is not null and
                        tt3.meid is not null then
                    ceil(t1.PRE1_TRMNL_USE_DATE / 30)
                   when tt1.meid is not null and tt2.meid is null and
                        tt3.meid is not null then
                    ceil(t1.PRE2_TRMNL_USE_DATE / 30)
                   when tt1.meid is not null and tt2.meid is not null and
                        tt3.meid is null then
                    ceil(t1.PRE3_TRMNL_USE_DATE / 30)
                   else
                    0
                 end) unuse_trmnl_dur,
                 t1.use_date,
                 t1.prov_id,
                 t1.month_id
            from (select mdn,
                         prov_name,
                         city_name,
                         change_cnt,
                         CHANGE_CYCLE,
                         PRE1_TRMNL_BRAND,
                         PRE1_TRMNL_MODEL,
                         case
                           when USE_DATE <> '-1' and PRE1_USE_DATE <> '-1' then
                            datediff(USE_DATE, pre1_use_date)
                           else
                            '-1'
                         end PRE1_TRMNL_USE_DATE,
                         pre1_use_date,
                         PRE2_TRMNL_BRAND,
                         PRE2_TRMNL_MODEL,
                         prov_id,
                         month_id,
                         case
                           when PRE1_USE_DATE <> '-1' and
                                PRE2_USE_DATE <> '-1' then
                            datediff(PRE1_USE_DATE, PRE2_USE_DATE)
                           else
                            '-1'
                         end PRE2_TRMNL_USE_DATE,
                         pre2_use_date,
                         pre3_trmnl_brand,
                         pre3_trmnl_model,
                         case
                           when PRE2_USE_DATE <> '-1' and
                                pre3_use_date <> '-1' then
                            datediff(PRE2_USE_DATE, pre3_use_date)
                           else
                            '-1'
                         end pre3_trmnl_use_date,
                         pre3_use_date,
                         trmnl_price,
                         pre1_trmnl_price,
                         pre2_trmnl_price,
                         pre3_trmnl_price,
                         ((if(trmnl_price != '-1', trmnl_price, 0) +
                         if(pre1_trmnl_price != '-1', pre1_trmnl_price, 0) +
                         if(pre2_trmnl_price != '-1', pre2_trmnl_price, 0) +
                         if(pre3_trmnl_price != '-1', pre3_trmnl_price, 0)) /
                         (if(trmnl_price != '-1', 1, 0) +
                         if(pre1_trmnl_price != '-1', 1, 0) +
                         if(pre2_trmnl_price != '-1', 1, 0) +
                         if(pre3_trmnl_price != '-1', 1, 0))) avg_price,
                         trmnl_brand,
                         trmnl_model,
                         use_date,
                         pre1_meid,
                         pre2_meid,
                         pre3_meid
                    from dwi_integ.dwi_res_serv_user_trmnl_info_m
                   where month_id = '${MONTH_ID}'
                     and prov_id = '${PROV_ID}') t1
            left join (select meid
                        from dwi_integ.dwi_res_serv_user_trmnl_info_m
                       where month_id = '${MONTH_ID}'
                         and prov_id = '${PROV_ID}') tt1
              on t1.pre1_meid = tt1.meid
            left join (select meid
                        from dwi_integ.dwi_res_serv_user_trmnl_info_m
                       where month_id = '${MONTH_ID}'
                         and prov_id = '${PROV_ID}') tt2
              on t1.pre2_meid = tt2.meid
            left join (select meid
                        from dwi_integ.dwi_res_serv_user_trmnl_info_m
                       where month_id = '${MONTH_ID}'
                         and prov_id = '${PROV_ID}') tt3
              on t1.pre3_meid = tt3.meid
           inner join (select mdn, trmnl_brand loyal_brand
                        from (select mdn,
                                     trmnl_brand,
                                     count(*) brand_use_times,
                                     max(regst_date) rgst_dt,
                                     row_number() over(distribute by mdn sort by count(*), max(regst_date) desc) loyal_brand_rn
                                from dwi_i_mid.DWI_RES_SERV_RGST_TRMNL_M_MID
                               where month_id = '${MONTH_ID}'
                                 and prov_id = '${PROV_ID}'
                                 and flag_new > 0
                               group by mdn, trmnl_brand) tt
                       where tt.loyal_brand_rn = 1) t2
              on t1.mdn = t2.mdn) t;"

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
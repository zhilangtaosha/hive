#!/bin/bash
#***********************************************************************************
# **  文件名称: dal_term_trmnl_main_slot_compet_m.sh
# **  创建日期: 20180503
# **  编写人员: yy
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
# 2018-5-3 yangyong
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
LOGNAME="dal_term_trmnl_main_slot_compet_m"

#用户名、队列名
USERNAME="dwi_m"
QUEUENAME="root.test.test15"

##############SQL变量############################################################################
#ods省份编码
#PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
PROVS=811
DATES=20170301
#开起并发参数,一般单独执行的sql,不建议开起并发参数
#concurrency=8
#################################################################################################
#报错发送信息,联系邮箱#邮件组
ARREMAIL=

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
set hive.hadoop.supports.splittable.combineinputformat=true;"
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
    PRE2_MONTH_ID=$(date -d "${MONTH_ID}01  -2 month" +%Y%m)
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
SQL="INSERT OVERWRITE TABLE dal_term.dal_term_trmnl_main_slot_compet_m partition
  (month_id)
  SELECT '${MONTH_ID}' mon,
         count(distinct(case
                          when data.prod_inst_status in (100000, 120000) then
                           mdn
                        end)), --在网用户
         count(distinct(case
                          when data.prod_inst_status in (100000, 120000) then
                           meid
                        end)), --在网终端
         (case
           when data.prov_name in ('广东', '广西', '福建', '海南') then
            '华南'
           when data.prov_name in ('江苏', '浙江', '上海', '台湾') then
            '华东'
           when data.prov_name in ('河南', '湖北', '安徽', '湖南', '江西') then
            '华中'
           when data.prov_name in ('西藏', '云南', '四川', '贵州', '重庆') then
            '西南'
           when data.prov_name in ('新疆', '甘肃', '青海', '宁夏', '陕西') then
            '西北'
           when data.prov_name in ('黑龙江', '吉林', '辽宁') then
            '东北'
           when data.prov_name in
                ('内蒙古', '山西', '河北', '北京', '天津', '山东') then
            '华北'
         end) redion_name,
         data.prov_name,
         data.city_name,
         data.trmnl_brand,
         data.trmnl_model,
         data.standby_type,
         data.flow,
         data.fee,
         data.is_mix,
         data.standby_type slot_type,
         data.is_one_to_more,
         data.if_one_card_slot,
         data.if_main_card_slot,
         data.if_vice_card_slot,
         ${MONTH_ID} month_id
    FROM (SELECT overall.prov_name,
                 overall.city_name,
                 overall.mdn,
                 overall.meid,
                 overall.trmnl_brand,
                 overall.trmnl_model,
                 overall.flow,
                 overall.fee,
                 overall.is_mix,
                 overall.prod_inst_status,
                 devinfo.standby_type,
                 card.if_one_card_slot,
                 card.if_main_card_slot,
                 card.if_vice_card_slot,
                 IF((devinfo.standby_type in ('双网双待','双卡','双卡单待') and
                    card.if_one_card_slot = '否'),
                    '是',
                    '否') is_one_to_more --是否一机多卡
            FROM (SELECT mdn,
                         prov_name,
                         city_name,
                         meid,
                         trmnl_brand,
                         trmnl_model,
                         -- 0、(0,50]；(50,100]；(100M,300M]；(300M,600M]；(600M,1G]；(1G,2G]；(2G,4G]；(4G,8G]；(8G,∞)
                         (case
                           when flow / (1024 * 1024) > 0 and
                                flow / (1024 * 1024) <= 50 then
                            '(0,50]'
                           when flow / (1024 * 1024) > 50 and
                                flow / (1024 * 1024) <= 100 then
                            '(50,100]'
                           when flow / (1024 * 1024) > 100 and
                                flow / (1024 * 1024) <= 300 then
                            '(100M,300M]'
                           when flow / (1024 * 1024) > 300 and
                                flow / (1024 * 1024) <= 600 then
                            '(300M,600M]'
                           when flow / (1024 * 1024) > 600 and
                                flow / (1024 * 1024) <= 1024 then
                            '(600M,1G]'
                           when flow / (1024 * 1024) > 1024 and
                                flow / (1024 * 1024) <= 1024 * 2 then
                            '(1G,2G]'
                           when flow / (1024 * 1024) > 1024 * 2 and
                                flow / (1024 * 1024) <= 1024 * 4 then
                            '(2G,4G]'
                           when flow / (1024 * 1024) > 1024 * 4 and
                                flow / (1024 * 1024) <= 1024 * 8 then
                            '(4G,8G]'
                           when flow / (1024 * 1024) > 1024 * 8 then
                            '(8G,∞)'
                           else
                            '0'
                         end) flow,
                         -- (0,50]；(50,100]； (100,150]；(150,200]；(200,300]；(300,500]；(500,1000]；(1000,∞)
                         (case
                           when fee > 0 and fee <= 50 then
                            '(0,50]'
                           when fee > 50 and fee <= 100 then
                            '(50,100]'
                           when fee > 100 and fee <= 150 then
                            '(100,150]'
                           when fee > 150 and fee <= 200 then
                            '(150,200]'
                           when fee > 200 and fee <= 300 then
                            '(200,300]'
                           when fee > 300 and fee <= 500 then
                            '(300,500]'
                           when fee > 500 and fee <= 1000 then
                            '(500,1000]'
                           when fee > 1000 then
                            '(1000,∞)'
                           else
                            ''
                         end) fee,
                         is_mix,
                         prod_inst_status
                    FROM dws_m.dws_wdtb_user_overall_info_msk_m
                   WHERE month_id = '${MONTH_ID}') overall
            LEFT JOIN (SELECT *
                        FROM dwi_m.dwi_res_serv_trmnl_card_slot_msk_m
                       WHERE month_id = '${MONTH_ID}') card
              ON overall.mdn = card.mdn
            LEFT JOIN dim.dim_devinfo devinfo
              ON overall.trmnl_brand = devinfo.model_brand
             and overall.trmnl_model = devinfo.dm_model) data
   group by data.prov_name,
            data.city_name,
            data.trmnl_brand,
            data.trmnl_model,
            data.standby_type,
            data.flow,
            data.fee,
            data.is_mix,
            data.is_one_to_more,
            data.if_one_card_slot,
            data.if_main_card_slot,
            data.if_vice_card_slot;"

RunScript "${SQL}"
#and accs_nbr rlike '^1[0-9]{10}$'

#执行mr
#SQL="hadoop jar sort.jar /apps/hive/warehouse/st001.db/ztj_test3/ /apps/hive/warehouse/st001.db/ztj_test2/ root.bigdata.motl.mt1"
#RunMr "${SQL}"

#计算单表条数
#value=0
#SQL="
#select count(*) from odsbstl.d_area_code where crm_code='811';
#"
#value "${SQL}"
##可以根据返回值进行表的条数，空，波动性判断
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
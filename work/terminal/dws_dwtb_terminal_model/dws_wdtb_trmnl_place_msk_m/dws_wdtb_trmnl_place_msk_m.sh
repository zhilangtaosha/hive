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
# .修改时间
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
LOGNAME="dws_wdtb_trmnl_place_msk_m"

#用户名、队列名
USERNAME="dws_m"
# 队列名 mt1  -   mt8   
#QUEUENAME="root.bigdata.motl.mt1"
QUEUENAME="root.test.test15"

##############SQL变量############################################################################
#ods省份编码
PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
#PROVS=815
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
	PRE_FIVE_MONTH=$(date -d "${MONTH_ID}01 -5 month" +%Y%m)
	PRE_FOUR_MONTH=$(date -d "${MONTH_ID}01 -4 month" +%Y%m)
	PRE_THREE_MONTH=$(date -d "${MONTH_ID}01 -3 month" +%Y%m)
	PRE_TWO_MONTH=$(date -d "${MONTH_ID}01 -2 month" +%Y%m)
	PRE_ONE_MONTH=$(date -d "${MONTH_ID}01 -1 month" +%Y%m)
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
SQL="insert overwrite table dws_m.dws_wdtb_trmnl_place_msk_m partition(prov_id,month_id) 
select tt1.mdn,
       tt1.prov_name,
       tt1.city_name             latn_name,
       tt1.meid,
       tt1.imsi,
       tt1.prod_inst_status      user_status_id,
       tt1.trmnl_brand,
       tt1.redion_name,
       tt3.tran_residence_county residence_id,
       tt3.tran_workplace_county workplace_id,
       tt2.active_range,
       tt1.trmnl_model           trmnl_mdl,
       tt1.prov_id,
       tt1.month_id
  from (select mdn,
               prov_name,
               city_name,
               meid,
               imsi,
               prod_inst_status,
               trmnl_brand,
               trmnl_model,
               (case
                 when prov_name in ('广东', '广西', '福建', '海南', '澳门') then
                  '华南'
                 when prov_name in ('江苏', '浙江', '上海', '台湾') then
                  '华东'
                 when prov_name in ('河南', '湖北', '安徽', '湖南', '江西') then
                  '华中'
                 when prov_name in ('西藏', '云南', '四川', '贵州', '重庆') then
                  '西南'
                 when prov_name in
                      ('内蒙古', '山西', '河北', '北京', '天津', '山东') then
                  '华北'
                 when prov_name in ('新疆', '甘肃', '青海', '宁夏', '陕西') then
                  '西北'
                 when prov_name in ('黑龙江', '吉林', '辽宁') then
                  '东北'
               end) redion_name, --大区
               prov_id,
               month_id
          from dws_m.dws_wdtb_user_overall_info_msk_m
         where month_id = '${MONTH_ID}'
           and prov_id = '${PROV_ID}') tt1
  left join (select mdn,
                    (case
                      when t2.prov_cnt = 1 and t2.city_cnt = 1 and
                           t2.county_cnt = 1 then
                       '同城单一区域'
                      when t2.prov_cnt = 1 and t2.city_cnt = 1 and
                           t2.county_cnt > 1 then
                       '同城跨区域'
                      when t2.prov_cnt = 1 and t2.city_cnt = 2 then
                       '同省跨1城市'
                      when t2.prov_cnt = 1 and t2.city_cnt > 2 then
                       '同省跨2个以上城市'
                      when t2.prov_cnt = 2 and t2.city_cnt = 2 then
                       '国内跨1省1城市'
                      when t2.prov_cnt = 2 and t2.city_cnt > 2 then
                       '国内跨1省2个以上城市'
                      when t2.prov_cnt > 2 then
                       '国内跨多省多城市'
                    end) active_range
               from (select mdn,
                            count(distinct substr(t1.city_name, 1, 3)) prov_cnt,
                            count(distinct t1.city_name) city_cnt,
                            count(distinct t1.county_name) county_cnt
                       from (select mdn,
                                    tran_residence_city   city_name,
                                    tran_residence_county county_name
                               from dws_m.dws_wdtb_work_resi_transit_msk_m
                              where tran_residence_city is not null
                                and tran_residence_city <> ''
                                and tran_residence_county is not null
                                and tran_residence_county <> ''
                                and month_id in
                                    ('${MONTH_ID}',
                                     '${PRE_FIVE_MONTH}',
                                     '${PRE_FOUR_MONTH}',
                                     '${PRE_THREE_MONTH}',
                                     '${PRE_TWO_MONTH}',
                                     '${PRE_ONE_MONTH}')
                             union all
                             select mdn,
                                    tran_workplace_city   city_name,
                                    tran_workplace_county county_name
                               from dws_m.dws_wdtb_work_resi_transit_msk_m
                              where tran_workplace_city is not null
                                and tran_workplace_city <> ''
                                and tran_workplace_county is not null
                                and tran_workplace_county <> ''
                                and month_id in
                                    ('${MONTH_ID}',
                                     '${PRE_FIVE_MONTH}',
                                     '${PRE_FOUR_MONTH}',
                                     '${PRE_THREE_MONTH}',
                                     '${PRE_TWO_MONTH}',
                                     '${PRE_ONE_MONTH}')) t1
                      group by t1.mdn) t2) tt2
    on tt1.mdn = tt2.mdn
  left join (select mdn, tran_residence_county, tran_workplace_county
               from dws_m.dws_wdtb_work_resi_transit_msk_m
              where month_id = '${MONTH_ID}') tt3
    on tt1.mdn = tt3.mdn;"

RunScript "${SQL}"

#Mergefile "${USERNAME}" "dm_ind_req_zhjt_4guser_m" "where month_id = '${MONTH_ID}'"


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
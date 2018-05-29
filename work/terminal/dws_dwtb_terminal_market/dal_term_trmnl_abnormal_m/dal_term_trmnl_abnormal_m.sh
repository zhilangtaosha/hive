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
LOGNAME="dal_term_trmnl_abnormal_m"

#用户名、队列名
USERNAME="dal_term"
# 队列名 mt1  -   mt8   
#QUEUENAME="root.bigdata.motl.mt1"
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
	PRE_ONE_MONTH=$(date -d "${MONTH_ID}01  -1 month" +%Y%m)
	PRE_TWO_MONTH=$(date -d "${MONTH_ID}01  -2 month" +%Y%m)
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
SQL="insert overwrite table dal_term.dal_term_trmnl_abnormal_m partition (month_id)
select t5.add_trmnl_cnt,
       t5.add_trmnl_user_cnt,
       t5.redion_name,
       t5.prov_name,
       t5.city_name,
       t5.trmnl_brand,
       t5.trmnl_mdl,
       t6.listed_price,
       t6.function_orientation,
       t6.ios,
       t6.standby_type,
       t6.dm_net_frequency,
       t6.trmnl_type,
       t6.core_num,
       t6.standard_type,
       t5.month_activ_days,
       t5.nextmonth_activ_days,
       t5.plug_card_cnt,
       t5.trmnl_many_user_cnt,
       t5.user_many_trmnl_cnt,
       t5.month_id
  from (select count(distinct(case
                                when t4.is_first_regst = 1 and
                                     substr(regexp_replace(t4.regst_date, '-', ''), 1, 6) =
                                     '${MONTH_ID}' then
                                 meid
                              end)) add_trmnl_cnt, --是否首次入网字段值不确定
               count(distinct(case
                                when t4.is_first_regst = 1 and
                                     substr(regexp_replace(t4.regst_date, '-', ''), 1, 6) =
                                     '${MONTH_ID}' then
                                 mdn
                              end)) add_trmnl_user_cnt,
               t4.redion_name,
               t4.prov_name,
               t4.city_name,
               t4.trmnl_brand,
               t4.trmnl_mdl,
               t4.month_activ_days,
               t4.nextmonth_activ_days,
               t4.plug_card_cnt,
               count(distinct meid) trmnl_many_user_cnt, --一用户多终端
               count(distinct mdn) user_many_trmnl_cnt, --一终端多用户
               t4.month_id
          from (select t1.mdn,
                       t1.meid,
                       t1.is_first_regst,
                       t1.regst_date,
                       (case
                         when t1.prov_name in ('广东', '广西', '福建', '海南') then
                          '华南'
                         when t1.prov_name in ('江苏', '浙江', '上海', '台湾') then
                          '华东'
                         when t1.prov_name in
                              ('河南', '湖北', '安徽', '湖南', '江西') then
                          '华中'
                         when t1.prov_name in
                              ('西藏', '云南', '四川', '贵州', '重庆') then
                          '西南'
                         when t1.prov_name in ('内蒙古',
                                               '山西',
                                               '河北',
                                               '北京',
                                               '天津',
                                               '山东') then
                          '华北'
                         when t1.prov_name in
                              ('新疆', '甘肃', '青海', '宁夏', '陕西') then
                          '西北'
                         when t1.prov_name in ('黑龙江', '吉林', '辽宁') then
                          '东北'
                       end) redion_name, --大区
                       t1.prov_name,
                       t1.city_name,
                       t1.trmnl_brand,
                       t1.trmnl_model trmnl_mdl,
                       t1.month_activ_days,
                       t1.nextmonth_activ_days,
                       t1.plug_card_cnt,
                       t1.month_id
                  from (select tt1.mdn,
                               tt1.meid,
                               tt1.prov_name,
                               tt1.city_name,
                               tt1.trmnl_brand,
                               tt1.trmnl_model,
                               tt1.month_activ_days,
                               tt1.nextmonth_activ_days,
                               tt1.open_date,
                               tt1.regst_date,
                               tt2.plug_card_cnt,
                               tt2.is_first_regst,
                               tt1.month_id
                          from (select mdn,
                                       meid,
                                       prov_name,
                                       city_name,
                                       trmnl_brand,
                                       trmnl_model,
                                       month_activ_days,
                                       nextmonth_activ_days,
                                       open_date, --开户时间取自入网时间
                                       regst_date,
                                       month_id
                                  from dws_m.dws_wdtb_user_overall_info_msk_m
                                 where month_id = '${MONTH_ID}') tt1
                          left join (select mdn,
                                           meid,
                                           is_first_regst,
                                           (case
                                             when plug_card_cnt >= 1 and
                                                  plug_card_cnt <= 2 then
                                              '[1,2]'
                                             when plug_card_cnt >= 3 and
                                                  plug_card_cnt <= 10 then
                                              '[3,10]'
                                             when plug_card_cnt >= 11 and
                                                  plug_card_cnt <= 20 then
                                              '[11,20]'
                                             when plug_card_cnt >= 21 and
                                                  plug_card_cnt <= 50 then
                                              '[21,50]'
											  when plug_card_cnt > 50 then  
											  '(50,∞)'
                                             else
                                              '0'
                                           end) plug_card_cnt --插拔卡次数
                                      from dws_m.dws_wdtb_trmnl_first_regst_stack_msk_m where month_id = '${MONTH_ID}') tt2
                            on tt1.mdn = tt2.mdn
                           and tt1.meid = tt2.meid) t1
                  left join (select temp1.mdn
                              from (select mdn, count(distinct meid) meid_cnt
                                      from dws_m.dws_wdtb_user_overall_info_msk_m
                                     where month_id in
                                           ('${PRE_ONE_MONTH}',
                                            '${PRE_TWO_MONTH}',
                                            '${MONTH_ID}')
                                     group by mdn) temp1
                             where temp1.meid_cnt > 1) t2 --选出一用户多终端的数据
                    on t1.mdn = t2.mdn
                  left join (select meid, mdn_cnt
                              from (select meid, count(distinct mdn) mdn_cnt
                                      from dws_m.dws_wdtb_user_overall_info_msk_m
                                     where month_id in
                                           ('${PRE_ONE_MONTH}',
                                            '${PRE_TWO_MONTH}',
                                            '${MONTH_ID}')
                                     group by meid) temp1
                             where temp1.mdn_cnt > 1) t3 --选出一终端多用户的数据
                    on t1.meid = t3.meid) t4
         group by t4.redion_name,
                  t4.prov_name,
                  t4.city_name,
                  t4.trmnl_brand,
                  t4.trmnl_mdl,
                  t4.month_activ_days,
                  t4.nextmonth_activ_days,
                  t4.plug_card_cnt,
                  t4.month_id) t5
  left join (select (case
                      when listed_price > 0 and listed_price < 600 then
                       '(0,600)'
                      when listed_price >= 600 and listed_price < 1000 then
                       '[600,1000)'
                      when listed_price >= 1000 and listed_price < 1500 then
                       '[1000,1500)'
                      when listed_price >= 1500 and listed_price < 2500 then
                       '[1500,2500)'
                      when listed_price >= 2500 and listed_price < 4000 then
                       '[2500,4000)'
                      when listed_price >= 4000 then
                       '[4000,∞)'
                      else
                       ''
                    end) listed_price,
                    '' function_orientation, --功能定位字段无法取值，置空
                    ios,
                    standby_type,
                    dm_net_frequency, --网络频段
                    model_type trmnl_type, --终端类型
                    core_num, --基带芯片
                    dm_net_type standard_type, --网络制式
                    dm_model
               from dim.dim_devinfo) t6
    on t5.trmnl_mdl = t6.dm_model;"

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
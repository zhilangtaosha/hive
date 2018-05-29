#!/bin/bash
#***********************************************************************************
# **  文件名称: dwi_integ.dwi_sev_user_creditrent_monitor_m.sh
# **  创建日期: 2017年10月9日
# **  编写人员: qiaoyin
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
#  20180412   yangyong
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
LOGNAME="dwi_integ.dwi_sev_user_creditrent_monitor_m"

#用户名、队列名
USERNAME="dwi_integ"
QUEUENAME="root.test.test15"

##############SQL变量############################################################################
#ods省份编码
#PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
#PROVS=836,837,845,861
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
if [ $# -eq 1 ];then
    if [ "$1"x != "dates"x ];then
            DATES=($1)
    fi
elif [ $# -eq 2 ];then
        DATES=($1)
        PROVS=($2)
else
    #默认上上个月
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
 PRE_MONTH_DAYID=${MONTH_ID}01
 NEXT_MONTH_ID=$(date -d "${MONTH_ID}01  1 month" +%Y%m)
 TWO_MONTH_BEFORE=$(date -d "${MONTH_ID}01 -2 month" +%Y%m)
 MONTH_LAST_DAY=$(date -d  "`date -d "${MONTH_ID}01 1 month" +%Y%m%d`  -1  day" +%Y%m%d)
 SEVEN_MONTH_BEFORE=$(date -d "${MONTH_ID}01 -6 month" +%Y%m)
 THREE_YEAR_BEFORE=$(date -d "${MONTH_ID}01 -36 month" +%Y%m)
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
START_DAY=${MONTH_ID}01
END_DAY=${NEXT_MONTH_ID}01
SQL="
INSERT OVERWRITE TABLE dwi_integ.dwi_sev_user_creditrent_monitor_m PARTITION(prov_id,MONTH_ID)
SELECT t1.mdn,
       t1.meid,
       t3.prov_name,
       t3.city_name,
       t1.call_dur,
       t1.ddr_dur,
       t2.eff_date,
       t2.exp_date,
       t3.online_dur,
       t4.num_count,
       t1.prov_id,
       '${MONTH_ID}' MONTH_ID
  from (SELECT mdn, meid, call_dur, online_dur ddr_dur,prov_id
          FROM dwi_integ.dwi_sev_user_creditrent_d
         WHERE substr(day_id,1, 6) = '${MONTH_ID}'
         group by mdn, meid, call_dur, online_dur,prov_id) t1
  left join (SELECT *
               FROM dwi_integ.dwi_sev_prod_offer_info_m
              WHERE month_id = '${PRE_MONTH_ID}'
                and is_lease in ('1','2','3')) t2
    on t1.mdn = t2.accs_nbr
  left join (select temp1.accs_nbr,
                    temp1.prod_inst_id,
                    temp1.open_date,
                    temp1.cert_nbr,
                    temp1.online_dur,
                    temp1.city_name,
                    temp1.prov_name
               from (select accs_nbr,
                            prod_inst_id,
                            open_date,
                            cert_nbr,
                            online_dur,
                            city_name,
                            prov_name,
                            row_number() over(partition by accs_nbr order by open_date desc) as rank
                       from dwi_integ.dwi_sev_user_main_info_m
                      where month_id = '${PRE_MONTH_ID}') temp1
              where temp1.rank = 1) t3
    on t1.mdn = t3.accs_nbr
  left join (select temp1.cert_nbr, count(temp1.accs_nbr) num_count
               from (select accs_nbr,
                            prod_inst_id,
                            open_date,
                            cert_nbr,
                            row_number() over(partition by accs_nbr order by open_date desc) as rank
                       from dwi_integ.dwi_sev_user_main_info_m
                      where month_id = '${PRE_MONTH_ID}'
                        and prod_inst_status in ('100000', '120000')) temp1
              where temp1.rank = 1
              group by temp1.cert_nbr) t4
    on t3.cert_nbr = t4.cert_nbr;"

RunScript "${SQL}"
#执行mr
#SQL="hadoop jar sort.jar /apps/hive/warehouse/st001.db/ztj_test3/ /apps/hive/warehouse/st001.db/ztj_test2/ mt1"
#RunMr "${SQL}"

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
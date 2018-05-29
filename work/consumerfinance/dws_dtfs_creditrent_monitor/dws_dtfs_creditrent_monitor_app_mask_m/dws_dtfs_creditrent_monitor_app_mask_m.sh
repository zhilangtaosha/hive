#!/bin/bash
#***********************************************************************************
# **  文件名称: dws_m.dws_dtfs_creditrent_monitor_app_mask_m.sh
# **  创建日期: 2018年4月13日
# **  编写人员: yangyong
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
LOGNAME="dws_m.dws_dtfs_creditrent_monitor_app_mask_m"

#用户名、队列名
USERNAME="dws_m"
#QUEUENAME="root.bigdata.motl.mt1"
QUEUENAME="root.test.test15"

##############SQL变量############################################################################
#ods省份编码
PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
#PROVS=1
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
    DATES=($(date -d "$(date +%Y%m)01 -2 month" +%Y%m))
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
SQL="add jar ${baseDirForScriptSelf}/md.jar;
CREATE TEMPORARY FUNCTION md5 AS 'com.udf.Md5';
INSERT OVERWRITE TABLE dws_m.dws_dtfs_creditrent_monitor_app_mask_m PARTITION(prov_id,MONTH_ID)
select md5(merge.mdn),
       merge.prov_name,
       merge.city_name,
       concat_ws(',', collect_set(merge.app_name)) as tags,
       merge.prov_id,
       merge.month_id
  from (SELECT CM.mdn,
               CM.prov_name,
               CM.city_name,
               t2.tag_id,
               t2.app_name,
               t2.all_cnt,
               cm.prov_id,
               cm.month_id,
               row_number() over(partition by CM.mdn order by t2.all_cnt desc) as rank
          from (SELECT *
                  FROM dwi_integ.dwi_sev_user_creditrent_monitor_m
                 WHERE month_id = '${MONTH_ID}'
                   and prov_id = '${PROV_ID}') CM
          left join (select temp1.*, temp2.app_name
                      from (SELECT *
                              FROM dws_m.dws_wdtb_mbl_dpi_tag_msk_m
                             WHERE month_id = '${PRE_MONTH_ID}'
                               and tag_flag = 'app'
                               and prov_id = '${PROV_ID}') temp1
                     inner join (select *
                                  from dim.dim_dpi_app_type
                                 where app_name in ('51人品',
                                                    'E速贷',
                                                    'E微贷',
                                                    '阿朋贷',
                                                    '帮贷宝',
                                                    '贷款导航',
                                                    '贷友帮',
                                                    '袋袋金',
                                                    '多贷贷',
                                                    '多美贷',
                                                    '合力贷',
                                                    '和信贷',
                                                    '互贷网',
                                                    '借贷网',
                                                    '借钱吧',
                                                    '开鑫贷',
                                                    '秒贷网',
                                                    '民信贷',
                                                    '朋友贷',
                                                    '品尚易贷',
                                                    '企额贷',
                                                    '千店贷',
                                                    '钱妈妈贷款',
                                                    '亲亲小贷',
                                                    '融360贷款',
                                                    '森昊好时贷',
                                                    '闪电借款',
                                                    '闪银',
                                                    '手机贷',
                                                    '搜易贷理财',
                                                    '速发贷',
                                                    '天下贷',
                                                    '团贷网',
                                                    '微贷网',
                                                    '我来贷大学生版',
                                                    '我要闪电贷',
                                                    '信财贷',
                                                    '信用贷款',
                                                    '宜贷网',
                                                    '宜人贷借款',
                                                    '易极贷',
                                                    '翼龙贷',
                                                    '在线贷',
                                                    '指尖贷',
                                                    '智投',
                                                    '中安信业贷款',
                                                    '分期乐')) temp2
                        on temp1.tag_id = temp2.app_id) t2
            on md5(cm.mdn) = t2.mdn) merge
 where merge.rank <= 10
 group by merge.mdn,
          merge.prov_name,
          merge.city_name,
          merge.prov_id,
          merge.month_id;"

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
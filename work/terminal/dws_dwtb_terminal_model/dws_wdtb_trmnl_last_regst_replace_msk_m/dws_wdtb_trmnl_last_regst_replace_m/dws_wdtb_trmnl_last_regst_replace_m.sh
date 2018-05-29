#!/bin/bash
#***********************************************************************************
# **  文件名称: dal_term_devinfo_m.sh
# **  创建日期: 2017年8月8日
# **  编写人员: hugd
# **  输入信息: 
# **  输出信息: 
# **
# **  功能描述: 
# **  处理过程:
#***********************************************************************************
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
LOGNAME="dws_wdtb_trmnl_last_regst_replace_msk_m"

#用户名、队列名
USERNAME="dws_integ"
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
SQL="add jar $baseDirForScriptSelf/md.jar;
CREATE TEMPORARY FUNCTION md5 AS 'com.udf.Md5';
INSERT OVERWRITE TABLE dws_integ.dws_wdtb_trmnl_last_regst_replace_m PARTITION (PROV_ID, MONTH_ID)
 SELECT A.MEID,
        A.MDN,
        A.IMSI,
        A.PROV_NAME,
        A.CITY_NAME,
        A.TRMNL_BRAND,
        A.TRMNL_MODEL,
        A.VERSION,
        A.REGST_DATE,
        B. EXCHANGE_CNT,
        B.TRMNL_ONLINE_DUR,
        B.TRMNL_RESIDENT_AVG_DUR,
        B.TRMNL_FIRST_RESIDENT_DUR,
        B.TRMNL_SECOND_RESIDENT_DUR,
        (case
          when C.brand_continu_dur >= 0 and C.brand_continu_dur <= 3 then
           '[0,3]'
          when C.brand_continu_dur >= 4 and C.brand_continu_dur <= 6 then
           '[4,6]'
          when C.brand_continu_dur >= 7 and C.brand_continu_dur <= 9 then
           '[7,9]'
          when C.brand_continu_dur >= 10 and C.brand_continu_dur <= 12 then
           '[10,12]'
          when C.brand_continu_dur >= 13 and C.brand_continu_dur <= 15 then
           '[13,15]'
          when C.brand_continu_dur >= 16 and C.brand_continu_dur <= 18 then
           '[16,18]'
          when C.brand_continu_dur >= 19 and C.brand_continu_dur <= 21 then
           '[19,21]'
          when C.brand_continu_dur >= 22 and C.brand_continu_dur <= 24 then
           '[22,24]'
          when C.brand_continu_dur >= 25 then
           '[25,∞]'
        end) brand_continu_dur,
        (C.brand_continu_dur / C.brand_continu_cnt) brand_continu_avg_dur,
        A.PROV_ID,
        A.MONTH_ID
   FROM (SELECT MEID,
                MDN,
                IMSI,
                PROV_NAME,
                CITY_NAME,
                TRMNL_BRAND,
                TRMNL_MODEL,
                VERSION,
                REGST_DATE,
                PROV_ID,
                MONTH_ID
           FROM DWI_INTEG.DWI_RES_SERV_TRMNL_REGST_SORT_M
          WHERE MONTH_ID = '${MONTH_ID}'
            and prov_id = '${PROV_ID}'
            AND MEID_DESC = 1
          GROUP BY MEID,
                   MDN,
                   IMSI,
                   PROV_NAME,
                   CITY_NAME,
                   TRMNL_BRAND,
                   TRMNL_MODEL,
                   VERSION,
                   REGST_DATE,
                   PROV_ID,
                   MONTH_ID) A
   LEFT JOIN (SELECT MEID,
                     EXCHANGE_CNT,
                     TRMNL_ONLINE_DUR,
                     TRMNL_RESIDENT_AVG_DUR,
                     TRMNL_FIRST_RESIDENT_DUR,
                     TRMNL_SECOND_RESIDENT_DUR
                FROM dws_i_mid.dws_dtfs_trmnl_info_m_mid
               WHERE MONTH_ID = '${MONTH_ID}'
                 and prov_id = '${PROV_ID}'
               GROUP BY MEID,
                        EXCHANGE_CNT,
                        TRMNL_ONLINE_DUR,
                        TRMNL_RESIDENT_AVG_DUR,
                        TRMNL_FIRST_RESIDENT_DUR,
                        TRMNL_SECOND_RESIDENT_DUR) B
     ON A.MEID = B.MEID
   LEFT join (select mdn,
                     (case
                       when trmnl_model = pre1_trmnl_model and
                            trmnl_model = pre2_trmnl_model then
                        ((year(from_unixtime(unix_timestamp('${MONTH_LAST_DAY}',
                                                            'yyyyMMdd'),
                                             'yyyy-MM-dd')) -
                        year(pre2_use_date)) * 12 +
                        (month(from_unixtime(unix_timestamp('${MONTH_LAST_DAY}',
                                                             'yyyyMMdd'),
                                              'yyyy-MM-dd'))) -
                        month(pre2_use_date))
                       when trmnl_model = pre1_trmnl_model then
                        ((year(from_unixtime(unix_timestamp('${MONTH_LAST_DAY}',
                                                            'yyyyMMdd'),
                                             'yyyy-MM-dd')) -
                        year(pre1_use_date)) * 12 +
                        (month(from_unixtime(unix_timestamp('${MONTH_LAST_DAY}',
                                                             'yyyyMMdd'),
                                              'yyyy-MM-dd'))) -
                        month(pre1_use_date))
                       else
                        ((year(from_unixtime(unix_timestamp('${MONTH_LAST_DAY}',
                                                            'yyyyMMdd'),
                                             'yyyy-MM-dd')) -
                        year(use_date)) * 12 +
                        (month(from_unixtime(unix_timestamp('${MONTH_LAST_DAY}',
                                                             'yyyyMMdd'),
                                              'yyyy-MM-dd'))) -
                        month(use_date))
                     end) brand_continu_dur,
                     (case
                       when trmnl_model = pre1_trmnl_model and
                            trmnl_model = pre2_trmnl_model then
                        3
                       when trmnl_model = pre1_trmnl_model then
                        2
                       else
                        1
                     end) brand_continu_cnt --品牌连续使用次数
                from dwi_integ.dwi_res_serv_user_trmnl_info_m
               where month_id = '${MONTH_ID}'
                 and prov_id = '${PROV_ID}') C
     ON A.mdn = C.mdn;"

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
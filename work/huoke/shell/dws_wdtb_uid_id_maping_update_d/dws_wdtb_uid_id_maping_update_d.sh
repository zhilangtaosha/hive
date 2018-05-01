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
LOGNAME="dws_wdtb_uid_id_maping_update_msk_d"

#用户名、队列名
USERNAME="dws_integ"
# 队列名 mt1  -   mt8   
QUEUENAME="root.bigdata.motl.mt1"


##############SQL变量############################################################################
#ods省份编码
PROVS=850,851,811,831,835,844,846,812,813,814,815,821,822,823,833,834,836,837,841,842,843,845,852,853,854,861,862,863,864,865,832
##############逗号分割###########################################################################
#PROVS=851
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

#移网用户UID月信息表  DAY_ID_MOB判断文件生成的第二天开始执行   测试的时候  注释掉  写成昨天固定值
#DAY_ID_MOB=$(hdfs dfs -ls /daas/motl/dwi/msk/evt/blog/dwi_evt_blog_dpi_present_quick_mobile_full_msk_m/prov_id=*/*|awk -F ' ' '{print $6}'|sort -r|head -n 1)
DAY_ID_MOB='2018-04-19'
DAY_ID_MBL=$(date -d "$DAY_ID_MOB +1 day" +%Y%m%d)

MONTH_ID_MBL=$(hive -e "show partitions dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m;"|awk -F '/' '{print $2}'|awk -F '=' '{print $2}'|sort -r|head -n 1)

echo $DAY_ID_MOB $DAY_ID_MBL $MONTH_ID_MBL  $DAY_ID

if [ "$DAY_ID_MBL"x = "$DAY_ID"x ];then
SQL="add jar ${baseDirForScriptSelf}/md.jar;
CREATE TEMPORARY FUNCTION md5 AS 'com.udf.Md5';
insert overwrite table dws_integ.dws_wdtb_uid_id_maping_update_d partition (id_source,prov_id,day_id)
select '' mdn,
       mdn mdn_md5,
       '' id,
       'MDN' id_type,
       mdn id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'IMSI' id_type,
       MD5(imsi) id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
   and (length(imsi) = 32 or length(imsi) = 64 or
       length(imsi) = 15 and substr(imsi, 1, 3) = 460 and imsi regexp
        '[0-9]{15}$')
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'MEID' id_type,
       md5(meid) id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
   and (length(meid) = 32 or length(meid) = 64 or
       length(meid) = 15 and
       (10 - (conv(substr(meid, 1, 1), 16, 10) +
       conv(substr(meid, 3, 1), 16, 10) +
       conv(substr(meid, 5, 1), 16, 10) +
       conv(substr(meid, 7, 1), 16, 10) +
       conv(substr(meid, 9, 1), 16, 10) +
       conv(substr(meid, 11, 1), 16, 10) +
       conv(substr(meid, 13, 1), 16, 10) +
       (conv(substr(meid, 2, 1), 16, 10) * 2)
        %10 + (conv(substr(meid, 4, 1), 16, 10) * 2)
        %10 + (conv(substr(meid, 6, 1), 16, 10) * 2)
        %10 + (conv(substr(meid, 8, 1), 16, 10) * 2)
        %10 + (conv(substr(meid, 10, 1), 16, 10) * 2)
        %10 + (conv(substr(meid, 12, 1), 16, 10) * 2)
        %10 + (conv(substr(meid, 14, 1), 16, 10) * 2)
        %10 + floor((conv(substr(meid, 2, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid, 4, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid, 6, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid, 8, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid, 10, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid, 12, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid, 14, 1), 16, 10) * 2) / 10)) %10) =
       conv(substr(meid, 15, 1), 16, 10))
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'IMEI' id_type,
       md5(imei) id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
   and (length(IMEI) = 32 or length(IMEI) = 64 or
       length(IMEI) = 15 and
       10 -
       (substr(IMEI, 1, 1) + substr(IMEI, 3, 1) + substr(IMEI, 5, 1) +
       substr(IMEI, 7, 1) + substr(IMEI, 9, 1) + substr(IMEI, 11, 1) +
       substr(IMEI, 13, 1) + (substr(IMEI, 2, 1) * 2)
        %10 + (substr(IMEI, 4, 1) * 2) %10 + (substr(IMEI, 6, 1) * 2)
        %10 + (substr(IMEI, 8, 1) * 2) %10 + (substr(IMEI, 10, 1) * 2)
        %10 + (substr(IMEI, 12, 1) * 2) %10 + (substr(IMEI, 14, 1) * 2)
        %10 + floor((substr(IMEI, 2, 1) * 2) / 10) +
        floor((substr(IMEI, 4, 1) * 2) / 10) +
        floor((substr(IMEI, 6, 1) * 2) / 10) +
        floor((substr(IMEI, 8, 1) * 2) / 10) +
        floor((substr(IMEI, 10, 1) * 2) / 10) +
        floor((substr(IMEI, 12, 1) * 2) / 10) +
        floor((substr(IMEI, 14, 1) * 2) / 10)) %10 = substr(IMEI, 15, 1))
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'IMSI' id_type,
       md5(sdkimsi) id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
   and length(sdkimsi) = 32
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'UDID' id_type,
       md5(sdkudid) id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
   and length(sdkudid) = 32
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       regexp_replace(mac, '-', ':') id,
       'MAC' id_type,
       md5(regexp_replace(mac, '-', ':')) id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
 and length(mac)>0 and mac<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       qq id,
       'QQ' id_type,
       qq id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
 and length(qq)>0 and qq<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       weibo id,
       'WEIBO' id_type,
       md5(weibo) id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
 and length(weibo)>0 and weibo<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       md5(e_mail) id,
       'Email' id_type,
       e_mail id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
 and length(e_mail)>0 and e_mail<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       md5(taobao_id) id,
       'Taobao_ID' id_type,
       taobao_id id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
 and length(taobao_id)>0 and taobao_id<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       md5(jd_id) id,
       'JD_ID' id_type,
       jd_id id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
 and length(jd_id)>0 and jd_id<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       md5(idfa) id,
       'IDFA' id_type,
       idfa id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
 and length(idfa)>0 and idfa<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       md5(androidid) id,
       'AndroidID' id_type,
       androidid id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
 and length(androidid)>0 and androidid<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}'
union all
select '' mdn,
       mdn mdn_md5,
       md5(weixinid) id,
       'WEIXINID' id_type,
       weixinid id_md5,
       '2' id_source,
       prov_id,
       month_id
  from dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m
 where mdn is not null and  mdn<>'null'
 and length(weixinid)>0 and weixinid<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID_MBL}';"

RunScript "${SQL}"
	fi


#C网用户UID月信息表
#测试的时候   注释掉  携程固定值
#DAY_ID_CNET=$(hdfs dfs -ls /daas/motl/dws/msk/wdtb/dws_wdtb_uid_info_mbl_msk_m/prov_id=*/*|awk -F ' ' '{print $6}'|sort -r|head -n 1)
DAY_ID_CNET='2018-04-19'

DAY_ID_C=$(date -d "$DAY_ID_CNET +1 day" +%Y%m%d)

MONTH_ID_C=$(hive -e "show partitions dws_m.dws_wdtb_uid_info_mbl_msk_m;"|awk -F '/' '{print $2}'|awk -F '=' '{print $2}'|sort -r|head -n 1)

if [ "$DAY_ID_C"x = "$DAY_ID"x ];then
SQL="add jar ${baseDirForScriptSelf}/md.jar;
CREATE TEMPORARY FUNCTION md5 AS 'com.udf.Md5';
insert overwrite table dws_integ.dws_wdtb_uid_id_maping_update_d partition (id_source,prov_id,day_id)
select '' mdn,
       mdn mdn_md5,
       '' id,
       'MDN' id_type,
       mdn id_md5,
       '0' id_source,
       prov_id,
       '$DAY_ID' day_id
  from dws_m.dws_wdtb_uid_info_mbl_msk_m
 where mdn is not null and  mdn<>'null'
   and prov_id = '${PROV_ID}'
   and month_id = '${MONTH_ID}'
 group by mdn,prov_id,month_id
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'IMSI' id_type,
       imsi id_md5,
       '0' id_source,
       prov_id,
       '$DAY_ID' day_id
from (select mdn,prov_id,imsi,
row_number() over(partition by mdn order by imsi_start_date desc) num
from dws_m.dws_wdtb_uid_info_mbl_msk_m
where mdn is not null and  mdn<>'null' 
and length(imsi)>0
and prov_id = '${PROV_ID}'
and month_id = '${MONTH_ID}'
) t where t.num=1
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'IMEI' id_type,
       IMEI id_md5,
       '0' id_source,
       prov_id,
       '$DAY_ID' day_id
from (select mdn,prov_id,IMEI,
row_number() over(partition by mdn order by IMEI_start_date desc) num
from dws_m.dws_wdtb_uid_info_mbl_msk_m
where mdn is not null and  mdn<>'null' 
and length(IMEI)>0
and prov_id = '${PROV_ID}'
and month_id = '${MONTH_ID}'
) t where t.num=1
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'IMEI' id_type,
       IMEI id_md5,
       '0' id_source,
       prov_id,
       '$DAY_ID' day_id
from (select mdn,prov_id,IMEI,
row_number() over(partition by mdn order by IMEI_start_date desc) num
from dws_m.dws_wdtb_uid_info_mbl_msk_m
where mdn is not null and  mdn<>'null' 
and length(IMEI)>0
and prov_id = '${PROV_ID}'
and month_id = '${MONTH_ID}'
) t where t.num=1
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'MEID' id_type,
       MEID id_md5,
       '0' id_source,
       prov_id,
       '$DAY_ID' day_id
from (select mdn,prov_id,MEID,
row_number() over(partition by mdn order by MEID_start_date desc) num
from dws_m.dws_wdtb_uid_info_mbl_msk_m
where mdn is not null and  mdn<>'null' 
and length(MEID)>0
and prov_id = '${PROV_ID}'
and month_id = '${MONTH_ID}'
) t where t.num=1
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'ESN' id_type,
       esn_code id_md5,
       '0' id_source,
       prov_id,
       '$DAY_ID' day_id
from (select mdn,prov_id,esn_code,
row_number() over(partition by mdn order by esn_code_start_date desc) num
from dws_m.dws_wdtb_uid_info_mbl_msk_m
where mdn is not null and  mdn<>'null' 
and length(esn_code)>0
and prov_id = '${PROV_ID}'
and month_id = '${MONTH_ID}'
) t where t.num=1
union all
select '' mdn,
       mdn mdn_md5,
       '' id,
       'ICCID' id_type,
       md5(ICCID) id_md5,
       '0' id_source,
       prov_id,
       '$DAY_ID' day_id
from (select mdn,prov_id,ICCID,
row_number() over(partition by mdn order by iccid_start_date desc) num
from dws_m.dws_wdtb_uid_info_mbl_msk_m
where mdn is not null and  mdn<>'null' 
and length(ICCID)>0
and prov_id = '${PROV_ID}'
and month_id = '${MONTH_ID}'
) t where t.num=1;"
	 
RunScript "${SQL}"
	fi
	
#固网DPI日表
SQL="add jar ${baseDirForScriptSelf}/md.jar;
CREATE TEMPORARY FUNCTION md5 AS 'com.udf.Md5';
insert overwrite table dws_integ.dws_wdtb_uid_id_maping_update_d partition (id_source,prov_id, day_id)
select mdn,
       md5(mdn) mdn_md5,
       mdn id,
       'MDN' id_type,
       md5(mdn) id_md5,
       '3' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_fix_out_d
 where mdn is not null and  mdn<>'null'
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       idfa id,
       'IDFA' id_type,
       md5(idfa) id_md5,
       '3' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_fix_out_d
 where mdn is not null and  mdn<>'null'
   and idfa regexp '[0-9a-z]{8}+-[0-9a-z]{4}+-[0-9a-z]{4}+-[0-9a-z]{4}+-[0-9a-z]{12}$'
   and idfa <> '00000000-0000-0000-000000000000'
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       mac id,
       'MAC' id_type,
       md5(Mac) id_md5,
       '3' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_fix_out_d
 where mdn is not null and  mdn<>'null'
   and Mac regexp '^([0-9A-Fa-f]{2}[\-|\:]?){5}[0-9A-Fa-f]{2}$'
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       tdid id,
       'TDID' id_type,
       md5(tdid) id_md5,
       '3' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_fix_out_d
 where mdn is not null and  mdn<>'null'
 and length(tdid)>0 and tdid<>'null'
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       devicetoken id,
       'DEVICETOKEN' id_type,
       md5(devicetoken) id_md5,
       '3' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_fix_out_d
 where mdn is not null and  mdn<>'null'
   and (devicetoken regexp '[0-9a-z]{64}$' and devicetoken regexp '[^0]{64}$')
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       (case
         when length(imsi) = 15 then
          imsi
         when length(imsi) = 32 then
          ''
       end) id,
       'IMSI' id_type,
       md5(imsi) id_md5,
       '3' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_fix_out_d
 where mdn is not null and  mdn<>'null'
   and (length(imsi) = 32 or length(imsi) = 64 or
       length(imsi) = 15 and substr(imsi, 1, 3) = 460 and imsi regexp '[0-9]{15}$')
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       (case
         when length(imei) = 15 then
          imei
         when length(imei) = 32 then
          ''
       end) id,
       'IMEI' id_type,
       md5(imei) id_md5,
       '3' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_fix_out_d
 where mdn is not null and  mdn<>'null'
   and (length(IMEI) = 32 or length(IMEI) = 64 or
       length(imei) = 15 and
       (10 -
       (substr(imei, 1, 1) + substr(IMEI, 3, 1) + substr(imei, 5, 1) +
       substr(imei, 7, 1) + substr(imei, 9, 1) + substr(imei, 11, 1) +
       substr(imei, 13, 1) + (substr(imei, 2, 1) * 2)
        %10 + (substr(IMEI, 4, 1) * 2) %10 + (substr(imei, 6, 1) * 2)
        %10 + (substr(IMEI, 8, 1) * 2) %10 + (substr(imei, 10, 1) * 2)
        %10 + (substr(IMEI, 12, 1) * 2) %10 + (substr(imei, 14, 1) * 2)
        %10 + floor((substr(IMEI, 2, 1) * 2) / 10) +
        floor((substr(IMEI, 4, 1) * 2) / 10) +
        floor((substr(IMEI, 6, 1) * 2) / 10) +
        floor((substr(IMEI, 8, 1) * 2) / 10) +
        floor((substr(IMEI, 10, 1) * 2) / 10) +
        floor((substr(IMEI, 12, 1) * 2) / 10) +
        floor((substr(IMEI, 14, 1) * 2) / 10)) %10) = substr(IMEI, 15, 1))
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}';"

RunScript "${SQL}"

#移网DPI日表
SQL="add jar ${baseDirForScriptSelf}/md.jar;
CREATE TEMPORARY FUNCTION md5 AS 'com.udf.Md5';
insert overwrite table dws_integ.dws_wdtb_uid_id_maping_update_d partition (id_source,prov_id, day_id)
select mdn,
       md5(mdn) mdn_md5,
       mdn id,
       'MDN' id_type,
       md5(mdn) id_md5,
       '4' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
 where mdn is not null and  mdn<>'null'
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       meid_cure id,
       (case
         when dpi_type = '3' then
          'MEID'
         when dpi_type = '4' then
          'IMEI'
       end) id_type,
       md5(meid_cure) id_md5,
       '4' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
 where mdn is not null and  mdn<>'null'
   and (length(meid_cure) = 32 or length(meid_cure) = 64 or
       length(meid_cure) = 15 and
       10 - (conv(substr(meid_cure, 1, 1), 16, 10) +
       conv(substr(meid_cure, 3, 1), 16, 10) +
       conv(substr(meid_cure, 5, 1), 16, 10) +
       conv(substr(meid_cure, 7, 1), 16, 10) +
       conv(substr(meid_cure, 9, 1), 16, 10) +
       conv(substr(meid_cure, 11, 1), 16, 10) +
       conv(substr(meid_cure, 13, 1), 16, 10) +
       (conv(substr(meid_cure, 2, 1), 16, 10) * 2)
        %10 + (conv(substr(meid_cure, 4, 1), 16, 10) * 2)
        %10 + (conv(substr(meid_cure, 6, 1), 16, 10) * 2)
        %10 + (conv(substr(meid_cure, 8, 1), 16, 10) * 2)
        %10 + (conv(substr(meid_cure, 10, 1), 16, 10) * 2)
        %10 + (conv(substr(meid_cure, 12, 1), 16, 10) * 2)
        %10 + (conv(substr(meid_cure, 14, 1), 16, 10) * 2)
        %10 + floor((conv(substr(meid_cure, 2, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid_cure, 4, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid_cure, 6, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid_cure, 8, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid_cure, 10, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid_cure, 12, 1), 16, 10) * 2) / 10) +
        floor((conv(substr(meid_cure, 14, 1), 16, 10) * 2) / 10))
        %10 = conv(substr(meid_cure, 15, 1), 16, 10))
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       (case
         when length(imei_url) = 15 then
          imei_url
         when length(imei_url) = 32 then
          ''
       end) id,
       'IMEI' id_type,
       (case
         when length(imei_url) = 15 then
          md5(imei_url)
         when length(imei_url) = 32 then
          imei_url
       end) id_md5,
       '4' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
 where mdn is not null and  mdn<>'null'
   and (length(imei_url) = 32 or length(imei_url) = 64 or
       length(imei_url) = 15 and
       (10 - substr(imei_url, 1, 1) + substr(imei_url, 3, 1) +
       substr(imei_url, 5, 1) + substr(imei_url, 7, 1) +
       substr(imei_url, 9, 1) + substr(imei_url, 11, 1) +
       substr(imei_url, 13, 1) + (substr(imei_url, 2, 1) * 2)
        %10 + (substr(imei_url, 4, 1) * 2)
        %10 + (substr(imei_url, 6, 1) * 2)
        %10 + (substr(imei_url, 8, 1) * 2)
        %10 + (substr(imei_url, 10, 1) * 2)
        %10 + (substr(imei_url, 12, 1) * 2)
        %10 + (substr(imei_url, 14, 1) * 2)
        %10 + floor((substr(imei_url, 2, 1) * 2) / 10) +
        floor((substr(imei_url, 4, 1) * 2) / 10) +
        floor((substr(imei_url, 6, 1) * 2) / 10) +
        floor((substr(imei_url, 8, 1) * 2) / 10) +
        floor((substr(imei_url, 10, 1) * 2) / 10) +
        floor((substr(imei_url, 12, 1) * 2) / 10) +
        floor((substr(imei_url, 14, 1) * 2) / 10))
        %10 = substr(imei_url, 15, 1))
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       imsi id,
       'IMSI' id_type,
       md5(imsi) id_md5,
       '4' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
 where mdn is not null and  mdn<>'null'
   and (length(imsi) = 32 or length(imsi) = 64 or
       length(imsi) = 15 and substr(imsi, 1, 3) = 460 and imsi regexp
        '[0-9]{15}$')
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       tdid id,
       'TDID' id_type,
       md5(tdid) id_md5,
       '4' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
 where mdn is not null and  mdn<>'null'
   and length(tdid) > 0
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       android_id id,
       'AndroidID' id_type,
       md5(android_id) id_md5,
       '4' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
 where mdn is not null and  mdn<>'null'
   and (android_id regexp '[0-9a-z]{16}$' and android_id regexp '[^0]{16}$')
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       idfa id,
       'IDFA' id_type,
       md5(idfa) id_md5,
       '4' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
 where mdn is not null and  mdn<>'null'
   and (idfa regexp '[0-9a-z]{8}[-][0-9a-z]{4}[-][0-9a-z]{4}[-][0-9a-z]{4}[-][0-9a-z]{12}$' and
        idfa <> '00000000-0000-0000-000000000000')
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       mac id,
       'MAC' id_type,
       md5(mac) id_md5,
       '4' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
 where mdn is not null and  mdn<>'null'
   and mac regexp '^([0-9A-Fa-f]{2}[\-|\:]?){5}[0-9A-Fa-f]{2}$'
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}'
union all
select mdn,
       md5(mdn) mdn_md5,
       baidu_id id,
       'BAIDU_ID' id_type,
       md5(baidu_id) id_md5,
       '4' id_source,
       prov_id,
       day_id
  from dwi_integ.dwi_evt_blog_dpi_mbl_user_prim_d
 where mdn is not null and  mdn<>'null'
   and length(baidu_id) > 0
   and prov_id = '${PROV_ID}'
   and day_id = '${DAY_ID}';"

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

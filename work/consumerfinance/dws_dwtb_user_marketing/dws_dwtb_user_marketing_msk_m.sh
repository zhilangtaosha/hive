#!/bin/bash
#***********************************************************************************
# **  文件名称: dws_dwtb_user_marketing_msk_m.sh
# **  创建日期: 2018年04月09日
# **  编写人员: qinlin
# **  输入信息: 
# **  输出信息: 
# **
# **  功能描述: 
# **  处理过程:
# **  
#***********************************************************************************
##################################建表语句##########################################
#create external table dws_m.dws_dwtb_user_marketing_msk_m( 
#mdn  string comment '手机号码加密',
#prov_name  string comment '省份',
#city_name  string comment '地市',
#country_name  string comment '区县',
#month  string comment '月份',
#cust_family_name  string comment '用户姓氏',
#gender  string comment '用户性别',
#mdn_tail_num  string comment '手机尾号',
#score  bigint comment '信用分',
#product_nbr_level  string comment '当前套餐档位（元）',
#bill_fee  string comment '用户上月实际话费（元）',
#subsidy_left_month  string comment '话补剩余月份数（月）',
#age_level  string comment '年龄区间',
#online_level  string comment '在网时长区间',
#trmnl_brand  string comment '当前使用终端品牌',
#trmnl_model  string comment '当前使用终端型号',
#trmnl_use_dur  string comment '当前终端使用时长（天）',
#trmnl_price_level  string comment '终端档次（价格档次）',
#flow_3mon_avg_level  string comment '近三个月流量均值区间',
#cdr_3mon_avg_leve  string comment '近三个月通话时长均值区间',
#card_type  string comment '主副卡标识',
#subsidy_start_date  string comment '补贴套餐生效月份',
#subsidy_end_date  string comment '补贴套餐失效月份',
#If_4G  string comment '是否4G用户',
#mdn_lat_long  string comment '手机号码经纬度'
#)partitioned by (prov_id string,month_id string) 
#row format delimited fields terminated by '\u0005' 
#location '/daas/motl/dws/msk/dwtb/dws_dwtb_user_marketing_msk_m';
#***********************************************************************************
#==修改日期==|===修改人=====|======================================================|
# 2018-4-9 qinlin.修改时间
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
LOGNAME="dws_dwtb_user_marketing_msk_m.sh"

#用户名、队列名
USERNAME="dws_i_mid"
QUEUENAME="root.bigdata.motl.mt1"

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
#add jar /home/st001/soft/MdnMd5.jar;
#CREATE TEMPORARY FUNCTION MD5Encode AS 'com.bonc.hive.MyMD5';
#常规参数
COMMON_VAR="use ${USERNAME};
set mapreduce.job.queuename=${QUEUENAME};
set mapred.max.map.failures.percent=10;
set mapred.max.reduce.failures.percent=10;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles= true;
set hive.merge.size.per.task = 134217728;
set hive.merge.smallfiles.avgsize = 110000000;
SET mapred.max.split.size = 134217728;
SET mapred.min.split.size.per.node = 100000000;
SET mapred.min.split.size.per.rack = 100000000;
set hive.exec.compress.output = true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.created.files=655350;
set hive.hadoop.supports.splittable.combineinputformat=true;
SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
add jar ${baseDirForScriptSelf}/Timeudf.jar;
CREATE TEMPORARY FUNCTION GetNewdate AS 'hive.udf.GetNewdate';
CREATE TEMPORARY FUNCTION GetMonth AS 'hive.udf.GetMonth';
add jar ${baseDirForScriptSelf}/GetNbrM5.jar;
CREATE TEMPORARY FUNCTION MD5 AS 'hive.udf.MdnMd5';"

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
	MONTH_LAST_DAY=$(date -d  "`date -d "${NEXT_MONTH_ID}01 1 month" +%Y%m%d`  -1  day" +%Y%m%d)
   
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
SQL="INSERT OVERWRITE table dws_m.dws_dwtb_user_marketing_msk_m PARTITION(PROV_ID, MONTH_ID) 
select a.mdn,
       j.prov_name,
       j.city_name,
       q.area_name country_name,
       a.month_id month,
       j.first_name_char as cust_family_name,
       j.gender,
       j.mdn_tail_num,
       f.score,
       h.main_offer_fee as product_nbr_level,
       b.bill_amt as bill_fee,
       h.subtime as subsidy_left_month,
       case
         when a.age > 0 and a.age <= 18 then
          '0-18' --年龄区间
         when a.age > 18 and a.age <= 22 then
          '18-22'
         when a.age > 22 and a.age <= 30 then
          '22-30'
         when a.age > 30 and a.age <= 40 then
          '30-40'
         when a.age > 40 and a.age <= 55 then
          '40-55'
         when a.age > 55 then
          '55以上'
       END as age_level,
       case
         when j.online_dur > 0 and j.online_dur <= 6 then
          '0-6' --在网时长区间
         when j.online_dur > 6 and j.online_dur <= 12 then
          '6-12'
         when j.online_dur > 12 and j.online_dur <= 24 then
          '12-24'
         when j.online_dur > 24 and j.online_dur <= 36 then
          '24-36'
         when j.online_dur > 36 then
          '36以上'
       END as online_level,
       d.trmnl_brand,
       d.trmnl_model,
       d.use_date as trmnl_use_dur,
       case
         when d.trmnl_price > 1 and d.trmnl_price < 1000 then
          'A(1-1000)' --终端档次
         when d.trmnl_price >= 1000 and d.trmnl_price < 2000 then
          'B[1000-2000)'
         when d.trmnl_price >= 2000 and d.trmnl_price < 3000 then
          'C[2000-3000)'
         when d.trmnl_price >= 3000 and d.trmnl_price < 4000 then
          'D[3000-4000)'
         when d.trmnl_price >= 4000 then
          '4000以上'
       END as trmnl_price_level,
       case
         when b.flow_avg_3m > 0 and b.flow_avg_3m < 100 then
          'A(0-100)' --近三个月流量均值区间
         when b.flow_avg_3m >= 100 and b.flow_avg_3m < 400 then
          'B[100-400)'
         when b.flow_avg_3m >= 400 and b.flow_avg_3m < 1000 then
          'C[400-1000)'
         when b.flow_avg_3m >= 1000 and b.flow_avg_3m < 2000 then
          'D[1000-2000)'
         when b.flow_avg_3m >= 2000 then
          'E[2000,+)'
         else
          '0'
       END as flow_3mon_avg_level,
       case
         when b.call_dur_3m > 0 and b.call_dur_3m < 50 then
          'A(0-50)' --近三个月通话时长均值区间
         when b.call_dur_3m >= 50 and b.call_dur_3m < 100 then
          'B[50-100)'
         when b.call_dur_3m >= 100 and b.call_dur_3m < 200 then
          'C[100-200)'
         when b.call_dur_3m >= 200 and b.call_dur_3m < 400 then
          'D[200-400)'
         when b.call_dur_3m >= 400 then
          'E[400,+)'
         else
          '0'
       END as cdr_3mon_avg_leve,
       j.card_type,
       h.eff_date as subsidy_start_date,
       h.exp_date as subsidy_end_date,
       if(h.is_4g_main_offer = 1, '是', '否') as If_4G,
       concat_ws(',', l.grid_longi, l.grid_lati) as mdn_lat_long,
       a.prov_id,
       a.month_id
  from (select mdn, age, prov_id, month_id
          from dwi_m.dwi_sev_user_base_info_msk_m
         where month_id = '${MONTH_ID}') a
  left join (select mdn, bill_amt, call_dur_3m, flow_avg_3m
               from dwi_m.dwi_sev_user_value_info_msk_m
              where month_id = '${MONTH_ID}') b
    on a.mdn = b.mdn
  left join (select MD5(mdn) mdn,
                    trmnl_brand,
                    trmnl_model,
                    ceil((UNIX_TIMESTAMP(concat('${MONTH_ID}', '01000000'),
                                         'yyyyMMddHHmmss') -
                         UNIX_TIMESTAMP('use_date', 'yyyy-MM-dd HH:mm:ss')) /
                         86400) use_date,
                    trmnl_price
               from dwi_integ.dwi_res_serv_user_trmnl_info_m
              where month_id = '${MONTH_ID}') d
    on a.mdn = d.mdn
  left join (select MD5(accs_nbr) mdn,
                    main_offer_fee,
                    if(exp_date is null,
                       '',
                       GetMonth(substr(exp_date, 1, 6), month_id)) subtime,
                    eff_date,
                    exp_date,
                    is_4g_main_offer
               from dwi_integ.dwi_sev_prod_offer_info_m
              where month_id = '${MONTH_ID}') h
    on a.mdn = h.mdn
  left join (select y.*
               from (select MD5(accs_nbr) mdn,
                            city_name,
                            prov_name,
                            first_name_char,
                            gender,
                            substr(accs_nbr, 8) as mdn_tail_num,
                            online_dur,
                            card_type,
                            row_number() over(partition by accs_nbr order by open_date desc) as rn
                       from dwi_integ.dwi_sev_user_main_info_m
                      where month_id = '${MONTH_ID}'
                        and prod_inst_type = '3') y
              where y.rn = 1) j
    on a.mdn = j.mdn
  left join (select p.*
               from (select z.mdn,
                            z.grid_longi,
                            z.grid_lati,
                            row_number() over(partition by z.mdn order by z.total desc) as rn
                       from (select mdn,
                                    grid_longi,
                                    grid_lati,
                                    sum(duration) total
                               from dwi_m.dwi_res_regn_staypoint_msk_d
                              where day_id >
                                    GetNewdate('${MONTH_LAST_DAY}', -7)
                              group by mdn, grid_longi, grid_lati) z) p
              where p.rn = 1) l
    on a.mdn = l.mdn
  left join (select mdn, score
               from dws_m.dws_wdtb_mdn_value_msk_m
              where month_id = '${MONTH_ID}') f
    on a.mdn = f.mdn
  left join (select mdn,
                    if(length(tran_residence_county) > 0,
                       tran_workplace_county,
                       tran_residence_county) country_name
               from dws_m.dws_wdtb_work_resi_transit_msk_m
              where month_id = '${MONTH_ID}') m
    on a.mdn = m.mdn
  left join (select MD5(mdn) mdn
               from dws_i_mid.dws_dtfs_othnet_telecom_black_m_mid
              where month_id = '${MONTH_ID}') x
    on a.mdn = x.mdn
  left join dim.dim_latn_bss q
    on m.country_name = q.city_id
 where x.mdn is null;"
RunScript "${SQL}" 

#执行mr
# SQL="hadoop jar sort.jar /apps/hive/warehouse/st001.db/ztj_test3/ /apps/hive/warehouse/st001.db/ztj_test2/ root.bigdata.motl.mt1"
# RunMr "${SQL}"

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

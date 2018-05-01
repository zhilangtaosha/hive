-- WCDR语音预处理表
create EXTERNAL table if not exists dwi_m.dwi_evt_bill_oth_wcdr_cdr_msk_d(
MDN string comment '计费号码',
other_party string comment '对端号码',
start_time string comment '起始时间',
end_time string comment '结束时间',
call_dur string comment '通话时长',
calling_visit_code string comment '计费号码到访地',
called_visit_code string comment '对端号码到访地',
Imsi string comment 'IMSI'
)
comment  'WCDR语音预处理表'
PARTITIONED BY (
prov_id string comment '省份分区',
day_id string comment '日分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dwi/msk/evt/bill/dwi_evt_bill_oth_wcdr_cdr_msk_d';

-- OIDD语音预处理表
create EXTERNAL table if not exists dwi_m.dwi_evt_bill_oth_oidd_cdr_msk_d(
MDN string comment '计费号码',
other_party string comment '对端号码',
third_party string comment '第三方号码',
call_type string comment '呼叫类型',
start_time string comment '起始时间',
end_time string comment '结束时间',
call_dur string comment '通话时长',
calling_visit_area string comment '计费号码到访地',
called_visit_code string comment '对端号码到访地',
third_visit_code string comment '第三方号码到访地',
Imsi string comment 'IMSI'
)
comment  'OIDD语音预处理表'
PARTITIONED BY (
prov_id string comment '省代码',
day_id string comment '天分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dwi/msk/evt/bill/dwi_evt_bill_oth_oidd_cdr_msk_d';
-- 语音详单预处理表
create EXTERNAL table if not exists dwi_m.dwi_evt_bill_oth_wash_cdr_msk_d(
MDN string comment '计费号码',
other_party string comment '对端号码',
call_type string comment '呼叫类型',
biz_type string comment '业务类型',
roam_type string comment '漫游类型',
Imsi string comment 'IMSI',
third_party string comment '第三方号码',
start_time string comment '开始时间',
end_time string comment '结束时间',
call_dur string comment '通话时长',
calling_home_code string comment '计费号码归属地',
called_home_code string comment '对端归属地',
calling_visit_area string comment '计费到访地',
called_visit_code string comment '对端到访地',
third_home_code string comment '第三方号码归属地',
third_visit_code string comment '第三方号码到访地',
corporation_id string comment '出访运营商代码'
)
comment  'CDR语音预处理表'
PARTITIONED BY (
prov_id string comment '省分区',
day_id string comment '天分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dwi/msk/evt/bill/dwi_evt_bill_oth_wash_cdr_msk_d';
-- 移动语音融合表
create EXTERNAL table if not exists dws_m.dws_wdtb_cdr_mix_msk_d(
MDN string comment '计费号码',
other_party string comment '对端号码',
call_type string comment '呼叫类型',
biz_type string comment '业务类型',
roam_type string comment '漫游类型',
Imsi string comment 'IMSI',
third_party string comment '第三方号码',
start_time string comment '开始时间',
end_time string comment '结束时间',
call_dur string comment '通话时长',
calling_home_code string comment '计费号码归属地',
called_home_code string comment '对端归属地',
calling_visit_area string comment '计费到访地',
called_visit_code string comment '对端到访地',
third_home_code string comment '第三方号码归属地',
third_visit_code string comment '第三方号码到访地',
corporation_id string comment '出访运营商代码（国际漫游）',
source_type string comment '数据来源'
)
comment  'CDR语音融合表'
PARTITIONED BY (
prov_id string comment '省分区',
Month_id string comment '月分区',
Day_id string comment '日分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dws/msk/wdtb/dws_wdtb_cdr_mix_msk_d'; 
-- WCDR短信预处理表
create EXTERNAL table if not exists dwi_m.dwi_evt_bill_oth_wcdr_mdr_msk_d(
MDN string comment '计费号码',
other_party string comment '对端号码',
start_time string comment '发生时间',
Start_BSID string comment '起始时的小区BSID',
Start_Lng string comment '业务发生的经度',
Start_Lat string comment '业务发生的纬度',
calling_visit_code string comment '计费号码到访地',
called_visit_code string comment '对端号码到访地',
Imsi string comment 'IMSI')
comment  'WCDR短信预处理表'
PARTITIONED BY (
prov_id string comment '省份分区',
day_id string comment '日分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dwi/msk/evt/bill/dwi_evt_bill_oth_wcdr_mdr_msk_d';
-- OIDD短信预处理表
create EXTERNAL table if not exists dwi_m.dwi_evt_bill_oth_oidd_mdr_msk_d(
MDN string comment '计费号码',
other_party string comment '对端号码',
start_time string comment '发送时间',
call_type string comment '信息类型',
calling_visit_code string comment '计费号码到访地',
called_visit_code string comment '对端号码到访地',
Imsi string comment 'IMSI',
Start_BSID string comment 'BSID',
Start_Lng string comment '经度',
Start_Lat string comment '纬度'
)
comment  'OIDD短信预处理表'
PARTITIONED BY (
prov_id string comment '省代码',
day_id string comment '天分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dwi/msk/evt/bill/dwi_evt_bill_oth_oidd_mdr_msk_d';
-- MDR短信预处理表
create EXTERNAL table if not exists dwi_integ.dwi_evt_bill_oth_wash_mdr_d(
MDN string comment '计费号码',
other_party string comment '对端号码',
call_type string comment '信息类型',
biz_type string comment '业务类型',
Imsi string comment 'IMSI',
start_date string comment '发送时间'
)
comment  'MDR短信预处理表'
PARTITIONED BY (
prov_id string comment '省代码',
day_id string comment '天分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dwi/integ/evt/bill/dwi_evt_bill_oth_wash_mdr_d';
-- 行业短信预处理表
create EXTERNAL table if not exists dwi_m.dwi_evt_bill_oth_duty_mdr_msk_d(
Biz_id String comment '企业产品标识',
Biz_cust_name String comment '企业名称',
Biz_area_code String comment '企业归属地',
Mdn String comment '发起方号码',
other_party String comment '接收方号码',
start_time String comment '业务开始时间',
Long_area_code String comment '市长途区号',
call_type String comment '短消息话单类型'
)
comment  '行业短信预处理表'
PARTITIONED BY (
prov_id String comment '省代码',
day_id String comment '天分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dwi/msk/evt/bill/dwi_evt_bill_oth_duty_mdr_msk_d';
-- 短信融合日表
create EXTERNAL table if not exists dws_m.dws_wdtb_mdr_mix_msk_d(
biz_type string comment '业务类型',
mdn string comment '计费号码|企业发生号码',
city_id string comment '计费号码归属地|企业归属地',
corporation_id string comment '计费号码运营商',
other_party string comment '对端号码',
other_city_id string comment '对端号码归属地',
other_corporation string comment '对端号码运营商',
call_type string comment '发生类型',
start_time string comment '短信发生时间',
imsi string comment 'IMSI',
calling_home_code string comment '主叫号码业务发生地',
mdr_type string comment '短信类型',
Biz_id string comment '企业产品标识',
Office_name string comment '企业名称',
Source_type string comment '数据来源'
)
comment  '短信融合日表'
PARTITIONED BY (
prov_id string comment '省分区',
Month_id string comment '月分区',
Day_id string comment '日分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dws/msk/wdtb/dws_wdtb_mdr_mix_msk_d';
-- 源表建表（移网用户UID月信息表，需求给定表名） 
CREATE EXTERNAL TABLE IF NOT EXISTS dwi_m.dwi_evt_blog_dpi_present_quick_mobile_full_msk_m(
MDN STRING comment 'MDN',
IMSI STRING comment 'IMSI',
MEID STRING comment 'MEID',
QQ STRING comment 'QQ号',
WEIBO STRING comment '微博号',
E_MAIL STRING comment '邮箱',
sdkimsi STRING comment 'sdkimsi',
sdkudid STRING comment 'sdkudid',
taobao_ID STRING comment 'taobao账号',
jd_ID STRING comment 'jd账号',
IDFA STRING comment 'IDFA',
AndroidID STRING comment 'AndroidID',
weixinID STRING comment 'weixinID',
mac STRING comment 'mac',
imei STRING comment 'imei'
)
comment  '移网用户UID月信息表'
PARTITIONED BY (
prov_id string comment '省份分区',
MONTH_id string comment '日分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dwi/msk/evt/blog/dwi_evt_blog_dpi_present_quick_mobile_full_msk_m';

-- ID_MAPING日增量表
CREATE EXTERNAL TABLE IF NOT EXISTS dws_integ.dws_wdtb_uid_id_maping_update_d(
MDN string comment '移动号码',
MDN_MD5 string comment '移动号码加密值',
ID string comment 'ID值',
ID_type string comment 'ID类型',
ID_MD5 string comment 'ID值加密值'
)
comment  'ID_MAPING日更新表'
PARTITIONED BY (
ID_source String comment 'ID来源',
prov_id string comment '省份分区',
day_id string comment '日分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dws/integ/dtfs/dws_wdtb_uid_id_maping_update_d';
--  ID_MAPING日全量表
CREATE EXTERNAL TABLE IF NOT EXISTS dws_integ.dws_wdtb_uid_id_maping_all_d(
MDN string comment '移动号码',
MDN_MD5 string comment '移动号码加密值',
ID string comment 'ID值',
ID_MD5 string comment 'ID值加密值',
ID_type string comment 'ID类型'
)
comment  'ID_MAPING日更新表'
PARTITIONED BY (
prov_id string comment '省份分区',
day_id string comment '日分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dws/integ/dtfs/dws_wdtb_uid_id_maping_all_d';
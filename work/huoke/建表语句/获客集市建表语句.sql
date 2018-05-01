-- 异网号码枚举表
CREATE EXTERNAL TABLE dwi_integ.dwi_evt_bill_other_num_list_info(
mdn string,
mdn_mn5 string
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0005', 
  'serialization.format'='\u0005') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'/daas/motl/dwi/integ/evt/bill/dwi_evt_bill_other_num_list_info';

-- 通话记录查询表（生产区已建）
CREATE EXTERNAL TABLE dal_bdcsc.dal_bdcsc_user_opp_nbr_dur_info_special_msk_d(
  mdn string, 
  other_party string, 
  calling_dur bigint, 
  calling_cnt int, 
  called_dur bigint, 
  called_cnt int)
PARTITIONED BY ( 
  month_id string, 
  day_id string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0005', 
  'serialization.format'='\u0005') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'/daas/subtl/dal/bdcsc/dal_bdcsc_user_opp_nbr_dur_info_special_msk_d';


-- 短信记录查询表（生产区已建）
CREATE EXTERNAL TABLE dal_bdcsc.dal_bdcsc_mdr_indsms_msk_d(
  start_date string, 
  mdn string, 
  other_party string, 
  calling_sms_cnt string, 
  called_sms_cnt string)
PARTITIONED BY ( 
  day_id string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0005', 
  'serialization.format'='\u0005') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/daas/subtl/dal/bdcsc/dal_bdcsc_mdr_indsms_msk_d';
-- ID记录查询表
CREATE EXTERNAL TABLE IF NOT EXISTS dws_integ.dws_wdtb_mdn_id_d(
MDN string comment '移动号码',
ID_MD5 string 'ID值加密值',
ID_type string 'ID类型'
)
comment  'MEID号码查询表'
PARTITIONED BY (
day_id string comment '日分区'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dws/integ/wdtb/dws_wdtb_mdn_id_d';
-- 记录查询解密表
CREATE EXTERNAL TABLE IF NOT EXISTS dal_bdcsc.dal_bdcsc_mdn_id_d(
ID_MD5 string 'ID值加密值',
MDN string comment '移动号码'
)
comment  'MEID号码查询表'
ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/subtl/dal/bdcsc/dal_bdcsc_mdn_id_d';
-- 通话记录出号码量预查询(生产区已经建立）
CREATE EXTERNAL TABLE dal_bdcsc.dal_bdcsc_user_opp_nbr_dur_info_total_msk_d(
  accs_nbr string, 
  calling_cnt int, 
  called_cnt bigint)
PARTITIONED BY ( 
  day_id string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0005', 
  'serialization.format'='\u0005') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/daas/subtl/dal/bdcsc/DAL_BDCSC_USER_OPP_NBR_DUR_INFO_TOTAL_MSK_D';
-- 短信记录出号码量预查询（生产区表已经建立）
CREATE EXTERNAL TABLE dal_bdcsc.dal_bdcsc_mdr_indsms_total_msk_d(
  accs_nbr string, 
  calling_cnt int, 
  called_cnt bigint)
PARTITIONED BY ( 
  day_id string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0005', 
  'serialization.format'='\u0005') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/daas/subtl/dal/bdcsc/DAL_BDCSC_MDR_INDSMS_TOTAL_MSK_D';
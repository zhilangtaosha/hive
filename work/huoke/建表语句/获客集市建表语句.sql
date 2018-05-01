-- ��������ö�ٱ�
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

-- ͨ����¼��ѯ���������ѽ���
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


-- ���ż�¼��ѯ���������ѽ���
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
-- ID��¼��ѯ��
CREATE EXTERNAL TABLE IF NOT EXISTS dws_integ.dws_wdtb_mdn_id_d(
MDN string comment '�ƶ�����',
ID_MD5 string 'IDֵ����ֵ',
ID_type string 'ID����'
)
comment  'MEID�����ѯ��'
PARTITIONED BY (
day_id string comment '�շ���'
)ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/motl/dws/integ/wdtb/dws_wdtb_mdn_id_d';
-- ��¼��ѯ���ܱ�
CREATE EXTERNAL TABLE IF NOT EXISTS dal_bdcsc.dal_bdcsc_mdn_id_d(
ID_MD5 string 'IDֵ����ֵ',
MDN string comment '�ƶ�����'
)
comment  'MEID�����ѯ��'
ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\u0005' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
location '/daas/subtl/dal/bdcsc/dal_bdcsc_mdn_id_d';
-- ͨ����¼��������Ԥ��ѯ(�������Ѿ�������
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
-- ���ż�¼��������Ԥ��ѯ�����������Ѿ�������
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
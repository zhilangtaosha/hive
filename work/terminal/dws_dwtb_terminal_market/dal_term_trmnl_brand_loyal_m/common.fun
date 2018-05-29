#!/bin/bash
#***********************************************************************************
# **  函数区
# **  功能描述:RunScript:并发执行函数。
# **           RunSQL：执行单个函数。
# **           Value：计算单个sql的值,如求和,求单个id值。
# **           RunMr：执行mr程序方法。
# **           Mergefile：合并小文件方法。
# **           SendMessage：SQL报错提示。
# **  All Rights Reserved.
#***********************************************************************************
############### 拼接日志输出路径 ##########################################
DACPDIR="/data11/dacp/dal_pro/logs"
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
#ITEM=`echo ${baseDirForScriptSelf}|awk -F "/" '{printf $NF}'`
LOGPATH=${DACPDIR}
echo LOGPATH=${LOGPATH}
if [ ! -d "${LOGPATH}" ]; then
        mkdir -p ${LOGPATH}
fi

###############SQL执行函数,函数参数:SQL 月份 省份##########################################
function RunSQL () {
        SQL=$1
        #echo ARREMAIL="${ARREMAIL[@]}" ${full_log_name}
        #老模板参数赋值
        if [[ $2 ]]; then
            full_log_name=$2
        fi
        if [ ${#1} -gt 1 ]
        then
            echo "============================变量  sql $i ======================================" 2>&1|tee -a ${full_log_name}
            printf "${COMMON_VAR}\n" 2>&1|tee -a ${full_log_name}
            echo "===============================================================================" 2>&1|tee -a ${full_log_name}
            st=`printf "${SQL}\n"`
            if [ ${PIPESTATUS[0]}  != 0 ]
             then
                echo "${SQL}" 2>&1|tee -a ${full_log_name}
             else
                printf "${SQL}\n" 2>&1|tee -a ${full_log_name}
            fi
            echo "===============================================================================" 2>&1|tee -a ${full_log_name}
            hive -e "${COMMON_VAR}${SQL}" 2>&1|tee -a ${full_log_name}
            if [ ${PIPESTATUS[0]}  != 0 ]
            then
                    SendMessage ${LOGNAME}
                    exit -1
            fi
        fi 
}

###########################多进程并发执行脚本##########################################
function RunScript () {
      sql=$1
      full_log_name=${LOGPATH}/${LOGNAME}_${DAY_ID}.log
      if [[ ! ${concurrency} ]]; then
            RunSQL "${sql}"
      elif [[ ${concurrency} =~ ^[1-9][0-9]*$ ]]; then
            while [[ 1 -eq 1 ]]
            do
                  flag=0
                  index=0
                  while [[ ${index} -lt ${concurrency} ]]
                  do
                        if ([[ ${pid_arr[$index]} ]] && (kill -0 ${pid_arr[$index]})); then
                              let index+=1
                        else
                              if [[ ${concurrency} -gt 1 ]]; then
                                    log_name_pre=$(echo "${full_log_name}"|awk -F '\\.log' '{print $1}')
                                    log_id=`expr ${index} + 1`
                                    full_log_name=${log_name_pre}_${log_id}.log
                              fi
                              RunSQL "${sql}" "${full_log_name}" &
                              pid_arr[$index]=$!
                              flag=1
                              break
                        fi
                  done
                  if [[ flag -eq 1 ]]; then
                        break
                  fi
                  sleep 5
            done
      else
            # SendMessage ${LOGNAME}
            echo "参数有误(并发数必须为正整数),脚本退出." 2>&1|tee -a ${full_log_name}
            exit -1
      fi
}

###############函数功能:计算单个sql的值,如求和,求单个id###################################
function value () {
        SQL=$1
        if [ ${#1} -gt 1 ]
        then
          printf "${COMMON_VAR}${SQL}\n" 2>&1|tee -a ${full_log_name}
          value=`hive -S -e "${COMMON_VAR}${SQL}"`
          if [ ${PIPESTATUS[0]}  != 0 ]
            then
                    SendMessage ${LOGNAME}
                    exit -1
            fi
        value=(${value//,/ })
        echo value= $value
        fi
}
###############执行mr程序##################################################################
function RunMr () {
        mr=$1
        if [ ${#1} -gt 1 ]
        then
            echo "============================变量  sql $i ======================================" 2>&1|tee -a ${full_log_name}
            printf "${mr}\n" 2>&1|tee -a ${full_log_name}
            echo "===============================================================================" 2>&1|tee -a ${full_log_name}
            ${mr}
          if [ ${PIPESTATUS[0]}  != 0 ]
            then
                    SendMessage ${LOGNAME}
                    exit -1
            fi
        fi
}
###############合并小文件##################################################################
###改进方向:自动获取表名和where条件  循环放在外面
function Mergefile () { 
    username=$1
    table_name=$2
    where=$3
    partitions=`hive -e "use ${username};show partitions ${table_name}"`
    p=`echo ${partitions}|awk '{print $1}'|head -n 1`
    #echo p=$p
    partition=`echo ${p%=*} |sed -e 's/=[0-9a-z]*\//,/g' |sed -e 's/=[0-9a-z]*//g'`
            echo "========================合并  mergesql $i =====================================" 2>&1|tee -a ${LOGPATH}/${LOGNAME}_${DAY_ID}.log
     
            sql="insert overwrite table ${table_name} partition(${partition}) select * from ${table_name} "${where}
            printf "${MERGE_VAR}\n" 2>&1|tee -a ${LOGPATH}/${LOGNAME}_${DAY_ID}.log
            printf "${sql}\n" 2>&1|tee -a ${LOGPATH}/${LOGNAME}_${DAY_ID}.log
            if [ ${PIPESTATUS[0]}  != 0 ]
            then
                    echo "${sql}\n" 2>&1|tee -a ${LOGPATH}/${LOGNAME}_${DAY_ID}.log
            fi
            echo "===============================================================================" 2>&1|tee -a ${LOGPATH}/${LOGNAME}_${DAY_ID}.log
              hive -e "${MERGE_VAR}${sql};" 2>&1|tee -a ${LOGPATH}/${LOGNAME}_${DAY_ID}.log
            if [ ${PIPESTATUS[0]} != 0 ]
                then
                        SendMessage ${LOGNAME}
                    exit -1
            fi  
         
            echo "============================成功合并小文件=====================================" 2>&1|tee -a ${LOGPATH}/${LOGNAME}_${DAY_ID}.log
}
###############SQL报错提示###########################################################################
function SendMessage () {
    echo "run error"
    id=`date  +%N`
    Script=$1
    echo send Script=$Script ARREMAIL=$ARREMAIL
    #添加判断，如果$ARREMAIL有值才往下进行，发送邮件
    if [ "$ARREMAIL" ]; then  
    i=1
    value="insert into ts_alarm_result_info_oidd (id,send_email , file_names , send_status , send_time , create_time) values "
    for email in ${ARREMAIL[@]};
    do 
    value=${value}"('${id}${i}','${email}','${Script} 错误，请及时处理','5',now(),now()),"
    let i=i+1
    done
    sql=${value%,*}";"
    echo $sql 2>&1|tee -a ${LOGPATH}/${LOGNAME}_${DAY_ID}.log
mysql -h10.0.174.5 -uasset -passet123 -Dassetdb <<EOF 
$sql
commit;
EOF
echo "--------send successfully....-------"
   exit -1
else  
  echo "ARREMAIL IS NULL"  
fi
}
###############记录开始和结束时间#####################################################################
function start_prc () {
echo "==========$start_dt Execution star ======================================="
}

function end_prc () {
echo "==========$end_dt Execution completed,run $time1 seconds.================="
}
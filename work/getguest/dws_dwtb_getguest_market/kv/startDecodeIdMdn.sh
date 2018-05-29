#!/bin/sh
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

HOME=${baseDirForScriptSelf}

#TABLE=DecodeMdnImsiMeid
TABLE=DecodeMd5Number
MYCLASSPATH=$HOME/libs/bdcsc-core-0.0.6-jar-with-dependencies.jar
MAINCLASS=cn.ctyun.bigdata.bdcsc.core.service.ldr4.DataLoader
#LOG4J_CONFIG_FILE=$HOME/config/log4j_terminal_dynamic.properties

#export LOG4J_CONFIG_FILE 

JAVA_OPTS="-Xmx1g -Xms1g"
nohup  java $JAVA_OPTS -classpath "$MYCLASSPATH" $MAINCLASS  --dataload.configfile=$HOME/conf/$TABLE.properties --log4j.location=$HOME/conf/log4j/log4j.properties >$HOME/logs/$TABLE/run.log &

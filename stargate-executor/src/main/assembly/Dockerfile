FROM        docker.apple.com/shuri/aiml-flink-1.16:latest
MAINTAINER  Madhu Babu Chakala <madhu.babu@apple.com>
RUN           mkdir -p /opt/stargate
COPY       config/logger/* $FLINK_HOME/conf/
COPY       config/logger/* $SPARK_HOME/conf/

WORKDIR     /opt
COPY        .build/docker-package/opt /opt

ENV       STARGATE_HOME="/opt/stargate" \
                APP_HOME="/opt/stargate" \
                STARGATE_RUNTIME_HOME="/opt/stargate-runtime" \
                STARGATE_BASE_SHARED_DIRECTORY="/mnt/app/shared" \
                APP_LOG_FILE_NAME="stargate-pipeline" \
                APP_LOG_OVERRIDES_FILE_PATH="/mnt/app/shared/log-overrides.xml" \
                AWS_REGION="us-west-2" \
                AWS_STS_REGIONAL_ENDPOINTS="regional" \
                ENABLE_CUSTOM_LOG_CONFIG="true" \
                HADOOP_USER_NAME="stargate" \
                LOG4J_CONFIGURATION_FILE="/opt/stargate/conf/log4j2.xml" \
                LOG4J_CONFIGURATION="/opt/stargate/conf/log4j2.xml" \
                SYNC_HTTP_SERVICE_IMPL="software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService" \
                PATH=/opt/stargate/bin:/opt/stargate:$PATH \
                STARGATE_RUNTIME_USER=root

RUN      cd $SPARK_HOME/jars && \
                ls -d -1 *.jar | sort | xargs -t -I % sh -c '{ tmp_name=%; tmp_lib_name=${tmp_name##*/}; mv % $SPARK_HOME/jars/spark-$tmp_lib_name; }' && \
                ls -d -1 $STARGATE_HOME/lib/*.jar | sort | xargs -t -I % sh -c '{ ln -s % $SPARK_HOME/jars; }' && \
                ls -d -1 $SPARK_HOME/jars/dependency-z-logback-*.jar | sort | xargs -t -I % sh -c '{ unlink %; }' && \
                ls -d -1 $SPARK_HOME/jars/dependency-z-log4j-*.jar | sort | xargs -t -I % sh -c '{ unlink %; }' && \
                mkdir -p $STARGATE_HOME/logs && \
                rm -fR $FLINK_HOME/log && \
                ln -s $STARGATE_HOME/logs  $FLINK_HOME/logs && \
                ln -s $STARGATE_HOME/logs  $SPARK_HOME/logs && \
                ln -s $STARGATE_HOME/logs  $FLINK_HOME/log && \
                rm -fR $FLINK_HOME/lib/log4j* && \
                rm -fR $FLINK_HOME/conf/log4j* && \
                rm -f $FLINK_HOME/conf/logback-console.xml && \
                rm -f $FLINK_HOME/conf/logback-session.xml && \
                ln -s $FLINK_HOME/conf/logback.xml $FLINK_HOME/conf/logback-console.xml && \
                ln -s $FLINK_HOME/conf/logback.xml $FLINK_HOME/conf/logback-session.xml && \
                ls -d -1 $STARGATE_HOME/lib/*.jar | sort | xargs -t -I % sh -c '{ ln -s % $FLINK_HOME/lib; }' && \
                mkdir -p /app && \
                ln -s /opt/stargate /app/stargate && \
                mkdir -p /app/lib && \
                mkdir -p /app/conf && \
                mkdir -p /opt/stargate-runtime && \
                cp $FLINK_HOME/plugins/metrics-prometheus/* $FLINK_HOME/lib && \
                rm -fR $FLINK_HOME/plugins/metrics-prometheus

WORKDIR   $FLINK_HOME
EXPOSE       6122 6123 6124 6125 8081 8888 8185 8186 8187 9105 4000
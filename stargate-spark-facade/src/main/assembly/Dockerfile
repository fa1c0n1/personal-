FROM        docker.apple.com/shuri/aci-spark-3.4.0:latest
MAINTAINER  Madhu Babu Chakala <madhu.babu@apple.com>
COPY        stargate-spark-facade/.out/distributions/stargate-spark-facade-*.zip /opt/
COPY        stargate-spark-facade/src/main/assembly/start-spark-facade.sh /opt/
COPY        stargate-spark-facade/src/main/resources/version.info /opt/
COPY        stargate-spark-facade/src/main/assembly/executor-pod-template.yaml /opt/
COPY       config/logger/* $SPARK_HOME/conf/

WORKDIR     /opt
RUN         unzip stargate-spark-facade-*.zip
RUN         rm stargate-spark-facade-*.zip
RUN         mv stargate-spark-facade-* stargate

EXPOSE      8888 8080 4040 5005 18080 7077 7078 9090

ENV       STARGATE_HOME="/opt/stargate" \
                APP_HOME="/opt/stargate" \
                APP_LOG_FILE_NAME="stargate-spark-facade" \
                AWS_REGION="us-west-2" \
                AWS_STS_REGIONAL_ENDPOINTS="regional" \
                HADOOP_USER_NAME="stargate" \
                LOG4J_CONFIGURATION_FILE="$SPARK_HOME/conf/log4j2.xml" \
                LOG4J_CONFIGURATION="$SPARK_HOME/conf/log4j2.xml" \
                SYNC_HTTP_SERVICE_IMPL="software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService" \
                PATH=/opt/stargate/bin:/opt/stargate:$PATH

RUN         mkdir /app  && \
                  ln -s /opt/stargate /app/stargate && \
                  mv /opt/stargate/lib/stargate-spark-facade*.jar /opt/stargate/lib/aml-stargate-spark-facade.jar && \
                  cd /opt/stargate/lib && \
                  ls -d -1 *.jar | sort | xargs -t -I % sh -c '{ ln -s /opt/stargate/lib/% $SPARK_HOME/jars/aml-sg-%; }' && \
                  mkdir -p $STARGATE_HOME/logs && \
                  rm -fR $SPARK_HOME/logs && \
                  ln -s $STARGATE_HOME/logs $SPARK_HOME/logs &&\
                  rm $SPARK_HOME/jars/guava-14.0.1.jar &&\
                  rm $SPARK_HOME/jars/protobuf-java-2.5.0.jar \

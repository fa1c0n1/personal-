#!/bin/bash

[[ -z "$APP_MODE" ]] && { export APP_MODE=UAT; }
[[ -z "$APP_K8S_API_URL" ]] && { export APP_K8S_API_URL=https://kubernetes.default.svc.cluster.local:443; }
[[ -z "$APP_SPARK_FACADE_VERSION" ]] && { export APP_SPARK_FACADE_VERSION="$(cat /opt/version.info)"; }
[[ -z "$APP_SECRETS_ROOT_PATH" ]] && { export APP_SECRETS_ROOT_PATH=/app-config; }
[[ -z "$HADOOP_CONF_DIR" ]] && { export HADOOP_CONF_DIR=$APP_SECRETS_ROOT_PATH; }
[[ -z "$APP_SECRETS_PROPS_PATH" ]] && { export APP_SECRETS_PROPS_PATH=/app-config/app.secrets; }
[[ -z "$APP_HMS_PRINCIPAL" ]] && { export APP_HMS_PRINCIPAL=stargate/stargate-hms@APPLECONNECT.APPLE.COM; }
[[ -z "$APP_HMS_KEYTAB_PATH" ]] && { export APP_HMS_KEYTAB_PATH=$APP_SECRETS_ROOT_PATH/keytab.txt; }
[[ -z "$APP_HMS_KRB5_PATH" ]] && { export APP_HMS_KRB5_PATH=$APP_SECRETS_ROOT_PATH/krb5.conf; }
[[ -z "$APP_SPARK_DRIVER_HOST" ]] && { export APP_SPARK_DRIVER_HOST=stargate-spark-facade; }
[[ -z "$APP_SPARK_DRIVER_NAMESPACE" ]] && { export APP_SPARK_DRIVER_NAMESPACE="$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)"; }
[[ -z "$APP_SPARK_EXECUTOR_PREFIX" ]] && { export APP_SPARK_EXECUTOR_PREFIX="$APP_SPARK_DRIVER_HOST-executor"; }
[[ -z "$APP_SPARK_EXTRA_CLASSPATH" ]] && { export APP_SPARK_EXTRA_CLASSPATH="$APP_SECRETS_ROOT_PATH:$APP_SECRETS_ROOT_PATH/hive-site.xml:$APP_SECRETS_ROOT_PATH/core-site.xml"; }
[[ -z "$APP_SPARK_EXTRA_JAVA_OPTIONS" ]] && { export APP_SPARK_EXTRA_JAVA_OPTIONS="-Djava.security.krb5.conf=$APP_HMS_KRB5_PATH"; }
[[ -z "$APP_SPLUNK_INDEX" ]] && { export APP_SPLUNK_INDEX=stargate_uat; }
[[ -z "$APP_SECRETS_REF_NAME" ]] && { export APP_SECRETS_REF_NAME=appconfig-uat; }
[[ -z "$APP_SECRETS_LABEL_NAME" ]] && { export APP_SECRETS_LABEL_NAME=UAT_WEST2; }
[[ -z "$APP_SECRETS_PROFILE_NAME" ]] && { export APP_SECRETS_PROFILE_NAME=SHURI_AWS; }
[[ -z "$APP_SPARK_EXECUTOR_SERVICE_ACCOUNT_NAME" ]] && { export APP_SPARK_EXECUTOR_SERVICE_ACCOUNT_NAME=default; }
[[ -z "$APP_SPARK_EXECUTOR_POD_TEMPLATE_FILE_PATH" ]] && { export APP_SPARK_EXECUTOR_POD_TEMPLATE_FILE_PATH=/opt/executor-pod-template.yaml; }

if [ -e $APP_SECRETS_ROOT_PATH/executor-pod-template.yaml ]; then
  cat $APP_SECRETS_ROOT_PATH/executor-pod-template.yaml > $APP_SPARK_EXECUTOR_POD_TEMPLATE_FILE_PATH
fi

sed -i "s/|EXECUTOR_SPLUNK_INDEX|/$APP_SPLUNK_INDEX/g" $APP_SPARK_EXECUTOR_POD_TEMPLATE_FILE_PATH
sed -i "s/EXECUTOR_APP_CONFIG_SECRET_REF/$APP_SECRETS_REF_NAME/g" $APP_SPARK_EXECUTOR_POD_TEMPLATE_FILE_PATH
sed -i "s/EXECUTOR_APP_CONFIG_LABEL_NAME/$APP_SECRETS_LABEL_NAME/g" $APP_SPARK_EXECUTOR_POD_TEMPLATE_FILE_PATH
sed -i "s/EXECUTOR_APP_CONFIG_PROFILE_NAME/$APP_SECRETS_PROFILE_NAME/g" $APP_SPARK_EXECUTOR_POD_TEMPLATE_FILE_PATH
sed -i "s/EXECUTOR_SERVICE_ACCOUNT_NAME/$APP_SPARK_EXECUTOR_SERVICE_ACCOUNT_NAME/g" $APP_SPARK_EXECUTOR_POD_TEMPLATE_FILE_PATH
sed -i "s/stargate-spark-facade:latest/stargate-spark-facade:$APP_SPARK_FACADE_VERSION/g" $APP_SPARK_EXECUTOR_POD_TEMPLATE_FILE_PATH


export JAVA_OPTS="$JAVA_OPTS $APP_SPARK_EXTRA_JAVA_OPTIONS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"

mkdir -p /etc/cron.d; cat $APP_HMS_KRB5_PATH>/etc/krb5.conf; echo "0 */6 * * *  kinit -kt $APP_HMS_KEYTAB_PATH $APP_HMS_PRINCIPAL" > /etc/cron.d/hmskinit; crontab /etc/cron.d/hmskinit; kinit -kt $APP_HMS_KEYTAB_PATH $APP_HMS_PRINCIPAL; cron

if [ -e $APP_SECRETS_ROOT_PATH/spark.conf ]; then
  cat $APP_SECRETS_ROOT_PATH/spark.conf >> $SPARK_HOME/conf/spark-defaults.conf
else
  echo "" >> $SPARK_HOME/conf/spark-defaults.conf
fi
echo "" >> $SPARK_HOME/conf/spark-defaults.conf

printf 'spark.kubernetes.container.image\t\t%s\n'  "docker.apple.com/aml/stargate-spark-facade:$APP_SPARK_FACADE_VERSION" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.kubernetes.executor.container.image\t\t%s\n'  "docker.apple.com/aml/stargate-spark-facade:$APP_SPARK_FACADE_VERSION" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.kubernetes.driver.pod.name\t\t%s\n'  "$HOSTNAME" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.driver.host\t\t%s\n'  "$APP_SPARK_DRIVER_HOST" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.driver.port\t\t%s\n'  "8080" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.kubernetes.namespace\t\t%s\n'  "$APP_SPARK_DRIVER_NAMESPACE" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.kubernetes.executor.podNamePrefix\t\t%s\n'  "$APP_SPARK_EXECUTOR_PREFIX" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.driver.extraJavaOptions\t\t%s\n'  "$APP_SPARK_EXTRA_JAVA_OPTIONS" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.executor.extraJavaOptions\t\t%s\n'  "$APP_SPARK_EXTRA_JAVA_OPTIONS" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.driver.extraClassPath\t\t%s\n'  "$APP_SPARK_EXTRA_CLASSPATH" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.executor.extraClassPath\t\t%s\n'  "$APP_SPARK_EXTRA_CLASSPATH" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.kubernetes.executor.podTemplateFile\t\t%s\n'  "$APP_SPARK_EXECUTOR_POD_TEMPLATE_FILE_PATH" >>$SPARK_HOME/conf/spark-defaults.conf
printf 'spark.kubernetes.executor.podTemplateContainerName\t\t%s\n'  "executor" >>$SPARK_HOME/conf/spark-defaults.conf

for line in $(cat $APP_SECRETS_PROPS_PATH); do
    if [[ $line =~ ^spark ]]; then
      echo "$line " | sed "s/=/   /1" >>$SPARK_HOME/conf/spark-defaults.conf
    fi
done

spark-submit \
    --master k8s://$APP_K8S_API_URL \
    --deploy-mode client \
    --name stargate-spark-facade \
    --class com.apple.aml.stargate.spark.facade.boot.SparkApp \
    $APP_SPARK_ADDITIONAL_CONF \
    --verbose \
    local:///opt/stargate/lib/aml-stargate-spark-facade.jar
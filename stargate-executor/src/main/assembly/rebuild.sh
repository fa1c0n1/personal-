[[ -z "$STARGATE_VERSION" ]] && { export STARGATE_VERSION="0.0.0.$(date '+%Y%M%d')"; }

rm -fR stargate-executor/.build
mkdir -p stargate-executor/.build
cp stargate-executor/.out/distributions/stargate-executor-*.zip stargate-executor/.build
if [ -z "$(ls -A stargate-executor/.build)" ]; then
  echo "No build files present!!"
  exit
fi
cd stargate-executor/.build
unzip stargate-executor-*.zip
rm -f stargate-executor-*.zip
mv stargate-executor-* stargate
mkdir stargate/sglib
cd stargate/lib
ls -d -1 "stargate"*.jar | sort | xargs -t -I % sh -c '{ mv % ../sglib/aml-%; }'
ls -d -1 "jackson"*.jar | sort | xargs -t -I % sh -c '{ mv % ../sglib/dependency-a-%; }'
ls -d -1 "joda"*.jar | sort | xargs -t -I % sh -c '{ mv % ../sglib/dependency-a-%; }'
ls -d -1 "iceberg"*.jar | sort | xargs -t -I % sh -c '{ mv % ../sglib/dependency-b-%; }'
ls -d -1 *apple*.jar | grep -v "hive*" | sort | xargs -t -I % sh -c '{ mv % ../sglib/dependency-c-%; }'
ls -d -1 "proto"*.jar | sort | xargs -t -I % sh -c '{ mv % ../sglib/dependency-d-%; }'
ls -d -1 "grpc"*.jar | sort | xargs -t -I % sh -c '{ mv % ../sglib/dependency-d-%; }'
ls -d -1 "beam"*.jar | sort | xargs -t -I % sh -c '{ mv % ../sglib/dependency-e-%; }'
ls -d -1 *avro*.jar | grep -v "beam*" | grep -v "apple*" | sort | xargs -t -I % sh -c '{ mv % ../sglib/dependency-f-%; }'
ls -d -1 *parquet*.jar | grep -v "beam*" | grep -v "apple*" | grep -v "avro*" | sort | xargs -t -I % sh -c '{ mv % ../sglib/dependency-f-%; }'

mv ../sglib/aml-stargate-executor*.jar ../sglib/aml-stargate-executor.jar

ls -d -1 *.jar | sort | xargs -t -I % sh -c '{ mv % ../sglib/dependency-x-%; }'
cd ../../
cp stargate/sglib/* stargate/lib
rm -fR stargate/sglib
cd stargate
mkdir conf
mkdir opt
cp ../../../config/logger/*.xml conf
cd opt
ls -d -1 *-sources.jar | sort | xargs -t -I % sh -c '{ rm %; }'
ls -d -1 "stargate"*.jar | sort | xargs -t -I % sh -c '{ mv % aml-%; }'
cd ../
echo $(ls -d -1 lib/*.jar | sort | paste -sd ":" -)":\$STARGATE_ADDITIONAL_CLASSPATH" > classpath
if ! [ -x "$(command -v gsed)" ]; then
  sed -i 's+lib/+${APP_HOME}/lib/+g' classpath
  sed -i "s+^CLASSPATH=.*+CLASSPATH=$(cat classpath)+g" bin/stargate-executor
else
  gsed -i 's+lib/+${APP_HOME}/lib/+g' classpath
  gsed -i "s+^CLASSPATH=.*+CLASSPATH=$(cat classpath)+g" bin/stargate-executor
fi
echo "mkdir -p /tmp/stargate/shared" >bin/stargate-executor-modified
#echo "export APP_WEB_ERROR_TRACE_PRINT=true" >> bin/stargate-executor-modified
echo "[[ -z \"\$APP_HOME\" ]] && { export APP_HOME=\`pwd\`; }" >>bin/stargate-executor-modified
echo "[[ -z \"\$APP_MODE\" ]] && { export APP_MODE=DEV; }" >>bin/stargate-executor-modified
echo "[[ -z \"\$APP_LOG_OVERRIDES_FILE_PATH\" ]] && { export APP_LOG_OVERRIDES_FILE_PATH=/tmp/stargate/shared/log-overrides.xml; }" >>bin/stargate-executor-modified
echo "export STARGATE_BASE_SHARED_DIRECTORY=/tmp/stargate/shared" >>bin/stargate-executor-modified
echo "export STARGATE_RM_USE_AVAILABLE_PORT=true" >>bin/stargate-executor-modified
echo "export AWS_REGION=\"\${AWS_REGION:-us-west-2}\"" >>bin/stargate-executor-modified
echo "[[ -z \"\$STARGATE_ADDITIONAL_CLASSPATH\" ]] && { export STARGATE_ADDITIONAL_CLASSPATH=\`pwd\`; }" >>bin/stargate-executor-modified
echo "export STARGATE_ADDITIONAL_CLASSPATH=\`(ls -d -1 \${STARGATE_ADDITIONAL_CLASSPATH}/*.jar | sort | paste -sd ":" -)\`:\$STARGATE_ADDITIONAL_CLASSPATH" >>bin/stargate-executor-modified
echo "STARGATE_EXECUTOR_OPTS='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005'" >bin/stargate-debugger
echo "STARGATE_EXECUTOR_OPTS='-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005'" >bin/stargate-debugger-silent
cat bin/stargate-executor >>bin/stargate-executor-modified
rm bin/stargate-executor
mv bin/stargate-executor-modified bin/stargate-executor
chmod +x bin/stargate-executor
cat bin/stargate-executor >>bin/stargate-debugger
chmod +x bin/stargate-debugger
cat bin/stargate-executor >>bin/stargate-debugger-silent
chmod +x bin/stargate-debugger-silent
rm classpath
zip -r ../stargate-executor.zip .
cd ../
rm -fR stargate

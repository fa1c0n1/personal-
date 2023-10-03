#!/bin/bash
MAIN_COMMAND=$1
MAIN_ARGS=("${@:2}")

export STARGATE_RUNTIME_HOME=/opt/stargate-runtime
[[ -z "$STARGATE_HOME" ]] && { export STARGATE_HOME=/opt/stargate; }
[[ -z "$APP_HOME" ]] && { export APP_HOME=/opt/stargate; }
[[ -z "$FLINK_HOME" ]] && { export FLINK_HOME=/opt/flink; }
[[ -z "$SPARK_HOME" ]] && { export SPARK_HOME=/opt/spark; }


RUNTIME_FOLDERS="stargate;flink;spark;glowroot;async-profiler;stargate-jvm-executor;stargate-python-executor"

for folder_name in ${RUNTIME_FOLDERS//;/ }; do
  if [[ -d "$STARGATE_RUNTIME_HOME/opt/$folder_name" ]] && [[ ! -d "/opt/$folder_name" ]]; then
    ln -s $STARGATE_RUNTIME_HOME/opt/$folder_name /opt/$folder_name
  fi
done

if [[ -d "$STARGATE_RUNTIME_HOME/app/lib" ]] && [ "$(ls -A $STARGATE_RUNTIME_HOME/app/lib)" ]; then
  ls -d -1 $STARGATE_RUNTIME_HOME/app/lib/*.jar | sort | xargs -t -I @ sh -c '{ sg_name=@; sg_lib_name=${sg_name##*/}; ln -s @ $STARGATE_HOME/lib/z-applib-0-$sg_lib_name >/dev/null; ln -s @ $FLINK_HOME/lib/z-applib-0-$sg_lib_name >/dev/null; ln -s @ $SPARK_HOME/jars/z-applib-0-$sg_lib_name >/dev/null; }'
fi

if [[ -d "/app/lib" ]] && [ "$(ls -A /app/lib)" ]; then
  ls -d -1 /app/lib/*.jar | sort | xargs -t -I @ sh -c '{ sg_name=@; sg_lib_name=${sg_name##*/}; ln -s @ $STARGATE_HOME/lib/z-applib-$sg_lib_name >/dev/null; ln -s @ $FLINK_HOME/lib/z-applib-$sg_lib_name >/dev/null; ln -s @ $SPARK_HOME/jars/z-applib-$sg_lib_name >/dev/null; }'
fi

if [[ -d "/mnt/app/shared/lib" ]] && [ "$(ls -A /mnt/app/shared/lib)" ]; then
  ls -d -1 /mnt/app/shared/lib/*.jar | sort | xargs -t -I @ sh -c '{ sg_name=@; sg_lib_name=${sg_name##*/}; ln -s @ $STARGATE_HOME/lib/a0-sharedlib-$sg_lib_name >/dev/null; ln -s @ $FLINK_HOME/lib/a0-sharedlib-$sg_lib_name >/dev/null; ln -s @ $SPARK_HOME/jars/a0-sharedlib-$sg_lib_name >/dev/null; }'
fi

mkdir -p $STARGATE_HOME/logs

rm -fR $FLINK_HOME/logs
rm -fR $FLINK_HOME/log
rm -fR $SPARK_HOME/logs

[[ ! -d $FLINK_HOME/logs ]] && { ln -s $STARGATE_HOME/logs  $FLINK_HOME/logs; }
[[ ! -d $FLINK_HOME/log ]] && { ln -s $STARGATE_HOME/logs  $FLINK_HOME/log; }
[[ ! -d $SPARK_HOME/logs ]] && { ln -s $STARGATE_HOME/logs  $SPARK_HOME/logs; }

mkdir -p /app
[[ ! -d /app/stargate ]] && { ln -s $STARGATE_HOME /app/stargate; }

export PATH=$STARGATE_HOME/bin:$FLINK_HOME/bin:$SPARK_HOME/bin:$PATH

chmod +x $MAIN_COMMAND > /dev/null
exec "$MAIN_COMMAND" "${MAIN_ARGS[@]}"
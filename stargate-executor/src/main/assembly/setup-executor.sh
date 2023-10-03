#!/bin/bash
export STARGATE_RUNTIME_HOME=/opt/stargate-runtime

cp /opt/setup-executor.sh $STARGATE_RUNTIME_HOME/
cp /opt/run-executor.sh $STARGATE_RUNTIME_HOME/

export SG_CONTAINER_TYPES=$1
mkdir -p $STARGATE_RUNTIME_HOME/opt

for SG_CONTAINER_TYPE in ${SG_CONTAINER_TYPES//;/ }; do
  case $SG_CONTAINER_TYPE in
    flink-main-container)
      [[ ! -d $STARGATE_RUNTIME_HOME/opt/stargate ]] && { cp -R /opt/stargate $STARGATE_RUNTIME_HOME/opt; }
      [[ ! -d $STARGATE_RUNTIME_HOME/opt/flink ]] && { cp -R /opt/flink $STARGATE_RUNTIME_HOME/opt; }
      [[ ! -d $STARGATE_RUNTIME_HOME/opt/spark ]] && { cp -R /opt/spark $STARGATE_RUNTIME_HOME/opt; }
      [[ ! -d $STARGATE_RUNTIME_HOME/opt/glowroot ]] && { cp -R /opt/glowroot $STARGATE_RUNTIME_HOME/opt; }
      [[ ! -d $STARGATE_RUNTIME_HOME/opt/async-profiler ]] && { cp -R /opt/async-profiler $STARGATE_RUNTIME_HOME/opt; }
      ;;

    jvm)
      [[ ! -d $STARGATE_RUNTIME_HOME/opt/stargate-jvm-executor ]] && { cp -R /opt/stargate-jvm-executor $STARGATE_RUNTIME_HOME/opt; }
      [[ ! -d $STARGATE_RUNTIME_HOME/opt/glowroot ]] && { cp -R /opt/glowroot $STARGATE_RUNTIME_HOME/opt; }
      [[ ! -d $STARGATE_RUNTIME_HOME/opt/async-profiler ]] && { cp -R /opt/async-profiler $STARGATE_RUNTIME_HOME/opt; }
      ;;

    python)
      [[ ! -d $STARGATE_RUNTIME_HOME/opt/stargate-python-executor ]] && { cp -R /opt/stargate-python-executor $STARGATE_RUNTIME_HOME/opt; }
      ;;

    *)
      ;;
  esac
done
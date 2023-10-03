export STARGATE_BUILD_BASE=$PWD
export DOCKER_PACKAGE_BASE=$STARGATE_BUILD_BASE/.build/docker-package
rm -fR $DOCKER_PACKAGE_BASE
mkdir -p $DOCKER_PACKAGE_BASE/opt/stargate

cp stargate-executor/src/main/assembly/setup-executor.sh $DOCKER_PACKAGE_BASE/opt
cp stargate-executor/src/main/assembly/run-executor.sh $DOCKER_PACKAGE_BASE/opt
chmod 755 $DOCKER_PACKAGE_BASE/opt/setup-executor.sh
chmod 755 $DOCKER_PACKAGE_BASE/opt/run-executor.sh

cp stargate-executor/.build/stargate-executor.zip $DOCKER_PACKAGE_BASE/opt/stargate
cp stargate-jvm-executor/.out/distributions/stargate-jvm-executor-*.zip $DOCKER_PACKAGE_BASE/opt/
cp stargate-python-executor-*.zip $DOCKER_PACKAGE_BASE/opt

cd $DOCKER_PACKAGE_BASE/opt

unzip stargate-jvm-executor-*.zip
rm stargate-jvm-executor-*.zip
mv stargate-jvm-executor-* stargate-jvm-executor
unzip stargate-python-executor-*.zip -d stargate-python-executor
rm stargate-python-executor-*.zip
cd $DOCKER_PACKAGE_BASE/opt/stargate
unzip stargate-executor.zip
rm stargate-executor.zip

cd $STARGATE_BUILD_BASE
cp stargate-jvm-executor/src/main/assembly/* $DOCKER_PACKAGE_BASE/opt/stargate-jvm-executor/
FROM                      docker.apple.com/aiml/dpi-flink/flink-oss-base:1.14
MAINTAINER       Madhu Babu Chakala <madhu.babu@apple.com>
WORKDIR              /opt
USER                        root
COPY                       base-images/aiml-flink-1.14/docker-entrypoint.sh /opt/flink
RUN                          rm /docker-entrypoint.sh && \
                                    cp /opt/flink/docker-entrypoint.sh / && \
                                    wget --no-check-certificate https://github.com/glowroot/glowroot/releases/download/v0.14.0-beta.3/glowroot-0.14.0-beta.3-dist.zip && \
                                    unzip glowroot-0.14.0-beta.3-dist.zip && \
                                    rm -f glowroot-0.14.0-beta.3-dist.zip && \
                                    wget --no-check-certificate  https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.9/async-profiler-2.9-linux-x64.tar.gz && \
                                    tar -xvf async-profiler-2.9-linux-x64.tar.gz && \
                                    mv async-profiler-2.9-linux-x64 async-profiler && \
                                    rm -f async-profiler-2.9-linux-x64.tar.gz && \
                                    unlink /opt/async-profiler/latest && \
                                    ln -s /opt/async-profiler/async-profiler-2.9-linux-x64 /opt/async-profiler/latest && \
                                    mkdir -p /opt/spark/jars && \
                                    chown -R root:root /opt

ENV                           SPARK_HOME='/opt/spark' \
                                    PSPRK_HADOOP_CONFIG_CLIENT_ENABLED="false" \
                                    PSPRK_PIE_INITIALIZE_KERBEROS="false" \
                                    PATH=$PATH:/opt/glowroot:/opt/async-profiler/latest
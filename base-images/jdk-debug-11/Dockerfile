FROM            docker.apple.com/base-images/ubi9/java11-builder:latest
LABEL           maintainer="Madhu Babu Chakala <madhu.babu@apple.com>"
RUN               dnf install -y --nodocs procps-ng && dnf clean all && \
                        cd /opt && \
                        wget --no-check-certificate https://github.com/glowroot/glowroot/releases/download/v0.14.0-beta.3/glowroot-0.14.0-beta.3-dist.zip && \
                        unzip glowroot-0.14.0-beta.3-dist.zip && \
                        rm -f glowroot-0.14.0-beta.3-dist.zip && \
                        wget --no-check-certificate  https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.9/async-profiler-2.9-linux-x64.tar.gz && \
                        tar -xvf async-profiler-2.9-linux-x64.tar.gz && \
                        mv async-profiler-2.9-linux-x64 async-profiler && \
                        rm -f async-profiler-2.9-linux-x64.tar.gz && \
                        wget --no-check-certificate  https://github.com/bojand/ghz/releases/download/v0.117.0/ghz-linux-x86_64.tar.gz && \
                        tar -xvf ghz-linux-x86_64.tar.gz && \
                        rm -f ghz-linux-x86_64.tar.gz && \
                        wget --no-check-certificate https://hey-release.s3.us-east-2.amazonaws.com/hey_linux_amd64 && \
                        ln -s hey_linux_amd64 hey && \
                        chmod +x hey_linux_amd64 && \
                        chmod +x hey

ENV PATH=$PATH:/opt/glowroot:/opt/async-profiler:/opt
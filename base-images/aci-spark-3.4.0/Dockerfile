ARG java_image_tag=11-jre
FROM ubuntu:jammy

MAINTAINER  Madhu Babu Chakala <madhu.babu@apple.com>

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

RUN apt-get update && apt-get -y install curl gnupg apt-transport-https ca-certificates
RUN curl -k https://artifacts.apple.com/api/gpg/key/public | apt-key add -

RUN grep -q "applejdk-apt-local universal release" /etc/apt/sources.list || echo 'deb https://artifacts.apple.com/applejdk-apt-local universal release' >> /etc/apt/sources.list

RUN mkdir -p /usr/share/man/man1
RUN apt-get update -o Acquire::https::artifacts.apple.com::Verify-Peer=false && apt-get -o Acquire::https::artifacts.apple.com::Verify-Peer=false -y install applejdk-11

USER  root
RUN set -ex && \
    sed -i 's/http:\/\/deb.\(.*\)/https:\/\/deb.\1/g' /etc/apt/sources.list && \
    apt-get -o Acquire::https::artifacts.apple.com::Verify-Peer=false update && \
    ln -s /lib /lib64 && \
    apt-get install -o Acquire::https::artifacts.apple.com::Verify-Peer=false -y bash tini libc6 libpam-modules krb5-user libnss3 procps net-tools wget cron zip unzip netcat net-tools vim && \
    cd /opt && \
    wget --no-check-certificate https://artifacts.apple.com/artifactory/oss-patched-binaries-local/org/apache/spark/spark-distribution_2.13/3.4.0.26-apple/spark-distribution_2.13-3.4.0.26-apple.tgz && \
    tar -xvf spark-distribution_2.13-3.4.0.26-apple.tgz && \
    mv spark-3.4.0.26-apple-bin-3.3.4.3-apple spark && \
    rm -f spark-distribution_2.13-3.4.0.26-apple.tgz && \
    mkdir -p /opt/spark && \
    mkdir -p /opt/spark/examples && \
    mkdir -p /opt/spark/work-dir && \
    touch /opt/spark/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/* && rm -rf /var/lib/apt/lists/* && \
    cd /opt && \
    mkdir -p /opt/spark/work-dir && \
    cp /opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh /opt/ && \
    cp /opt/spark/kubernetes/dockerfiles/spark/decom.sh /opt/ && \
    chmod g+w /opt/spark/work-dir && \
    chmod a+x /opt/decom.sh && \
    wget --no-check-certificate https://github.com/glowroot/glowroot/releases/download/v0.14.0-beta.3/glowroot-0.14.0-beta.3-dist.zip && \
    unzip glowroot-0.14.0-beta.3-dist.zip && \
    rm -f glowroot-0.14.0-beta.3-dist.zip && \
    wget --no-check-certificate  https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.9/async-profiler-2.9-linux-x64.tar.gz && \
    tar -xvf async-profiler-2.9-linux-x64.tar.gz && \
    mv async-profiler-2.9-linux-x64 async-profiler && \
    rm -f async-profiler-2.9-linux-x64.tar.gz && \
    ln -s /opt/async-profiler/async-profiler-2.9-linux-x64 /opt/async-profiler/latest && \
    chown -R root:root /opt

ENV SPARK_HOME /opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$SPARK_HOME:$PATH

ENTRYPOINT [ "/opt/entrypoint.sh" ]

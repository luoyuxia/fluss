#
# Copyright (c) 2024 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# --------------
# Stream edition
# --------------
FROM registry.cn-hangzhou.aliyuncs.com/alinux/aliyunlinux:aliyun_2_1903_x64_20G_alibase_20200904 AS stream
USER root
RUN echo pwd

RUN set -e
RUN yum install -y tar \
        && cd /etc/ \
        && curl -L http://vvr-daily.oss-cn-hangzhou-zmf.aliyuncs.com/apt.tar.gz -o apt.tar.gz \
        && tar xf apt.tar.gz \
        && rm -rf apt.tar.gz \
        && cd /usr/lib/ \
	&& curl -L http://yum.tbsite.net/taobao/8/x86_64/current/ajdk/ajdk-8.20.24-20230608121657.al8.x86_64.rpm -o ajdk-8.20.24-20230608121657.al8.x86_64.rpm \
	&& rpm -ivh ajdk-8.20.24-20230608121657.al8.x86_64.rpm \
	&& rm -f ajdk-8.20.24-20230608121657.al8.x86_64.rpm \
	&& curl -L http://yum.tbsite.net/taobao/7/x86_64/current/ajdk11/ajdk11-11.0.22.22-20240417113200.alios7.x86_64.rpm -o ajdk11-11.0.22.22-20240417113200.alios7.x86_64.rpm \
	&& rpm -ivh ajdk11-11.0.22.22-20240417113200.alios7.x86_64.rpm \
	&& rm -f ajdk11-11.0.22.22-20240417113200.alios7.x86_64.rpm \
    && curl -L http://yum.tbsite.net/taobao/7/x86_64/current/ajdk17/ajdk17-17.0.5.0.5-20221117161857.alios7.x86_64.rpm -o ajdk17-17.0.5.0.5-20221117161857.alios7.x86_64.rpm \
    && rpm -ivh ajdk17-17.0.5.0.5-20221117161857.alios7.x86_64.rpm \
    && rm -f ajdk17-17.0.5.0.5-20221117161857.alios7.x86_64.rpm \
    && (yum install -y jemalloc-devel || yum install -y jemalloc-devel) \
    && rm -rf /var/cache/yum # retry because the installation of jemalloc-devel may fail

ENV JAVA_HOME=/opt/taobao/install/ajdk_8.20.24/
ENV JAVA_HOME11=/opt/taobao/install/ajdk11_11.0.22.22/
ENV JAVA_HOME17=/opt/taobao/install/ajdk17_17.0.5.0.5/
ENV PATH=${JAVA_HOME17}/bin:$PATH

RUN sed -i "s/\/\/.*deb.debian.org/\/\/mirrors.aliyun.com/g;s/\/\/.*security.debian.org/\/\/mirrors.aliyun.com/g" /etc/apt/sources.list \
	&& yum install -y tini \
    && yum install -y glibc-common \
	&& yum install -y net-tools \
	&& yum install -y telnet \
	&& yum install -y wget \
        && yum install -y curl \
	&& yum install -y apr-devel openssl-devel \
        && yum install -y procps \
        && yum install -y iputils \
	&& yum install -y initscripts \
	&& yum install -y crontabs \
    && cd / \
        && yum install -y mpfr-devel perf \
        && cd /opt/ \
        && wget https://native-resource.oss-cn-beijing.aliyuncs.com/native-tools_profiler-fix.tar.gz -O native-tools.tar.gz \
        && tar xf native-tools.tar.gz \
        && rm -rf native-tools.tar.gz \
        && cd /usr/local \
        && wget http://vvr-daily.oss-cn-hangzhou-zmf.aliyuncs.com/gcc.tar.gz \
        && tar xf gcc.tar.gz \
        && printf "/usr/local/gcc/lib64\n" > /etc/ld.so.conf.d/gcc10.conf \
        && ldconfig \
        && rm -rf gcc.tar.gz \
        && printf "\nkernel.perf_event_paranoid = -1\nkernel.yama.ptrace_scope = 0\n" >> /etc/sysctl.conf \
        && rm -rf /usr/lib/locale \
	&& rm -rf /var/cache/yum

ENV PATH=/fluss/bin:$PATH
COPY --chown=root:root build-target/ /fluss/

WORKDIR /fluss
USER root
CMD ["bash"]
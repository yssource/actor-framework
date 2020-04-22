FROM centos:7

RUN yum update -y \
   && yum install -y centos-release-scl \
   && yum install -y epel-release \
   && yum update -y \
   && yum clean all

RUN yum install -y \
  cmake3 \
  devtoolset-7 \
  devtoolset-7-libasan-devel \
  devtoolset-7-libubsan-devel \
  git \
  make \
  openssl-devel \
 && yum clean all

RUN useradd jenkins -m -s /bin/bash

RUN mkdir /home/jenkins/bin
RUN chown -R jenkins /home/jenkins
RUN chgrp -R jenkins /home/jenkins

RUN ln -s /usr/bin/cmake3 /home/jenkins/bin/cmake
RUN ln -s /usr/bin/ctest3 /home/jenkins/bin/ctest

ENV CXX=/opt/rh/devtoolset-7/root/usr/bin/g++

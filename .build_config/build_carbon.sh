#!/usr/bin/env bash
set -xe

env

export JAVA_HOME=/root/buildbox/jdk1.8.0_191
export PATH=$PATH:$JAVA_HOME/bin
export SCALA_HOME=/root/dplatform/scala-2.11.8
export PATH=$PATH:$SCALA_HOME/bin
export M2_HOME=/root/dplatform/apache-maven-3.3.9
export PATH=$PATH:$M2_HOME/bin
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m -Xms2g -Dmaven.multiModuleProjectDirectory=$M2_HOME"
export ANT_HOME=/root/buildbox/apache-ant-1.9.13
export PATH=$PATH:$ANT_HOME/bin

whoami
echo $PATH
which java
which ant
whoami

Carbon_FOLDER="${PWD}"
git log -3

# get carbon branch from 'CID_REPO_BRANCH' or 'CID_TAG_INFO'
if  [ ! -n "${CID_REPO_BRANCH}" ] ;then
    CARBON_BRANCH=${CID_TAG_INFO}
else
    CARBON_BRANCH=${CID_REPO_BRANCH}
fi
echo "Carbon branch: ${CARBON_BRANCH}"

# eg. EI_CarbonData_Kernel_Component
export COMPONENT_NAME=${COMPONENT_NAME:-EI_CarbonData_Kernel_Component}
# origin version
export COMPONENT_VERSION=${COMPONENT_VERSION:-1.6.1.0100}
export DP_VERSION=${DP_VERSION:-dplatform}
# HW_internal_version, tag name, eg. EI_CarbonData_Kernel_Component_1.6.0.0100.B001
export INTERNAL_VERSION=${CARBON_BRANCH:-1.6.0.0100.B001}
export HADOOP_VERSION=${HADOOP_VERSION:-3.1.1.0100}
export SPARK_VERSION=${SPARK_VERSION:-2.3.2.0101}
# HW_display_version
export DISPLAY_VERSION=${COMPONENT_VERSION}-${DP_VERSION}
export BUILD_VERSION=${DISPLAY_VERSION}
export CI_LOCAL_REPOSITORY="carbon_local_repository"
#mvn -s /home/tool/apache-maven-3.3.3/conf/carbon_settings.xml clean install -U -Pbuild-with-format -Pspark-2.3 -Pmv -DskipTests -Dfindbugs.skip=true -Dcheckstyle.skip=true
#-Dspark.version=${SPARK_VERSION} -Dhadoop.version=${HADOOP_VERSION} -Dbuild.version=${BUILD_VERSION}

sed -i "s/<localRepository>.*/<localRepository>${CI_LOCAL_REPOSITORY}<\/localRepository>/" .build_config/carbon_settings.xml

# change build version
echo "Carbon build version: ${BUILD_VERSION}"
cd ${Carbon_FOLDER}
mvn -s .build_config/carbon_settings.xml  versions:set -DnewVersion=${BUILD_VERSION}
mvn -s .build_config/carbon_settings.xml  versions:commit
# change dependency version
find -name pom.xml |xargs sed -i "s/<spark\.version>.*/<spark.version>${SPARK_VERSION}<\/spark.version>/"
find -name pom.xml |xargs sed -i "s/<hadoop\.version>.*/<hadoop.version>${HADOOP_VERSION}<\/hadoop.version>/"

#download dependency jars
cd ${Carbon_FOLDER}
cd .build_config/CI
mkdir -p dependency_jars/
cd dependency_jars/
yunlongRepo="http://wlg1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/sz-maven-public"
wget ${yunlongRepo}/it/unimi/dsi/fastutil/8.2.3/fastutil-8.2.3.jar
wget ${yunlongRepo}/com/google/code/gson/gson/2.4/gson-2.4.jar
wget ${yunlongRepo}/com/github/luben/zstd-jni/1.3.2-2/zstd-jni-1.3.2-2.jar
cp ${Carbon_FOLDER}/cloud/lib/huawei/ais-client-sdk-1.0.1.jar ./
cp ${Carbon_FOLDER}/cloud/lib/huawei/java-sdk-core-2.0.1.jar ./

cd ${Carbon_FOLDER}
if [  -d ${Carbon_FOLDER}/CI ]; then
rm -r CI
fi
mkdir -p CI

cp -r .build_config/CI/* CI/

cd ${Carbon_FOLDER}/CI

ant -DVERSION=${COMPONENT_VERSION} -DDISPLAY_VERSION=${DISPLAY_VERSION} -DINTERNAL_VERSION=${INTERNAL_VERSION} -DHADOOP_VERSION=${HADOOP_VERSION} -DSPARK_VERSION=${SPARK_VERSION} -DBUILD_VERSION=${BUILD_VERSION} -DCOMPONENT_NAME=${COMPONENT_NAME} -f 1.6.1_KernelCarbon_2.3_MRS.xml package

cd ${Carbon_FOLDER}
if [  -d ${Carbon_FOLDER}/package ]; then
rm -r package
fi
mkdir -p package

cp -r CI/release/*  ${Carbon_FOLDER}/package

# package the carbondata jars, poms files
cd ${Carbon_FOLDER}
mkdir -p ${Carbon_FOLDER}/org/apache/
cp -r ${CI_LOCAL_REPOSITORY}/org/apache/carbondata ${Carbon_FOLDER}/org/apache/
cd ${Carbon_FOLDER}
find ./org -regex '.*\.repositories\|.*\.zip\|.*\.sha1\|.*\.md5\|.*\.xml\|.*javadoc\.jar\|.*sources\.jar' > list.txt
cat list.txt | while read line
do
rm -rf $line
done
rm -rf list.txt
tar -zcvf carbondata_jars.tar.gz org
#rm -rf carbondata
if [ -e carbondata_jars.tar.gz ];then
  mv carbondata_jars.tar.gz ${Carbon_FOLDER}/package
fi

cd ${Carbon_FOLDER}
cd package/
# EI_CarbonData_Kernel_Component_MRS_ same as EI_CarbonData_Kernel_Component_
rm -rf EI_CarbonData_Kernel_Component_MRS_*

tar -czf ${COMPONENT_NAME}_${COMPONENT_VERSION}-${DP_VERSION}_release.tar.gz *

echo "Finished."

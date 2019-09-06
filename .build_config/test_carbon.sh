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
echo $PATH
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
export HADOOP_VERSION=${HADOOP_VERSION:-3.1.1.0200-dplatform}
export SPARK_VERSION=${SPARK_VERSION:-2.3.2.0100-dplatform}
# HW_display_version
export DISPLAY_VERSION=${COMPONENT_VERSION}-${DP_VERSION}
export BUILD_VERSION=${DISPLAY_VERSION}
export CI_LOCAL_REPOSITORY="carbon_local_repository"
sed -i "s/<localRepository>.*/<localRepository>${CI_LOCAL_REPOSITORY}<\/localRepository>/" .build_config/carbon_settings.xml
echo -e "
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <configuration>
          <forkCount>10</forkCount>
          <reuseForks>true</reuseForks>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.scalatest</groupId>
        <artifactId>scalatest-maven-plugin</artifactId>
      </plugin>
      <plugin>
        <groupId>org.codehaus.mojo</groupId>
        <artifactId>cobertura-maven-plugin</artifactId>
      </plugin>" > s.txt
keyWord="<artifactId>maven-checkstyle-plugin</artifactId>"
r1=`grep -n ${keyWord} pom.xml|awk -F: '{print $1}'|head -1`
((r2=r1-3))
sed -i "${r2} r s.txt" pom.xml
rm s.txt -rf

echo -e "
    <plugin>
      <groupId>org.codehaus.mojo</groupId>
      <artifactId>cobertura-maven-plugin</artifactId>
      <version>2.7</version>
      <configuration>
        <formats>
          <format>html</format>
          <format>xml</format>
        </formats>
        <aggregate>true</aggregate>
      </configuration>
     </plugin>" > s.txt
keyWord="<pluginManagement>"
r1=`grep -n ${keyWord} pom.xml|awk -F: '{print $1}'|head -1`
((r2=r1+1))
sed -i "${r2} r s.txt" pom.xml
rm s.txt -rf

cd ${Carbon_FOLDER}
mvn_param="-fae  -U -Pbuild-with-format -Pspark-2.3 -Pmv -Dfindbugs.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip=true -Dmaven.test.failure.ignore=true -Dspark.version=${SPARK_VERSION} -Dhadoop.version=${HADOOP_VERSION} -Dbuild.version=${BUILD_VERSION} -s ./.build_config/carbon_settings.xml"
mvn clean install $mvn_param cobertura:cobertura
echo "Test finished"
mvn $mvn_param surefire-report:report-only -Daggregate=true
echo "Report Finished."
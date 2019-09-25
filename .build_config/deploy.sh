#!/bin/bash
set -xe

export COMPONENT_NAME=${COMPONENT_NAME:-EI_CarbonData_Kernel_Component}
export COMPONENT_VERSION=${COMPONENT_VERSION:-1.6.1.0100}
export DP_VERSION=${DP_VERSION:-dplatform}
if [[ $BUILD_PLATFORM = 'aarch64' ]]; then
  DP_VERSION=${BUILD_PLATFORM}-${DP_VERSION}
fi
CARBON_RELEASE_PACKAGE=${COMPONENT_NAME}_${COMPONENT_VERSION}-${DP_VERSION}_release.tar.gz
JAR_VERSION=${COMPONENT_VERSION}-${DP_VERSION}
GROUP_ID="org.apache.carbondata"
JAR_PACKAGE="carbondata_jars.tar.gz"
WORK_DIR="${PWD}"
SETTINGS_FILE=${WORK_DIR}/carbon_settings.xml

cd ${WORK_DIR}
# download release package and get jars
wget https://dplatform-ci.obs.cn-north-5.myhuaweicloud.com/components/carbon/${CARBON_RELEASE_PACKAGE} --no-check-certificate

cd ${WORK_DIR}
#mvn setting
cat << EOF > $SETTINGS_FILE
<?xml version="1.0" encoding="UTF-8" ?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <username>cbucommon</username>
      <password>huawei@123</password>
      <id>Product-CBUCommon-release</id>
    </server>
    <server>
      <username>cbucommon</username>
      <password>huawei@123</password>
      <id>Product-CBUCommon-snapshot</id>
    </server>
  </servers>


  <mirrors>
    <mirror>
      <id>central</id>
      <mirrorOf>*,!releases,!cmc</mirrorOf>
      <url>http://szxy1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/sz-maven-public/</url>
    </mirror>
  </mirrors>
  <profiles>
    <profile>
      <id>JDK1.7</id>
      <activation>
        <activeByDefault>true</activeByDefault>
        <jdk>1.7</jdk>
      </activation>
      <properties>
        <maven.compiler.source>1.7</maven.compiler.source>
        <maven.compiler.target>1.7</maven.compiler.target>
        <maven.compiler.compilerVersion>1.7</maven.compiler.compilerVersion>
      </properties>
    </profile>
    <profile>
      <id>central</id>
      <repositories>
        <repository>
          <id>releases</id>
          <url>http://wlg1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/Product-CBUCommon-release</url>
          <releases>
            <enabled>true</enabled>
          </releases>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </repository>
        <repository>
          <id>snapshot</id>
          <url>http://wlg1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/Product-CBUCommon-snapshot</url>
          <releases>
            <enabled>true</enabled>
          </releases>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </repository>
      </repositories>
      <pluginRepositories>
        <pluginRepository>
          <id>releases_plugin</id>
          <url>http://wlg1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/Product-CBUCommon-release</url>
          <releases>
            <enabled>true</enabled>
          </releases>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </pluginRepository>
      </pluginRepositories>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>central</activeProfile>
    <activeProfile>JDK1.7</activeProfile>
  </activeProfiles>
</settings>

EOF

cd ${WORK_DIR}
tar -xzf ${CARBON_RELEASE_PACKAGE}
tar -xzf ${JAR_PACKAGE}

if [[ ${DP_VERSION} =~ "SNAPSHOT" ]]
then
    repositoryId="Product-CBUCommon-snapshot"
else
    repositoryId="Product-CBUCommon-release"
fi

cd ${WORK_DIR}
cd org/apache/carbondata
for artifact in `ls`
do
    echo "artifact: ${artifact}"
    jarPath=${artifact}/${JAR_VERSION}
    pomFile=${artifact}-${JAR_VERSION}.pom
    jarFile=${artifact}-${JAR_VERSION}.jar
    testJarFile=${artifact}-${JAR_VERSION}-tests.jar

    if [[ ! -f "${jarPath}/${pomFile}" ]]; then
        echo "error: ${jarPath}/${pomFile} not exists"
        exit 1
    fi

    if [[ -f "${jarPath}/${jarFile}" ]]; then
        echo "deploy ${pomFile} ${jarFile}"
        mvn deploy:deploy-file -DgroupId=${GROUP_ID} -DartifactId=${artifact} -Dversion=${JAR_VERSION} -Dpackaging=jar -Dfile=${jarPath}/${jarFile} -DpomFile=${jarPath}/${pomFile} -Durl=http://wlg1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/${repositoryId} -DrepositoryId=${repositoryId} -s ${SETTINGS_FILE}
    else
        echo "warn: ${jarPath}/${jarFile} not exists, only upload ${jarPath}/${pomFile}"
        mvn deploy:deploy-file -DgroupId=${GROUP_ID} -DartifactId=${artifact} -Dversion=${JAR_VERSION} -Dpackaging=pom -Dfile=${jarPath}/${pomFile} -DpomFile=${jarPath}/${pomFile} -Durl=http://wlg1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/${repositoryId} -DrepositoryId=${repositoryId} -s ${SETTINGS_FILE}
    fi

    if [[ -f "${jarPath}/${testJarFile}" ]]; then
        echo "deploy ${pomFile} ${testJarFile}"
        mvn deploy:deploy-file   -Dclassifier=tests -DgroupId=${GROUP_ID} -DartifactId=${artifact} -Dversion=${JAR_VERSION} -Dpackaging=jar -Dfile=${jarPath}/${testJarFile} -DpomFile=${jarPath}/${pomFile} -Durl=http://wlg1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/${repositoryId} -DrepositoryId=${repositoryId} -s ${SETTINGS_FILE}
    fi
done
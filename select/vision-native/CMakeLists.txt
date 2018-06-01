cmake_minimum_required(VERSION 3.9)
project(carbonvision)

set(CMAKE_CXX_STANDARD 11)

LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/thirdlib)
LINK_LIBRARIES(dl)
LINK_LIBRARIES(${PROJECT_SOURCE_DIR}/thirdlib/libblas.so.3)
LINK_LIBRARIES(${PROJECT_SOURCE_DIR}/thirdlib/libgfortran.so.3)
LINK_LIBRARIES(${PROJECT_SOURCE_DIR}/thirdlib/libcalcDistance.so)

ADD_DEFINITIONS(-D_GLIBCXX_USE_CXX11_ABI=0)

INCLUDE_DIRECTORIES($ENV{JAVA_HOME}/include)
INCLUDE_DIRECTORIES($ENV{JAVA_HOME}/include/linux)

SET(LIBRARY_OUTPUT_PATH  ${PROJECT_SOURCE_DIR}/build/lib)
SET(EXECUTABLE_OUTPUT_PATH  ${PROJECT_SOURCE_DIR}/build)

SET(SRCS
        ${PROJECT_SOURCE_DIR}/src/main/c/head/carbonvision.h
        ${PROJECT_SOURCE_DIR}/src/main/c/carbonvision.cpp)

ADD_EXECUTABLE(vision ${PROJECT_SOURCE_DIR}/src/main/c/carbonvision.cpp)

ADD_LIBRARY(carbonvision SHARED ${SRCS})
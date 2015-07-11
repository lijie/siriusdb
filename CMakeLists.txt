cmake_minimum_required(VERSION 2.8)
project(immortal)
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

# default: complie x64 release version
set(RELEASEFLAG "release")
set(PLATFLAG "x64")

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(RELEASEFLAG "debug")
else(CMAKE_BUILD_TYPE STREQUAL "")
  set(RELEASEFLAG "debug")
else(CMAKE_BUILD_TYPE STREQUAL "Release")
  set(RELEASEFLAG "release")
endif(CMAKE_BUILD_TYPE STREQUAL "Debug")

set(CMAKE_C_FLAGS "-Wall -fPIC -fno-omit-frame-pointer -DOS_LINUX -D__STDC_FORMAT_MACROS")
set(CMAKE_CXX_FLAGS "-Wall -fPIC -fno-omit-frame-pointer -DOS_LINUX -D__STDC_FORMAT_MACROS -std=c++11")

# get CWD
execute_process(
  COMMAND pwd
  OUTPUT_VARIABLE _PWD
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# helper function
#include(CMakePrintHelpers)
#cmake_print_variables(_PWD SIRIUS_SOURCE_DIR)

# default inc dir
include_directories(
  ${PROJECT_SOURCE_DIR}/protocol
)

# default lib dir
#set(PREBUILD_LIB "${PROJECT_SOURCE_DIR}/lib/linux/${PLATFLAG}/${RELEASEFLAG}")
link_directories(
  ${PROJECT_SOURCE_DIR}/lib
)

# sub projects
add_subdirectory(dbproxy)


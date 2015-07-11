include(${PROJECT_SOURCE_DIR}/cmake/common.cmake)

include_directories(
  ${SIRIUS_SOURCE_DIR}/include
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_SOURCE_DIR}/include/uv
)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DGOOGLE_CPU_PROFILE")

set(DBA_SOURCES
  dbproxy.cpp
  st_setplayer.cpp
  st_getplayer.cpp
  st_del.cpp
  st_rawsql.cpp
  mysqlwrapper.cpp
  rediswrapper.cpp
  uvwrapper.cpp
  util.cpp
  hashkit/crc32.c
  hashkit/fnv.c
  ${PROJECT_SOURCE_DIR}/protocol/dbproxy.pb.cc
)

add_executable(dbproxyd ${DBA_SOURCES} main.cpp)
target_link_libraries(dbproxyd immortalcommon sirius_net sirius_core luajit  hiredis mysqlclient_r protobuf uv pthread rt dl)
#target_link_libraries(dbproxyd profiler tcmalloc)

#add_executable(mysqltest mysqltest.cpp)
#target_link_libraries(mysqltest mysqlclient pthread rt dl)

add_executable(dbclient
  dbclient.cpp
  ${PROJECT_SOURCE_DIR}/protocol/immortaldb.pb.cc
  ${PROJECT_SOURCE_DIR}/protocol/dbproxy.pb.cc
)
target_link_libraries(dbclient protobuf uv pthread rt)

#add_executable(uvtest
#  uvtest.c
#)
#target_link_libraries(uvtest uv pthread rt)

add_executable(pb2createsql
  pb2createsql.cpp
  ${PROJECT_SOURCE_DIR}/protocol/dbproxy.pb.cc
)
target_link_libraries(pb2createsql protobuf pthread)

install(TARGETS dbproxyd
  RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/run/bin)

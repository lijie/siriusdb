#include(CMakePrintHelpers)
set(PROTOC "${PROJECT_SOURCE_DIR}/bin/protoc")
set(PROTOPATH "${PROJECT_SOURCE_DIR}/protocol")
set(GOSRC "${PROJECT_SOURCE_DIR}/go/src")
set(GOPROTOSRC "${PROJECT_SOURCE_DIR}/go/src/seasun.com/sirius/proto")

function(add_protobuf_file arg1 arg2 arg3 arg4 arg5)
  #cmake_print_variables(arg1 arg2 arg3 arg4 arg5)
  add_custom_command(
    OUTPUT ${arg1} ${args2} ${arg4}
    COMMAND ${PROTOC} -I${PROTOPATH} -I${PROJECT_SOURCE_DIR}/include --cpp_out=./ ${arg3}
    COMMAND ${PROTOC} -I${PROTOPATH} -I${PROJECT_SOURCE_DIR}/include --plugin=${PROJECT_SOURCE_DIR}/go/bin/protoc-gen-go --go_out=import_prefix=seasun.com/sirius/proto/:./ ${arg3}
    COMMAND mkdir -p ${GOPROTOSRC}/`basename ${arg5}`
    COMMAND cp -p ${arg4} ${GOPROTOSRC}/`basename ${arg5}`/
    WORKING_DIRECTORY ${PROTOPATH}
    DEPENDS ${arg3}
    DEPENDS protoc-gen-go
    )
endfunction(add_protobuf_file)

file(GLOB PROTOFILES ${PROTOPATH}/*.proto)
foreach(PROTOFILE ${PROTOFILES})
  string(REPLACE ".proto" ".pb.cc" CCNAME ${PROTOFILE})
  string(REPLACE ".proto" ".pb.h" HHNAME ${PROTOFILE})
  string(REPLACE ".proto" ".pb.go" GONAME ${PROTOFILE})
  string(REPLACE ".proto" ".pb" GODIR ${PROTOFILE})
  add_protobuf_file(${CCNAME} ${HHNAME} ${PROTOFILE} ${GONAME} ${GODIR})
  set(PROTOFILES_CC ${PROTOFILES_CC} ${CCNAME})
endforeach(PROTOFILE)

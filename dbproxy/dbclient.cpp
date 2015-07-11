#include <stdio.h>
#include <stdio.h>

#include "uv/uv.h"

#include "immortaldb.pb.h"
#include "dbproxy_comm.h"
#include "./uvwrapper.h"

using namespace immortal::common;
using immortal::uv::Header;
using immortal::uv::RespHeader;
using immortal::uv::kHeaderSize;

const int kDefaultBufferSize = 1024 * 1024;

static uv_loop_t *loop;
static uv_connect_t connect_req;
// static uv_timer_t timer;
static uv_tcp_t conn;
static uv_write_t write_req;
static char buffer[kDefaultBufferSize];
static size_t readpos = 0;

static char **cmd_argv;
static int cmd_argc;

static void read_cb(uv_stream_t* stream,
                    ssize_t nread,
                    const uv_buf_t* buf) {
  if (nread <= 0)
    return;

  readpos += nread;
  if (readpos < sizeof(RespHeader))
    return;

  uint32_t len = immortal::uv::HeaderLen(buffer);
  printf("readpos %u len %u\n", readpos, len);
  if (readpos < len)
    return;

  uint32_t cmd = immortal::uv::HeaderCmd(buffer);
  uint32_t err = immortal::uv::HeaderErr(buffer);

  if (err) {
    fprintf(stderr, "CMD %u resp ERR %u\n", cmd, err);
    uv_stop(loop);
    return;
  }

  if (cmd == dbproto::CMD_SET_RSP) {
    assert(err == 0);
    fprintf(stdout, "Received result:\n");
  } else if (cmd == dbproto::CMD_GET_RSP) {
    immortaldb::Player rsp;
    bool succ = rsp.ParseFromArray(buffer + sizeof(RespHeader),
                                   len - sizeof(RespHeader));
    if (!succ) {
      fprintf(stderr, "ParseFromArray failed\n");
      uv_stop(loop);
      return;
    }

    fprintf(stdout, "Received result:\n");
    rsp.PrintDebugString();
    printf("extend size %zd\n", rsp.extend().size());
  } else if (cmd == dbproto::CMD_RAWSQL_RSP) {
    dbproto::RawSQLResult rsp;
    bool succ = rsp.ParseFromArray(buffer + sizeof(RespHeader),
                                   len - sizeof(RespHeader));
    if (!succ) {
      fprintf(stderr, "ParseFromArray failed\n");
      uv_stop(loop);
      return;
    }

    fprintf(stdout, "Received result:\n");
    rsp.PrintDebugString();

    for (int i = 0; i < rsp.values_size(); i++) {
      immortaldb::Player p;
      p.ParseFromString(rsp.values(i));
      p.PrintDebugString();
    }
  }

  uv_stop(loop);
}

static void alloc_cb(uv_handle_t* handle,
                     size_t suggested_size,
                     uv_buf_t* buf) {
  buf->base = buffer + readpos;
  buf->len = kDefaultBufferSize - readpos;
}

static void write_cb(uv_write_t* req, int status) {
  printf("%s\n", __func__);
  uv_read_start((uv_stream_t *)&conn, alloc_cb, read_cb);
}

static void do_setplayer(char **argv) {
  Header *header = (Header *)buffer;
  header->seq = 0;
  header->cmd = dbproto::CMD_SET_REQ;

  immortaldb::Player p;
  p.set_account("testplayerid");
  p.set_name("testplayername");
  p.set_level(1);
  p.set_exp(200);
  uint32_t a = 0xdeadbeaf;
  p.set_extend((char *)&a, sizeof(a));
  immortaldb::ItemList *il = p.mutable_itemlist();
  immortaldb::Item *i = il->add_items();
  i->set_id(1);

  p.PrintDebugString();

  dbproto::Set msg;
  msg.set_msg_type("Player");
  msg.set_key("testplayername");

  bool succ = p.SerializeToString(msg.mutable_value());
  assert(succ);

  succ = msg.SerializeToArray(buffer + kHeaderSize,
                              1024 - kHeaderSize);
  assert(succ);

  header->len = kHeaderSize + msg.ByteSize();
}

static void do_getplayer(char **argv) {
  Header *header = (Header *)buffer;
  header->seq = 0;
  header->cmd = dbproto::CMD_GET_REQ;

  dbproto::Get msg;
  msg.set_msg_type("Player");
  msg.set_key("1");

  msg.PrintDebugString();
  msg.SerializeToArray(buffer + kHeaderSize,
                       1024 - kHeaderSize);
  header->len = kHeaderSize + msg.ByteSize();
}

static void do_rawsql(char **argv) {
  Header *header = (Header *)buffer;
  header->seq = 0;
  header->cmd = dbproto::CMD_RAWSQL_REQ;

  dbproto::RawSQL msg;
  msg.set_msg_type("Player");
  msg.set_sql("SELECT * from Player;");

  msg.PrintDebugString();
  msg.SerializeToArray(buffer + kHeaderSize,
                       1024 - kHeaderSize);
  header->len = kHeaderSize + msg.ByteSize();
}

static void connect_cb(uv_connect_t* req, int status) {
  uv_stream_t* stream;
  uv_buf_t send_buf;
  int r;

  assert(status == 0);
  stream = req->handle;

  if (strcmp(cmd_argv[3], "setplayer") == 0) {
    do_setplayer(&cmd_argv[4]);
  } else if (strcmp(cmd_argv[3], "getplayer") == 0) {
    do_getplayer(&cmd_argv[4]);
  } else if (strcmp(cmd_argv[3], "rawsql") == 0) {
    do_rawsql(&cmd_argv[4]);
  } else {
    fprintf(stderr, "invalid command\n");
  }

  Header *header = (Header *)buffer;
  send_buf = uv_buf_init(buffer, header->len);

  printf("send %zd bytes\n", send_buf.len);
  r = uv_write(&write_req, stream, &send_buf, 1, write_cb);
  assert(r == 0);
}

int main(int argc, char **argv) {
  struct sockaddr_in addr;
  int r;
  loop = uv_default_loop();
  
  r = uv_ip4_addr(argv[1], atoi(argv[2]), &addr);
  assert(r == 0);

  uv_tcp_init(loop, &conn);

  cmd_argv = argv;
  cmd_argc = argc;
  r = uv_tcp_connect(&connect_req, &conn, (const struct sockaddr *)&addr, connect_cb);
  assert(r == 0);

  uv_run(loop, UV_RUN_DEFAULT);
  return 0;
}

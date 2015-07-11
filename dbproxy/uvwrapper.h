#ifndef CODE_SOURCE_SERVER_UVWRAPPER_H_
#define CODE_SOURCE_SERVER_UVWRAPPER_H_

#include <assert.h>
#include <arpa/inet.h>
#include <string>
#include "common/netpack/net_pack.h"
#include "common/utility/statemachine.h"
#include "uv/uv.h"

namespace immortal {
namespace uv {
using common::MsgPack;

typedef MsgPack Header;

const int kHeaderSize = sizeof(Header);

inline uint32_t HeaderLen(const char *p) {
  return (reinterpret_cast<const Header *>(p))->len;
}
inline uint32_t HeaderCmd(const char *p) {
  return (reinterpret_cast<const Header *>(p))->cmd;
}
inline uint32_t HeaderSeq(const char *p) {
  return (reinterpret_cast<const Header *>(p))->seq;
}

typedef MsgPack RespHeader;
inline uint16_t HeaderErr(const char *p) {
  return (reinterpret_cast<const RespHeader *>(p))->retcode;
}

const int kMaxRecvBufferSize = 32 * 1024;
// using immortal::common::Handle;

// event loop
class Loop;
// tcp
class TCP;
class TCPConn;
class TCPWriteRequest;
class TCPConnListener;

// timer
class Timer;
class TimerListener;

// Loop 事件主循环
class Loop {
 public:
  friend class TCP;
  friend class TCPConn;
  Loop();
  int Run(uv_run_mode mode);
  void Stop();
  // Now returns current time
  uint64_t Now();

  // TODO(lijie3): will remove
  uv_loop_t * loop() { return &loop_; }

 protected:
  uv_loop_t loop_;
};

// TCPConnListener 监听TCP连接的各种事件
class TCPConnListener {
 public:
  // 连接有数据可读
  virtual void OnRead(TCPConn *, ssize_t nread, const uv_buf_t* buf);
  // 为读分配空间
  virtual void OnAlloc(TCPConn *, size_t suggested_size, uv_buf_t *buf);
  // 发送数据完毕
  virtual void OnWriteDone(TCPConn *, TCPWriteRequest *, int status);
  // 对端关闭连接
  virtual void OnClose(TCPConn *) {};
  // 主动关闭连接完成
  virtual void OnCloseDone(TCPConn *) {};
  // 主动发起连接完成
  virtual void OnConnected(TCPConn *, int status) {};

  // < 0 error, close this conn
  // == 0 request not complete yet
  // > 0 complete, returns size
  virtual ssize_t CheckComplete(const char *buf, size_t len);
  virtual void HandleRequest(TCPConn *, const char *buf, size_t len) = 0;

 private:
  ssize_t decode(TCPConn *conn, const char *buf, size_t len);
};

class TCPWriteRequest {
 public:
  friend class TCPConn;
  char *buf;
  size_t size;
  int cmd;
  uint64_t start_time;
  virtual ~TCPWriteRequest() {}

 private:
  uv_write_t write_req_;
  TCPConn *conn_;
};

// TCP Server
class TCP {
  // friend class TCPServerListener;
 public:
  // 初始化, 须绑定主循环
  int Init(Loop *);
  // 监听
  int Listen(const char *ip, uint16_t port);
  // 接受新连接
  int Accept(TCPConn *new_conn);
  // 默认就是执行Accept, 并使用NewTCPConn()分配新连接
  virtual void OnNewConnection(TCP *tcp);
  // override该函数可以在accept时自定义TCPConn
  virtual TCPConn *NewTCPConn();
  
  void set_conn_listener(TCPConnListener *l) { conn_listener_ = l; }

  virtual ~TCP() {}

  struct sockaddr_in LocalAddr_;

 protected:
  TCPConnListener *conn_listener_;

 private:
  // callback of libuv
  static void TCPOnNewConnection(uv_stream_t *, int );
  // handle of libuv
  uv_tcp_t handle_;
  // pointer of main loop
  Loop *loop_;
};

// TCP connection
class TCPConn {
 public:
  friend class TCP;
  friend class Loop;

  // 发起连接
  int Connect(const std::string& ip, uint16_t port, Loop *loop);
  // 监听读事件
  int ReadStart();
  // 向对端发送数据
  int Write(TCPWriteRequest *, const char *buf, size_t size);
  // 关闭连接
  void Close();
  //
  bool IsConnected() { return alive_; }
  // interfaces can be override:
  
  // 连接有数据可读
  virtual void OnRead(TCPConn *, ssize_t nread, const uv_buf_t* buf);
  // 为读分配空间
  virtual void OnAlloc(TCPConn *, size_t suggested_size, uv_buf_t *buf);
  // 发送数据完毕
  virtual void OnWriteDone(TCPConn *, TCPWriteRequest *, int status);
  // 对端关闭连接
  virtual void OnClose(TCPConn *) {};
  // 主动关闭连接完成
  virtual void OnCloseDone(TCPConn *) {};
  // 主动发起连接完成
  virtual void OnConnected(TCPConn *, int status) {};

  // 包完整性检查函数
  // Override处理自定义header
  // < 0 error, close this conn
  // == 0 request not complete yet
  // > 0 complete, returns size
  virtual ssize_t CheckComplete(const char *buf, size_t len);

  // 包处理
  virtual void HandleRequest(TCPConn *, const char *buf, size_t len) {};

  TCPConn(): alive_(false), rdpos_(0) {}
  virtual ~TCPConn() {}

 private:
  uv_tcp_t handle_;

  bool alive_;
  size_t rdpos_;
  char recvbuf_[kMaxRecvBufferSize];

  static void ConnAlloc(uv_handle_t* handle,
                        size_t suggested_size,
                        uv_buf_t* buf);

  static void ConnRead(uv_stream_t* handle,
                       ssize_t nread,
                       const uv_buf_t* buf);

  static void TCPWriteCB(uv_write_t* wreq, int status);
  static void TCPConnClose(uv_handle_t *handle);
  static void OnConnected(uv_connect_t *, int status);
  ssize_t decode(TCPConn *conn, const char *buf, size_t len);
};

class TimerListener {
 public:
  virtual void OnTimeout(Timer *timer) = 0;
  virtual ~TimerListener() {}
};

class Timer {
 public:
  Timer(): listener_(NULL), loop_(NULL), running_(false) {};
  void Init(Loop *, TimerListener *);
  int Start(uint64_t timeout, uint64_t repeat);
  int Stop();

  static void OnTimeout(uv_timer_t* handle);
 private:
  uv_timer_t uv_timer_;
  TimerListener *listener_;
  Loop *loop_;
  bool running_;
};

// helper function
TCPWriteRequest * NewTCPWriteRequest(size_t allocsize);
void DelTCPWriteRequest(TCPWriteRequest *r);
}
}

#endif  // CODE_SOURCE_SERVER_UVWRAPPER_H_

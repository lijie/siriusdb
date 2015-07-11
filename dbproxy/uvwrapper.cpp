#include <stdio.h>
#include <string.h>
#include "uv/uv.h"
#include "./uvwrapper.h"

extern void dbproxy_get_stat(uint64_t start_time);
extern void dbproxy_set_stat(uint64_t start_time);
namespace immortal {
namespace uv {

Loop::Loop() {
  uv_loop_init(&loop_);
}

int Loop::Run(uv_run_mode mode) {
  return uv_run(&loop_, mode);
}

void Loop::Stop() {
  uv_stop(&loop_);
}

uint64_t Loop::Now() {
  return uv_now(&loop_);
}

int TCP::Init(Loop *l) {
  loop_ = l;
  return 0;
}

void TCP::TCPOnNewConnection(uv_stream_t *stream, int status) {
  if (status != 0) {
    // TODO(lijie3): ignore this error ?
    fprintf(stderr, "create new client connection failed!\n");
    return;
  }

  TCP *s = (TCP *)(stream->data);
  // s->listener_->OnNewConnection(s);
  s->OnNewConnection(s);
}

int TCP::Listen(const char *ip, uint16_t port) {
  uv_tcp_init(&loop_->loop_, &handle_);
  struct sockaddr_in addr;

  int r = uv_ip4_addr(ip, port, &addr);
  uv_tcp_bind(&handle_, (struct sockaddr *)&addr, 0);
  if (r) {
    fprintf(stderr, "Listen: bind failed\n");
    return -1;
  }

  // listener_ = listener;
  handle_.data = this;
  r = uv_listen((uv_stream_t *)&handle_, 8192,
                TCP::TCPOnNewConnection);
  if (r) {
    fprintf(stderr, "InitTCPServer uv_listen failed\n");
    return -1;
  }
  int len = sizeof(LocalAddr_);
  uv_tcp_getsockname(&handle_, (struct sockaddr *)&LocalAddr_, &len);
  // fprintf(stdout, "listen port %d\n", ntohs(addr2.sin_port));
  return 0;
}

TCPConn * TCP::NewTCPConn() {
  return new TCPConn;
}

void TCP::OnNewConnection(TCP *tcp) {
  TCPConn *conn = NewTCPConn();
  int n = tcp->Accept(conn);
  if (n != 0) {
    fprintf(stderr, "Accept error \n");
    return;
  }
  conn->alive_ = true;
}

void TCPConn::ConnAlloc(uv_handle_t* handle,
                        size_t suggested_size,
                        uv_buf_t* buf) {
  TCPConn *c = (TCPConn *)(handle->data);
  // printf("c %p listener %p\n", c, c->listener_);
  c->OnAlloc(c, suggested_size, buf);
}

void TCPConn::ConnRead(uv_stream_t* handle,
                       ssize_t nread,
                       const uv_buf_t* buf) {
  TCPConn *c = (TCPConn *)(handle->data);
  c->OnRead(c, nread, buf);
}

void TCPConn::TCPWriteCB(uv_write_t* wreq, int status) {
  TCPWriteRequest *r = (TCPWriteRequest *)(wreq->data);
  TCPConn *c = r->conn_;
  c->OnWriteDone(c, r, status);
}

void TCPConn::TCPConnClose(uv_handle_t *handle) {
  TCPConn *c = (TCPConn *)(handle->data);
  c->alive_ = false;
  c->OnCloseDone(c);
}

void TCPConn::OnConnected(uv_connect_t *req, int status) {
  TCPConn *c = (TCPConn *)req->data;
  free(req);
  c->alive_ = true;
  c->OnConnected(c, status);
}

int TCPConn::Connect(const std::string& ip, uint16_t port,
                     Loop *loop) {
  struct sockaddr_in addr;
  int r = uv_ip4_addr(ip.c_str(), port, &addr);
  if (r != 0) {
    return r;
  }

  uv_tcp_init(&loop->loop_, &handle_);

  uv_connect_t *req = (uv_connect_t *)calloc(sizeof(*req), 1);
  req->data = this;
  r = uv_tcp_connect(req, &handle_, (struct sockaddr *)&addr, TCPConn::OnConnected);
  if (r != 0) {
    free(req);
    return r;
  }

  return 0;
}

void TCPConn::OnAlloc(TCPConn *conn,
                      size_t suggested_size,
                      uv_buf_t* buf) {
  buf->base = conn->recvbuf_ + conn->rdpos_;
  buf->len = kMaxRecvBufferSize - conn->rdpos_;
}

void TCPConn::OnWriteDone(TCPConn *conn,
                          TCPWriteRequest *req,
                          int status) {
  if (status != 0) {
    fprintf(stderr, "Write err %d\n", status);
  }

#if 1
  if (req->cmd == 110) {
    dbproxy_get_stat(req->start_time);
  } else if (req->cmd == 108) {
    dbproxy_set_stat(req->start_time);
  }
#endif

  DelTCPWriteRequest(req);
  return;
}

ssize_t TCPConn::CheckComplete(const char *buf, size_t len) {
  if (len < kHeaderSize) {
    // not complete, wait
    return 0;
  }
  uint32_t packetlen = HeaderLen(buf);
  if (len < packetlen) {
    // not complete, wait
    // printf("current len %zd, expect %zd\n", len, packetlen);
    return 0;
  }
  return (ssize_t)packetlen;
}

ssize_t TCPConn::decode(TCPConn *conn, const char *buf, size_t len) {
  ssize_t n;
  size_t decode_size = 0;

  do {
    n = CheckComplete(buf + decode_size, len - decode_size);
    if (n < 0) {
      SI_ASSERT(0);
      return n;
    }

    if (n == 0) {
      // printf("len %zd but node complete\n", len - decode_size);
      return decode_size;
    }

    HandleRequest(conn, buf + decode_size, n);
    decode_size += n;
  } while (1);

  return decode_size;
}

void TCPConn::OnRead(TCPConn *conn,
                     ssize_t nread,
                     const uv_buf_t* buf) {
  if (nread < 0) {
    /* Error or EOF */
    fprintf(stderr, "close client for error %s\n", uv_err_name(nread));
    uv_read_stop((uv_stream_t *)&conn->handle_);
    conn->alive_ = false;
    conn->OnClose(conn);
    // TODO(lijie3): use factory
    // delete conn;
    return;
  }

  if (nread == 0)
    return;

  // printf("tcp conn read %ld %zd\n", nread, conn->rdpos_);
  conn->rdpos_ += nread;

  ssize_t decode_size = decode(conn, conn->recvbuf_, conn->rdpos_);
  if (decode_size < 0) {
    // TODO(lijie3):
    // error, should close conn?
    SI_ASSERT(0);
    return;
  }

  // if has reset data
  if (conn->rdpos_ > (size_t)decode_size) {
    // copy rest data
    memmove(conn->recvbuf_, conn->recvbuf_ + decode_size, conn->rdpos_ - decode_size);
    conn->rdpos_ = conn->rdpos_ - decode_size;
  } else {
    conn->rdpos_ = 0;
  }
}

int TCP::Accept(TCPConn *conn) {
  int r = uv_tcp_init(&loop_->loop_, &conn->handle_);
  if (r) {
    fprintf(stderr, "tcp conn init failed\n");
    return -1;
  }

  r = uv_accept((uv_stream_t *)&handle_, (uv_stream_t *)&conn->handle_);
  if (r) {
    fprintf(stderr, "client tcp accept failed\n");
    return -1;
  }

  conn->handle_.data = conn;
  uv_tcp_nodelay(&conn->handle_, 1);
  r = uv_read_start((uv_stream_t *)&conn->handle_,
                    TCPConn::ConnAlloc, TCPConn::ConnRead);
  if (r) {
    fprintf(stderr, "client tcp read start failed\n");
    return -1;
  }

  return 0;
}

int TCPConn::ReadStart() {
  handle_.data = this;
  int r = uv_read_start((uv_stream_t *)&handle_,
                        TCPConn::ConnAlloc, TCPConn::ConnRead);
  if (r) {
    fprintf(stderr, "client tcp read start failed\n");
    return -1;
  }
  return 0;
}

int TCPConn::Write(TCPWriteRequest *r, const char *buf, size_t len) {
  uv_buf_t uvbuf;
  uvbuf.base = (char *)buf;
  uvbuf.len = len;
  memset(&r->write_req_, 0, sizeof(r->write_req_));
  r->write_req_.data = r;
  r->conn_ = this;
  return uv_write(&r->write_req_, (uv_stream_t *)&handle_, &uvbuf, 1, TCPConn::TCPWriteCB);
}

void TCPConn::Close() {
  handle_.data = this;
  uv_close((uv_handle_t*) &handle_, TCPConn::TCPConnClose);
}

TCPWriteRequest * NewTCPWriteRequest(size_t allocsize) {
  char *ptr = (char *)malloc(allocsize + sizeof(TCPWriteRequest));
  if (ptr == NULL)
    return NULL;

  TCPWriteRequest *r = new (ptr) TCPWriteRequest;
  r->buf = ptr + sizeof(TCPWriteRequest);
  return r;
}

void DelTCPWriteRequest(TCPWriteRequest *r) {
  r->~TCPWriteRequest();
  free(r);
}

void Timer::OnTimeout(uv_timer_t* handle) {
  Timer *t = (Timer *)handle->data;
  t->listener_->OnTimeout(t);
}

void Timer::Init(Loop *l, TimerListener *listener) {
  loop_ = l;
  running_ = false;
  listener_ = listener;
  memset(&uv_timer_, 0, sizeof(uv_timer_));
  uv_timer_.data = this;
  uv_timer_init(l->loop(), &uv_timer_);
}


int Timer::Start(uint64_t timeout, uint64_t repeat) {
  return uv_timer_start(&uv_timer_, Timer::OnTimeout, timeout, repeat);
}

}  // namespace online
}  // namespace immortal 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <uv/uv.h>

#define kMaxRecvBufferSize (32 * 1024)
#define kBodySize 4096

#define container_of(ptr, type, member) \
  ((type *) ((char *) (ptr) - offsetof(type, member)))

struct tcp_client {
  uv_tcp_t handle;
  char *recvbuf;
  size_t rdpos;
};

struct request {
  uv_write_t w;
  uint64_t start;
  char payload[0];
};

static uv_loop_t loop;
static uint32_t delay1 = 0;
static uint32_t delay2 = 0;

struct Header {
  uint32_t  cmd;
  uint32_t  len;
  uint32_t  seq;
  uint16_t  magic;
  uint16_t  retcode;
} __attribute__((packed));

const int kHeaderSize = sizeof(struct Header);

inline uint32_t HeaderLen(const char *p) {
  return ((const struct Header *)(p))->len;
}
inline uint32_t HeaderCmd(const char *p) {
  return ((const struct Header *)(p))->cmd;
}
inline uint32_t HeaderSeq(const char *p) {
  return ((const struct Header *)(p))->seq;
}

void on_write_done(uv_write_t* w, int status) {
  struct request *req;
  uint64_t now;
  if (status != 0) {
    fprintf(stderr, "Write err %d\n", status);
  }
  req = container_of(w, struct request, w);
  now = uv_now(&loop);
  if (now - req->start > 100)
    delay1++;
  free(req);
}
  
int reply(struct tcp_client *client, struct request *req, char *buf, uint32_t len) {
  uv_buf_t uvbuf;
  uvbuf.base = (char *)buf;
  uvbuf.len = len;
  memset(&req->w, 0, sizeof(req->w));
  return uv_write(&req->w, (uv_stream_t *)&client->handle, &uvbuf, 1, on_write_done);
}

void proc(struct tcp_client *client, const char *src, size_t len) {
  struct request *req = (struct request *)malloc(sizeof(struct request) + sizeof(struct Header) + kBodySize);
  req->start = uv_now(&loop);
  struct Header *header = (struct Header *)(&req->payload[0]);
  header->cmd = 110;
  header->seq = HeaderSeq(src);
  header->retcode = 0;
  header->len = sizeof(struct Header) + kBodySize;
  header->magic = 53556;
  reply(client, req, (char *)(&req->payload[0]), header->len);
}

void on_alloc(uv_handle_t* handle,
              size_t suggested_size,
              uv_buf_t* buf) {
  struct tcp_client *client = container_of(handle, struct tcp_client, handle);
  buf->base = client->recvbuf + client->rdpos;
  buf->len = kMaxRecvBufferSize - client->rdpos;
}

ssize_t CheckComplete(const char *buf, size_t len) {
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

ssize_t decode(struct tcp_client *client, const char *buf, size_t len) {
  ssize_t n;
  size_t decode_size = 0;

  do {
    n = CheckComplete(buf + decode_size, len - decode_size);
    if (n < 0) {
      assert(0);
      return n;
    }

    if (n == 0) {
      // printf("len %zd but node complete\n", len - decode_size);
      return decode_size;
    }

    proc(client, buf + decode_size, n);
    decode_size += n;
  } while (1);

  return decode_size;
}

void on_read(uv_stream_t* handle,
             ssize_t nread,
             const uv_buf_t* buf) {
  struct tcp_client *client = container_of(handle, struct tcp_client, handle);
  if (nread < 0) {
    /* Error or EOF */
    fprintf(stderr, "close client for error %s\n", uv_err_name(nread));
    uv_read_stop(handle);
    free(client);

    printf("delay1 %u\n", delay1);
    printf("delay2 %u\n", delay2);
    delay1 = 0;
    delay2 = 0;
    return;
  }

  if (nread == 0)
    return;

  // printf("tcp conn read %ld %zd\n", nread, client->rdpos);
  client->rdpos += nread;

  ssize_t decode_size = decode(client, client->recvbuf, client->rdpos);
  if (decode_size < 0) {
    assert(0);
    return;
  }

  // if has reset data
  if (client->rdpos > (size_t)decode_size) {
    // copy rest data
    memmove(client->recvbuf, client->recvbuf + decode_size, client->rdpos - decode_size);
    client->rdpos = client->rdpos - decode_size;
  } else {
    client->rdpos = 0;
  }
}
  
void on_new_connection(uv_stream_t *stream, int status) {
  struct tcp_client *client;
  int r;
  
  if (status != 0) {
    // TODO(lijie3): ignore this error ?
    fprintf(stderr, "create new client connection failed!\n");
    return;
  }

  client = (struct tcp_client *)malloc(sizeof(struct tcp_client));
  r = uv_tcp_init(&loop, &client->handle);
  assert(r == 0);
  uv_tcp_nodelay(&client->handle, 1);

  r = uv_accept(stream, (uv_stream_t *)&client->handle);
  assert(r == 0);

  client->recvbuf = (char *)malloc(kMaxRecvBufferSize);
  client->rdpos = 0;

  r = uv_read_start((uv_stream_t *)&client->handle, on_alloc, on_read);
  assert(r == 0);
}

int main(int argc, char **argv) {
  uv_tcp_t handle;
  struct sockaddr_in addr;
  int r;
  
  uv_loop_init(&loop);
  uv_tcp_init(&loop, &handle);
  
  r = uv_ip4_addr("0.0.0.0", 30100, &addr);
  uv_tcp_bind(&handle, (struct sockaddr *)&addr, 0);
  if (r) {
    fprintf(stderr, "Listen: bind failed\n");
    return -1;
  }

  // handle.data = this;
  r = uv_listen((uv_stream_t *)&handle, 8192,
                on_new_connection);
  if (r) {
    fprintf(stderr, "InitTCPServer uv_listen failed\n");
    return -1;
  }

  return uv_run(&loop, UV_RUN_DEFAULT);
}

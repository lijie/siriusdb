// Copyright (c) 2014 kingsoft

#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "hiredis/adapters/libuv.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"

#include "./rediswrapper.h"

#ifndef see_here
#define see_here dba_err("check here [%s:%d:%s]\n", __FILE__, __LINE__, __func__)
#endif

#ifndef dba_dbg
#define dba_dbg(fmt, ...) \
  fprintf(stdout, fmt, ##__VA_ARGS__);
#endif

#ifndef dba_err
#define dba_err(fmt, ...) \
  fprintf(stderr, fmt, ##__VA_ARGS__);
#endif

namespace immortal {
namespace db {

using std::string;
std::tuple<string, uint16_t> ParseAddr(const string& addr);

static void ConnectCallback(const redisAsyncContext *c, int status) {
  if (status != REDIS_OK) {
    fprintf(stderr, "Error: %s\n", c->errstr);
    auto r = (RedisClient *)c->data;
    r->InitRedis();
    return;
  }
  printf("Connected...\n");
}

static void DisconnectCallback(const redisAsyncContext *c, int status) {
  if (status != REDIS_OK) {
    fprintf(stderr, "Error: %s\n", c->errstr);
  }
  printf("Disconnected...and try to reconnect\n");
  auto r = (RedisClient *)c->data;
  r->InitRedis();
}

static void RedisSetCallback2(redisAsyncContext *c, void *r, void *privdata) {
  uint32_t seq = (uintptr_t)privdata;
  auto cb = ((RedisClient *)c->data)->cb_;
  redisReply *reply = (redisReply *)r;
  int err = 0;

  if (reply == NULL) {
    dba_err("redis bug!? reply is NULL\n");
    err = -1;
    return;
  } else if (reply->type != REDIS_REPLY_STATUS) {
    dba_err("redis bug!? HMSET reply should be status, but %d\n", reply->type);
  } else {
    // dba_dbg("%s\n", reply->str);
  }

  cb(seq, NULL, 0, err);
}

static void RedisCommandCallback(redisAsyncContext *c, void *r, void *privdata) {
  uint32_t seq = (uintptr_t)privdata;
  auto cb = ((RedisClient *)c->data)->cb_;
  redisReply *reply = (redisReply *)r;
  int err = 0;

  if (reply == NULL) {
    dba_err("redis bug!? reply is NULL\n");
    err = -1;
    return;
  } else if (reply->type == REDIS_REPLY_STATUS) {
  } else {
  }

  cb(seq, NULL, 0, err);
}

static void RedisGetCallback2(redisAsyncContext *c, void *r, void *privdata) {
  uint32_t seq = (uintptr_t)privdata;
  auto cb = ((RedisClient *)c->data)->cb_;
  redisReply *reply = (redisReply *)r;
  int err = 0;

  if (reply == NULL) {
    dba_err("redis bug!? reply is NULL\n");
    return;
  } else if (reply->type == REDIS_REPLY_NIL) {
    // dba_dbg("no data\n");
  } else if (reply->type != REDIS_REPLY_STRING) {
    dba_err("redis bug!? GET reply should be str, but %d\n", reply->type);
    err = -1;
  }

  cb(seq, reply->str, reply->len, err);
}

int RedisClient::Get(uint32_t seq, const char *key) {
  int r;
  r = redisAsyncCommand(redis_ctx_, RedisGetCallback2,
                        (void *)(uintptr_t)seq, "GET %s", key);
  if (r != 0) {
    dba_err("redisAsyncCommand GET error\n");
    return -1;
  }

  return 0;
}

int RedisClient::Set(uint32_t seq, const char *key,
                   const char *buf, size_t len) {
  int r;
  r = redisAsyncCommand(redis_ctx_, RedisSetCallback2,
                        (void *)(uintptr_t)seq, "SET %s %b", key, buf, len);
  if (r != 0) {
    dba_err("redisAsyncCommand SET error\n");
    return -1;
  }

  return 0;
}

int RedisClient::Del(uint32_t seq, const char *key) {
  int r;
  r = redisAsyncCommand(redis_ctx_, RedisSetCallback2,
                        (void *)(uintptr_t)seq, "DEL %s", key);
  if (r != 0) {
    dba_err("redisAsyncCommand DEL error\n");
    return -1;
  }

  return 0;
}

int RedisClient::Command(uint32_t seq, const char *fmt, ...) {
  int r;
  va_list ap;
  va_start(ap, fmt);
  r = redisvAsyncCommand(redis_ctx_, RedisCommandCallback, (void *)(uintptr_t)seq, fmt, ap);
  va_end(ap);

  if (r != 0) {
    dba_err("redisvAsyncCommand error\n");
    return -1;
  }

  return 0;
}

#if 0
int RedisClient::HSet(Handle *req, google::protobuf::Message *msg) {
  dbaproto::Record *save = (dbaproto::Record *)(msg);
  // TODO(lijie3): select redis conn by table name
  req->data = dba_;

  int r;

  // Start copy save to redis protocol

  // COMMAND KEY args...
  int argc = 2 + save->values_size() * 2;

  const char **argv = (const char **)malloc(sizeof(char *) * argc);
  size_t *argvlen = (size_t *)malloc(sizeof(size_t) * argc);

  // set command
  argv[0] = "HMSET";
  argvlen[0] = strlen(argv[0]);
  // set key
  argv[1] = save->key().value().c_str();
  argvlen[1] = save->key().value().size();
  // set args
  for (int i = 0, j = 2; i < save->values_size(); i++) {
    argv[j] = (char *)(save->values(i).field().c_str());
    argvlen[j] = save->values(i).field().size();
    j++;
    argv[j] = (char *)(save->values(i).value().c_str());
    argvlen[j] = save->values(i).value().size();
    j++;
  }

  r = redisAsyncCommandArgv(redis_ctx_, RedisHSetCallback, req,
                            argc, argv, argvlen);
  free(argv);
  free(argvlen);

  if (r != 0) {
    fprintf(stderr, "redisAsyncCommand HMSET error\n");
    return -1;
  }
  return 0;
}
#endif

int RedisClient::InitRedis() {
  printf("connect redis %s:%d\n", ip_.c_str(), port_);
  redis_ctx_ = redisAsyncConnect(ip_.c_str(), port_);
  if (redis_ctx_->err) {
    fprintf(stderr, "Error: %s\n", redis_ctx_->errstr);
    redisAsyncFree(redis_ctx_);
    return -1;
  }

  int r = redisLibuvAttach(redis_ctx_, loop_);
  if (r != REDIS_OK) {
    fprintf(stderr, "redis attach to libuv failed\n");
    redisAsyncFree(redis_ctx_);
    return -1;
  }

  // save current env
  redis_ctx_->data = (void *)this;

  redisAsyncSetConnectCallback(redis_ctx_, ConnectCallback);
  redisAsyncSetDisconnectCallback(redis_ctx_, DisconnectCallback);
  return 0;
}

int RedisClient::Init(RedisCallback_t cb, void *loop, const std::string& ip, uint32_t port) {
  cb_ = cb;
  loop_ = (uv_loop_t *)loop;
  ip_ = ip;
  port_ = port;
  return InitRedis();
}

int RedisClient::Init(RedisCallback_t cb, void *loop, const std::string& url) {
  auto tup = ParseAddr(url);
  auto ip = std::get<0>(tup);
  auto port = std::get<1>(tup);
  return Init(cb, loop, ip, port);
}

}  // namespace dba
}  // namespace immortal

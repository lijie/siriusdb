#ifndef CODE_SOURCE_SERVER_REDISWRAPPER_H_
#define CODE_SOURCE_SERVER_REDISWRAPPER_H_

#include <assert.h>
#include <string>
#include <functional>
#include "./uvwrapper.h"

struct redisAsyncContext;

namespace immortal {
namespace db {
// const int kRedisGetNoData = -10000;

typedef std::function<void(uint32_t seq, char *buf, size_t len, int err)> RedisCallback_t;

class RedisClient {
 public:
  int Init(RedisCallback_t cb, void *loop, const std::string& ip, uint32_t port);
  int Init(RedisCallback_t cb, void *loop, const std::string& url);

  int Get(uint32_t seq, const char *key);
  int Set(uint32_t seq, const char *key,
          const char *buf, size_t len);
  int Del(uint32_t seq, const char *key);
  int Command(uint32_t seq, const char *fmt, ...);
  int InitRedis();

  RedisCallback_t cb_;

 private:
  uv_loop_t *loop_;
  struct redisAsyncContext *redis_ctx_;
  std::string ip_;
  uint32_t port_;
};

}
}

#endif  // CODE_SOURCE_SERVER_REDISWRAPPER_H_

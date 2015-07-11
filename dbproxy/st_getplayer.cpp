#include "./dbproxy_comm.h"
#include "dbproxy.pb.h"

namespace immortal {
namespace db {

using std::string;
using google::protobuf::Message;

enum {
  kGetStart = 1,
  kGetGetCache,
  kGetGetDB,
  kGetSetCache,
  kGetNR,
};

struct GetCtx : public Handle {
  dbproto::Get req;
  Message *msg;
  GetCtx(): msg(NULL)  {}
};

class GetState : public st::StateMachine {
 public:
  GetState();
  void set_server(DBProxy *s) { server_ = s; };

 public:
  int Start(st::State *s, st::Action *act);
  int GetCache(st::State *s, st::Action *act);
  int GetDB(st::State *s, st::Action *act);
  int SetCache(st::State *s, st::Action *act);

  st::State * StateInit(st::Action *act);
  int StateTimeout(st::State *s);
  int StateFinish(st::State *s);

 private:
  DBProxy *server_;
};

int GetState::Start(st::State *s, st::Action *act) {
  GetCtx *ctx = (GetCtx *)s->ctx;
  // Handle *h = &ctx->h;
  bool succ = ctx->req.ParseFromArray(act->data.buf + uv::kHeaderSize,
                                      act->data.len - uv::kHeaderSize);
  if (!succ) {
    ctx->err = dbproto::ERR_UNPACK;
    server_->Reply(ctx, NULL);
    return 0;
  }

  LogMessage1("Get", &ctx->req);

  server_->InitDBAndRedis(ctx, ctx->req.key());

  if (ctx->req.flags() & dbproto::FLAG_DB_ONLY) {
    server_->DBGet(ctx, ctx->req.msg_type(), ctx->req.key());
    return kGetGetDB;
  }

  // TODO(lijie3): cacheset mayfailed here?
  string cache_key = ctx->req.msg_type() + "_" + ctx->req.key();
  server_->CacheGet(ctx, cache_key.c_str());
  return kGetGetCache;
}

int GetState::GetCache(st::State *s, st::Action *act) {
  GetCtx *ctx = (GetCtx *)s->ctx;

  if (act->err) {
    ctx->err = dbproto::ERR_RWCACHE;
    server_->Reply(ctx, NULL);
    return 0;
  }

  if (act->data.buf == NULL) {
    if (ctx->req.flags() & dbproto::FLAG_CACHE_ONLY) {
      ctx->err = dbproto::ERR_NODATA;
      server_->Reply(ctx, NULL);
      LogDebug("no data\n");
      return 0;
    }
    server_->dbstat_[kDBStatGet].cachemiss++;
    LogDebug("cache miss!\n");
    // no data from cache, goto db
    server_->DBGet(ctx, ctx->req.msg_type(), ctx->req.key());
    return kGetGetDB;
  }

  server_->Reply(ctx, act->data.buf, act->data.len);
  return 0;
}

int GetState::GetDB(st::State *s, st::Action *act) {
  GetCtx *ctx = (GetCtx *)s->ctx;

  if (act->err) {
    ctx->err = dbproto::ERR_RWCACHE;
    server_->Reply(ctx, NULL);
    return 0;
  }
  auto res = (std::vector<void *> *)act->data.buf;
  if (res->size() == 0) {
    ctx->err = dbproto::ERR_NODATA;
    server_->Reply(ctx, NULL);
    return 0;
  }

  ctx->msg = ctx->dbclient->NewMessage(ctx->req.msg_type());

  // db has data, convert to protobuf
  int n = Res2PB((*res)[0], ctx->msg);
  if (n == kErrNoData) {
    ctx->err = dbproto::ERR_NODATA;
    server_->Reply(ctx, NULL);
    return 0;
  }
    
  // flush data to cache
  string cache_key = ctx->req.msg_type() + "_" + ctx->req.key();
  server_->CacheSetMsg(ctx, cache_key.c_str(), ctx->msg);
  return kGetSetCache;
}

int GetState::SetCache(st::State *s, st::Action *act) {
  GetCtx *ctx = (GetCtx *)s->ctx;
  if (act->err) {
    ctx->err = dbproto::ERR_RWCACHE;
  }
  server_->Reply(ctx, ctx->msg);
  return 0;
}

int GetState::StateTimeout(st::State *s) {
  GetCtx *ctx = (GetCtx *)s->ctx;
  // LogError("Get operation timeout %d\n", s->step_);
  server_->DelHandleSeq(ctx->curr_seq);
  ctx->err = dbproto::ERR_TIMEOUT;
  server_->Reply(ctx, NULL);
  server_->dbstat_[kDBStatGet].errcnt++;
  return 0;
}

int GetState::StateFinish(st::State *s) {
  // LogDebug("Get operation done\n");
  GetCtx *ctx = (GetCtx *)s->ctx;
  // release all resources here
  if (ctx->msg)
    delete ctx->msg;
  delete ctx;
  // server_->AddStat(&server_->dbstat_[kDBStatGet], ctx->start_time);
  return 0;
}

st::State * GetState::StateInit(st::Action *act) {
  GetCtx *ctx = new GetCtx;
  assert(ctx);
  ctx->st.ctx = ctx;
  ctx->cmd = uv::HeaderCmd(act->data.buf);
  ctx->seq = uv::HeaderSeq(act->data.buf);
  ctx->rcmd = dbproto::CMD_GET_RSP;
  ctx->owner = (dynamic_cast<DBAction *>(act))->conn;
  ctx->start_time = uv_now(server_->loop());
  server_->BindConnHandle(ctx->owner, ctx);
  set_next_step(&ctx->st, kGetStart);
  return &ctx->st;
}

GetState::GetState() : st::StateMachine(kGetNR, 500) {
  AddState(kGetStart, STATE_SELECTOR(GetState::Start));
  AddState(kGetGetCache, STATE_SELECTOR(GetState::GetCache));
  AddState(kGetGetDB, STATE_SELECTOR(GetState::GetDB));
  AddState(kGetSetCache, STATE_SELECTOR(GetState::SetCache));
}

st::StateMachine *NewGetState(DBProxy *s) {
  GetState *m = new GetState;
  m->set_server(s);
  return m;
}

}  // namespace db
}  // immortal


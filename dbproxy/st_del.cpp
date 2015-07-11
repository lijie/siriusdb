#include "./dbproxy_comm.h"
#include "dbproxy.pb.h"

namespace immortal {
namespace db {

using std::string;
using google::protobuf::Message;

enum {
  kDelStart = 1,
  kDelCache,
  kDelDB,
  kDelNR,
};

struct DelCtx : public Handle {
  // request from client
  dbproto::Del req;
  // object from client
  Message *msg;

  DelCtx(): msg(NULL) {}
};

class DelState : public st::StateMachine {
 public:
  DelState();
  void set_server(DBProxy *s) { server_ = s; };

 public:
  int Start(st::State *s, st::Action *act);
  int DelCache(st::State *s, st::Action *act);
  int DelDB(st::State *s, st::Action *act);

  st::State * StateInit(st::Action *act);
  int StateTimeout(st::State *s);
  int StateFinish(st::State *s);

 private:
  DBProxy *server_;
};

int DelState::Start(st::State *s, st::Action *act) {
  DelCtx *ctx = (DelCtx *)s->ctx;
  bool succ = ctx->req.ParseFromArray(act->data.buf + uv::kHeaderSize,
                                      act->data.len - uv::kHeaderSize);
  if (!succ) {
    ctx->err = dbproto::ERR_UNPACK;
    server_->Reply(ctx, NULL);
    return 0;
  }

  LogMessage1("Del", &ctx->req);
  server_->InitDBAndRedis(ctx, ctx->req.key());

  if (ctx->req.flags() & dbproto::FLAG_DB_ONLY) {
    server_->DBDel(ctx, ctx->req.msg_type(), ctx->req.key());
    return kDelDB;
  }

  // TODO(lijie3): cacheset mayfailed here?
  string cache_key = ctx->req.msg_type() + "_" + ctx->req.key();
  server_->CacheDel(ctx, cache_key.c_str());
  return kDelCache;
}

int DelState::DelCache(st::State *s, st::Action *act) {
  DelCtx *ctx = (DelCtx *)s->ctx;

  if (act->err){
    ctx->err = dbproto::ERR_RWCACHE;
    server_->Reply(ctx, NULL);
  }

  if (!(ctx->req.flags() & dbproto::FLAG_CACHE_ONLY)) {
    server_->DBDel(ctx, ctx->req.msg_type(), ctx->req.key());
  }

  // reply to client
  server_->Reply(ctx, NULL);
  return kDelDB;
}

int DelState::DelDB(st::State *s, st::Action *act) {
  DelCtx *ctx = (DelCtx *)s->ctx;
  // Handle *h = ctx->h;

  if (act->err) {
    LogError("flush player %s to db failed!\n", ctx->req.DebugString().c_str());
  }

  return 0;
}

int DelState::StateTimeout(st::State *s) {
  DelCtx *ctx = (DelCtx *)s->ctx;
  LogError("Del operation timeout\n");
  server_->DelHandleSeq(ctx->curr_seq);
  ctx->err = dbproto::ERR_TIMEOUT;
  server_->Reply(ctx, NULL);
  return 0;
}

int DelState::StateFinish(st::State *s) {
  LogDebug("Del operation done\n");
  DelCtx *ctx = (DelCtx *)s->ctx;
  delete ctx;
  return 0;
}

st::State * DelState::StateInit(st::Action *act) {
  DelCtx *ctx = new DelCtx;
  assert(ctx);

  LogDebug("Del operation init\n");
  ctx->st.ctx = ctx;
  ctx->cmd = uv::HeaderCmd(act->data.buf);
  ctx->seq = uv::HeaderSeq(act->data.buf);
  ctx->rcmd = dbproto::CMD_DEL_RSP;
  ctx->owner = (dynamic_cast<DBAction *>(act))->conn;
  server_->BindConnHandle(ctx->owner, ctx);
  set_next_step(&ctx->st, kDelStart);
  return &ctx->st;
}

DelState::DelState() : st::StateMachine(kDelNR) {
  AddState(kDelStart, STATE_SELECTOR(DelState::Start));
  AddState(kDelCache, STATE_SELECTOR(DelState::DelCache));
  AddState(kDelDB, STATE_SELECTOR(DelState::DelDB));
}

st::StateMachine *NewDelState(DBProxy *s) {
  DelState *m = new DelState;
  m->set_server(s);
  return m;
}

}  // namespace db
}  // immortal


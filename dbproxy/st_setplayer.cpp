#include "./dbproxy_comm.h"
#include "dbproxy.pb.h"

namespace immortal {
namespace db {

using std::string;
using google::protobuf::Message;

enum {
  kSetStart = 1,
  kSetSetCache,
  kSetAppendFlush,
  kSetSetDB,
  kSetGetLastID,
  kSetNR,
};

struct SetCtx : public Handle {
  // request from client
  dbproto::Set req;
};

class SetState : public st::StateMachine {
 public:
  SetState();
  void set_server(DBProxy *s) { server_ = s; };

 public:
  int Start(st::State *s, st::Action *act);
  int SetCache(st::State *s, st::Action *act);
  int SetAppendFlush(st::State *s, st::Action *act);
  int SetDB(st::State *s, st::Action *act);

  st::State * StateInit(st::Action *act);
  int StateTimeout(st::State *s);
  int StateFinish(st::State *s);

 private:
  DBProxy *server_;
};

int SetState::Start(st::State *s, st::Action *act) {
  SetCtx *ctx = (SetCtx *)s->ctx;
  bool succ = ctx->req.ParseFromArray(act->data.buf + uv::kHeaderSize,
                                      act->data.len - uv::kHeaderSize);
  if (!succ) {
    ctx->err = dbproto::ERR_UNPACK;
    server_->Reply(ctx, NULL);
    return 0;
  }

  // LogMessage1("Set", &ctx->req);
  server_->InitDBAndRedis(ctx, ctx->req.key());

  if (ctx->req.flags() & dbproto::FLAG_DB_ONLY) {
    server_->DBSet(ctx, ctx->req.msg_type(), ctx->req.key(), ctx->req.value());
    return kSetSetDB;
  }
  // lijie3:
  // SetNX不会影响Cache, 因为:
  // 1. cache有数据, 说明DB一定也有数据, SetNX执行会失败
  // 2. cache无数据, 无论SetNX在mysql端是否成功都不影响cache跟db的一致性
  if (ctx->req.flags() & dbproto::FLAG_SET_ONLY) {
    server_->DBSetNX(ctx, ctx->req.msg_type(), ctx->req.key(), ctx->req.value());
    return kSetSetDB;
  }

  // TODO(lijie3): cacheset mayfailed here?
  // server_->CacheSetMsg(h, ctx->req.name().c_str(), &ctx->req);
  string cache_key = ctx->req.msg_type() + "_" + ctx->req.key();
  server_->CacheSet(ctx, cache_key.c_str(),
                    ctx->req.value().c_str(), ctx->req.value().size());
  return kSetSetCache;
}

int SetState::SetCache(st::State *s, st::Action *act) {
  SetCtx *ctx = (SetCtx *)s->ctx;
  if (act->err){
    ctx->err = dbproto::ERR_RWCACHE;
    server_->Reply(ctx, NULL);
  }

  if (ctx->req.flags() & dbproto::FLAG_CACHE_ONLY) {
    server_->Reply(ctx, NULL);
    return 0;
  }

  // TODO:
  if (true) {
    server_->dbstat_[kDBStatSetCache].total++;
    // LogDebug("appendflush %s\n", ctx->req.key().c_str());
    server_->AppendFlush(ctx, ctx->req);
    return kSetAppendFlush;
  }
  
  server_->DBSet(ctx, ctx->req.msg_type(), ctx->req.key(), ctx->req.value());
  // reply to client
  server_->Reply(ctx, NULL);
  return kSetSetDB;
}

int SetState::SetAppendFlush(st::State *s, st::Action *act) {
  SetCtx *ctx = (SetCtx *)s->ctx;

  if (act->err){
    ctx->err = dbproto::ERR_RWCACHE;
  }
  server_->Reply(ctx, NULL);
  // server_->AddStat(&server_->dbstat_[kDBStatSetCache], ctx->start_time);
  return 0;
}

int SetState::SetDB(st::State *s, st::Action *act) {
  SetCtx *ctx = (SetCtx *)s->ctx;

  if (act->err) {
    LogError("flush player %s to db failed!\n", ctx->req.DebugString().c_str());
    ctx->err = act->err;
    server_->Reply(ctx, NULL);
    return 0;
  }

  server_->dbstat_[kDBStatSetDB].total++;
  server_->AddStat(&server_->dbstat_[kDBStatSetDB], ctx->start_time);
  // direct write db, need reply
  if (ctx->req.flags() & dbproto::FLAG_DB_ONLY) {
    server_->Reply(ctx, NULL);
    return 0;
  }

  // SetNX, need get lastid and reply
  if (ctx->req.flags() & dbproto::FLAG_SET_ONLY) {
    auto res = (std::vector<void *> *)act->data.buf;
    // no result !?
    if (res->size() < 2) {
      LogError("SetNX succ, but no last id!?\n");
      ctx->err = dbproto::ERR_NODATA;
      server_->Reply(ctx, NULL);
      return 0;
    }
    auto msg = ctx->dbclient->NewMessage("SetResult");
    if (msg == NULL) {
      // proto config BUG !?
      LogError("SetResult proto not found!\n");
      SI_ASSERT(0);
    }
    int n = Res2PB((*res)[1], msg);
    if (n != 0) {
      LogError("parser last insert id error %d\n", n);
      ctx->err = n;
      server_->Reply(ctx, NULL);
      delete msg;
      return 0;
    }
    server_->Reply(ctx, msg);
    delete msg;
    return 0;
  }
    
  return 0;
}

int SetState::StateTimeout(st::State *s) {
  SetCtx *ctx = (SetCtx *)s->ctx;
  LogDebug("Set operation timeout, state step %d\n", s->step_);
  server_->DelHandleSeq(ctx->curr_seq);
  ctx->err = dbproto::ERR_TIMEOUT;
  server_->Reply(ctx, NULL);
  server_->dbstat_[kDBStatSetCache].errcnt++;
  return 0;
}

int SetState::StateFinish(st::State *s) {
  // LogDebug("Set operation done\n");
  SetCtx *ctx = (SetCtx *)s->ctx;
  delete ctx;
  return 0;
}

st::State * SetState::StateInit(st::Action *act) {
  SetCtx *ctx = new SetCtx;
  assert(ctx);

  ctx->st.ctx = ctx;
  ctx->cmd = uv::HeaderCmd(act->data.buf);
  ctx->seq = uv::HeaderSeq(act->data.buf);
  ctx->rcmd = dbproto::CMD_SET_RSP;
  ctx->owner = (dynamic_cast<DBAction *>(act))->conn;
  ctx->start_time = uv_now(server_->loop());
  server_->BindConnHandle(ctx->owner, ctx);
  set_next_step(&ctx->st, kSetStart);
  return &ctx->st;
}

SetState::SetState() : st::StateMachine(kSetNR, 2000) {
  AddState(kSetStart, STATE_SELECTOR(SetState::Start));
  AddState(kSetSetCache, STATE_SELECTOR(SetState::SetCache));
  AddState(kSetAppendFlush, STATE_SELECTOR(SetState::SetAppendFlush));
  AddState(kSetSetDB, STATE_SELECTOR(SetState::SetDB));
}

st::StateMachine *NewSetState(DBProxy *s) {
  SetState *m = new SetState;
  m->set_server(s);
  return m;
}

}  // namespace db
}  // immortal


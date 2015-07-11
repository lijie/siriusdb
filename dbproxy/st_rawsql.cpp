#include "./dbproxy_comm.h"
#include "dbproxy.pb.h"

namespace immortal {
namespace db {

using std::string;
using google::protobuf::Message;

enum {
  kRawSQLStart = 1,
  kRawSQLDB,
  kRawSQLNR,
};

struct RawSQLCtx : public Handle {
  // request from client
  dbproto::RawSQL req;
  dbproto::RawSQLResult rsp;
  Message **array;
  size_t count;

  RawSQLCtx(): array(NULL), count(0) {}
};

class RawSQLState : public st::StateMachine {
 public:
  RawSQLState();
  void set_server(DBProxy *s) { server_ = s; };

 public:
  int Start(st::State *s, st::Action *act);
  int RawSQLDB(st::State *s, st::Action *act);

  st::State * StateInit(st::Action *act);
  int StateTimeout(st::State *s);
  int StateFinish(st::State *s);

 private:
  DBProxy *server_;
};

int RawSQLState::Start(st::State *s, st::Action *act) {
  RawSQLCtx *ctx = (RawSQLCtx *)s->ctx;
  bool succ = ctx->req.ParseFromArray(act->data.buf + uv::kHeaderSize,
                                      act->data.len - uv::kHeaderSize);
  if (!succ) {
    ctx->err = dbproto::ERR_UNPACK;
    server_->Reply(ctx, NULL);
    return 0;
  }

  LogMessage1("RawSQL", &ctx->req);

  server_->InitDBAndRedis(ctx, "");
  server_->DBRawSQL(ctx, ctx->req.msg_type(), ctx->req.sql());
  return kRawSQLDB;
}

int RawSQLState::RawSQLDB(st::State *s, st::Action *act) {
  RawSQLCtx *ctx = (RawSQLCtx *)s->ctx;
  if (act->err) {
    LogError("raw sql failed! %s\n", ctx->req.DebugString().c_str());
    ctx->err = act->err;
    server_->Reply(ctx, NULL);
    return 0;
  }

  auto res = (std::vector<void *> *)act->data.buf;
  if (res->size() == 0) {
    ctx->err = dbproto::ERR_NODATA;
    server_->Reply(ctx, NULL);
    return 0;
  }

  void *query_res = (*res)[0];

  size_t count = ResRowCount(query_res);
  if (count == 0) {
    ctx->err = 0;
    server_->Reply(ctx, &ctx->rsp);
    return 0;
  }

  auto db = ctx->dbclient;
  Message **array = db->NewMessageArray(ctx->req.msg_type(), count);
  if (array == NULL) {
    LogError("alloc protobuf message array failed, msg_type: %s\n",
             ctx->req.msg_type().c_str());
    ctx->err = -1;
    server_->Reply(ctx, NULL);
    return 0;
  }

  Res2PBArray(query_res, array, count);

  for (size_t i = 0; i < count; i++) {
    string *value = ctx->rsp.add_values();
    bool succ = array[i]->SerializeToString(value);
    if (!succ) {
      LogError("SerializeToString failed\n");
      value->clear();
    }
  }

  server_->Reply(ctx, &ctx->rsp);
  delete[] array;

  return 0;
}

int RawSQLState::StateTimeout(st::State *s) {
  RawSQLCtx *ctx = (RawSQLCtx *)s->ctx;
  LogError("RawSQL operation timeout\n");
  server_->DelHandleSeq(ctx->curr_seq);
  ctx->err = dbproto::ERR_TIMEOUT;
  server_->Reply(ctx, NULL);
  return 0;
}

int RawSQLState::StateFinish(st::State *s) {
  LogDebug("RawSQL operation done\n");
  RawSQLCtx *ctx = (RawSQLCtx *)s->ctx;
  delete ctx;
  return 0;
}

st::State * RawSQLState::StateInit(st::Action *act) {
  RawSQLCtx *ctx = new RawSQLCtx;
  assert(ctx);

  LogDebug("RawSQL operation init\n");
  ctx->st.ctx = ctx;
  ctx->cmd = uv::HeaderCmd(act->data.buf);
  ctx->seq = uv::HeaderSeq(act->data.buf);
  ctx->rcmd = dbproto::CMD_RAWSQL_RSP;
  ctx->owner = (dynamic_cast<DBAction *>(act))->conn;
  server_->BindConnHandle(ctx->owner, ctx);
  set_next_step(&ctx->st, kRawSQLStart);
  return &ctx->st;
}

RawSQLState::RawSQLState() : st::StateMachine(kRawSQLNR) {
  AddState(kRawSQLStart, STATE_SELECTOR(RawSQLState::Start));
  AddState(kRawSQLDB, STATE_SELECTOR(RawSQLState::RawSQLDB));
}

st::StateMachine *NewRawSQLState(DBProxy *s) {
  RawSQLState *m = new RawSQLState;
  m->set_server(s);
  return m;
}

}  // namespace db
}  // immortal


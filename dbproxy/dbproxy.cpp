// Copyright (c) 2014 kingsoft

#include "./dbproxy_comm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <assert.h>
#include <string>
#include <iostream>

#include <uv/uv.h>
#ifdef GOOGLE_CPU_PROFILE
#include <gperftools/profiler.h>
#endif

#include "./uvwrapper.h"
#include "common/define/server_def.h"
#include "dbproxy.pb.h"
#include "center.pb.h"

extern "C" uint32_t hash_crc32(const char *, size_t);
extern "C" uint32_t hash_crc32a(const char *, size_t);

static immortal::db::DBProxy *dbproxy_server = NULL;

static void dbproxy_exit_cb(uv_signal_t *handle, int num) {
  if (dbproxy_server)
    dbproxy_server->Shutdown();
}

void dbproxy_get_stat(uint64_t start_time) {
  g_GetDBService()->AddGetStat(start_time);
}

void dbproxy_set_stat(uint64_t start_time) {
  g_GetDBService()->AddSetStat(start_time);
}

namespace immortal {
namespace db {
const int kTimerTick = 1000;
const int kCenterPingTick = 60 * 1000;

using namespace std::placeholders;

std::tuple<string, string, string, string, string> ParseMySQLURL(const string& url);
std::tuple<string, uint16_t> ParseAddr(const string& addr);
std::tuple<string, int> ParseSplitTable(const string&);

class Store;

class DBTCP : public uv::TCP {
 public:
  DBTCP(DBProxy *s) : db_(s) {}
  uv::TCPConn * NewTCPConn() {
    db_->conn_count_++;
    return dynamic_cast<uv::TCPConn *>(new DBConn(db_));
  }

 protected:
  DBProxy *db_;
};

// 处理Center请求
class CenterConn : public uv::TCPConn {
 public:
  CenterConn(DBProxy *s) : server_(s), alive_(false) {}
  int Connect(const std::string& ip, uint16_t port, Loop *loop) {
    ip_ = ip;
    port_ = port;
    loop_ = loop;
    return TCPConn::Connect(ip, port, loop);
  }

  int Reconnect() {
    if (alive_)
      return 0;

    return TCPConn::Connect(ip_, port_, loop_);
  }
  
  void OnClose(TCPConn *conn) {
    auto c = dynamic_cast<CenterConn *>(conn);
    c->alive_ = false;
    LogDebug("center close conn!");
    conn->Close();
  };
  
  void OnConnected(TCPConn *conn, int status) {
    if (status != 0) {
      LogError("connect center error %d\n", status);
      return;
    }
    LogDebug("Center Connected.\n");
    auto c = dynamic_cast<CenterConn *>(conn);
    c->alive_ = true;

    server_->SendCenterReg(conn);
    c->ReadStart();
  }
  
  void HandleRequest(TCPConn *conn, const char *buf, size_t len) {
    const uv::Header *header = (const uv::Header *)buf;
    if (header->retcode != 0) {
      LogError("Center resp error %d\n", header->retcode);
    } else {
      LogInfo("Register to Center OK.\n");
    }
  }

 protected:
  DBProxy *server_;
  bool alive_;
  string ip_;
  uint16_t port_;
  uv::Loop *loop_;
};

class DBTimerListener : public uv::TimerListener {
 public:
  DBTimerListener(DBProxy *s): server_(s) {}
  virtual void OnTimeout(uv::Timer *timer) {
    server_->OnTimer();
  }

 private:
  DBProxy *server_;
};

void DBConn::HandleRequest(TCPConn *conn, const char *buf, size_t len) {
  db_->InitHandle(conn, buf, len);
}

void DBConn::OnClose(TCPConn *conn) {
  LogDebug("Close client conn\n");
  ListHead<Handle *> *pos = NULL;
  ListHead<Handle *> *next = NULL;
  if (!handle_list_.Empty()) {
    list_for_each_safe (pos, next, &handle_list_) {
      LogDebug("delete in process handle %u %p %p\n", pos->Entry()->curr_seq, pos, pos->next_);
      db_->DelHandle(pos->Entry());
    }
  }
  conn->Close();
}

void DBConn::OnCloseDone(TCPConn *conn) {
  LogDebug("Close client conn done\n");
  db_->TryShutdown();
  delete conn;
}

void DBProxy::ShowStat(void) {
  char buf[1024];
  int n = 0;
  n = sprintf(buf + n, "\n");
  n += sprintf(buf + n, "[10]\t[50]\t[100]\t[200]\t[500]\t[500+]\n");
  for (int i = 0; i < kDBStatNR; i++) {
    auto st = &dbstat_[i];
    n += sprintf(buf + n, "%d\t%d\t%d\t%d\t%d\t%d\n",
                st->delay[0], st->delay[1], st->delay[2], st->delay[3],
                st->delay[4], st->delay[5]);
  }
  memset(dbstat_, 0, sizeof(DBStat) * kDBStatNR);
  LogInfo("%s\n", buf);
}

void DBProxy::OnTimer() {
  center_conn_->Reconnect();
  if (last_ping_ <= 0) {
    SendCenterPing(center_conn_);
    last_ping_ = kCenterPingTick;
  } else {
    last_ping_ -= kTimerTick;
  }
  mm_->Elapse(loop_.Now());

  if (last_stat_ <= 0) {
    last_stat_ = kTimerTick * 10;
    ShowStat();
  } else {
    last_stat_ -= kTimerTick;
  }
}

int DBProxy::InitSplitTableFromConfig(DBClient *db, LuaRef& table) {
  string hash;
  string tablename;
  int count = table.length();
  auto hashfunc = hash_crc32a;
  for (int i = 1; i <= count; i++) {
    auto v = table[i].cast<std::string>();
    auto tup = ParseSplitTable(v);
    db->AddSplitTable(std::get<0>(tup), hashfunc, std::get<1>(tup));
  }
  return 0;
}

int DBProxy::InitDBClients(LuaRef& db, void *loop) {
  LuaRef my = db["mysql_url"];
  if (my.isNil()) {
    fprintf(stderr, "No mysql config ?\n");
    return -1;
  }

  int pre_alloc_conn = db["pre_alloc_conn"].cast<int>();

  dbclient_count_ = my.length();
  dbclients_ = new DBClient*[dbclient_count_];

  for (int i = 0; i < dbclient_count_; i++) {
    LuaRef v = my[i+1];
    string mysql_url = v.cast<std::string>();
    dbclients_[i] = new DBClient;
    dbclients_[i]->Init(std::bind(&DBProxy::AfterDB, this, _1, _2, _3), loop, mysql_url);

    if (pre_alloc_conn > 0) {
      dbclients_[i]->PreAllocConns(pre_alloc_conn);
    }
  }

  return 0;
}

int DBProxy::InitRedisClients(LuaRef& db, void *loop) {
  LuaRef my = db["redis_url"];
  if (my.isNil()) {
    fprintf(stderr, "No redis config ?\n");
    return -1;
  }

  rdclient_count_ = my.length();
  rdclients_ = new RedisClient*[rdclient_count_];

  for (int i = 0; i < rdclient_count_; i++) {
    LuaRef v = my[i+1];
    string redis_url = v.cast<std::string>();
    rdclients_[i] = new RedisClient;
    rdclients_[i]->Init(std::bind(&DBProxy::AfterCache, this, _1, _2, _3, _4), loop, redis_url);
  }

  return 0;
}

int DBProxy::InitFromConfig(LuaRef& db, void *loop) {
  InitDBClients(db, loop);
  InitRedisClients(db, loop);

  std::vector<std::string> proto_file_search_path;
  std::vector<std::string> proto_file_load;
  
  LuaRef search_path = db["proto_file_search_path"];
  for (int i = 1; i <= search_path.length(); i++) {
    proto_file_search_path.push_back(search_path[i].cast<std::string>());
  }

  LuaRef file_load = db["proto_file_load"];
  for (int i = 1; i <= file_load.length(); i++) {
    proto_file_load.push_back(file_load[i].cast<std::string>());
  }

  LuaRef v = db["split_table"];

  for (size_t i = 0; i < dbclient_count_; i++) {
    dbclients_[i]->AddSearchPath(proto_file_search_path);
    for (size_t j = 0; j < proto_file_load.size(); j++) {
      dbclients_[i]->AddMsgDesc(proto_file_load[j]);
    }
    if (!v.isNil()) {
      InitSplitTableFromConfig(dbclients_[i], v);
    }
  }

  return 0;
}

void Test() {
#if 0
  ListHead<Handle *> head(NULL);
  SI_ASSERT(head.next_ == &head);
  for (size_t i = 0; i < 10000; i++) {
    auto h = new Handle;
    h->curr_seq = i + 1;
    head.AddTail(&h->list);
  }
  SI_ASSERT(head.next_ != &head);
  ListHead<Handle *> *pos = NULL;
  uint32_t seq = 1;
  list_for_each (pos, &head) {
    auto h = pos->Entry();
    SI_ASSERT(h->curr_seq == seq);
    seq++;
  }
  ListHead<Handle *> *next = NULL;
  seq = 1;
  list_for_each_safe(pos, next, &head) {
    auto h = pos->Entry();
    pos->Del();
    delete h;
  }
  SI_ASSERT(head.Empty());
#endif
}

int DBProxy::Init() {
  Test();
  int r = 0;

  if ((r = ReadLuaConfig()) != 0) {
    LogError("read config err\n");
    return r;
  }

  if ((r = InitMachine()) != 0) {
    LogError("init state machine err\n");
    return r;
  }

  tcpserver_ = new DBTCP(this);
  tcpserver_->Init(&loop_);
  tcpserver_->Listen(ip_.c_str(), port_);
  if (port_ == 0) {
    port_ = ntohs(tcpserver_->LocalAddr_.sin_port);
  }
  LogInfo("DB: Listen %s:%d\n", ip_.c_str(), port_);

  LogInfo("DBProxy: Connect to center...\n");
  center_conn_ = new CenterConn(this);
  r = center_conn_->Connect(center_ip_, center_port_, &loop_);
  if (r != 0) {
    LogError("connect to center err %d\n", r);
  }

  timer_.Init(&loop_, new DBTimerListener(this));
  timer_.Start(3000, 3000);

  memset(dbstat_, 0, sizeof(DBStat) * kDBStatNR);

  uv_signal_init(loop(), &sig_handle_);
  uv_signal_start(&sig_handle_, dbproxy_exit_cb, 15);
  return 0;
}

int DBProxy::SendCenterPing(TCPConn *conn) {
  if (!conn->IsConnected())
    return -1;
  Handle handle;
  handle.rcmd = PbCenter::CMD_PING_REQ;
  handle.seq = nextseq();
  handle.owner = conn;
  
  PbCenter::PingReq req;
  req.set_reqnum(0);
  req.set_delay(0);
  req.set_linknum(0);
  req.set_thres(0);
  return SendMsg(&handle, &req);
}

void DBProxy::AddGetStat(uint64_t start_time) {
  DBStat *st = &dbstat_[0];
  auto delay = uv_now(loop_.loop()) - start_time;
  if (delay > 500)
    st->delay[5]++;
  else if (delay > 200)
    st->delay[4]++;
  else if (delay > 100)
    st->delay[3]++;
  else if (delay > 50)
    st->delay[2]++;
  else if (delay > 10)
    st->delay[1]++;
  else
    st->delay[0]++;
}

void DBProxy::AddSetStat(uint64_t start_time) {
  DBStat *st = &dbstat_[1];
  auto delay = uv_now(loop_.loop()) - start_time;
  if (delay > 500)
    st->delay[5]++;
  else if (delay > 200)
    st->delay[4]++;
  else if (delay > 100)
    st->delay[3]++;
  else if (delay > 50)
    st->delay[2]++;
  else if (delay > 10)
    st->delay[1]++;
  else
    st->delay[0]++;
}
  
void DBProxy::AddStat(DBStat *st, uint64_t start_time) {
  auto delay = uv_now(loop_.loop()) - start_time;
  if (delay > 500)
    st->delay[5]++;
  else if (delay > 200)
    st->delay[4]++;
  else if (delay > 100)
    st->delay[3]++;
  else if (delay > 50)
    st->delay[2]++;
  else if (delay > 10)
    st->delay[1]++;
  else
    st->delay[0]++;
}

int DBProxy::AppendFlush(Handle *h, const dbproto::Set& set) {
  dbproto::FlushItem item;
  item.set_time(time(NULL));
  item.set_key(set.key());
  item.set_msg_type(set.msg_type());
  item.set_cmd(dbproto::CMD_SET_REQ);
  item.set_dbname("");

  size_t len = item.ByteSize();
  char *p = (char *)alloca(len);
  assert(p);
  bool succ = item.SerializeToArray(p, len);
  assert(succ);

  // auto store = dynamic_cast<DBConn *>(h->owner)->store_;
  auto rdc = rdclients_[0];

  uint32_t seq = nextseq();
  int r = rdc->Command(seq, "RPUSH %s %b", "dbproxy_flush_list", p, len);
  if (r == 0)
    AddHandleSeq(seq, h);
  return 0;
}

void DBProxy::BindConnHandle(TCPConn *conn, Handle *h) {
  auto c = dynamic_cast<DBConn *>(conn);
  c->handle_list_.Add(&h->list);
}

uint32_t DBProxy::nextseq() {
  if (++dbseq_ == 0)
    dbseq_++;
  return dbseq_;
}

void DBProxy::AddHandleSeq(uint32_t seq, Handle *h) {
  if (h == NULL)
    return;
  assert(seq != 0);
  SI_ASSERT(FindHandleSeq(seq) == NULL);
  handlemap_[seq] = h;
  h->curr_seq = seq;
  // LogDebug("add handle %u %p\n", seq, h);
}

void DBProxy::DelHandleSeq(uint32_t seq) {
  if (seq == 0)
    return;
  handlemap_.erase(seq);
}

Handle * DBProxy::FindHandleSeq(uint32_t seq) {
  std::map<uint32_t, Handle *>::iterator it = handlemap_.find(seq);
  if (it == handlemap_.end())
    return NULL;
  return it->second;
}

Handle * DBProxy::TakeHandleSeq(uint32_t seq) {
  std::map<uint32_t, Handle *>::iterator it = handlemap_.find(seq);
  if (it == handlemap_.end())
    return NULL;
  Handle * h = it->second;
  assert(h->curr_seq == seq);
  h->curr_seq = 0;
  handlemap_.erase(it);
  return h;
}

int DBProxy::CacheSet(Handle *h, const char *key, const char *buf, size_t bufsize) {
  uint32_t seq = nextseq();
  int r = h->redisclient->Set(seq, key, buf, bufsize);
  if (r == 0) {
    AddHandleSeq(seq, h);
  } else {
    LogError("CacheSet error %d\n", r);
  }
  return r;
}

int DBProxy::CacheGet(Handle *h, const char *key) {
  uint32_t seq = nextseq();
  int r = h->redisclient->Get(seq, key);
  if (r == 0)
    AddHandleSeq(seq, h);
  return r;
}

int DBProxy::CacheDel(Handle *h, const char *key) {
  uint32_t seq = nextseq();
  int r = h->redisclient->Del(seq, key);
  if (r == 0)
    AddHandleSeq(seq, h);
  return r;
}

int DBProxy::InitMachine() {
  mm_ = new st::MachineManger();
  assert(mm_);
  mm_->AddMachine(dbproto::CMD_SET_REQ, NewSetState(this));
  mm_->AddMachine(dbproto::CMD_GET_REQ, NewGetState(this));
  mm_->AddMachine(dbproto::CMD_DEL_REQ, NewDelState(this));
  mm_->AddMachine(dbproto::CMD_RAWSQL_REQ, NewRawSQLState(this));
  return 0;
}

int DBProxy::ReadLuaConfig() {
  aid_ = parser_.GetValue<int>("AID");
  sid_ = parser_.GetValue<int>("SID");
  
  center_ip_ = parser_.GetValue<std::string>("CENTER_IP");
  center_port_ = parser_.GetValue<uint16_t>("CENTER_PORT");
        
  LuaRef db = parser_.GetValue<LuaRef>("dbproxy");
  ip_ = db["ip"].cast<std::string>();
  port_ = db["port"].cast<uint32_t>();

  LuaRef database = db["database"];
  if (database.isNil()) {
    fprintf(stderr, "No database config ?\n");
    exit(255);
  }

  for (int i = 1; i <= database.length(); i++) {
    LuaRef v = database[i];
    InitFromConfig(v, loop_.loop());
  }
  LogDebug("LoadConfig Done\n");
  return 0;
}

int DBProxy::Reply(Handle *handle, const char *src, size_t len) {
  TCPConn *conn = handle->owner;
  uint32_t totallen = len + sizeof(RespHeader);

  TCPWriteRequest *req = NewTCPWriteRequest(totallen);
  SI_ASSERT(req != NULL);
  char *buf = req->buf;
  req->cmd = handle->rcmd;
  req->start_time = handle->start_time;

  RespHeader *h = (RespHeader *)(buf);
  h->cmd = handle->rcmd;
  h->seq = handle->seq;
  h->len = totallen;

  if (handle->err) {
    LogError("DB: reply error %d\n", handle->err);
    h->retcode = handle->err;
    h->len = sizeof(RespHeader);
    return conn->Write(req, buf, sizeof(RespHeader));
  }

  h->retcode = 0;
  if (len > 0)
    memcpy(buf + sizeof(RespHeader), src, len);
  return conn->Write(req, buf, totallen);
}

int DBProxy::Reply(Handle *handle, Message *msg) {
  TCPConn *conn = handle->owner;
  uint32_t len = sizeof(RespHeader);
  if (msg) {
    len += msg->ByteSize();
  }

  TCPWriteRequest *req = NewTCPWriteRequest(len);
  SI_ASSERT(req != NULL);
  char *buf = req->buf;
  req->cmd = handle->rcmd;
  req->start_time = handle->start_time;

  RespHeader *h = (RespHeader *)(buf);
  h->cmd = handle->rcmd;
  h->seq = handle->seq;
  h->len = len;

  if (handle->err) {
    LogError("DB: reply error %d\n", handle->err);
    h->retcode = handle->err;
    h->len = sizeof(RespHeader);
    return conn->Write(req, buf, sizeof(RespHeader));
  }

  h->retcode = 0;
  if (msg) {
    bool succ = msg->SerializeToArray(buf + sizeof(RespHeader),
                                      len - sizeof(RespHeader));
    if (!succ) {
      LogError("SerializeToArray failed\n");
      DelTCPWriteRequest(req);
      return -1;
    }
  }
  return conn->Write(req, buf, len);
}

int DBProxy::SendMsg(Handle *handle, Message *msg) {
  TCPConn *conn = handle->owner;
  uint32_t len = sizeof(Header);
  if (msg) {
    len += msg->ByteSize();
  }

  TCPWriteRequest *req = NewTCPWriteRequest(len);
  SI_ASSERT(req != NULL);
  char *buf = req->buf;

  Header *h = (Header *)(buf);
  h->cmd = handle->rcmd;
  h->seq = handle->seq;
  h->len = len;
  h->magic = MAGIC_NUM;

  if (handle->err) {
    LogError("DB: send msg error %d\n", handle->err);
    h->retcode = handle->err;
    h->len = sizeof(Header);
    return conn->Write(req, buf, sizeof(Header));
  }

  h->retcode = 0;
  if (msg) {
    bool succ = msg->SerializeToArray(buf + sizeof(Header),
                                      len - sizeof(Header));
    if (!succ) {
      LogError("SerializeToArray failed\n");
      DelTCPWriteRequest(req);
      return -1;
    }
  }
  return conn->Write(req, buf, len);
}

int DBProxy::SendCenterReg(TCPConn *conn) {
  Handle handle;
  handle.rcmd = PbCenter::CMD_REG_REQ;
  handle.seq = nextseq();
  handle.owner = conn;
  
  // send regreq
  PbCenter::RegReq req;
  req.set_type(common::DBPROXY_SERVER);
  auto addr = req.mutable_addr();
  addr->set_ip(ip_);
  addr->set_port(port_);
  addr->set_aid(aid_);
  addr->set_sid(sid_);
  addr->set_gid(-1);

  return SendMsg(&handle, &req);
}

int DBProxy::CacheSetMsg(Handle *h, const char *key, Message *msg) {
  size_t len = msg->ByteSize();
  char *p = (char *)alloca(len);
  assert(p);
  bool succ = msg->SerializeToArray(p, len);
  assert(succ);
  return CacheSet(h, key, p, len);
}

int DBProxy::DBGet(Handle *h, const std::string& msg_type,
               const std::string& key) {
  uint32_t seq = nextseq();
  int n = h->dbclient->Get(seq, msg_type, key);
  if (n == 0)
    AddHandleSeq(seq, h);
  return n;
}

int DBProxy::DBDel(Handle *h, const std::string& msg_type,
               const std::string& key) {
  uint32_t seq = nextseq();
  int n = h->dbclient->Del(seq, msg_type, key);
  if (n == 0)
    AddHandleSeq(seq, h);
  return n;
}

int DBProxy::DBSet(Handle *h, const std::string& msg_type,
                   const std::string& key, const std::string& value) {
  uint32_t seq = nextseq();
  int n = h->dbclient->Set(seq, msg_type, key, value);
  if (n == 0)
    AddHandleSeq(seq, h);
  return n;
}

int DBProxy::DBSetNX(Handle *h, const std::string& msg_type,
                     const std::string& key, const std::string& value) {
  uint32_t seq = nextseq();
  int n = h->dbclient->SetNX(seq, msg_type, key, value);
  if (n == 0)
    AddHandleSeq(seq, h);
  return n;
}

int DBProxy::DBRawSQL(Handle *h, const std::string& msg_type,
                      const std::string& sql) {
  uint32_t seq = nextseq();
  int n = h->dbclient->RawSQL(seq, msg_type, sql);
  if (n == 0)
    AddHandleSeq(seq, h);
  return n;
}

void DBProxy::AfterCache(uint32_t seq, char *buf, size_t len, int err) {
  Handle * h = TakeHandleSeq(seq);
  // nobody care this cache reply
  if (h == NULL)
    return;
  assert(h->cmd != 0);

  st::Action act;
  act.err = err;
  act.data.buf = buf;
  act.data.len = len;
  mm_->RunMachine(&h->st, &act);
  return;
}

void DBProxy::AfterDB(uint32_t seq, int err, const std::vector<void *>& res) {
  Handle * h = TakeHandleSeq(seq);
  // nobody care this reply
  if (h == NULL)
    return;

  st::Action act;
  act.err = err;
  act.data.buf = (char *)&res;
  act.data.len = 0;
  mm_->RunMachine(&h->st, &act);
  return;
}

// ignore USEDB command, reply ok
void DBProxy::ProcUseDB(TCPConn *conn, const char *buf, size_t bufsize) {
  Handle handle;
  handle.seq = uv::HeaderSeq(buf);
  handle.rcmd = dbproto::CMD_USEDB_RSP;
  handle.owner = conn;
  handle.err = 0;
  Reply(&handle, NULL);
}

#if 0
bool DBProxy::CheckUseDB(Handle *h, TCPConn *conn) {
  if (dynamic_cast<DBConn *>(conn)->store_ == NULL) {
    LogError("db not specified\n");
    h->err = dbproto::ERR_EMPTYDB;
    Reply(h, NULL);
    return false;
  }
  return true;
}
#endif

void DBProxy::InitDBAndRedis(Handle *h, const string& key) {
  // 因为分库分表都是使用的crc32
  // 分表使用key来计算, 如果分库也只使用key的话
  // 会导致某个库只有一个表会被使用到.
  // 这里添加了一个suffix用来区分分库跟分表的hash结果
  string dup = key;
  dup.append("turing");

  auto s1 = hash_crc32a(dup.c_str(), dup.length()) % dbclient_count_;
  auto s2 = hash_crc32a(dup.c_str(), dup.length()) % rdclient_count_;

  h->dbclient = dbclients_[s1];
  h->redisclient = rdclients_[s2];
}

void DBProxy::InitHandle(TCPConn *conn, const char *buf, size_t bufsize) {
  uint32_t len = uv::HeaderLen(buf);
  uint32_t cmd = uv::HeaderCmd(buf);

  assert(len <= bufsize);

  if (cmd == dbproto::CMD_USEDB_REQ) {
    ProcUseDB(conn, buf, bufsize);
    return;
  }

  DBAction act;
  act.conn = conn;
  act.data.buf = (char *)buf;
  act.data.len = bufsize;
  act.err = 0;
  mm_->StartMachine(cmd, &act, loop_.Now());
}

void DBProxy::DelHandle(Handle *h) {
  DelHandleSeq(h->curr_seq);
  mm_->StopMachine(&h->st);
}

void DBProxy::TryShutdown() {
  conn_count_--;
  if (!closing_)
    return;
  Shutdown();
}

void DBProxy::Shutdown() {
  if (conn_count_ > 0) {
    LogError("stopping dbproxy but client connection count is %zd, waiting...\n", conn_count_);
    closing_ = true;
    return;
  }

#ifdef GOOGLE_CPU_PROFILE
  ProfilerFlush();
  ProfilerStop();
  // HeapProfilerStop();
#endif
  loop_.Stop();
}

int DBProxy::Run() {
  if (running_)
    return 0;
  running_ = true;
#ifdef GOOGLE_CPU_PROFILE
  ProfilerStart("dbproxy_profile");
  // HeapProfilerStart("dbproxy_heap_profile");
#endif
  dbproxy_server = this;
  // signal(15, ::signal_stop_dbproxy);
  loop_.Run(UV_RUN_DEFAULT);
#ifdef GOOGLE_CPU_PROFILE
  exit(0);
#endif
  return 0;
}

void DBProxy::ParseCommandArgs(int argc, char **argv) {
  if (argc < 2)
    return;

  for (int i = 1; i < argc; i++) {
    char * arg = argv[i];

    if (strcmp(arg, "--log") == 0) {
      i++;
      log_path_ = string(argv[i]);
    } else if (strcmp(arg, "--conf") == 0) {
      i++;
      conf_path_ = string(argv[i]);
    } else if (strcmp(arg, "--addr") == 0) {
      i++;
      addr_path_ = string(argv[i]);
    }
  }
}

int DBProxy::InitFromArgs(int argc, char **argv) {
  ParseCommandArgs(argc, argv);

  if (conf_path_.length() == 0) {
    conf_path_ = "./config.lua";
  }

  // init parser_
  if (parser_.OpenFile(conf_path_.c_str()) != 0) {
    return -1;
  }

  // init logger
  int  log_level     = parser_.GetValue<int>("LOG_LEVEL");
  int  log_file_num  = parser_.GetValue<int>("LOG_FILE_NUM");
  int  log_file_size = parser_.GetValue<int>("LOG_FILE_SIZE");
  auto log_print     = parser_.GetValue<LuaRef>("LOG_PRINT_CONSOLE");
  bool print         = false;
  if (!log_print.isNil()) {
    print = (log_print.cast<int>() == 1);
  }
  if (log_path_.length() == 0)
    log_path_ = parser_.GetValue<std::string>("LOG_FILE_PATH");
  common::ImmortalLog::Init(log_level, log_path_.c_str(),
                            log_file_size, log_file_num, print);

  return Init();
}

}  // namespace db
}  // namespace immortal


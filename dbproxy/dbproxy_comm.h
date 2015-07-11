// Copyright (c) 2014 kingsoft

#ifndef CODE_SOURCE_SERVER_DBSERVER_DB_COMM_H_
#define CODE_SOURCE_SERVER_DBSERVER_DB_COMM_H_

#include <stdio.h>
#include <arpa/inet.h>
#include "sirius/core/os.h"
#include "sirius/core/lua_parser.h"
#include "sirius/core/singleton.h"
#include "common/utility/queue.h"
#include "common/utility/log.h"
#include "common/utility/list.h"
#include "dbproxy.pb.h"
#include "./rediswrapper.h"
#include "./mysqlwrapper.h"

#define SERVICE_NAME "[DBProxy]"

#define LogInfo(fmt, ...) \
  ImmortalLog::LogInfo(SERVICE_NAME "[%s:%d] " fmt, __FUNCTION__, __LINE__, ##__VA_ARGS__)
#define LogDebug(fmt, ...) \
  ImmortalLog::LogDebug(SERVICE_NAME "[%s:%d] " fmt, __FUNCTION__, __LINE__, ##__VA_ARGS__)
#define LogWarn(fmt, ...)  \
  ImmortalLog::LogWarn(SERVICE_NAME "[%s:%d] " fmt, __FUNCTION__, __LINE__, ##__VA_ARGS__)
#define LogError(fmt, ...) \
  ImmortalLog::LogError(SERVICE_NAME "[%s:%d] " fmt, __FUNCTION__, __LINE__, ##__VA_ARGS__)
#define LogFatal(fmt, ...) \
  ImmortalLog::LogFatal(SERVICE_NAME "[%s:%d] " fmt, __FUNCTION__, __LINE__, ##__VA_ARGS__)

#ifdef NDEBUG
#define LogMessage1(str, msg)
#else
#define LogMessage1(str, msg) \
  LogDebug(str ":\n%s\n", (msg)->DebugString().c_str())
#endif

#define LogHeader(p) \
  LogDebug("Header: cmd %u, seq %u, err %u\n", HeaderCmd(p), HeaderSeq(p), HeaderErr(p))

namespace google {
namespace protobuf {
class Message;
}
}

namespace immortal {
namespace db {
using std::string;
using google::protobuf::Message;
using sirius::utility::ManualSingleton;
using sirius::parser::LuaParser;
using immortal::common::ImmortalLog;
using immortal::utility::ListHead;
using namespace immortal::uv;
using namespace immortal::common;

class DBConn;
class DBTCP;
class DBConnLisenter;
class CenterConn;

class Handle {
 public:
  uint32_t cmd;
  uint32_t rcmd;
  uint32_t seq;
  uint32_t err;
  st::State st;
  uint32_t curr_seq;
  TCPConn *owner;
  DBClient *dbclient;
  RedisClient *redisclient;
  uint64_t start_time;
  ListHead<Handle *> list;

  Handle(): cmd(0), rcmd(0), seq(0), err(0), curr_seq(0), owner(NULL), start_time(0), list(this) {
    st.machine_ = NULL;
  }
  virtual ~Handle() {
    if (!list.Empty()) {
      list.Del();
    }
    // LogDebug("release Handle\n");
  }
};

enum {
  kDBStatGet = 0,
  kDBStatSetCache,
  kDBStatSetDB,
  kDBStatRawSQL,
  kDBStatNR,
};

struct DBStat {
  // 0: 0~10ms
  // 1: 10~50ms
  // 2: 50~100ms
  // 3: 100~200ms
  // 4: 200~500ms
  // 5: 500ms+
  uint32_t delay[6];
  uint32_t errcnt;
  uint32_t cachemiss;
  uint32_t total;
};

class DBProxy : public ManualSingleton<DBProxy> {
  friend class DBTCPLisenter;
  friend class DBConnLisenter;

 private:
  st::MachineManger *mm_ = NULL;
  DBClient **dbclients_ = NULL;
  RedisClient **rdclients_ = NULL;
  size_t dbclient_count_ = 0;
  size_t rdclient_count_ = 0;
  uv::Loop loop_;
  DBTCP *tcpserver_;
  bool running_= false;
  bool closing_ = false;
  // bool delay_flush_;
  std::map<uint32_t, Handle *> handlemap_;
  uint32_t dbseq_;
  uint16_t port_;
  std::string ip_;
  uint16_t center_port_;
  std::string center_ip_;
  CenterConn *center_conn_;
  uv::Timer timer_;
  LuaParser parser_;
  int aid_ = -1;
  int sid_ = -1;
  int64_t last_ping_ = 0;
  int64_t last_stat_ = 0;
  std::string log_path_ = "";
  std::string conf_path_ = "./config.lua";
  std::string addr_path_ = "";
  uv_signal_t sig_handle_;
  
  int InitConfig();
  int ReadLuaConfig();
  void LoadProtoFiles();
  int InitMachine();
  int InitTCPServer();
  int InitSplitTableFromConfig(DBClient *db, LuaRef& table);
  int InitFromConfig(LuaRef& db, void *loop);
  int InitDBClients(LuaRef& db, void *loop);
  int InitRedisClients(LuaRef& db, void *loop);

 public:
  // 0: get
  // 1: set_cache
  // 2: set_db
  // 3: rawsql
  DBStat dbstat_[kDBStatNR];
  size_t conn_count_ = 0;
  
  void AfterCache(uint32_t, char *buf, size_t len, int err);
  void AfterDB(uint32_t seq, int err, const std::vector<void *>& res);

  int CacheSet(Handle *h, const char *key, const char *buf, size_t bufsize);
  int CacheGet(Handle *h, const char *key);
  int CacheDel(Handle *h, const char *key);
  int CacheSetMsg(Handle *h, const char *key, Message *msg);

  int DBGet(Handle *h, const std::string& msg_type, const std::string& key);
  int DBDel(Handle *h, const std::string& msg_type, const std::string& key);
  int DBSet(Handle *h, const std::string& msg_type, const std::string& key, const std::string& value);
  int DBSetNX(Handle *h, const std::string& msg_type, const std::string& key, const std::string& value);
  int DBRawSQL(Handle *h, const std::string& msg_type, const std::string& sql);

  int AppendFlush(Handle *h, const dbproto::Set& set);
  int AppendFlush(Handle *h, const dbproto::Del& del);

  int SendCenterReg(TCPConn *conn);
  int SendCenterPing(TCPConn *conn);
  int SendMsg(Handle *handle, Message *msg);

  void OnTimer();
  
  uint32_t nextseq();
  void AddHandleSeq(uint32_t seq, Handle *h);
  void DelHandleSeq(uint32_t seq);
  Handle * FindHandleSeq(uint32_t seq);
  Handle * TakeHandleSeq(uint32_t seq);

  void InitHandle(TCPConn *conn, const char *buf, size_t len);
  void DelHandle(Handle *h);
  void InitDBAndRedis(Handle *h, const string& key);

  int Init();
  int InitAsService();
  int InitFromArgs(int argc, char **argv);
  int Run();
  void Shutdown();
  void TryShutdown();
  void ParseCommandArgs(int argc, char **argv);

  int Reply(Handle *req, Message *msg);
  int Reply(Handle *handle, const char *src, size_t len);

  void ProcUseDB(TCPConn *conn, const char *buf, size_t bufsize);
  // bool CheckUseDB(Handle *, TCPConn *conn);
  void BindConnHandle(TCPConn *, Handle *);

  uv_loop_t *loop() { return loop_.loop(); }
  void AddStat(DBStat *, uint64_t start_time);
  void ShowStat(void);
  void AddGetStat(uint64_t start_time);
  void AddSetStat(uint64_t start_time);
};

class DBConn : public uv::TCPConn {
 public:
  void HandleRequest(TCPConn *conn, const char *buf, size_t len);
  void OnClose(TCPConn *conn);
  void OnCloseDone(TCPConn *conn);
  DBConn(DBProxy *s) : handle_list_(NULL), db_(s) {}
  // Store *store_ = NULL;
  ListHead<Handle *> handle_list_;

 protected:
  DBProxy *db_;
};

class DBAction : public st::Action {
 public:
  TCPConn *conn;
};

extern st::StateMachine *NewSetState(DBProxy *s);
extern st::StateMachine *NewGetState(DBProxy *s);
extern st::StateMachine *NewDelState(DBProxy *s);
extern st::StateMachine *NewRawSQLState(DBProxy *s);

#define  g_GetDBService() (immortal::db::DBProxy::Instance())
#define  g_DelDBService() (immortal::db::DBProxy::Destroy())
}  // namespace db
}  // namespace immortal

#endif  // CODE_SOURCE_SERVER_DBSERVER_DB_COMM_H_

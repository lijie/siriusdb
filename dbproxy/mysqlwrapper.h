#ifndef CODE_SOURCE_SERVER_MYSQLWRAPPER_H_
#define CODE_SOURCE_SERVER_MYSQLWRAPPER_H_

#include <assert.h>

#include <string>
#include <vector>
#include <functional>

#include <google/protobuf/message.h>
#include <mysql/mysql.h>
#include "google/protobuf/message.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/compiler/importer.h"

#include "./uvwrapper.h"

namespace immortal {
namespace db {
using namespace google::protobuf;
using namespace google::protobuf::compiler;
using google::protobuf::Message;
using std::string;
using std::vector;

const int kErrNoData = -10000;

struct DBInitParams {
  uint32_t port;
  string ip;
  string username;
  string password;
  string dbname;
};

typedef std::function<void(uint32_t seq, int err, const std::vector<void *>& res)> DBCallback_t;

struct SplitTable;
struct MsgDesc;
struct Work;
struct DBClientConn;

class DBClient {
 public:
  int Init(DBCallback_t cb, void *loop, const DBInitParams& params);
  int Init(DBCallback_t cb, void *l, const string& url);

  int Get(uint32_t seq, const string& msg_type, const string& key);
  int Del(uint32_t seq, const string& msg_type, const string& key);
  int Set(uint32_t seq, const string& msg_type, const string& key, const string& value);
  int SetNX(uint32_t seq, const string& msg_type, const string& key, const string& value);
  int RawSQL(uint32_t seq, const string& msg_type, const string& sql);
  int PreAllocConns(size_t count);

  void AddMsgDesc(const string&);
  void AddSearchPath(vector<string>& paths);
  Message * ParseFromArray(const std::string& msg_type, const char *buf, size_t len);
  Message * ParseFromString(const std::string& msg_type, const string& value);
  Message * NewMessage(const std::string& msg_type);
  Message ** NewMessageArray(const string& msg_type, size_t count);

  void DoWork(Work *w);
  struct Work * AllocWork();
  void FreeWork(Work *w);
  int AfterWork(Work *w);

  struct DBClientConn * AllocConn();
  void FreeConn(DBClientConn *);

  void AddSplitTable(const string& tablename,
                     uint32_t (*hash)(const char *, size_t),
                     uint32_t mod);

 private:
  uv_loop_t *loop_;
  DBCallback_t cb_;
  QUEUE work_queue_;
  QUEUE conn_queue_;
  DBInitParams params_;
  DiskSourceTree srctree_;
  Importer *importer_;
  DynamicMessageFactory MsgFactory_;

  std::map<string, MsgDesc *> descmap_;
  std::map<string, SplitTable *> split_;

  int QueueWork(Work *w);
  void PrepareSQL(Work *w);
  bool GenerateSQL(MYSQL *db, Work *w);
  bool GenerateInsertUpdate(MYSQL *db, const std::string& tablename,
                            const std::string& key, const std::string& value, std::string *sql);
  bool GenerateInsert(MYSQL *db, const std::string& tablename,
                      const std::string& key, const std::string& value, std::string *sql);
  bool GenerateSelect(MYSQL *db, const std::string& tablename,
                      const std::string& key, std::string *sql);

  bool NeedHash(string *out, const string& tablename, Message *msg,
                const Descriptor *msg_desc);
  bool NeedHash(string *out, const string& tablename, const string& key);
  uint32_t keyhash(SplitTable *, Message *msg, const Descriptor *msg_desc);
  uint32_t fieldhash(SplitTable *, Message *msg, const FieldDescriptor *field,
                     FieldDescriptor::Type type);
};

extern int Res2PB(void *res, Message *msg);
extern int Res2PBArray(void *res1, Message **array, size_t count);
extern size_t ResRowCount(void *res);

}  // namespace mysql
}  // namespace immortal

#endif  // CODE_SOURCE_SERVER_MYSQLWRAPPER_H_

// Copyright (c) 2014 kingsoft

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sstream>
#include <iostream>
#include <vector>
#include <algorithm>

#include "uv/uv.h"
#include "common/utility/queue.h"
#include "dbproxy.pb.h"
#include "./mysqlwrapper.h"

#ifndef see_here
#define see_here db_err("check here [%s:%d:%s]\n", __FILE__, __LINE__, __func__)
#endif

#ifndef db_dbg
#define db_dbg(fmt, ...) \
  fprintf(stdout, fmt, ##__VA_ARGS__);
#endif

#ifndef db_err
#define db_err(fmt, ...) \
  fprintf(stderr, fmt, ##__VA_ARGS__);
#endif

extern "C" uint32_t hash_crc32(const char *, size_t);
extern "C" uint32_t hash_crc32a(const char *, size_t);

namespace immortal {
namespace db {

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::compiler;

std::tuple<string, string, string, string, string> ParseMySQLURL(const string& url);
std::tuple<string, uint16_t> ParseAddr(const string& addr);

enum {
  kSQLSelect = 1,
  kSQLInsertUpdate,
  kSQLDelete,
  kSQLRawSQL,
  kSQLInsert,
  kSQLInsertWithLastID,
  kSQLPreAlloc,
};

struct MsgDesc {
  const FileDescriptor *filedesc;
  const Descriptor *desc;
  const Message *prototype;
};

struct SplitTable {
  string tablename;
  uint32_t mod;
  uint32_t (*hash)(const char *key, size_t len);
};

struct DBClientConn {
  MYSQL db;
  bool connected;
  QUEUE queue;
  string dbname;
};

struct Work {
  uint32_t seq;
  uv_work_t uvwork;
  int sqltype;
  string sql;
  string msg_type;
  string key;
  string value;
  QUEUE queue;
  DBClientConn *conn;
  // MYSQL db;
  MYSQL_RES *res;
  std::vector<void *> vec_res;
  DBClient *dq;
  // bool connected;
  int err;
};

static void InitWork(Work *w) {
  w->seq = 0;
  memset(&w->uvwork, 0, sizeof(uv_work_t));
  // w->msg = NULL;
  QUEUE_INIT(&w->queue);
  w->sqltype = -1;
  w->res = NULL;
  w->dq = NULL;
  w->err = 0;
  w->vec_res.clear();
  w->conn = NULL;
}

DBClientConn * DBClient::AllocConn() {
  DBClientConn *conn;
  if (QUEUE_EMPTY(&conn_queue_)) {
    conn = new DBClientConn;
    conn->connected = false;
    // printf("workcout %zd\n", ++workcount);
  } else {
    QUEUE *q = QUEUE_HEAD(&conn_queue_);
    QUEUE_REMOVE(q);
    conn = QUEUE_DATA(q, struct DBClientConn, queue);
  }
  return conn;
}

void DBClient::FreeConn(DBClientConn *conn) {
  QUEUE_INSERT_HEAD(&conn_queue_, &conn->queue);
}

void DBClient::AddSplitTable(const string& tablename,
                           uint32_t (*hash)(const char *, size_t),
                           uint32_t mod) {
  auto st = new SplitTable;
  st->tablename = tablename;
  st->hash = hash;
  st->mod = mod;
  split_[tablename] = st;
}

static void fieldvalue(MYSQL *db, Message *msg, const FieldDescriptor *field, FieldDescriptor::Type type, std::string *buf) {
  char tmp[64];
  size_t len;
  const Reflection *refl = msg->GetReflection();
    switch (type) {
      case FieldDescriptor::TYPE_INT32:
      case FieldDescriptor::TYPE_SINT32:
      case FieldDescriptor::TYPE_SFIXED32: {
        int32_t v = refl->GetInt32(*msg, field);
        len = snprintf(tmp, 64, "%d", v);
        buf->append(tmp, len);
        return;
      }

      case FieldDescriptor::TYPE_UINT32:
      case FieldDescriptor::TYPE_FIXED32: {
        uint32_t v = refl->GetUInt32(*msg, field);
        len = snprintf(tmp, 64, "%u", v);
        buf->append(tmp, len);
        return;
      }

      case FieldDescriptor::TYPE_UINT64:
      case FieldDescriptor::TYPE_FIXED64: {
        uint64_t v = refl->GetUInt64(*msg, field);
        len = snprintf(tmp, 64, "%" PRIu64, v);
        buf->append(tmp, len);
        return;
      }

      case FieldDescriptor::TYPE_STRING:
      case FieldDescriptor::TYPE_BYTES: {
        std::string str = refl->GetString(*msg, field);
        char *tmp = (char *)alloca(str.length() * 2);
        mysql_real_escape_string(db, tmp, (char *)str.c_str(), str.length());
        buf->append("\'");
        buf->append(tmp);
        buf->append("\'");
        return;
      }

      case FieldDescriptor::TYPE_BOOL: {
        std::stringstream ss;
        bool v = refl->GetBool(*msg, field);
        ss << v;
        buf->append(ss.str());
        return;
      }

      case FieldDescriptor::TYPE_MESSAGE: {
        const Message& m = refl->GetMessage(*msg, field);
        int size = m.ByteSize();
        char *tmp = (char *)alloca(size);
        bool succ = m.SerializeToArray(tmp, size);
        if (!succ)
          return;
        char *tmp2 = (char *)alloca(size * 2);
        mysql_real_escape_string(db, tmp2, tmp, size);
        buf->append("\'");
        buf->append(tmp2);
        buf->append("\'");
        return;
      }

      default:
        printf("unsupported type %d\n", type);
        assert(0);
    }
}

static void appendfield(MYSQL *db, Message *msg,
                        const Descriptor *msg_desc, string *buf) {
  int field_count = msg_desc->field_count();
  int comma_count = field_count - 1;

  for (int j = 0; j < field_count; j++) {
    const FieldDescriptor * field_desc = msg_desc->field(j);
    buf->append(field_desc->name() + "=");
    // printf("get field name %s\n", field_desc->name().c_str());
    fieldvalue(db, msg, field_desc, field_desc->type(), buf);
    if (comma_count-- > 0)
      buf->append(",");
  }
}

class FileErrorCollector : public MultiFileErrorCollector {
 public:
  void AddError(const string& filename, int line, int column,
                const string& message) {
    cout << filename << endl;
    cout << line << endl;
    cout << column << endl;
    cout << message << endl;
  }
};

void DBClient::AddSearchPath(vector<string>& paths) {
  for (size_t i = 0; i < paths.size(); i++) {
    srctree_.MapPath("", paths[i]);
  }
  auto err = new FileErrorCollector;
  importer_ = new Importer(&srctree_, err);
}

void DBClient::AddMsgDesc(const string& proto_file) {
  const FileDescriptor *file_desc = importer_->Import(proto_file);

  int count = file_desc->message_type_count();
  if (count <= 0)
    return;

  // string key = proto_file.substr(0, proto_file.find_last_of("."));

  for (int i = 0; i < count; i++) {
    const Descriptor * msg_desc = file_desc->message_type(i);
    printf("message type: %s\n", msg_desc->name().c_str());

    bool m = msg_desc->options().GetExtension(dbproto::orm);
    if (m) {
      MsgDesc *msgdesc = new MsgDesc;
      msgdesc->filedesc = file_desc;
      msgdesc->desc = msg_desc;
      msgdesc->prototype = MsgFactory_.GetPrototype(msg_desc);
      descmap_[msg_desc->name()] = msgdesc;
    }
  }
}

Message * DBClient::ParseFromArray(const std::string& msg_type, const char *buf, size_t len) {
  map<string, MsgDesc *>::iterator it = descmap_.find(msg_type);
  if (it == descmap_.end())
    return NULL;

  MsgDesc *msgdesc = it->second;
  Message *msg = msgdesc->prototype->New();
  if (msg == NULL)
    return NULL;

  msg->ParseFromArray(buf, len);
  return msg;
}

Message * DBClient::NewMessage(const string& msg_type) {
  map<string, MsgDesc *>::iterator it = descmap_.find(msg_type);
  if (it == descmap_.end())
    return NULL;

  MsgDesc *msgdesc = it->second;
  Message *msg = msgdesc->prototype->New();
  return msg;
}

Message ** DBClient::NewMessageArray(const string& msg_type, size_t count) {
  map<string, MsgDesc *>::iterator it = descmap_.find(msg_type);
  if (it == descmap_.end())
    return NULL;

  MsgDesc *msgdesc = it->second;
  Message** array = new Message*[count];
  if (array == NULL)
    return array;

  for (size_t i = 0; i < count; i++) {
    array[i] = msgdesc->prototype->New();
  }

  return array;
}

Message * DBClient::ParseFromString(const std::string& msg_type, const string& data) {
  map<string, MsgDesc *>::iterator it = descmap_.find(msg_type);
  if (it == descmap_.end())
    return NULL;

  MsgDesc *msgdesc = it->second;
  Message *msg = msgdesc->prototype->New();
  if (msg == NULL)
    return NULL;

  msg->ParseFromString(data);
  return msg;
}

bool DBClient::GenerateSQL(MYSQL *db, Work *w) {
  switch (w->sqltype) {
    case kSQLInsertUpdate:
      return GenerateInsertUpdate(db, w->msg_type, w->key, w->value, &w->sql);
    case kSQLInsert:
      return GenerateInsert(db, w->msg_type, w->key, w->value, &w->sql);
    case kSQLInsertWithLastID:
      return GenerateInsert(db, w->msg_type, w->key, w->value, &w->sql);
    case kSQLRawSQL:
      return true;
    case kSQLSelect:
      return GenerateSelect(db, w->msg_type, w->key, &w->sql);
    case kSQLPreAlloc:
    default:
      return false;
  }

  return false;
}

bool DBClient::GenerateInsert(MYSQL *db, const std::string& tablename,
                              const std::string& key, const std::string& value,
                              std::string *sql) {
  // printf("find msg type %s\n", tablename.c_str());
  auto it = descmap_.find(tablename);
  if (it == descmap_.end()) {
    fprintf(stderr, "msg type %s not found\n", tablename.c_str());
    for (auto iit = descmap_.begin(); iit != descmap_.end(); iit++) {
      std::cout << iit->first << std::endl;
    }
    return false;
  }

  MsgDesc *msgdesc = it->second;
  const Descriptor *msg_desc = msgdesc->desc;
  Message *msg = msgdesc->prototype->New();
  if (!msg->ParseFromString(value)) {
    see_here;
    delete msg;
    return false;
  }

  string hashed_tbname;
  string *to = (string *)&tablename;
  if (NeedHash(&hashed_tbname, tablename, key))
    to = &hashed_tbname;
  
  sql->append("INSERT INTO " + *to + " SET ");
  appendfield(db, msg, msg_desc, sql);

  if (true) {
    sql->append("; select LAST_INSERT_ID() as last_insert_id;");
  }
  
  // end
  // printf("generated sql:\n%s\n", sql->c_str());
  delete msg;
  return true;
}

bool DBClient::GenerateInsertUpdate(MYSQL *db, const std::string& tablename,
                                    const std::string& key, const std::string& value,
                                    std::string *sql) {
  // printf("find msg type %s\n", tablename.c_str());
  auto it = descmap_.find(tablename);
  if (it == descmap_.end()) {
    fprintf(stderr, "msg type %s not found\n", tablename.c_str());
    for (auto iit = descmap_.begin(); iit != descmap_.end(); iit++) {
      std::cout << iit->first << std::endl;
    }
    return false;
  }

  MsgDesc *msgdesc = it->second;
  const Descriptor *msg_desc = msgdesc->desc;
  Message *msg = msgdesc->prototype->New();
  if (!msg->ParseFromString(value)) {
    see_here;
    delete msg;
    return false;
  }

  string hashed_tbname;
  string *to = (string *)&tablename;
  if (NeedHash(&hashed_tbname, tablename, key))
    to = &hashed_tbname;
        
  sql->append("INSERT INTO " + *to + " SET ");
  appendfield(db, msg, msg_desc, sql);
  sql->append(" ON DUPLICATE KEY UPDATE ");
  appendfield(db, msg, msg_desc, sql);
  // end
  // printf("generated sql:\n%s\n", sql->c_str());
  delete msg;
  return true;
}

bool DBClient::NeedHash(string *out, const string& tablename, const string& key) {
  auto it = split_.find(tablename);
  if (it == split_.end())
    return false;
    
  char tmp[16];
  out->append(tablename);

  SplitTable *st = it->second;
  snprintf(tmp, sizeof(tmp), "%u", st->hash(key.c_str(), key.size()) % st->mod);
  out->append(tmp);
  return true;
}

bool DBClient::GenerateSelect(MYSQL *db, const std::string& tablename,
                            const std::string& key, std::string *sql) {
  auto it = descmap_.find(tablename);
  if (it == descmap_.end())
    return false;

  string hashed_tbname;
  string *to = (string *)&tablename;
  if (NeedHash(&hashed_tbname, tablename, key))
    to = &hashed_tbname;
  
  MsgDesc *msgdesc = it->second;
  string indexfield = msgdesc->desc->options().GetExtension(dbproto::indexfield);
  sql->append("SELECT * FROM " + *to + " WHERE " + indexfield + "=\'" + key + "\'");
  // end
  // printf("generated sql:\n%s\n", sql->c_str());
  return true;
}

// lijie3:
// 利用Protobuf的反射实现从MySQL Result还原到Protobuf对象
using namespace google::protobuf;
size_t ResRowCount(void *res1) {
  MYSQL_RES *res = (MYSQL_RES *)res1;
  if (res == NULL)
    return 0;
  return (size_t)mysql_num_rows(res);
}

int Res2PBArray(void *res1, google::protobuf::Message **array, size_t count) {  
  MYSQL_RES *res = (MYSQL_RES *)res1;
  MYSQL_FIELD *fields;
  unsigned long *lengths;
  MYSQL_ROW row;
  unsigned long rowcount;
  unsigned int num_field;

  if (res) {
    num_field = mysql_num_fields(res);
    fields = mysql_fetch_fields(res);
    // row = mysql_fetch_row(res);
    rowcount = mysql_num_rows(res);
  } else {
    return kErrNoData;
  }

  size_t total = std::min(rowcount, count);

  if (total == 0)
    return kErrNoData;

  for (size_t j = 0; j < total; j++) {
    Message *msg = array[j];
    const Descriptor *desc = msg->GetDescriptor();
    const Reflection *refl = msg->GetReflection();
    const FieldDescriptor *field = NULL;
    char *name;

    if ((row = mysql_fetch_row(res)) == NULL)
      continue;
    lengths = mysql_fetch_lengths(res);

    for (unsigned int i = 0; i < num_field; i++) {
      if (row[i] == NULL)
        continue;
          
      google::protobuf::Message *m;
      name = fields[i].name;
      field = desc->FindFieldByName(name);
      if (field == NULL)
        continue;
      switch (field->type()) {
        case FieldDescriptor::TYPE_SFIXED32:
        case FieldDescriptor::TYPE_SINT32:
        case FieldDescriptor::TYPE_INT32:
          refl->SetInt32(msg, field, (int32_t)atoll(row[i]));
          break;
        case FieldDescriptor::TYPE_FIXED32:
        case FieldDescriptor::TYPE_UINT32:
          refl->SetUInt32(msg, field, (uint32_t)atoll(row[i]));
          break;
        case FieldDescriptor::TYPE_FIXED64:
        case FieldDescriptor::TYPE_UINT64:
          // BUG(lijie3):
          // atoll not works if value is too big
          refl->SetUInt64(msg, field, (uint64_t)atoll(row[i]));
          break;
        case FieldDescriptor::TYPE_STRING:
        case FieldDescriptor::TYPE_BYTES:
          refl->SetString(msg, field, std::string(row[i], lengths[i]));
          break;
        case FieldDescriptor::TYPE_BOOL:
          refl->SetBool(msg, field, (bool)atoi(row[i]));
          break;
        case FieldDescriptor::TYPE_MESSAGE:
          m = refl->MutableMessage(msg, field);
          assert(m);
          m->ParseFromArray(row[i], lengths[i]);
          break;
        default:
          assert(0);
          break;
      }
    }
  }

  return 0;
}

int Res2PB(void *res1, google::protobuf::Message *msg) {
  MYSQL_RES *res = (MYSQL_RES *)res1;
  MYSQL_FIELD *fields;
  unsigned long *lengths;
  MYSQL_ROW row;
  unsigned int num_field;

  if (res) {
    num_field = mysql_num_fields(res);
    fields = mysql_fetch_fields(res);
    row = mysql_fetch_row(res);
    lengths = mysql_fetch_lengths(res);
  } else {
    return kErrNoData;
  }

  if (row == NULL)
    return kErrNoData;

  const Descriptor *desc = msg->GetDescriptor();
  const Reflection *refl = msg->GetReflection();
  const FieldDescriptor *field = NULL;

  char *name;
  for (unsigned int i = 0; i < num_field; i++) {
    // from 何龙:
    // 这里数据库如果定义时未指定列的默认值,
    // 则存在row[i]为NULL的情况
    //
    // 如果某列为空, 且该列的PB定义未指定默认值
    // 则当前msg序列化会失败.
    if (row[i] == NULL)
      continue;

    google::protobuf::Message *m;
    name = fields[i].name;
    field = desc->FindFieldByName(name);
    if (field == NULL)
      continue;
    switch (field->type()) {
      case FieldDescriptor::TYPE_SFIXED32:
      case FieldDescriptor::TYPE_SINT32:
      case FieldDescriptor::TYPE_INT32:
        refl->SetInt32(msg, field, (int32_t)atoll(row[i]));
        break;
      case FieldDescriptor::TYPE_FIXED32:
      case FieldDescriptor::TYPE_UINT32:
        refl->SetUInt32(msg, field, (uint32_t)atoll(row[i]));
        break;
      case FieldDescriptor::TYPE_FIXED64:
      case FieldDescriptor::TYPE_UINT64:
        // atoll may failed here
        refl->SetUInt64(msg, field, (uint64_t)atoll(row[i]));
        break;
      case FieldDescriptor::TYPE_SFIXED64:
      case FieldDescriptor::TYPE_SINT64:
      case FieldDescriptor::TYPE_INT64:
        // atoll may failed here
        refl->SetInt64(msg, field, (int64_t)atoll(row[i]));
        break;
      case FieldDescriptor::TYPE_STRING:
      case FieldDescriptor::TYPE_BYTES:
        refl->SetString(msg, field, std::string(row[i], lengths[i]));
        break;
      case FieldDescriptor::TYPE_MESSAGE:
        m = refl->MutableMessage(msg, field);
        assert(m);
        m->ParseFromArray(row[i], lengths[i]);
        break;
      case FieldDescriptor::TYPE_BOOL:
        refl->SetBool(msg, field, (bool)atoi(row[i]));
        break;
      default:
        fprintf(stderr, "unknown type %d\n", field->type());
        assert(0);
        break;
    }
  }

  return 0;
}

int DBClient::AfterWork(Work *w) {
  if (w->sqltype != kSQLPreAlloc) {
    uint32_t seq = w->seq;
    cb_(seq, w->err, w->vec_res);
    for (size_t i = 0; i < w->vec_res.size(); i++) {
      mysql_free_result((MYSQL_RES *)w->vec_res[i]);
    }
    w->vec_res.clear();
  }

  FreeConn(w->conn);
  FreeWork(w);
  return 0;
}

int DBClient::PreAllocConns(size_t count) {
  for (size_t i = 0; i < count; i++) {
    Work *w = AllocWork();
    if (w == NULL) {
      db_err("no work ?\n");
      return -1;
    }
    w->seq = 0;
    w->sqltype = kSQLPreAlloc;
    // queue work to threadpool
    QueueWork(w);
  }
  return 0;
}

int DBClient::Del(uint32_t seq, const string& msg_type, const string& key) {
  return -1;
}

int DBClient::Get(uint32_t seq, const string& msg_type, const string& key) {
  Work *w = AllocWork();
  if (w == NULL) {
    db_err("no work ?\n");
    return -1;
  }
  w->seq = seq;
  w->sqltype = kSQLSelect;
  w->msg_type = msg_type;
  w->key = key;
  // queue work to threadpool
  QueueWork(w);
  return 0;
}

int DBClient::SetNX(uint32_t seq, const string& msg_type, const string& key, const string& value) {
  Work *w = AllocWork();
  if (w == NULL) {
    db_err("no work ?\n");
    return -1;
  }
  w->key = key;
  w->value = value;
  w->seq = seq;
  if (true)
    w->sqltype = kSQLInsertWithLastID;
  else
    w->sqltype = kSQLInsert;
  w->msg_type = msg_type;
  // queue work to threadpool
  QueueWork(w);
  return 0;
}

int DBClient::Set(uint32_t seq, const string& msg_type, const string& key, const string& value) {
  Work *w = AllocWork();
  if (w == NULL) {
    db_err("no work ?\n");
    return -1;
  }
  w->key = key;
  w->value = value;
  w->seq = seq;
  w->sqltype = kSQLInsertUpdate;
  w->msg_type = msg_type;
  // queue work to threadpool
  QueueWork(w);
  return 0;
}

int DBClient::RawSQL(uint32_t seq, const string& msg_type, const string& sql) {
  Work *w = AllocWork();
  if (w == NULL) {
    db_err("no work ?\n");
    return -1;
  }
  w->seq = seq;
  w->sqltype = kSQLRawSQL;
  w->msg_type = msg_type;
  w->sql = sql;
  QueueWork(w);
  return 0;
}

int DBClient::Init(DBCallback_t cb, void *l, const DBInitParams& params) {
  mysql_library_init(0, NULL, NULL);
  loop_ = (uv_loop_t *)l;
  params_ = params;
  QUEUE_INIT(&work_queue_);
  QUEUE_INIT(&conn_queue_);
  cb_ = cb;
  // PreAllocConns(10);
  return 0;
}

int DBClient::Init(DBCallback_t cb, void *l, const string& url) {
  DBInitParams params;
  auto tup = ParseMySQLURL(url);
  params.username = std::get<0>(tup);
  params.password = std::get<1>(tup);
  params.dbname = std::get<4>(tup);

  auto addr = std::get<3>(tup);
  auto tup2 = ParseAddr(addr);

  params.ip = std::get<0>(tup2);
  params.port = std::get<1>(tup2);

  return Init(cb, l, params);
}

// static size_t workcount = 0;
struct Work * DBClient::AllocWork() {
  Work *w;

  // TODO(lijie3):
  // 需要限制work_queue的size ?
  if (QUEUE_EMPTY(&work_queue_)) {
    w = new Work;
    InitWork(w);
    w->dq = this;
    // printf("workcout %zd\n", ++workcount);
  } else {
    QUEUE *q = QUEUE_HEAD(&work_queue_);
    QUEUE_REMOVE(q);
    w = QUEUE_DATA(q, struct Work, queue);
    InitWork(w);
    w->dq = this;
  }

  return w;
}

void DBClient::FreeWork(Work *w) {
  w->sql.clear();
  w->sqltype = -1;
  w->res = NULL;
  QUEUE_INSERT_HEAD(&work_queue_, &w->queue);
}

void DBClient::DoWork(struct Work *w) {
  SI_ASSERT(w->conn != NULL);

  MYSQL *db;
  auto conn = w->conn;
  if (!conn->connected) {
    mysql_init(&conn->db);

    char v = 1;
    mysql_options(&conn->db, MYSQL_OPT_RECONNECT, &v);
    // fprintf(stdout, "connect db %s %s %s %s\n",
    //         params_.ip.c_str(), params_.username.c_str(),
    //         params_.password.c_str(), params_.dbname.c_str());
    // connect db
    db = mysql_real_connect(&conn->db, params_.ip.c_str(),
                            params_.username.c_str(),
                            params_.password.c_str(),
                            params_.dbname.c_str(),
                            params_.port, NULL, CLIENT_MULTI_STATEMENTS);
    if (db == NULL) {
      db_err("connect db failed %s %d\n", mysql_error(&conn->db), mysql_errno(&conn->db));
      w->err = -1;
      return;
    }
    conn->dbname = params_.dbname;
  } else {
    db = &conn->db;
  }

  conn->connected = true;
  mysql_ping(db);

  if (!GenerateSQL(db, w)) {
    w->err = dbproto::ERR_UNKNOWMSG;
    return;
  }

  // db_dbg("dbname: %s, sql %s\n", conn->dbname.c_str(), w->sql.c_str());
  int r = mysql_real_query(db, w->sql.c_str(), w->sql.size());
  if (r != 0) {
    db_err("mysql_real_query error %s %d\n", mysql_error(db), mysql_errno(db));
    w->err = mysql_errno(db);
  } else {
    w->err = 0;
  }
  do {
    MYSQL_RES *res = mysql_store_result(db);
    w->vec_res.push_back((void *)res);
  } while (mysql_next_result(db) == 0);
  return;
}

// run in queue thread
static void DBSQLWorkCB(uv_work_t *uvwork) {
  struct Work *w = (struct Work *)uvwork->data;
  assert(w);
  assert(w->dq);
  if (w && w->dq)
    w->dq->DoWork(w);
}

// run in main thread
static void DBSQLAfterWorkCB(uv_work_t *uvwork, int status) {
  Work *w = reinterpret_cast<Work *>(uvwork->data);
  assert(w->dq);
  w->dq->AfterWork(w);
}

int DBClient::QueueWork(Work *w) {
  w->conn = AllocConn();
  w->uvwork.data = w;
  return uv_queue_work(loop_, &w->uvwork, DBSQLWorkCB, DBSQLAfterWorkCB);
}

}  // namespace mysql
}  // namespace immortal


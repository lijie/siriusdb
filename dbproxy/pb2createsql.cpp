#include <stdio.h>
#include <vector>
#include <map>
#include <iostream>
#include "google/protobuf/message.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/compiler/importer.h"
#include "dbproxy.pb.h"

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::compiler;

struct MsgDesc {
  const FileDescriptor *filedesc;
  const Descriptor *desc;
  const Message *prototype;
};

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

static DiskSourceTree srctree_;
static Importer *importer_;
static DynamicMessageFactory MsgFactory_;
std::map<string, MsgDesc *> descmap_;

void AddSearchPath(vector<string>& paths) {
  for (size_t i = 0; i < paths.size(); i++) {
    srctree_.MapPath("", paths[i]);
  }
  FileErrorCollector err;
  importer_ = new Importer(&srctree_, &err);
}

void AddMsgDesc(const string& proto_file) {
  const FileDescriptor *file_desc = importer_->Import(proto_file);

  int count = file_desc->message_type_count();
  if (count <= 0)
    return;

  // string key = proto_file.substr(0, proto_file.find_last_of("."));

  for (int i = 0; i < count; i++) {
    const Descriptor * msg_desc = file_desc->message_type(i);

    bool m = msg_desc->options().GetExtension(dbproto::orm);
    if (!m)
      continue;

    int field_count = msg_desc->field_count();
    if (field_count <= 0)
      continue;

    int comma = field_count - 1;
    if (msg_desc->options().HasExtension(dbproto::mysql_create_table_option))
      comma++;
    
    string sql = "CREATE TABLE IF NOT EXISTS";
    sql.append(" " + msg_desc->name() + " (");
    for (int i = 0; i < field_count; i++) {
      const FieldDescriptor *field_desc = msg_desc->field(i);
      const FieldOptions& opt = field_desc->options();
      // cout << opt.GetExtension(dbproto::mysql_create_option) << endl;
      sql.append(field_desc->name() + " " + opt.GetExtension(dbproto::mysql_create_option));
      if (comma--) {
        sql.append(", ");
      }
    }
    sql.append(msg_desc->options().GetExtension(dbproto::mysql_create_table_option));
    sql.append(");");
    cout << sql << endl;
  }
}

int main(int argc, char **argv) {
  vector<string> v;
  v.push_back("../protocol");
  v.push_back("../include");
  AddSearchPath(v);
  AddMsgDesc("dbproxy.proto");
  AddMsgDesc("onlinedb.proto");
  AddMsgDesc("immortaldb.proto");
  return 0;
}

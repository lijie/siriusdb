#include <stdio.h>
#include <string>
#include <tuple>

namespace immortal {
namespace db {

using std::string;

std::tuple<string, string, string, string, string> ParseMySQLURL(const string& url) {
  string user;
  string pass;
  string protocol;
  string addr;
  string dbname;

  size_t pos = url.find_first_of(":", 0);
  if (pos != string::npos) {
    user = url.substr(0, pos);
  }

  size_t start = pos + 1;
  pos = url.find_first_of("@", start);
  if (pos != string::npos) {
    pass = url.substr(start, pos - start);
  }

  start = pos + 1;
  pos = url.find_first_of("(", start);
  if (pos != url.npos) {
    protocol = url.substr(start, pos - start);
  }

  start = pos + 1;
  pos = url.find_first_of(")", start);
  if (pos != url.npos) {
    addr = url.substr(start, pos - start);
  }

  start = pos + 2;
  if (start < url.length()) {
    dbname = url.substr(start);
  }

  return std::make_tuple(user, pass, protocol, addr, dbname);
}

std::tuple<string, uint16_t> ParseAddr(const string& addr) {
  string ip;
  uint16_t port;

  size_t pos = addr.find_first_of(":", 0);
  if (pos != addr.npos) {
    ip = addr.substr(0, pos);
  }

  port = (uint16_t)atoi(addr.substr(pos + 1).c_str());
  return std::make_tuple(ip, port);
}

std::tuple<string, int> ParseSplitTable(const string& str) {
  string s;
  int mod;

  size_t pos = str.find_first_of("/", 0);
  if (pos != str.npos) {
    s = str.substr(0, pos);
  }

  mod = atoi(str.substr(pos + 1).c_str());
  return std::make_tuple(s, mod);
}

}
}

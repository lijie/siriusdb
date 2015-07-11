#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <mysql/mysql.h>
#include <string>

using std::string;

int main() {
  MYSQL db;
  MYSQL *dbp;
  MYSQL_RES *res;

  mysql_init(&db);
  // connect db
  dbp = mysql_real_connect(&db, "127.0.0.1", "root", "root", "test_for_dba", 3306, NULL, CLIENT_MULTI_STATEMENTS);
  assert(dbp != NULL);

  string sql;
  sql.append("insert into NameTable set name=\"testplayer3\"; select LAST_INSERT_ID() as last_inert_id;");
  int r = mysql_real_query(dbp, sql.c_str(), sql.size());
  printf("query result %d\n", r);
  if (r != 0) {
    printf("mysql_real_query error %s %d\n", mysql_error(dbp), mysql_errno(dbp));
  }
  do {
    res = mysql_store_result(dbp);
    if (res) {
      printf("has res\n");
    } else {
      printf("no res\n");
    }
    mysql_free_result(res);
  } while (mysql_next_result(dbp) == 0);
}

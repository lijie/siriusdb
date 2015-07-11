#include <stdio.h>
#include "./dbproxy_comm.h"

int main(int argc, char **argv) {
  auto s = g_GetDBService();
  auto r = s->InitFromArgs(argc, argv);
  if (r != 0) {
    fprintf(stderr, "dbproxy init error %d\n", r);
    return 255;
  }
  s->Run();
  return 0;
}

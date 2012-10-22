#ifndef _LENV_H_4486E7C8_DB24_4617_B269_0E2653784BE4_
#define _LENV_H_4486E7C8_DB24_4617_B269_0E2653784BE4_

#include "lodbc.h"

typedef struct lodbc_env{
  uchar    flags;
  SQLHENV  handle;
  int      conn_counter;
} lodbc_env;

lodbc_env *lodbc_getenv_at (lua_State *L, int i);
#define lodbc_getenv(L) lodbc_getenv_at((L),1)

void lodbc_env_initlib (lua_State *L, int nup);

int lodbc_environment_create(lua_State *L, SQLHENV henv, uchar own);

#endif 
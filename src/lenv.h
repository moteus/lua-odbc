#ifndef _LENV_H_4486E7C8_DB24_4617_B269_0E2653784BE4_
#define _LENV_H_4486E7C8_DB24_4617_B269_0E2653784BE4_

#include "lodbc.h"

typedef struct lodbc_env{
  uchar    flags;
  SQLHENV  handle;
  int      conn_counter;
  int      conn_list_ref;
} lodbc_env;

LODBC_INTERNAL lodbc_env *lodbc_getenv_at (lua_State *L, int i);
#define lodbc_getenv(L) lodbc_getenv_at((L),1)

LODBC_INTERNAL void lodbc_env_initlib (lua_State *L, int nup);

LODBC_INTERNAL int lodbc_environment_create(lua_State *L, SQLHENV henv, uchar own);

#endif 
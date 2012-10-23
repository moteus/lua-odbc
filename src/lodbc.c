#include "lodbc.h"
#include "lenv.h"
#include "lcnn.h"
#include "lstmt.h"
#include "lerr.h"
#include "l52util.h"
#include "luaodbc.h"

LODBC_EXPORT unsigned int lodbc_odbcver(){
  return LODBC_ODBCVER;
}

LODBC_EXPORT int lodbc_environment(lua_State *L, SQLHENV henv, unsigned char own){
  return lodbc_environment_create(L, henv, own);
}

LODBC_EXPORT int lodbc_connection(lua_State *L, SQLHDBC hdbc,  unsigned char own){
  return lodbc_connection_create(L, hdbc, NULL, 0, own);
}

LODBC_EXPORT int lodbc_statement(lua_State *L, SQLHSTMT hstmt, unsigned char own){
  return lodbc_statement_create(L,hstmt, NULL, 0, own, 0, 0);
}


static int lodbc_environment_(lua_State *L){
  return lodbc_environment_create(L, SQL_NULL_HANDLE, 1);
}

static const struct luaL_Reg lodbc_func[]   = {
  { "environment",  lodbc_environment_    },

  {NULL, NULL}
};

static void lodbc_init_lib(lua_State *L, int nup){
  lua_newtable(L); 

  lua_newtable(L); // registry
  lua_pushvalue(L,-1); lodbc_err_initlib (L, 1);
  lua_pushvalue(L,-1); lodbc_env_initlib (L, 1);
  lua_pushvalue(L,-1); lodbc_cnn_initlib (L, 1);
  lua_pushvalue(L,-1); lodbc_stmt_initlib(L, 1);
  lua_pop(L,1);

  luaL_setfuncs(L, lodbc_func, nup);
}

LODBC_EXPORT int luaopen_lodbc (lua_State *L){
  lodbc_init_lib(L, 0);
  return 1;
}

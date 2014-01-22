#include "lodbc.h"
#include "lenv.h"
#include "lcnn.h"
#include "lstmt.h"
#include "lerr.h"
#include "lval.h"
#include "l52util.h"
#include "luaodbc.h"
#include "utils.h"

LODBC_EXPORT void lodbc_version(int *major, int *minor, int *patch){
  if(major) *major = LODBC_VERSION_MAJOR;
  if(minor) *minor = LODBC_VERSION_MINOR;
  if(patch) *patch = LODBC_VERSION_PATCH;
}

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

static int lodbc_version_(lua_State *L){
  lua_pushnumber(L, LODBC_VERSION_MAJOR);
  lua_pushnumber(L, LODBC_VERSION_MINOR);
  lua_pushnumber(L, LODBC_VERSION_PATCH);
#ifdef LODBC_VERSION_COMMENT
  if(LODBC_VERSION_COMMENT[0]){
    lua_pushliteral(L, LODBC_VERSION_COMMENT);
    return 4;
  }
#endif
  return 3;
}

static int lodbc_push_version(lua_State *L){
  lua_pushnumber(L, LODBC_VERSION_MAJOR);
  lua_pushliteral(L, ".");
  lua_pushnumber(L, LODBC_VERSION_MINOR);
  lua_pushliteral(L, ".");
  lua_pushnumber(L, LODBC_VERSION_PATCH);
#ifdef LODBC_VERSION_COMMENT
  if(LODBC_VERSION_COMMENT[0]){
    lua_pushliteral(L, "-"LODBC_VERSION_COMMENT);
    lua_concat(L, 6);
  }
  else
#endif
  lua_concat(L, 5);
  return 1;
}

static int lodbc_environment_(lua_State *L){
  return lodbc_environment_create(L, SQL_NULL_HANDLE, 1);
}

static int lodbc_init_connection(lua_State *L){
  void *src = lua_touserdata(L, 1);
  int   own = lua_toboolean(L, 2);
  luaL_argcheck(L, lua_islightuserdata(L, 1), 1, "lightuserdata expected");

  LODBC_STATIC_ASSERT(sizeof(src) <= sizeof(SQLHDBC));
  return lodbc_connection(L, (SQLHDBC)src, own);
}

static int lodbc_getenvmeta(lua_State *L){
  lutil_getmetatablep(L, LODBC_ENV);
  return 1;
}

static int lodbc_getcnnmeta(lua_State *L){
  lutil_getmetatablep(L, LODBC_CNN);
  return 1;
}

static int lodbc_getstmtmeta(lua_State *L){
  lutil_getmetatablep(L, LODBC_STMT);
  return 1;
}

static const struct luaL_Reg lodbc_func[]   = {
  { "version",      lodbc_version_        },
  { "environment",  lodbc_environment_    },

  { "getenvmeta",   lodbc_getenvmeta      },
  { "getcnnmeta",   lodbc_getcnnmeta      },
  { "getstmtmeta",  lodbc_getstmtmeta     },

  { "init_connection",  lodbc_init_connection },

  {NULL, NULL}
};

static void lodbc_init_lib(lua_State *L, int nup){
  lua_newtable(L);

  lua_pushliteral(L, "NULL");
  lodbc_pushnull(L);
  lua_rawset(L, -3);

  lua_pushliteral(L, "_VERSION");
  lodbc_push_version(L);
  lua_rawset(L, -3);

  lua_newtable(L); // registry
  lua_pushvalue(L,-1); lodbc_err_initlib (L, 1);
  lua_pushvalue(L,-1); lodbc_env_initlib (L, 1);
  lua_pushvalue(L,-1); lodbc_cnn_initlib (L, 1);
  lua_pushvalue(L,-1); lodbc_stmt_initlib(L, 1);
  lua_pushvalue(L,-1); lodbc_val_initlib (L, 1);
  lua_pop(L,1);

  luaL_setfuncs(L, lodbc_func, nup);
}

LODBC_EXPORT int luaopen_lodbc (lua_State *L){
  lodbc_init_lib(L, 0);
  lodbc_init_user_value(L);
  return 1;
}

LODBC_EXPORT int luaopen_odbc_core (lua_State *L){
  return luaopen_lodbc(L);
}


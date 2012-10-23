#include "lerr.h"
#include "lualib.h"
#include "utils.h"
#include "l52util.h"

static const char* LODBC_ERR = LODBC_PREFIX "Error";

int lodbc_pass(lua_State *L){
  lua_pushboolean(L, 1);
  return 1;
}


int lodbc_push_diagnostics_obj(lua_State *L, const SQLSMALLINT type, const SQLHANDLE handle){
  SQLCHAR State[6];
  SQLINTEGER NativeError;
  SQLSMALLINT MsgSize, i;
  SQLRETURN ret;
  char Msg[SQL_MAX_MESSAGE_LENGTH];

  lua_newtable(L);
  i = 1;
  while (1) {
    ret = SQLGetDiagRec(type, handle, i, State, &NativeError, Msg, sizeof(Msg), &MsgSize);
    if (ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) break;
    lua_newtable(L);
    lua_pushlstring(L, Msg, MsgSize); lua_setfield(L, -2, "message");
    lua_pushlstring(L, State, 5);     lua_setfield(L, -2, "state");
    lua_pushnumber(L, NativeError);   lua_setfield(L, -2, "code");
    lua_rawseti(L,-2, i);
    i++;
  }
  return 1;
}

int lodbc_fail_obj(lua_State *L, const SQLSMALLINT type, const SQLHANDLE handle){
  lua_pushnil(L);
  lodbc_push_diagnostics_obj(L, type, handle);
  lutil_setmetatablep(L, LODBC_ERR);
  return 2;
}

int lodbc_faildirect_obj(lua_State *L, const char *err){
  lua_pushnil(L);
  lua_newtable(L);
  lua_newtable(L);
  lua_pushliteral(L, LODBC_PREFIX);
  lua_pushstring(L, err);
  lua_concat(L, 2);
  lua_setfield(L, -2, "message");
  lutil_setmetatablep(L, LODBC_ERR);
  return 2;
}

int lodbc_push_diagnostics_str(lua_State *L, const SQLSMALLINT type, const SQLHANDLE handle){
  SQLCHAR State[6];
  SQLINTEGER NativeError;
  SQLSMALLINT MsgSize, i;
  SQLRETURN ret;
  char Msg[SQL_MAX_MESSAGE_LENGTH];
  luaL_Buffer b;

  luaL_buffinit(L, &b);
  i = 1;
  while (1) {
    ret = SQLGetDiagRec(type, handle, i, State, &NativeError, Msg, sizeof(Msg), &MsgSize);
    if (ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) break;
    if(i > 1) luaL_addchar(&b, '\n');
    luaL_addlstring(&b, Msg, MsgSize);
    luaL_addchar(&b, '\n');
    luaL_addlstring(&b, State, 5);
    i++;
  }
  luaL_pushresult(&b);
  return 1;
}

int lodbc_fail_str(lua_State *L, const SQLSMALLINT type, const SQLHANDLE handle){
  lua_pushnil(L);
  lodbc_push_diagnostics_str(L, type, handle);
  return 2;
}

int lodbc_faildirect_str(lua_State *L, const char *err){
  lua_pushnil(L);
  lua_pushliteral(L, LODBC_PREFIX);
  lua_pushstring(L, err);
  lua_concat(L, 2);
  return 2;
}

static int err_tostring(lua_State *L){
  int i = 1;
  int top = lua_gettop(L);
  luaL_checktype(L, 1, LUA_TTABLE);
  while(1){
    if(!lua_checkstack(L, 3)) break;
    lua_rawgeti(L, 1, i);
    if (lua_isnil(L,-1)) {
      lua_pop(L, 1);
      break;
    }
    lua_getfield(L, -1, "message");
    lua_remove(L, -2);
    if(!lua_isstring(L,-1)) lua_pop(L,1);
    else if (i > 1) lua_pushliteral(L, "\n");
    i++;
  }
  lua_concat(L, lua_gettop(L) - top);
  return 1;
}

static int err_index(lua_State *L){
  luaL_checktype(L, 1, LUA_TTABLE);
  luaL_checkany(L,2);
  lua_rawgeti(L, 1, 1);
  if(lua_isnil(L,-1)) return 1;
  lua_pushvalue(L, 2);
  lua_gettable(L,-2);
  return 1;
}

static const struct luaL_Reg lodbc_err_meta[] = {
  {"__tostring",    err_tostring},
  {"__index",       err_index},

  {NULL,NULL}
};

void lodbc_err_initlib (lua_State *L, int nup){
#ifdef LODBC_ERROR_AS_OBJECT
  lutil_newmetatablep(L, LODBC_ERR);
  lua_insert(L, -1 - nup);         /* move mt prior upvalues */
  luaL_setfuncs (L, lodbc_err_meta, nup); /* define methods */
  lua_pushstring(L, LODBC_ERR);
  lua_setfield(L,-2,"__metatable");
  lua_pop(L, 1);
#endif
}

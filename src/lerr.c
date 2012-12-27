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
  SQLCHAR Msg[SQL_MAX_MESSAGE_LENGTH];

  lua_newtable(L);
  i = 1;
  while (1) {
    ret = SQLGetDiagRec(type, handle, i, State, &NativeError, Msg, sizeof(Msg), &MsgSize);
    if (ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) break;
    lua_newtable(L);
    lua_pushlstring(L, (char*)Msg, MsgSize); lua_setfield(L, -2, "message");
    lua_pushlstring(L, (char*)State, 5);     lua_setfield(L, -2, "state");
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
  lua_rawseti(L,-2,1);
  lutil_setmetatablep(L, LODBC_ERR);
  return 2;
}

int lodbc_push_diagnostics_str(lua_State *L, const SQLSMALLINT type, const SQLHANDLE handle){
  SQLCHAR State[6];
  SQLINTEGER NativeError;
  SQLSMALLINT MsgSize, i;
  SQLRETURN ret;
  SQLCHAR Msg[SQL_MAX_MESSAGE_LENGTH];
  luaL_Buffer b;

  luaL_buffinit(L, &b);
  i = 1;
  while (1) {
    ret = SQLGetDiagRec(type, handle, i, State, &NativeError, Msg, sizeof(Msg), &MsgSize);
    if (ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) break;
    if(i > 1) luaL_addchar(&b, '\n');
    luaL_addlstring(&b, (char*)Msg, MsgSize);
    luaL_addchar(&b, '\n');
    luaL_addlstring(&b, (char*)State, 5);
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

static int is_odbc_err(lua_State *L, int i){
  int ret;
  if(!lua_istable(L, i)) return 0;
  lua_getmetatable(L, i);
  if(!lua_istable(L,-1)){
    lua_pop(L, 1);
    return 0;
  }
  lutil_getmetatablep(L, LODBC_ERR);
  ret = lua_rawequal(L, -1, -2);
  lua_pop(L, 2);
  return ret;
}

static int lodbc_assert (lua_State *L){
  if (!lua_toboolean(L, 1)){
    if(is_odbc_err(L, 2)){
      lua_remove(L, 1);
      lua_settop(L, 1);
      err_tostring(L);
      return lua_error(L);
    }
    return luaL_error(L, "%s", luaL_optstring(L, 2, "assertion failed!"));
  }
  return lua_gettop(L);
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

static const struct luaL_Reg lodbc_err_func[] = {
  {"assert", lodbc_assert},

  {NULL, NULL}
};

void lodbc_err_initlib (lua_State *L, int nup){
#ifdef LODBC_ERROR_AS_OBJECT
  lutil_newmetatablep(L, LODBC_ERR);
  lua_insert(L, -1 - nup);         /* move mt prior upvalues */
  luaL_setfuncs (L, lodbc_err_meta, nup); /* define methods */
  lua_pushstring(L, LODBC_ERR);
  lua_setfield(L,-2,"__metatable");
  lua_pop(L, 1);

  lua_pushvalue(L, -1 - nup);
  luaL_setfuncs(L, lodbc_err_func, 0);
  lua_pop(L, 1);
#endif
}

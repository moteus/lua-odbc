#include "lodbc.h"
#include "utils.h"
#include "l52util.h"
#include "lcnn.h"
#include "lenv.h"
#include "lstmt.h"
#include "lerr.h"
#include "libopt.h"
#include "driverinfo.h"

LODBC_EXPORT const char *LODBC_CNN = LODBC_PREFIX "Connection";

//{ declarations
static int cnn_supportsTransactions(lua_State *L);
//}

//{ utils

static void add_keyval_table(lua_State *L, luaL_Buffer *b, int t){
  assert(t > 0); // absolute index
  assert(lua_istable(L,t));
  lua_pushnil(L);
  while (lua_next(L, t) != 0) {
    /* uses 'key' (at index -2) and 'value' (at index -1) */
    if(lua_isnumber(L,-2)){
      lua_pop(L,1);
      continue;
    }
    lua_pushvalue(L,-2);
    luaL_addvalue(b);
    luaL_addchar(b, '=');
    luaL_addvalue(b);
    luaL_addchar(b, ';');
  }
}

static void table_to_cnnstr(lua_State *L, int t){
  int top = lua_gettop(L);
  luaL_Buffer b;
  int i = 1;
  assert(t>0);
  assert(lua_istable(L,t));
  luaL_buffinit(L, &b);

  lua_rawgeti(L,t,i);
  while(!lua_isnil(L,-1)){
    i++;
    if(lua_isstring(L,-1)){
      luaL_addvalue(&b);
    }
    else if(lua_istable(L,-1)){
      add_keyval_table(L, &b, lua_gettop(L));
      lua_pop(L,1);
    }
    lua_rawgeti(L,t,i);
  }
  lua_pop(L,1);
  add_keyval_table(L,&b,t);

  lua_remove(L,t);
  luaL_pushresult(&b);
  lua_insert(L,t);

  assert(top == lua_gettop(L));
}

//}

lodbc_cnn *lodbc_getcnn_at (lua_State *L, int i) {
  lodbc_cnn * cnn= (lodbc_cnn *)lutil_checkudatap (L, i, LODBC_CNN);
  luaL_argcheck (L, cnn != NULL, 1, LODBC_PREFIX "environment expected");
  luaL_argcheck (L, !(cnn->flags & LODBC_FLAG_DESTROYED), 1, LODBC_PREFIX "environment is closed");
  return cnn;
}

//{ ctor/dtor

static int create_stmt_list(lua_State *L){
  int top = lua_gettop(L);
  lua_newtable(L);
  lua_newtable(L);
  lua_pushliteral(L, "k");
  lua_setfield(L, -2, "__mode");
  lua_setmetatable(L,-2);
  assert((top+1) == lua_gettop(L));
  return luaL_ref(L, LODBC_LUA_REGISTRY);
}

int lodbc_connection_create(lua_State *L, SQLHDBC hdbc, lodbc_env *env, int env_idx, uchar own){
  lodbc_cnn *cnn;
  if((!env) && env_idx) env = lodbc_getenv_at(L, env_idx);
  cnn = lutil_newudatap(L, lodbc_cnn, LODBC_CNN);
  memset(cnn, 0, sizeof(lodbc_cnn));
  if(!own) cnn->flags |= LODBC_FLAG_DONT_DESTROY;
  cnn->handle = hdbc;
  if(env){
    cnn->env    = env;
    env->conn_counter++;
  }
  cnn->env_ref = LUA_NOREF;
  if(env_idx){
    lua_pushvalue(L, env_idx);
    cnn->env_ref = luaL_ref(L, LODBC_LUA_REGISTRY);
  }
  cnn->stmt_ref = LUA_NOREF;
  if(LODBC_OPT_INT(CNN_AUTOCLOSESTMT)){
    cnn->stmt_ref = create_stmt_list(L);
  }
  return 1;
}

static void call_stmt_destroy(lua_State *L){
  int top = lua_gettop(L);
  assert(lutil_checkudatap(L, -1, LODBC_STMT));
  lua_getfield(L, -1, "destroy");
  assert(lua_isfunction(L, -1));
  lua_pushvalue(L, -2);
  lua_pcall(L,1,0,0);
  assert(lua_gettop(L) == top);
}

static int cnn_destroy (lua_State *L) {
  lodbc_cnn *cnn = (lodbc_cnn *)lutil_checkudatap (L, 1, LODBC_CNN);
  luaL_argcheck (L, cnn != NULL, 1, LODBC_PREFIX "connection expected");

  if(!(cnn->flags & LODBC_FLAG_DESTROYED)){
    if(LUA_NOREF != cnn->stmt_ref){
      lua_rawgeti(L, LODBC_LUA_REGISTRY, cnn->stmt_ref);
      assert(lua_istable(L, -1));
      lua_pushnil(L);
      while(lua_next(L, -2)){
        lua_pop(L, 1); // we do not need value
        call_stmt_destroy(L);
      }
    }

    if (cnn->stmt_counter > 0)
      return luaL_error (L, LODBC_PREFIX"there are open statements");

    if(!(cnn->flags & LODBC_FLAG_DONT_DESTROY)){
      SQLDisconnect(cnn->handle);

#ifdef LODBC_CHECK_ERROR_ON_DESTROY
      { SQLRETURN ret =
#endif
      SQLFreeHandle (hDBC, cnn->handle);

#ifdef LODBC_CHECK_ERROR_ON_DESTROY
      if (lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);
      }
#endif

      cnn->handle = SQL_NULL_HANDLE;
    }
    if(cnn->env){
      cnn->env->conn_counter--;
      assert(cnn->env->conn_counter >= 0);
    }
    luaL_unref(L, LODBC_LUA_REGISTRY, cnn->env_ref);
    cnn->env_ref = LUA_NOREF;

    luaL_unref(L, LODBC_LUA_REGISTRY, cnn->stmt_ref);
    cnn->stmt_ref = LUA_NOREF;

    cnn->flags |= LODBC_FLAG_DESTROYED;
  }

  lua_pushnil(L);
  // lua_rawsetp(L, LODBC_LUA_REGISTRY, (void*)cnn);
  lodbc_set_user_value(L, 1);
  return lodbc_pass(L);
}

static int cnn_destroyed (lua_State *L) {
  lodbc_cnn *cnn = (lodbc_cnn *)lutil_checkudatap (L, 1, LODBC_CNN);
  luaL_argcheck (L, cnn != NULL, 1, LODBC_PREFIX "connection expected");
  lua_pushboolean(L, cnn->flags & LODBC_FLAG_DESTROYED);
  return 1;
}

static int cnn_environment(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn(L);
  lua_rawgeti(L, LODBC_LUA_REGISTRY, cnn->env_ref);
  return 1;
}

//}

static int cnn_statement (lua_State *L) {
  lodbc_cnn *cnn = lodbc_getcnn(L);
  SQLHSTMT hstmt;
  SQLRETURN ret = SQLAllocHandle (hSTMT, cnn->handle, &hstmt);
  if(lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);
  return lodbc_statement_create(L,hstmt,cnn,1,1,0,0);
}

static int cnn_getuservalue(lua_State *L){
  lodbc_getcnn(L);
  lodbc_get_user_value(L, 1);
  return 1;
}

static int cnn_setuservalue(lua_State *L){
  lodbc_getcnn(L);lua_settop(L, 2);
  lodbc_set_user_value(L, 1);
  return 1;
}

//{ connect

static int conn_after_connect(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn(L);

  int top = lua_gettop(L);
  memset(&cnn->supports[0],0,LODBC_CNN_SUPPORT_MAX * sizeof(cnn->supports[0]));

  {SQLUSMALLINT val = 0;
  SQLRETURN ret = SQLGetFunctions(cnn->handle,SQL_API_SQLPREPARE,&val);
  if(!lodbc_iserror(ret)) cnn->supports[LODBC_CNN_SUPPORT_PREPARE] = val?1:0;}

  {SQLUSMALLINT val = 0;
  SQLRETURN ret = SQLGetFunctions(cnn->handle,SQL_API_SQLBINDPARAMETER,&val);
  if(!lodbc_iserror(ret)) cnn->supports[LODBC_CNN_SUPPORT_BINDPARAM] = val?1:0;}

  {SQLUSMALLINT val = 0;
  SQLRETURN ret = SQLGetFunctions(cnn->handle,SQL_API_SQLNUMPARAMS,&val);
  if(!lodbc_iserror(ret)) cnn->supports[LODBC_CNN_SUPPORT_NUMPARAMS] = val?1:0;}

  {int ret;
    lua_pushvalue(L,1);
    lua_insert(L,1);
    ret = cnn_supportsTransactions(L);
    if(!lodbc_is_unknown(L, ret)) cnn->supports[LODBC_CNN_SUPPORT_TXN] = lua_toboolean(L, -1);
    lua_pop(L,ret);
    lua_remove(L,1);
  }

  /* luasql compatability */
  if(cnn->supports[LODBC_CNN_SUPPORT_TXN])/* set auto commit mode */
      SQLSetConnectAttr(cnn->handle, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_ON, 0);

  assert(lua_gettop(L) == top);

  return 1; // self
}

static int cnn_driverconnect(lua_State *L){
  const int MIN_CONNECTSTRING_SIZE = 1024; // msdn say

  lodbc_cnn *cnn = lodbc_getcnn(L);
  size_t connectStringSize;
  const char *connectString = lua_istable(L,2)?table_to_cnnstr(L,2),luaL_checklstring(L, 2, &connectStringSize):
                                                                    luaL_checklstring(L, 2, &connectStringSize);
  SQLUSMALLINT drvcompl = SQL_DRIVER_NOPROMPT;
  SQLCHAR *buf;
  SQLSMALLINT bufSize;
  SQLHDBC hdbc = cnn->handle;
  SQLRETURN ret;

  buf = malloc(MIN_CONNECTSTRING_SIZE * sizeof(SQLCHAR));
  if(!buf) return LODBC_ALLOCATE_ERROR(L);

  ret = SQLDriverConnect(hdbc,0,(SQLPOINTER)connectString,connectStringSize,
                             buf,MIN_CONNECTSTRING_SIZE,&bufSize,drvcompl);

  //! @todo check if buf size too smal
  if(lodbc_iserror(ret)){
    free(buf);
    return lodbc_fail(L, hDBC, hdbc);
  }

  lua_pushvalue(L,1);
  lua_pushstring(L,(char*)buf);
  free(buf);

  conn_after_connect(L);
  return 2;
}

static int cnn_connect (lua_State *L) {
  lodbc_cnn *cnn = lodbc_getcnn(L);
  const char *sourcename = luaL_checkstring (L, 2);
  const char *username = luaL_optstring (L, 3, NULL);
  const char *password = luaL_optstring (L, 4, NULL);

  SQLRETURN ret = SQLConnect (cnn->handle, (SQLCHAR *) sourcename, SQL_NTS,
    (SQLCHAR *) username, SQL_NTS, (SQLCHAR *) password, SQL_NTS);
  if (lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);

  conn_after_connect(L);
  lua_pushvalue(L,1);
  return 1;
}

static int cnn_disconnect (lua_State *L) {
  lodbc_cnn *cnn = lodbc_getcnn(L);
  SQLRETURN ret;
  if (cnn->stmt_counter > 0)
    return luaL_error (L, LODBC_PREFIX"there are open cursors");
  ret = SQLDisconnect(cnn->handle);
  if (lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);
  return lodbc_pass(L);
}

//}

//{ transactions

static int cnn_commit (lua_State *L) {
  lodbc_cnn *cnn = lodbc_getcnn(L);
  SQLRETURN ret = SQLEndTran(hDBC, cnn->handle, SQL_COMMIT);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, cnn->handle);
  return lodbc_pass(L);
}

static int cnn_rollback (lua_State *L) {
  lodbc_cnn *cnn = lodbc_getcnn(L);
  SQLRETURN ret = SQLEndTran(hDBC, cnn->handle, SQL_ROLLBACK);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, cnn->handle);
  return lodbc_pass(L);
}

//}

//-----------------------------------------------------------------------------
// attr/info/metadata/catalog
//{----------------------------------------------------------------------------

//{ get / set attr

static int cnn_get_uint_attr_(lua_State*L, lodbc_cnn *cnn, SQLINTEGER optnum){
  return lodbc_get_uint_attr_(L, hDBC, cnn->handle, optnum);
}

static int cnn_get_str_attr_(lua_State*L, lodbc_cnn *cnn, SQLINTEGER optnum){
  return lodbc_get_str_attr_(L, hDBC, cnn->handle, optnum);
}

static int cnn_set_uint_attr_(lua_State*L, lodbc_cnn *cnn, SQLINTEGER optnum, SQLINTEGER value){
  return lodbc_set_uint_attr_(L, hDBC, cnn->handle, optnum, value);
}

static int cnn_set_str_attr_(lua_State*L, lodbc_cnn *cnn, SQLINTEGER optnum,
  const char* value, size_t len)
{
  return lodbc_set_str_attr_(L, hDBC, cnn->handle, optnum, value, len);
}

//}

//{ get / set info

static int cnn_get_uint32_info_(lua_State*L, lodbc_cnn *cnn, SQLUSMALLINT optnum){
  SQLUINTEGER res;
  SQLSMALLINT dummy;
  SQLRETURN ret = SQLGetInfo(cnn->handle, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
  if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
  if(lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);
  lua_pushnumber(L,res);
  return 1;
}

static int cnn_get_uint16_info_(lua_State*L, lodbc_cnn *cnn, SQLUSMALLINT optnum){
    SQLUSMALLINT res;
    SQLSMALLINT dummy;
    SQLRETURN ret = SQLGetInfo(cnn->handle, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
    if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);
    lua_pushnumber(L,res);
    return 1;
}

static int cnn_get_str_info_(lua_State*L, lodbc_cnn *cnn, SQLUSMALLINT optnum){
    SQLSMALLINT got;
    char buffer[256];
    SQLRETURN ret;

    ret = SQLGetInfo(cnn->handle, optnum, (SQLPOINTER)buffer, 255, &got);
    if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);

    if(got > 255){
        char* tmp = malloc(got+1);
        if(!tmp)
            return LODBC_ALLOCATE_ERROR(L);
        ret = SQLGetInfo(cnn->handle, optnum, (SQLPOINTER)tmp, got, &got);
        if(lodbc_iserror(ret)){
            free(tmp);
            if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
            return lodbc_fail(L, hDBC, cnn->handle);
        }
        lua_pushstring(L, tmp);
        free(tmp);
    }
    else lua_pushstring(L, buffer);
    return 1;
}

static int cnn_get_equal_char_info_(lua_State*L, lodbc_cnn *cnn, SQLUSMALLINT optnum,
    char value
){
    SQLSMALLINT got;
    char buffer[2];
    SQLRETURN ret;

    ret = SQLGetInfo(cnn->handle, optnum, (SQLPOINTER)buffer, 2, &got);
    if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);

    lua_pushboolean(L, (buffer[0] == value)?1:0);
    return 1;
}

static int cnn_get_equal_uint32_info_(lua_State*L, lodbc_cnn *cnn, SQLUSMALLINT optnum,
    SQLUINTEGER value
){
    SQLUINTEGER res;
    SQLSMALLINT dummy;
    SQLRETURN ret = SQLGetInfo(cnn->handle, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
    if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);

    lua_pushboolean(L, (res == value)?1:0);
    return 1;
}

static int cnn_get_and_uint32_info_(lua_State*L, lodbc_cnn *cnn, SQLUSMALLINT optnum,
    SQLUINTEGER value
){
    SQLUINTEGER res;
    SQLSMALLINT dummy;
    SQLRETURN ret = SQLGetInfo(cnn->handle, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
    if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);

    lua_pushboolean(L, (res & value)?1:0);
    return 1;
}

static int cnn_get_equal_uint16_info_(lua_State*L, lodbc_cnn *cnn, SQLUSMALLINT optnum,
    SQLUSMALLINT value
){
    SQLUSMALLINT res;
    SQLSMALLINT dummy;
    SQLRETURN ret = SQLGetInfo(cnn->handle, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
    if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);

    lua_pushboolean(L, (res == value)?1:0);
    return 1;
}

//}

//{ some attributes

static int cnn_setautocommit (lua_State *L) {
  lodbc_cnn *cnn = lodbc_getcnn (L);
  return cnn_set_uint_attr_(L,cnn,SQL_ATTR_AUTOCOMMIT,
    lua_toboolean (L, 2)?SQL_AUTOCOMMIT_ON:SQL_AUTOCOMMIT_OFF
  );
}

static int cnn_getautocommit(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int ret = cnn_get_uint_attr_(L,cnn,SQL_ATTR_AUTOCOMMIT);
  if(lodbc_is_fail(L,ret)) return ret;
  if(0 == ret){
      lua_pushboolean(L, SQL_AUTOCOMMIT_ON == SQL_AUTOCOMMIT_DEFAULT);
      return 1;
  }
  assert(1 == ret);
  lua_pushboolean(L, SQL_AUTOCOMMIT_ON == lua_tointeger(L,-1));
  lua_remove(L,-2);
  return 1;
}

static int cnn_setlogintimeout (lua_State *L) {
  lodbc_cnn *cnn = lodbc_getcnn (L);
  return cnn_set_uint_attr_(L,cnn,SQL_ATTR_LOGIN_TIMEOUT,lua_tointeger (L, 2));
}

static int cnn_getlogintimeout (lua_State *L) {
  lodbc_cnn *cnn = lodbc_getcnn (L);
  return cnn_get_uint_attr_(L,cnn,SQL_ATTR_LOGIN_TIMEOUT);
}

static int cnn_setcatalog(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  size_t len;
  const char *str = luaL_checklstring(L,2,&len);
  return cnn_set_str_attr_(L, cnn, SQL_ATTR_CURRENT_CATALOG, str, len);
}

static int cnn_getcatalog(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  return cnn_get_str_attr_(L,cnn,SQL_ATTR_CURRENT_CATALOG);
}

static int cnn_setreadonly (lua_State *L) {
  lodbc_cnn *cnn = lodbc_getcnn (L);
  return cnn_set_uint_attr_(L,cnn,SQL_ACCESS_MODE,
    lua_toboolean (L, 2)?SQL_MODE_READ_ONLY:SQL_MODE_READ_WRITE
  );
}

static int cnn_getreadonly(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int ret = cnn_get_uint_attr_(L,cnn,SQL_ACCESS_MODE);
  if(lodbc_is_fail(L,ret)) return ret;
  if(0 == ret){
    lua_pushboolean(L, SQL_MODE_READ_ONLY == SQL_MODE_DEFAULT);
    return 1;
  }
  assert(1 == ret);
  lua_pushboolean(L, SQL_MODE_READ_ONLY == lua_tointeger(L,-1));
  lua_remove(L,-2);
  return 1;
}

static int cnn_settrace (lua_State *L) {
    lodbc_cnn *cnn = lodbc_getcnn (L);
    return cnn_set_uint_attr_(L,cnn,SQL_ATTR_TRACE,
      lua_toboolean (L, 2)?SQL_OPT_TRACE_ON:SQL_OPT_TRACE_OFF
    );
}

static int cnn_gettrace(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int ret = cnn_get_uint_attr_(L,cnn,SQL_ATTR_TRACE);
  if(lodbc_is_fail(L,ret)) return ret;
  if(0 == ret){
    lua_pushboolean(L, SQL_OPT_TRACE_ON == SQL_OPT_TRACE_DEFAULT);
    return 1;
  }
  assert(1 == ret);
  lua_pushboolean(L, SQL_OPT_TRACE_ON == lua_tointeger(L,-1));
  lua_remove(L,-2);
  return 1;
}

static int cnn_supportsTransactions(lua_State *L);

static int cnn_gettransactionisolation(lua_State *L) {
  int ret;
  lodbc_cnn *cnn = lodbc_getcnn (L);
  ret = cnn_supportsTransactions(L);
  if(lodbc_is_unknown(L, ret))return ret;
  assert(ret == 1);
  if(lua_toboolean(L, -1)){
    SQLUINTEGER lvl;
    lua_pop(L,1);
    ret = cnn_get_uint_attr_(L, cnn,
      (LODBC_ODBC3_C(SQL_ATTR_TXN_ISOLATION,SQL_TXN_ISOLATION)));
    if(ret == 0) return TRANSACTION_NONE;
    if(ret == 2) return ret;
    assert(ret == 1);
    lvl = (SQLUINTEGER)lua_tonumber(L,-1);
    lua_pop(L,1);

    switch(lvl) {
      case SQL_TXN_READ_UNCOMMITTED:
        lua_pushinteger(L,TRANSACTION_READ_UNCOMMITTED);
        return 1;
      case SQL_TXN_READ_COMMITTED:
        lua_pushinteger(L,TRANSACTION_READ_COMMITTED);
        return 1;
      case SQL_TXN_REPEATABLE_READ:
        lua_pushinteger(L,TRANSACTION_REPEATABLE_READ);
        return 1;
      case SQL_TXN_SERIALIZABLE:
#if LODBC_ODBCVER < 0x0300 && defined SQL_TXN_VERSIONING
      case SQL_TXN_VERSIONING:
#endif
        lua_pushinteger(L,TRANSACTION_SERIALIZABLE);
        return 1;
    }
  }
  lua_pushinteger(L,TRANSACTION_NONE);
  return 1;
}

static int cnn_settransactionisolation(lua_State *L){
  int ret;
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int i = luaL_checkinteger(L, 2);
  ret = cnn_supportsTransactions(L);
  if(lodbc_is_unknown(L, ret))return ret;
  if(lua_toboolean(L, -1)){
    SQLUINTEGER lvl;
    lua_pop(L,1);
    switch(i) {
      case TRANSACTION_READ_UNCOMMITTED:
        lvl = SQL_TXN_READ_UNCOMMITTED;
        break;

      case TRANSACTION_READ_COMMITTED:
        lvl = SQL_TXN_READ_COMMITTED;
        break;

      case TRANSACTION_REPEATABLE_READ:
        lvl = SQL_TXN_REPEATABLE_READ;
        break;

      case TRANSACTION_SERIALIZABLE:
        lvl = SQL_TXN_SERIALIZABLE;
        break;

      default:
        lua_pushnil(L);
        lua_pushliteral(L,"Invalid transaction isolation");
        return 2;
    }
    return cnn_set_uint_attr_(L,cnn,(LODBC_ODBC3_C(SQL_ATTR_TXN_ISOLATION,SQL_TXN_ISOLATION)),lvl);
  }
  if(i == TRANSACTION_NONE) return lodbc_pass(L);

  lua_pushnil(L);
  lua_pushliteral(L,"Data source does not support transactions");
  return 2;
}

static int cnn_settracefile(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  size_t len;
  const char *str = luaL_checklstring(L,2,&len);
  return cnn_set_str_attr_(L, cnn, SQL_ATTR_TRACEFILE, str, len);
}

static int cnn_gettracefile(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  return cnn_get_str_attr_(L,cnn,SQL_ATTR_TRACEFILE);
}

static int cnn_connected(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int ret = cnn_get_uint_attr_(L,cnn,SQL_ATTR_CONNECTION_DEAD);
  if(!lodbc_is_fail(L,ret)){
      lua_pushboolean(L, SQL_CD_FALSE == lua_tointeger(L,-1));
      lua_remove(L,-2);
  };
  return ret;
}

static int cnn_get_uint_attr(lua_State*L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return cnn_get_uint_attr_(L, cnn, optnum);
}

static int cnn_get_str_attr(lua_State*L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return cnn_get_str_attr_(L, cnn, optnum);
}

static int cnn_set_uint_attr(lua_State*L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return cnn_set_uint_attr_(L, cnn, optnum,luaL_checkinteger(L,3));
}

static int cnn_set_str_attr(lua_State*L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  size_t len;
  const char *str = luaL_checklstring(L,3,&len);
  return cnn_set_str_attr_(L, cnn, optnum, str, len);
}

static int cnn_get_uint16_info(lua_State*L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUSMALLINT optnum = luaL_checkinteger(L,2);
  return cnn_get_uint16_info_(L, cnn, optnum);
}

static int cnn_get_uint32_info(lua_State*L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUSMALLINT optnum = luaL_checkinteger(L,2);
  return cnn_get_uint16_info_(L, cnn, optnum);
}

static int cnn_get_str_info(lua_State*L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUSMALLINT optnum = luaL_checkinteger(L,2);
  return cnn_get_str_info_(L, cnn, optnum);
}

static int cnn_has_prepare(lua_State*L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  lua_pushboolean(L, cnn->supports[LODBC_CNN_SUPPORT_PREPARE]);
  return 1;
}

static int cnn_has_bindparam(lua_State*L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  lua_pushboolean(L, cnn->supports[LODBC_CNN_SUPPORT_BINDPARAM]);
  return 1;
}

static int cnn_has_txn(lua_State*L){
  lodbc_cnn *cnn = lodbc_getcnn(L);
  lua_pushboolean(L, cnn->supports[LODBC_CNN_SUPPORT_TXN]);
  return 1;
}

static int cnn_setasyncmode (lua_State *L) {
  lodbc_cnn *cnn = lodbc_getcnn (L);
  return cnn_set_uint_attr_(L,cnn,SQL_ATTR_ASYNC_ENABLE,
    lua_toboolean (L, 2)?SQL_ASYNC_ENABLE_ON:SQL_ASYNC_ENABLE_OFF
  );
}

static int cnn_getasyncmode(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int ret = cnn_get_uint_attr_(L,cnn,SQL_ATTR_ASYNC_ENABLE);
  if(lodbc_is_fail(L,ret)) return ret;
  if(0 == ret){
      lua_pushboolean(L, SQL_ASYNC_ENABLE_ON == SQL_ASYNC_ENABLE_DEFAULT);
      return 1;
  }
  assert(1 == ret);
  lua_pushboolean(L, SQL_ASYNC_ENABLE_ON == lua_tointeger(L,-1));
  lua_remove(L,-2);
  return 1;
}

static int cnn_setautoclosestmt(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int val = lua_toboolean(L, 2);

  if(val && (cnn->stmt_ref == LUA_NOREF)){
    cnn->stmt_ref = create_stmt_list(L);
    return lodbc_pass(L);
  }

  if((!val) && (cnn->stmt_ref != LUA_NOREF)){
    luaL_unref(L, LODBC_LUA_REGISTRY, cnn->stmt_ref);
    cnn->stmt_ref = LUA_NOREF;
    return lodbc_pass(L);
  }

  return lodbc_pass(L);
}

static int cnn_getautoclosestmt(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  lua_pushboolean(L, (cnn->stmt_ref == LUA_NOREF)?0:1);
  return 1;
}

//}

//{ driver metadata

static int cnn_getdbmsname(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  return cnn_get_str_info_(L,cnn,SQL_DBMS_NAME);
}

static int cnn_getdbmsver(lua_State *L){
    lodbc_cnn *cnn = lodbc_getcnn (L);
    return cnn_get_str_info_(L,cnn,SQL_DBMS_VER);
}

static int cnn_getdrvname(lua_State *L){
    lodbc_cnn *cnn = lodbc_getcnn (L);
    return cnn_get_str_info_(L,cnn,SQL_DRIVER_NAME);
}

static int cnn_getdrvver(lua_State *L){
    lodbc_cnn *cnn = lodbc_getcnn (L);
    return cnn_get_str_info_(L,cnn,SQL_DRIVER_VER);
}

static int cnn_getodbcver(lua_State *L){
    lodbc_cnn *cnn = lodbc_getcnn (L);
    return cnn_get_str_info_(L, cnn, SQL_DRIVER_ODBC_VER);
}

static int cnn_getodbcvermm_(lua_State *L, lodbc_cnn *cnn){
    int ret = cnn_get_str_info_(L, cnn, SQL_DRIVER_ODBC_VER);
    size_t len;
    const char*str;
    int minor,major;
    if(ret != 1)
        return ret;
    str = lua_tolstring(L,-1,&len);
    if((!str)||(len != 5)||(str[2] != '.')){
        lua_pushnil(L);
        return 2;
    }
    major = atoi(&str[0]);
    minor = atoi(&str[3]);
    lua_pop(L,1);

    lua_pushnumber(L, major);
    lua_pushnumber(L, minor);
    return 2;
}

static int cnn_getodbcvermm(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  return cnn_getodbcvermm_(L, cnn);
}

//}

//{ database metadata
// port DatabaseMetadata from libodbc++

#if LODBC_ODBCVER >= 0x0300
static int getCursorAttributes1For(int odbcType){
    int infoType;
    switch(odbcType) {
        case SQL_CURSOR_FORWARD_ONLY:
            infoType = SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1;
            break;
        case SQL_CURSOR_STATIC:
            infoType = SQL_STATIC_CURSOR_ATTRIBUTES1;
            break;
        case SQL_CURSOR_KEYSET_DRIVEN:
            infoType = SQL_KEYSET_CURSOR_ATTRIBUTES1;
            break;
        case SQL_CURSOR_DYNAMIC:
        default:
            infoType = SQL_DYNAMIC_CURSOR_ATTRIBUTES1;
            break;
    }
    return infoType;
}

static int getCursorAttributes2For(int odbcType){
    int infoType;
    switch(odbcType) {
        case SQL_CURSOR_FORWARD_ONLY:
            infoType = SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2;
            break;
        case SQL_CURSOR_STATIC:
            infoType = SQL_STATIC_CURSOR_ATTRIBUTES2;
            break;
        case SQL_CURSOR_KEYSET_DRIVEN:
            infoType = SQL_KEYSET_CURSOR_ATTRIBUTES2;
            break;
        case SQL_CURSOR_DYNAMIC:
        default:
            infoType = SQL_DYNAMIC_CURSOR_ATTRIBUTES2;
            break;
    }
    return infoType;
}

#endif // ODBCVER >= 0x0300


#define DEFINE_GET_STRING_INFO(FNAME, WHAT)\
static int cnn_##FNAME(lua_State *L){      \
  lodbc_cnn *cnn = lodbc_getcnn (L);       \
  return cnn_get_str_info_(L,cnn,(WHAT));  \
}

#define DEFINE_GET_UINT32_AS_BOOL_INFO(FNAME, WHAT, TVALUE)    \
static int cnn_##FNAME(lua_State *L){                          \
  lodbc_cnn *cnn = lodbc_getcnn (L);                           \
  return cnn_get_equal_uint32_info_(L, cnn, (WHAT), (TVALUE)); \
}

#define DEFINE_GET_UINT16_AS_BOOL_INFO(FNAME, WHAT, TVALUE)    \
static int cnn_##FNAME(lua_State *L){                          \
  lodbc_cnn *cnn = lodbc_getcnn (L);                           \
  return cnn_get_equal_uint16_info_(L, cnn, (WHAT), (TVALUE));\
}

#define DEFINE_GET_UINT32_AS_MASK_INFO(FNAME, WHAT, TVALUE)    \
static int cnn_##FNAME(lua_State *L){                          \
  lodbc_cnn *cnn = lodbc_getcnn (L);                           \
  return cnn_get_and_uint32_info_(L, cnn, (WHAT), (TVALUE));   \
}

#define DEFINE_GET_UINT16_AS_NOTBOOL_INFO(FNAME, WHAT, TVALUE)     \
static int cnn_##FNAME(lua_State *L){                              \
  lodbc_cnn *cnn = lodbc_getcnn (L);                               \
  int ret = cnn_get_equal_uint16_info_(L, cnn, (WHAT), (TVALUE)); \
  if(lodbc_is_unknown(L,ret)) return ret;                          \
  assert(ret == 1);                                                \
  lua_pushboolean(L, lua_toboolean(L, -1)?0:1);                    \
  lua_remove(L, -2);                                               \
  return 1;                                                        \
}

#define DEFINE_GET_CHAR_AS_BOOL_INFO(FNAME, WHAT, TVALUE)          \
static int cnn_##FNAME(lua_State *L){                              \
  lodbc_cnn *cnn = lodbc_getcnn (L);                               \
  return cnn_get_equal_char_info_(L, cnn, (WHAT), (TVALUE));       \
}

#define DEFINE_GET_CHAR_AS_NOTBOOL_INFO(FNAME, WHAT, TVALUE)       \
static int cnn_##FNAME(lua_State *L){                              \
  lodbc_cnn *cnn = lodbc_getcnn (L);                               \
  int ret = cnn_get_equal_char_info_(L, cnn, (WHAT), (TVALUE));    \
  if(lodbc_is_unknown(L,ret)) return ret;                          \
  assert(ret == 1);                                                \
  lua_pushboolean(L, lua_toboolean(L, -1)?0:1);                    \
  lua_remove(L, -2);                                               \
  return 1;                                                        \
}

#define GET_UINT32_INFO(VAR, WHAT)               \
  (VAR) = cnn_get_uint32_info_(L, cnn, (WHAT));  \
  if((VAR) != 1){return (VAR);}                  \
  (VAR) = luaL_checkinteger(L,-1);               \
  lua_pop(L, 1);

#define GET_UINT16_INFO(VAR, WHAT)               \
  (VAR) = cnn_get_uint16_info_(L, cnn, (WHAT));  \
  if((VAR) != 1){return (VAR);}                  \
  (VAR) = luaL_checkinteger(L,-1);               \
  lua_pop(L, 1);

#define DEFINE_GET_UINT32_INFO(FNAME, WHAT)      \
static int cnn_##FNAME(lua_State *L){            \
  lodbc_cnn *cnn = lodbc_getcnn (L);             \
  return cnn_get_uint32_info_(L, cnn, (WHAT));   \
}

#define DEFINE_GET_UINT16_INFO(FNAME, WHAT)      \
static int cnn_##FNAME(lua_State *L){            \
  lodbc_cnn *cnn = lodbc_getcnn (L);             \
  return cnn_get_uint32_info_(L, cnn, (WHAT));   \
}

DEFINE_GET_UINT32_AS_MASK_INFO(supportsAsyncConnection, 
  SQL_ASYNC_MODE, SQL_AM_CONNECTION
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsAsyncStatement, 
  SQL_ASYNC_MODE, SQL_AM_STATEMENT
)

static int cnn_supportsAsync(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);             \
  SQLUINTEGER res;
  SQLSMALLINT dummy;
  SQLUSMALLINT optnum = SQL_ASYNC_MODE;

  SQLRETURN ret = SQLGetInfo(cnn->handle, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
  if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
  if(lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);

  dummy = 0;
  if(res & SQL_AM_STATEMENT){ 
    lua_pushliteral(L, "statement");
    ++dummy;
  }
  if(res & SQL_AM_CONNECTION){
    lua_pushliteral(L, "connection");
    ++dummy;
  }

  if(dummy) return dummy;
  lua_pushboolean(L, 0);
  return 1;
}


DEFINE_GET_STRING_INFO(getIdentifierQuoteString,
  SQL_IDENTIFIER_QUOTE_CHAR
)

DEFINE_GET_STRING_INFO(getCatalogTerm,
  LODBC_ODBC3_C(SQL_CATALOG_TERM,SQL_QUALIFIER_TERM)
)

DEFINE_GET_STRING_INFO(getSchemaTerm,
  LODBC_ODBC3_C(SQL_SCHEMA_TERM,SQL_OWNER_TERM)
)

DEFINE_GET_STRING_INFO(getTableTerm,
  LODBC_ODBC3_C(SQL_SCHEMA_TERM,SQL_OWNER_TERM)
)

DEFINE_GET_STRING_INFO(getProcedureTerm,
  SQL_PROCEDURE_TERM
)

DEFINE_GET_STRING_INFO(getUserName,
  SQL_USER_NAME
)

DEFINE_GET_STRING_INFO(getCatalogSeparator,
  LODBC_ODBC3_C(SQL_CATALOG_NAME_SEPARATOR, SQL_QUALIFIER_NAME_SEPARATOR)
)

DEFINE_GET_CHAR_AS_BOOL_INFO(isCatalogName,
  SQL_CATALOG_NAME,
  'Y'
)

DEFINE_GET_UINT16_AS_BOOL_INFO(isCatalogAtStart,
  LODBC_ODBC3_C(SQL_CATALOG_LOCATION,SQL_QUALIFIER_LOCATION),
  SQL_QL_START
)

DEFINE_GET_STRING_INFO(getSQLKeywords,
    SQL_KEYWORDS
)


DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsTransactions,
  SQL_TXN_CAPABLE, SQL_TC_NONE
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsDataDefinitionAndDataManipulationTransactions,
  SQL_TXN_CAPABLE, SQL_TC_ALL
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsDataManipulationTransactionsOnly,
  SQL_TXN_CAPABLE, SQL_TC_DML
)

DEFINE_GET_UINT16_AS_BOOL_INFO(dataDefinitionCausesTransactionCommit,
  SQL_TXN_CAPABLE, SQL_TC_DDL_COMMIT
)

DEFINE_GET_UINT16_AS_BOOL_INFO(dataDefinitionIgnoredInTransactions,
  SQL_TXN_CAPABLE, SQL_TC_DDL_IGNORE
)

static int cnn_getDefaultTransactionIsolation(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn(L);
  int top = lua_gettop(L);
  SQLUINTEGER r;
  GET_UINT32_INFO(r, SQL_DEFAULT_TXN_ISOLATION);
  switch(r) {
    case SQL_TXN_READ_UNCOMMITTED:
      lua_pushnumber(L, TRANSACTION_READ_UNCOMMITTED);
      break;
    case SQL_TXN_READ_COMMITTED:
      lua_pushnumber(L, TRANSACTION_READ_COMMITTED);
      break;
    case SQL_TXN_REPEATABLE_READ:
      lua_pushnumber(L, TRANSACTION_REPEATABLE_READ);
      break;
    case SQL_TXN_SERIALIZABLE:
    #if defined(SQL_TXN_VERSIONING)
    case SQL_TXN_VERSIONING:
    #endif
      lua_pushnumber(L, TRANSACTION_SERIALIZABLE);
      break;
    default:
      lua_pushnumber(L, TRANSACTION_NONE);
  }
  assert(1 == (lua_gettop(L) - top));
  return 1;
}

static int cnn_supportsTransactionIsolationLevel(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUINTEGER r;
  SQLUINTEGER ret=0;
  SQLUINTEGER lev = luaL_checkint(L, 2);
  int top = lua_gettop(L);

  GET_UINT32_INFO(r, SQL_TXN_ISOLATION_OPTION);
  switch(lev){
    case TRANSACTION_READ_UNCOMMITTED:
      ret = r&SQL_TXN_READ_UNCOMMITTED;
      break;
    case TRANSACTION_READ_COMMITTED:
      ret = r&SQL_TXN_READ_COMMITTED;
      break;
    case TRANSACTION_REPEATABLE_READ:
      ret = r&SQL_TXN_REPEATABLE_READ;
      break;
    case TRANSACTION_SERIALIZABLE:
      ret=(r&SQL_TXN_SERIALIZABLE)
    #if defined(SQL_TXN_VERSIONING)
        || (r&SQL_TXN_VERSIONING)
    #endif
        ;
      break;
  }
  lua_pushboolean(L, (ret!=0)?1:0);
  assert(1 == (lua_gettop(L) - top));
  return 1;
}


DEFINE_GET_UINT16_AS_BOOL_INFO(supportsOpenCursorsAcrossCommit,
  SQL_CURSOR_COMMIT_BEHAVIOR, SQL_CB_PRESERVE
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsOpenStatementsAcrossCommit,
  SQL_CURSOR_COMMIT_BEHAVIOR, SQL_CB_PRESERVE
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsOpenCursorsAcrossRollback,
  SQL_CURSOR_ROLLBACK_BEHAVIOR, SQL_CB_PRESERVE
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsOpenStatementsAcrossRollback,
  SQL_CURSOR_ROLLBACK_BEHAVIOR, SQL_CB_PRESERVE
)

#ifdef LUASQL_USE_DRIVERINFO

static int cnn_supportsResultSetType(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUINTEGER type = luaL_checkint(L, 2);
  int ret = conn_init_di_(L, conn);
  const drvinfo_data *di = conn->di;
  if(is_fail(L, ret)) return ret;

  switch(type) {
    case RS_TYPE_FORWARD_ONLY:
      lua_pushboolean(L, di_supports_forwardonly(di));
      break;

    case RS_TYPE_SCROLL_INSENSITIVE:
      lua_pushboolean(L, di_supports_static(di));
      break;

    case RS_TYPE_SCROLL_SENSITIVE:
      lua_pushboolean(L, di_supports_scrollsensitive(di));
      break;
    default:
      return lodbc_faildirect(L, "Invalid ResultSet type");
  }
  return 1;
}


static int cnn_supportsResultSetConcurrency(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUINTEGER type = luaL_checkint(L, 2);
  SQLUINTEGER concurrency = luaL_checkint(L, 3);
  int ct = 0;
  int ret = conn_init_di_(L, conn);
  const drvinfo_data *di = conn->di;
  if(is_fail(L, ret)) return ret;

  switch(type) {
    case RS_TYPE_SCROLL_SENSITIVE:
      if(!di_supports_scrollsensitive(di)){
        lua_pushboolean(L, 0);
        return 1;
      }
      ct = di_getscrollsensitive(di);
      break;

    case RS_TYPE_SCROLL_INSENSITIVE:
      if(!di_supports_static(di)){
        lua_pushboolean(L, 0);
        return 1;
      }
      ct=SQL_CURSOR_STATIC;
      break;

    case RS_TYPE_FORWARD_ONLY:
      if(!di_supports_forwardonly(di)){
        lua_pushboolean(L, 0);
        return 1;
      }
      // forward only cursors are read-only by definition
      lua_pushboolean(L, (concurrency == RS_CONCUR_READ_ONLY)?1:0);
      return 1;

    default:
      return lodbc_faildirect(L, "Invalid ResultSet type");
  }

  switch(concurrency) {
    case RS_CONCUR_READ_ONLY:
      lua_pushboolean(L, di_supports_readonly(di, ct));
      break;
    case RS_CONCUR_UPDATABLE:
      lua_pushboolean(L, di_supports_updatable(di, ct));
      break;
    default:
      return lodbc_faildirect(L, "Invalid ResultSet concurrency");
  }
  return 1;
}

#endif

DEFINE_GET_UINT16_AS_BOOL_INFO(nullPlusNonNullIsNull,
  SQL_CONCAT_NULL_BEHAVIOR, SQL_CB_NULL
)

DEFINE_GET_CHAR_AS_BOOL_INFO(supportsColumnAliasing,
  SQL_COLUMN_ALIAS, 'Y'
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsAlterTableWithAddColumn,
  SQL_ALTER_TABLE, SQL_AT_ADD_COLUMN
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsAlterTableWithDropColumn,
  SQL_ALTER_TABLE, SQL_AT_DROP_COLUMN
)

DEFINE_GET_STRING_INFO(getExtraNameCharacters,
  SQL_SPECIAL_CHARACTERS
)

DEFINE_GET_STRING_INFO(getSearchStringEscape,
  SQL_SEARCH_PATTERN_ESCAPE
)

static int cnn_getNumericFunctions(lua_State *L){
  static struct {
    SQLUINTEGER funcId;
    const char* funcName;
  } fmap[] = {
    { SQL_FN_NUM_ABS,      ("ABS") },
    { SQL_FN_NUM_ACOS,     ("ACOS") },
    { SQL_FN_NUM_ASIN,     ("ASIN") },
    { SQL_FN_NUM_ATAN,     ("ATAN") },
    { SQL_FN_NUM_ATAN2,    ("ATAN2") },
    { SQL_FN_NUM_CEILING,  ("CEILING") },
    { SQL_FN_NUM_COS,      ("COS") },
    { SQL_FN_NUM_COT,      ("COT") },
    { SQL_FN_NUM_DEGREES,  ("DEGREES") },
    { SQL_FN_NUM_EXP,      ("EXP") },
    { SQL_FN_NUM_FLOOR,    ("FLOOR") },
    { SQL_FN_NUM_LOG,      ("LOG") },
    { SQL_FN_NUM_LOG10,    ("LOG10") },
    { SQL_FN_NUM_MOD,      ("MOD") },
    { SQL_FN_NUM_PI,       ("PI") },
    { SQL_FN_NUM_POWER,    ("POWER") },
    { SQL_FN_NUM_RADIANS,  ("RADIANS") },
    { SQL_FN_NUM_RAND,     ("RAND") },
    { SQL_FN_NUM_ROUND,    ("ROUND") },
    { SQL_FN_NUM_SIGN,     ("SIGN") },
    { SQL_FN_NUM_SIN,      ("SIN") },
    { SQL_FN_NUM_SQRT,     ("SQRT") },
    { SQL_FN_NUM_TAN,      ("TAN") },
    { SQL_FN_NUM_TRUNCATE, ("TRUNCATE") },

#if LODBC_ODBCVER >= 0x0300
    { SQL_SNVF_BIT_LENGTH,       ("BIT_LENGTH") },
    { SQL_SNVF_CHAR_LENGTH,      ("CHAR_LENGTH") },
    { SQL_SNVF_CHARACTER_LENGTH, ("CHARACTER_LENGTH") },
    { SQL_SNVF_EXTRACT,          ("EXTRACT") },
    { SQL_SNVF_OCTET_LENGTH,     ("OCTET_LENGTH") },
    { SQL_SNVF_POSITION,         ("POSITION") },
#endif  /* ODBCVER >= 0x0300 */

    { 0, NULL }
  };
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUINTEGER r;
  int i = 0;
  int n = 0;
  int top = lua_gettop(L);

  GET_UINT32_INFO(r, SQL_NUMERIC_FUNCTIONS);
  lua_newtable(L);
  for(; fmap[i].funcId > 0; i++){
    if(r & (fmap[i].funcId)){
      lua_pushstring(L, fmap[i].funcName);
      lua_rawseti(L,-2,++n);
    }
  }
  assert(1 == (lua_gettop(L) - top));
  return 1;
}

static int cnn_getStringFunctions(lua_State *L){
  static struct {
    SQLUINTEGER funcId;
    const char* funcName;
  } fmap[] = {
  #if LODBC_ODBCVER>=0x0300
    { SQL_FN_STR_BIT_LENGTH,       ("BIT_LENGTH") },
    { SQL_FN_STR_CHAR_LENGTH,      ("CHAR_LENGTH") },
    { SQL_FN_STR_CHARACTER_LENGTH, ("CHARACTER_LENGTH") },
    { SQL_FN_STR_OCTET_LENGTH,     ("OCTET_LENGTH") },
    { SQL_FN_STR_POSITION,         ("POSITION") },
  #endif
    { SQL_FN_STR_ASCII,            ("ASCII") },
    { SQL_FN_STR_CHAR,             ("CHAR") },
    { SQL_FN_STR_CONCAT,           ("CONCAT") },
    { SQL_FN_STR_DIFFERENCE,       ("DIFFERENCE") },
    { SQL_FN_STR_INSERT,           ("INSERT") },
    { SQL_FN_STR_LCASE,            ("LCASE") },
    { SQL_FN_STR_LEFT,             ("LEFT") },
    { SQL_FN_STR_LENGTH,           ("LENGTH") },
    { SQL_FN_STR_LOCATE,           ("LOCATE") },
    { SQL_FN_STR_LOCATE_2,         ("LOCATE_2") },
    { SQL_FN_STR_LTRIM,            ("LTRIM") },
    { SQL_FN_STR_REPEAT,           ("REPEAT") },
    { SQL_FN_STR_REPLACE,          ("REPLACE") },
    { SQL_FN_STR_RIGHT,            ("RIGHT") },
    { SQL_FN_STR_RTRIM,            ("RTRIM") },
    { SQL_FN_STR_SOUNDEX,          ("SOUNDEX") },
    { SQL_FN_STR_SPACE,            ("SPACE") },
    { SQL_FN_STR_SUBSTRING,        ("SUBSTRING") },
    { SQL_FN_STR_UCASE,            ("UCASE") },
    { 0, NULL }
  };
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUINTEGER r;
  int i = 0;
  int n = 0;
  int top = lua_gettop(L);
  GET_UINT32_INFO(r, SQL_STRING_FUNCTIONS);
  lua_newtable(L);

  for(; fmap[i].funcId > 0; i++){
    if(r & (fmap[i].funcId)){
      lua_pushstring(L, fmap[i].funcName);
      lua_rawseti(L,-2,++n);
    }
  }
  assert(1 == (lua_gettop(L) - top));
  return 1;
}

static int cnn_getSystemFunctions(lua_State *L){
  static struct {
    SQLUINTEGER funcId;
    const char* funcName;
  } fmap[] = {
    { SQL_FN_SYS_DBNAME,   ("DBNAME") },
    { SQL_FN_SYS_IFNULL,   ("IFNULL") },
    { SQL_FN_SYS_USERNAME, ("USERNAME") },
    { 0, NULL }
  };
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUINTEGER r;
  int i = 0;
  int n = 0;
  int top = lua_gettop(L);
  GET_UINT32_INFO(r, SQL_SYSTEM_FUNCTIONS);
  lua_newtable(L);

  for(; fmap[i].funcId > 0; i++){
    if(r & (fmap[i].funcId)){
      lua_pushstring(L, fmap[i].funcName);
      lua_rawseti(L,-2,++n);
    }
  }
  assert(1 == (lua_gettop(L) - top));
  return 1;
}

static int cnn_getTimeDateFunctions(lua_State *L){
  static struct {
    SQLUINTEGER funcId;
    const char* funcName;
  } fmap[] = {
  #if LODBC_ODBCVER>=0x0300
    { SQL_FN_TD_CURRENT_DATE,      ("CURRENT_DATE") },
    { SQL_FN_TD_CURRENT_TIME,      ("CURRENT_TIME") },
    { SQL_FN_TD_CURRENT_TIMESTAMP, ("CURRENT_TIMESTAMP") },
    { SQL_FN_TD_EXTRACT,           ("EXTRACT") },
  #endif
    { SQL_FN_TD_CURDATE,           ("CURDATE") },
    { SQL_FN_TD_CURTIME,           ("CURTIME") },
    { SQL_FN_TD_DAYNAME,           ("DAYNAME") },
    { SQL_FN_TD_DAYOFMONTH,        ("DAYOFMONTH") },
    { SQL_FN_TD_DAYOFWEEK,         ("DAYOFWEEK") },
    { SQL_FN_TD_DAYOFYEAR,         ("DAYOFYEAR") },
    { SQL_FN_TD_HOUR,              ("HOUR") },
    { SQL_FN_TD_MINUTE,            ("MINUTE") },
    { SQL_FN_TD_MONTH,             ("MONTH") },
    { SQL_FN_TD_MONTHNAME,         ("MONTHNAME") },
    { SQL_FN_TD_NOW,               ("NOW") },
    { SQL_FN_TD_QUARTER,           ("QUARTER") },
    { SQL_FN_TD_SECOND,            ("SECOND") },
    { SQL_FN_TD_TIMESTAMPADD,      ("TIMESTAMPADD") },
    { SQL_FN_TD_TIMESTAMPDIFF,     ("TIMESTAMPDIFF") },
    { SQL_FN_TD_WEEK,              ("WEEK") },
    { SQL_FN_TD_YEAR,              ("YEAR") },
    { 0, NULL }
  };
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUINTEGER r;
  int i = 0;
  int n = 0;
  int top = lua_gettop(L);

  GET_UINT32_INFO(r, SQL_TIMEDATE_FUNCTIONS);
  lua_newtable(L);

  for(; fmap[i].funcId > 0; i++){
    if(r & (fmap[i].funcId)){
      lua_pushstring(L, fmap[i].funcName);
      lua_rawseti(L,-2,++n);
    }
  }
  assert(1 == (lua_gettop(L) - top));
  return 1;
}

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCatalogsInDataManipulation,
  LODBC_ODBC3_C(SQL_CATALOG_USAGE,SQL_QUALIFIER_USAGE),
  LODBC_ODBC3_C(SQL_CU_DML_STATEMENTS, SQL_QU_DML_STATEMENTS)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCatalogsInProcedureCalls,
  LODBC_ODBC3_C(SQL_CATALOG_USAGE,SQL_QUALIFIER_USAGE),
  LODBC_ODBC3_C(SQL_CU_PROCEDURE_INVOCATION,SQL_QU_PROCEDURE_INVOCATION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCatalogsInTableDefinitions,
  LODBC_ODBC3_C(SQL_CATALOG_USAGE,SQL_QUALIFIER_USAGE),
  LODBC_ODBC3_C(SQL_CU_TABLE_DEFINITION,SQL_QU_TABLE_DEFINITION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCatalogsInIndexDefinitions,
  LODBC_ODBC3_C(SQL_CATALOG_USAGE,SQL_QUALIFIER_USAGE),
  LODBC_ODBC3_C(SQL_CU_INDEX_DEFINITION,SQL_QU_INDEX_DEFINITION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCatalogsInPrivilegeDefinitions,
  LODBC_ODBC3_C(SQL_CATALOG_USAGE,SQL_QUALIFIER_USAGE),
  LODBC_ODBC3_C(SQL_CU_PRIVILEGE_DEFINITION,SQL_QU_PRIVILEGE_DEFINITION)
)


DEFINE_GET_UINT32_AS_MASK_INFO(supportsSchemasInDataManipulation,
  LODBC_ODBC3_C(SQL_SCHEMA_USAGE,SQL_OWNER_USAGE),
  LODBC_ODBC3_C(SQL_SU_DML_STATEMENTS,SQL_OU_DML_STATEMENTS)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSchemasInProcedureCalls,
  LODBC_ODBC3_C(SQL_SCHEMA_USAGE,SQL_OWNER_USAGE),
  LODBC_ODBC3_C(SQL_SU_PROCEDURE_INVOCATION,SQL_OU_PROCEDURE_INVOCATION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSchemasInTableDefinitions,
  LODBC_ODBC3_C(SQL_SCHEMA_USAGE,SQL_OWNER_USAGE),
  LODBC_ODBC3_C(SQL_SU_TABLE_DEFINITION,SQL_OU_TABLE_DEFINITION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSchemasInIndexDefinitions,
  LODBC_ODBC3_C(SQL_SCHEMA_USAGE,SQL_OWNER_USAGE),
  LODBC_ODBC3_C(SQL_SU_INDEX_DEFINITION,SQL_OU_INDEX_DEFINITION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSchemasInPrivilegeDefinitions,
  LODBC_ODBC3_C(SQL_SCHEMA_USAGE,SQL_OWNER_USAGE),
  LODBC_ODBC3_C(SQL_SU_PRIVILEGE_DEFINITION, SQL_OU_PRIVILEGE_DEFINITION)
)


DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsGroupBy,
  SQL_GROUP_BY, SQL_GB_NOT_SUPPORTED
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsGroupByUnrelated,
  SQL_GROUP_BY, SQL_GB_NO_RELATION
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsGroupByBeyondSelect,
  SQL_GROUP_BY, SQL_GB_GROUP_BY_CONTAINS_SELECT
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsUnion,
  SQL_UNION, SQL_U_UNION
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsUnionAll,
  SQL_UNION, (SQL_U_UNION | SQL_U_UNION_ALL)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsOuterJoins,
  SQL_OJ_CAPABILITIES,
  (SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_NESTED)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsFullOuterJoins,
  SQL_OJ_CAPABILITIES,
  (SQL_OJ_FULL | SQL_OJ_NESTED)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsLimitedOuterJoins,
  SQL_OJ_CAPABILITIES,
  (SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_NESTED) | (SQL_OJ_FULL | SQL_OJ_NESTED)
)


DEFINE_GET_UINT16_AS_BOOL_INFO(usesLocalFilePerTable,
  SQL_FILE_USAGE, SQL_FILE_TABLE
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(usesLocalFiles,
  SQL_FILE_USAGE, SQL_FILE_NOT_SUPPORTED
)

DEFINE_GET_UINT16_AS_BOOL_INFO(nullsAreSortedHigh,
  SQL_NULL_COLLATION, SQL_NC_HIGH
)

DEFINE_GET_UINT16_AS_BOOL_INFO(nullsAreSortedLow,
  SQL_NULL_COLLATION, SQL_NC_LOW
)

DEFINE_GET_UINT16_AS_BOOL_INFO(nullsAreSortedAtStart,
  SQL_NULL_COLLATION, SQL_NC_START
)

DEFINE_GET_UINT16_AS_BOOL_INFO(nullsAreSortedAtEnd,
  SQL_NULL_COLLATION, SQL_NC_END
)

DEFINE_GET_CHAR_AS_BOOL_INFO(allProceduresAreCallable,
  SQL_ACCESSIBLE_PROCEDURES, 'Y'
)

DEFINE_GET_CHAR_AS_BOOL_INFO(allTablesAreSelectable,
  SQL_ACCESSIBLE_TABLES, 'Y'
)

DEFINE_GET_CHAR_AS_BOOL_INFO(isReadOnly,
  SQL_DATA_SOURCE_READ_ONLY, 'Y'
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsTableCorrelationNames,
  SQL_CORRELATION_NAME, SQL_CN_NONE
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCorrelatedSubqueries,
  SQL_SUBQUERIES, SQL_SQ_CORRELATED_SUBQUERIES
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSubqueriesInComparisons,
  SQL_SUBQUERIES, SQL_SQ_COMPARISON
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSubqueriesInExists,
  SQL_SUBQUERIES, SQL_SQ_EXISTS
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSubqueriesInIns,
  SQL_SUBQUERIES, SQL_SQ_IN
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSubqueriesInQuantifieds,
  SQL_SUBQUERIES, SQL_SQ_QUANTIFIED
)



DEFINE_GET_CHAR_AS_BOOL_INFO(supportsExpressionsInOrderBy,
  SQL_EXPRESSIONS_IN_ORDERBY, 'Y'
)

DEFINE_GET_CHAR_AS_BOOL_INFO(supportsLikeEscapeClause,
  SQL_LIKE_ESCAPE_CLAUSE, 'Y'
)

DEFINE_GET_CHAR_AS_BOOL_INFO(supportsMultipleResultSets,
  SQL_MULT_RESULT_SETS, 'Y'
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsNonNullableColumns,
  SQL_NON_NULLABLE_COLUMNS, SQL_NNC_NON_NULL
)

static int cnn_supportsMinimumSQLGrammar(lua_State *L){
    lua_pushboolean(L, 1);
    return 1;
}

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsCoreSQLGrammar,
    SQL_ODBC_SQL_CONFORMANCE, SQL_OSC_MINIMUM
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsExtendedSQLGrammar,
    SQL_ODBC_SQL_CONFORMANCE, SQL_OSC_EXTENDED
)

#if LODBC_ODBCVER >= 3

DEFINE_GET_UINT32_AS_MASK_INFO(supportsANSI92EntryLevelSQL,
    SQL_SQL_CONFORMANCE, SQL_SC_SQL92_ENTRY
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsANSI92FullSQL,
    SQL_SQL_CONFORMANCE, SQL_SC_SQL92_FULL
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsANSI92IntermediateSQL,
    SQL_SQL_CONFORMANCE, SQL_SC_SQL92_INTERMEDIATE
)

#else

static int cnn_supportsANSI92EntryLevelSQL(lua_State *L){
    lua_pushboolean(L, 0);
    return 1;
}

static int cnn_supportsANSI92FullSQL(lua_State *L){
    lua_pushboolean(L, 0);
    return 1;
}

static int cnn_supportsANSI92IntermediateSQL(lua_State *L){
    lua_pushboolean(L, 0);
    return 1;
}

#endif



DEFINE_GET_UINT32_INFO(getMaxBinaryLiteralLength,SQL_MAX_BINARY_LITERAL_LEN)
DEFINE_GET_UINT32_INFO(getMaxCharLiteralLength,SQL_MAX_CHAR_LITERAL_LEN)
DEFINE_GET_UINT16_INFO(getMaxColumnNameLength,SQL_MAX_COLUMN_NAME_LEN)
DEFINE_GET_UINT16_INFO(getMaxColumnsInGroupBy,SQL_MAX_COLUMNS_IN_GROUP_BY)
DEFINE_GET_UINT16_INFO(getMaxColumnsInIndex,SQL_MAX_COLUMNS_IN_INDEX)
DEFINE_GET_UINT16_INFO(getMaxColumnsInOrderBy,SQL_MAX_COLUMNS_IN_ORDER_BY)
DEFINE_GET_UINT16_INFO(getMaxColumnsInSelect,SQL_MAX_COLUMNS_IN_SELECT)
DEFINE_GET_UINT16_INFO(getMaxColumnsInTable,SQL_MAX_COLUMNS_IN_TABLE)
DEFINE_GET_UINT16_INFO(getMaxCursorNameLength,SQL_MAX_CURSOR_NAME_LEN)
DEFINE_GET_UINT32_INFO(getMaxIndexLength,SQL_MAX_INDEX_SIZE)
DEFINE_GET_UINT16_INFO(getMaxSchemaNameLength,LODBC_ODBC3_C(SQL_MAX_SCHEMA_NAME_LEN,SQL_MAX_OWNER_NAME_LEN))
DEFINE_GET_UINT16_INFO(getMaxProcedureNameLength,SQL_MAX_PROCEDURE_NAME_LEN)
DEFINE_GET_UINT16_INFO(getMaxCatalogNameLength,LODBC_ODBC3_C(SQL_MAX_CATALOG_NAME_LEN,SQL_MAX_QUALIFIER_NAME_LEN))
DEFINE_GET_UINT32_INFO(getMaxRowSize,SQL_MAX_ROW_SIZE)
DEFINE_GET_UINT32_INFO(getMaxStatementLength,SQL_MAX_STATEMENT_LEN)
DEFINE_GET_UINT16_INFO(getMaxTableNameLength,SQL_MAX_TABLE_NAME_LEN)
DEFINE_GET_UINT16_INFO(getMaxTablesInSelect,SQL_MAX_TABLES_IN_SELECT)
DEFINE_GET_UINT16_INFO(getMaxUserNameLength,SQL_MAX_USER_NAME_LEN)
DEFINE_GET_UINT16_INFO(getMaxConnections,LODBC_ODBC3_C(SQL_MAX_DRIVER_CONNECTIONS,SQL_ACTIVE_CONNECTIONS))
DEFINE_GET_UINT16_INFO(getMaxStatements,LODBC_ODBC3_C(SQL_MAX_CONCURRENT_ACTIVITIES,SQL_ACTIVE_STATEMENTS))
DEFINE_GET_CHAR_AS_BOOL_INFO(doesMaxRowSizeIncludeBlobs,
  SQL_MAX_ROW_SIZE_INCLUDES_LONG, 'Y'
)

DEFINE_GET_CHAR_AS_BOOL_INFO(supportsMultipleTransactions,
  SQL_MULTIPLE_ACTIVE_TXN, 'Y'
)

DEFINE_GET_CHAR_AS_NOTBOOL_INFO(supportsOrderByUnrelated,
  SQL_ORDER_BY_COLUMNS_IN_SELECT, 'Y'
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsDifferentTableCorrelationNames,
  SQL_CORRELATION_NAME, SQL_CN_DIFFERENT
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsConvertFn,
  SQL_CONVERT_FUNCTIONS, SQL_FN_CVT_CONVERT
)

#if (LODBC_ODBCVER >= 0x0300) 
#  ifndef SQL_CONVERT_GUID
#    define SQL_CONVERT_GUID 173
#  endif
#  ifndef  SQL_CVT_GUID
#    define SQL_CVT_GUID 0x01000000L
#  endif
#endif

static int cnn_supportsConvert(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  SQLUINTEGER fromType = luaL_checkint(L, 2);
  SQLUINTEGER toType   = luaL_checkint(L, 3);
  int i = 0, j = 0;

  static struct {
    int id;
    int value;
    } convertMap[TYPES_COUNT] = {
    { TYPE_BIGINT,        SQL_CONVERT_BIGINT },
    { TYPE_BINARY,        SQL_CONVERT_BINARY },
    { TYPE_BIT,           SQL_CONVERT_BIT },
    { TYPE_CHAR,          SQL_CONVERT_CHAR },
    { TYPE_DATE,          SQL_CONVERT_DATE },
    { TYPE_DECIMAL,       SQL_CONVERT_DECIMAL },
    { TYPE_DOUBLE,        SQL_CONVERT_DOUBLE },
    { TYPE_FLOAT,         SQL_CONVERT_FLOAT },
    { TYPE_INTEGER,       SQL_CONVERT_INTEGER },
    { TYPE_LONGVARBINARY, SQL_CONVERT_LONGVARBINARY },
    { TYPE_LONGVARCHAR,   SQL_CONVERT_LONGVARCHAR },
    { TYPE_NUMERIC,       SQL_CONVERT_NUMERIC },
    { TYPE_REAL,          SQL_CONVERT_REAL },
    { TYPE_SMALLINT,      SQL_CONVERT_SMALLINT },
    { TYPE_TIME,          SQL_CONVERT_TIME },
    { TYPE_TIMESTAMP,     SQL_CONVERT_TIMESTAMP },
    { TYPE_TINYINT,       SQL_CONVERT_TINYINT },
    { TYPE_VARBINARY,     SQL_CONVERT_VARBINARY },
    { TYPE_VARCHAR,       SQL_CONVERT_VARCHAR }
#if (LODBC_ODBCVER >= 0x0300)
    ,{ TYPE_WCHAR,         SQL_CONVERT_WCHAR }
    ,{ TYPE_WLONGVARCHAR,  SQL_CONVERT_WLONGVARCHAR }
    ,{ TYPE_WVARCHAR,      SQL_CONVERT_WVARCHAR }
    ,{ TYPE_GUID,          SQL_CONVERT_GUID }
#endif
  };
  static struct {
    int id;
    int value;
  } cvtMap[TYPES_COUNT] = {
    { TYPE_BIGINT,        SQL_CVT_BIGINT },
    { TYPE_BINARY,        SQL_CVT_BINARY },
    { TYPE_BIT,           SQL_CVT_BIT },
    { TYPE_CHAR,          SQL_CVT_CHAR },
    { TYPE_DATE,          SQL_CVT_DATE },
    { TYPE_DECIMAL,       SQL_CVT_DECIMAL },
    { TYPE_DOUBLE,        SQL_CVT_DOUBLE },
    { TYPE_FLOAT,         SQL_CVT_FLOAT },
    { TYPE_INTEGER,       SQL_CVT_INTEGER },
    { TYPE_LONGVARBINARY, SQL_CVT_LONGVARBINARY },
    { TYPE_LONGVARCHAR,   SQL_CVT_LONGVARCHAR },
    { TYPE_NUMERIC,       SQL_CVT_NUMERIC },
    { TYPE_REAL,          SQL_CVT_REAL },
    { TYPE_SMALLINT,      SQL_CVT_SMALLINT },
    { TYPE_TIME,          SQL_CVT_TIME },
    { TYPE_TIMESTAMP,     SQL_CVT_TIMESTAMP },
    { TYPE_TINYINT,       SQL_CVT_TINYINT },
    { TYPE_VARBINARY,     SQL_CVT_VARBINARY },
    { TYPE_VARCHAR,       SQL_CVT_VARCHAR }
#if (LODBC_ODBCVER >= 0x0300)
    ,{ TYPE_WCHAR,         SQL_CVT_WCHAR }
    ,{ TYPE_WLONGVARCHAR,  SQL_CVT_WLONGVARCHAR }
    ,{ TYPE_WVARCHAR,      SQL_CVT_WVARCHAR }
    ,{ TYPE_GUID,          SQL_CVT_GUID }
#endif
  };

  for(;i<TYPES_COUNT; i++){
    if(convertMap[i].id == fromType){
      for(;j<TYPES_COUNT; j++){
        if(cvtMap[j].id == toType){
          SQLUINTEGER ret;
          GET_UINT32_INFO(ret, ((convertMap[i].value) & (cvtMap[i].value)));
          lua_pushboolean(L, (ret!=0)?1:0);
          return 1;
        }
      }
      return lodbc_faildirect(L, "Unknown toType");
    }
  }
  return lodbc_faildirect(L, "Unknown fromType");
}

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesLowerCaseIdentifiers,
  SQL_IDENTIFIER_CASE, SQL_IC_LOWER
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesLowerCaseQuotedIdentifiers,
  SQL_QUOTED_IDENTIFIER_CASE, SQL_IC_LOWER
)


DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesMixedCaseIdentifiers,
  SQL_IDENTIFIER_CASE, SQL_IC_MIXED
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesMixedCaseQuotedIdentifiers,
  SQL_QUOTED_IDENTIFIER_CASE, SQL_IC_MIXED
)


DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesUpperCaseIdentifiers,
  SQL_IDENTIFIER_CASE, SQL_IC_UPPER
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesUpperCaseQuotedIdentifiers,
  SQL_QUOTED_IDENTIFIER_CASE, SQL_IC_UPPER
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsMixedCaseIdentifiers,
  SQL_IDENTIFIER_CASE, SQL_IC_SENSITIVE
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsMixedCaseQuotedIdentifiers,
  SQL_QUOTED_IDENTIFIER_CASE, SQL_IC_SENSITIVE
)

DEFINE_GET_CHAR_AS_BOOL_INFO(supportsStoredProcedures,
  SQL_PROCEDURES, 'Y'
)

#ifdef LODBC_USE_DRIVERINFO

static int cnn_ownXXXAreVisible_(lua_State *L, conn_data *conn, int type, int what){
  int odbcType;
  int ret = conn_init_di_(L, conn);
  const drvinfo_data *di = conn->di;
  if(is_fail(L, ret)) return ret;

  if(!IS_VALID_RS_TYPE(type)){
    return lodbc_faildirect(L, "Invalid ResultSet type");
  }
  odbcType = di_getODBCCursorTypeFor(conn->di, type);

  #if LODBC_ODBCVER >= 0x0300
  if(conn->di->majorVersion_ > 2){
    SQLUINTEGER r;
    GET_UINT32_INFO(r,(getCursorAttributes2For(odbcType)));
    switch(what) {
      case UPDATES:
        lua_pushboolean(L, (r&SQL_CA2_SENSITIVITY_UPDATES)?1:0);
        return 1;
      case INSERTS:
        lua_pushboolean(L, (r&SQL_CA2_SENSITIVITY_ADDITIONS)?1:0);
        return 1;
      case DELETES:
        lua_pushboolean(L, (r&SQL_CA2_SENSITIVITY_DELETIONS)?1:0);
        return 1;
    }
    //notreached
    assert(0);
  }
  #endif

  // for odbc 2, assume false for forward only, true for dynamic
  // and check for static and keyset driven
  switch(odbcType) {
    case SQL_CURSOR_FORWARD_ONLY:
      lua_pushboolean(L,0);
      return 1;
    case SQL_CURSOR_DYNAMIC:
      lua_pushboolean(L,1);
      return 1;
    case SQL_CURSOR_KEYSET_DRIVEN:
    case SQL_CURSOR_STATIC:{
      SQLUINTEGER r;
      GET_UINT32_INFO(r,SQL_STATIC_SENSITIVITY);
      switch(what) {
        case UPDATES:
          lua_pushboolean(L, (r&SQL_SS_UPDATES)?1:0);
          return 1;
        case INSERTS:
          lua_pushboolean(L, (r&SQL_SS_ADDITIONS)?1:0);
          return 1;
        case DELETES:
          lua_pushboolean(L, (r&SQL_SS_DELETIONS)?1:0);
          return 1;
      }
    }
  }
  // notreached
  assert(0);
  return 0;
}

static int cnn_ownUpdatesAreVisible(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int type = luaL_checkint(L, 2);
  return conn_ownXXXAreVisible_(L, conn, type, UPDATES);
}

static int cnn_ownDeletesAreVisible(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int type = luaL_checkint(L, 2);
  return conn_ownXXXAreVisible_(L,conn,type,DELETES);
}

static int cnn_ownInsertsAreVisible(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int type = luaL_checkint(L, 2);
  return conn_ownXXXAreVisible_(L,conn,type,INSERTS);
}

// If I get this correct, the next 3 methods are only
// true when we're using a dynamic cursor

static int cnn_othersUpdatesAreVisible(lua_State *L){
  lodbc_cnn *cnn = lodbc_getcnn (L);
  int type = luaL_checkint(L, 2);
  int ct;
  int ret = conn_init_di_(L, conn);
  const drvinfo_data *di = conn->di;
  if(is_fail(L, ret)) return ret;

  if(!IS_VALID_RS_TYPE(type)){
    return lodbc_faildirect(L, "Invalid ResultSet type");
  }
  ct = di_getODBCCursorTypeFor(conn->di, type);
  lua_pushboolean(L, (ct == SQL_CURSOR_DYNAMIC)?1:0);
  return 1;
}

#define conn_othersInsertsAreVisible conn_othersUpdatesAreVisible
#define conn_othersDeletesAreVisible conn_othersUpdatesAreVisible

static int cnn_deletesAreDetected(lua_State *L){
  int type = luaL_checkint(L, 2);
  if(type == RS_TYPE_FORWARD_ONLY){
    lua_pushboolean(L, 0);
    return 1;
  }
  return conn_ownDeletesAreVisible(L);
}

static int cnn_insertsAreDetected(lua_State *L){
  int type = luaL_checkint(L, 2);
  if(type == RS_TYPE_FORWARD_ONLY){
    lua_pushboolean(L, 0);
    return 1;
  }
  return conn_ownInsertsAreVisible(L);
}

#endif

static int cnn_updatesAreDetected(lua_State *L){
    int type = luaL_checkint(L, 2);
    lua_pushboolean(L, (type!=RS_TYPE_FORWARD_ONLY)?1:0);
    return 1;
}


#undef DEFINE_GET_STRING_INFO
#undef DEFINE_GET_UINT16_AS_BOOL_INFO
#undef DEFINE_GET_UINT32_AS_MASK_INFO
#undef DEFINE_GET_UINT16_AS_NOTBOOL_INFO
#undef DEFINE_GET_CHAR_AS_BOOL_INFO
#undef DEFINE_GET_CHAR_AS_NOTBOOL_INFO
#undef GET_UINT32_INFO
#undef GET_UINT16_INFO
#undef DEFINE_GET_UINT32_INFO
#undef DEFINE_GET_UINT16_INFO

//}

//{ database catalog

#define CONN_BEFORE_CALL()                          \
  lodbc_cnn *cnn = lodbc_getcnn (L);                \
  SQLHSTMT hstmt;\
  SQLSMALLINT numcols;\
  SQLRETURN ret = SQLAllocHandle(hSTMT, cnn->handle, &hstmt);\
  if (lodbc_iserror(ret)) return lodbc_fail(L, hDBC, cnn->handle);

#define CONN_AFTER_CALL() \
    if (lodbc_iserror(ret)){\
      ret = lodbc_fail(L, hSTMT, hstmt);\
      SQLFreeHandle(hSTMT, hstmt);\
      return ret;\
    }\
    ret = SQLNumResultCols (hstmt, &numcols);\
    if (lodbc_iserror(ret)) {\
      ret = lodbc_fail(L, hSTMT, hstmt);\
      SQLFreeHandle(hSTMT, hstmt);\
      return ret;\
    }\
    if (numcols > 0)\
      return lodbc_statement_create (L,hstmt,cnn,1,1,numcols,1);\
    else { \
      SQLLEN numrows;\
      ret = SQLRowCount(hstmt, &numrows);\
      if (lodbc_iserror(ret)) {\
        ret = lodbc_fail(L, hSTMT, hstmt);\
        SQLFreeHandle(hSTMT, hstmt);\
        return ret;\
      }\
      lua_pushnumber(L, numrows);\
      SQLFreeHandle(hSTMT, hstmt);\
      return 1;\
    }

static int cnn_gettypeinfo(lua_State *L) {
    SQLSMALLINT sqltype = SQL_ALL_TYPES;
    CONN_BEFORE_CALL();

    if(lua_gettop(L)>1)
      sqltype = luaL_checkint(L,2);

    ret = SQLGetTypeInfo(hstmt, sqltype);

    CONN_AFTER_CALL();
}

static int cnn_gettables(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName, *types;
    size_t scatalog, sschema, stableName, stypes;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);
    types      = luaL_optlstring(L,5,EMPTY_STRING,&stypes);

    ret = SQLTables(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema,
      (SQLPOINTER)tableName, stableName, (SQLPOINTER)types, stypes);

    CONN_AFTER_CALL();
}

static int cnn_gettabletypes(lua_State *L){
  lua_settop(L, 1);
  lua_pushliteral(L,"");
  lua_pushliteral(L,"");
  lua_pushliteral(L,"");
  lua_pushliteral(L,"%");
  return cnn_gettables(L);
}

static int cnn_getschemas(lua_State *L){
  lua_settop(L, 1);
  lua_pushliteral(L,"");
  lua_pushliteral(L,"%");
  lua_pushliteral(L,"");
  lua_pushliteral(L,"");
  return cnn_gettables(L);
}

static int cnn_getcatalogs(lua_State *L){
  lua_settop(L, 1);
  lua_pushliteral(L,"%");
  lua_pushliteral(L,"");
  lua_pushliteral(L,"");
  lua_pushliteral(L,"");
  return cnn_gettables(L);
}

static int cnn_getstatistics(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName;
    size_t scatalog, sschema, stableName;
    SQLUSMALLINT unique, reserved;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);
    unique     = lua_toboolean(L,5)?SQL_INDEX_UNIQUE:SQL_INDEX_ALL;
    reserved   = lua_toboolean(L,6)?SQL_QUICK:SQL_ENSURE;

    ret = SQLStatistics(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema,
      (SQLPOINTER)tableName, stableName, unique, reserved);

    CONN_AFTER_CALL();
}

static int cnn_gettableprivileges(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName;
    size_t scatalog, sschema, stableName;
    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);

    ret = SQLTablePrivileges(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema,
      (SQLPOINTER)tableName, stableName);

    CONN_AFTER_CALL();
}

static int cnn_getcolumnprivileges(lua_State *L){
  static const char* EMPTY_STRING=NULL;
  const char *catalog, *schema, *tableName, *columnName;
  size_t scatalog, sschema, stableName, scolumnName;

  CONN_BEFORE_CALL();

  catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
  schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
  tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);
  columnName = luaL_optlstring(L,5,EMPTY_STRING,&scolumnName);

  ret = SQLColumnPrivileges(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema,
    (SQLPOINTER)tableName, stableName, (SQLPOINTER)columnName, scolumnName);

  CONN_AFTER_CALL();
}

static int cnn_getprimarykeys(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName;
    size_t scatalog, sschema, stableName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);

    ret = SQLPrimaryKeys(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema,
      (SQLPOINTER)tableName, stableName);

    CONN_AFTER_CALL();
}

static int cnn_getindexinfo(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName;
    size_t scatalog, sschema, stableName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);

    ret = SQLStatistics(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema,
      (SQLPOINTER)tableName, stableName,
      lua_toboolean(L,5)?SQL_INDEX_UNIQUE:SQL_INDEX_ALL,
      lua_toboolean(L,6)?SQL_QUICK:SQL_ENSURE
    );

    CONN_AFTER_CALL();
}

static int cnn_crossreference(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *pc, *ps, *pt, *fc, *fs, *ft;
    size_t spc, sps, spt, sfc, sfs, sft;

    CONN_BEFORE_CALL();

    pc = luaL_optlstring(L,2,EMPTY_STRING,&spc);  // primaryCatalog
    ps = luaL_optlstring(L,3,EMPTY_STRING,&sps);  // primarySchema
    pt = luaL_optlstring(L,4,EMPTY_STRING,&spt);  // primaryTable
    fc = luaL_optlstring(L,5,EMPTY_STRING,&sfc);  // foreignCatalog
    fs = luaL_optlstring(L,6,EMPTY_STRING,&sfs);  // foreignSchema
    ft = luaL_optlstring(L,7,EMPTY_STRING,&sft);  // foreignTable

    ret = SQLForeignKeys(hstmt,
      (SQLPOINTER)pc, spc,
      (SQLPOINTER)ps, sps,
      (SQLPOINTER)pt, spt,
      (SQLPOINTER)fc, sfc,
      (SQLPOINTER)fs, sfs,
      (SQLPOINTER)ft, sft
    );

    CONN_AFTER_CALL();
}

static int cnn_getprocedures(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *procName;
    size_t scatalog, sschema, sprocName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    procName   = luaL_optlstring(L,4,EMPTY_STRING,&sprocName);

    ret = SQLProcedures(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema,
      (SQLPOINTER)procName, sprocName);

    CONN_AFTER_CALL();
}

static int cnn_getprocedurecolumns(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *procName, *colName;
    size_t scatalog, sschema, sprocName, scolName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    procName   = luaL_optlstring(L,4,EMPTY_STRING,&sprocName);
    colName    = luaL_optlstring(L,5,EMPTY_STRING,&scolName);

    ret = SQLProcedureColumns(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema,
      (SQLPOINTER)procName, sprocName, (SQLPOINTER)colName, scolName);

    CONN_AFTER_CALL();
}

static int cnn_getspecialcolumns(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName;
    size_t scatalog, sschema, stableName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);

    ret=SQLSpecialColumns(hstmt,lua_tointeger(L,5)/*wat*/,
                                (SQLPOINTER)catalog,scatalog,
                                (SQLPOINTER)schema,sschema,
                                (SQLPOINTER)tableName,stableName,
                                lua_tointeger(L,6)/*scope*/,lua_tointeger(L,7)/*nullable*/);


    CONN_AFTER_CALL();
}

static int cnn_getcolumns(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName, *columnName;
    size_t scatalog, sschema, stableName, scolumnName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);
    columnName = luaL_optlstring(L,5,EMPTY_STRING,&scolumnName);

    ret = SQLColumns(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema,
      (SQLPOINTER)tableName, stableName, (SQLPOINTER)columnName, scolumnName);

    CONN_AFTER_CALL();
}

#undef CONN_BEFORE_CALL
#undef CONN_AFTER_CALL

//}

//}----------------------------------------------------------------------------


static const struct luaL_Reg lodbc_cnn_methods[] = {
  {"__gc",          cnn_destroy},
  {"destroy",       cnn_destroy},
  {"destroyed",     cnn_destroyed},

  {"environment",   cnn_environment},

  {"driverconnect", cnn_driverconnect},
  {"connect",       cnn_connect},
  {"disconnect",    cnn_disconnect},
  {"connected",     cnn_connected},

  {"commit",        cnn_commit},
  {"rollback",      cnn_rollback},

  {"statement",     cnn_statement},

  {"getuservalue",  cnn_getuservalue},
  {"setuservalue",  cnn_setuservalue},

  {"supportsPrepare",        cnn_has_prepare},
  {"supportsBindParam",      cnn_has_bindparam},
  {"supportsTransactions",   cnn_has_txn},

  {"setautocommit",          cnn_setautocommit},
  {"getautocommit",          cnn_getautocommit},
  {"setcatalog",             cnn_setcatalog},
  {"getcatalog",             cnn_getcatalog},
  {"setreadonly",            cnn_setreadonly},
  {"getreadonly",            cnn_getreadonly},
  {"settracefile",           cnn_settracefile},
  {"gettracefile",           cnn_gettracefile},
  {"settrace",               cnn_settrace},
  {"gettrace",               cnn_gettrace},
  {"gettransactionisolation",cnn_gettransactionisolation},
  {"settransactionisolation",cnn_settransactionisolation},
  {"setlogintimeout",        cnn_setlogintimeout},
  {"getlogintimeout",        cnn_getlogintimeout},
  {"setasyncmode",           cnn_setasyncmode},
  {"getasyncmode",           cnn_getasyncmode},

  {"setautoclosestmt",       cnn_setautoclosestmt},
  {"getautoclosestmt",       cnn_getautoclosestmt},

  {"dbmsname"       ,        cnn_getdbmsname},
  {"dbmsver"        ,        cnn_getdbmsver},
  {"drvname"        ,        cnn_getdrvname},
  {"drvver"         ,        cnn_getdrvver},
  {"odbcver"        ,        cnn_getodbcver},
  {"odbcvermm"      ,        cnn_getodbcvermm},

  {"typeinfo"          ,  cnn_gettypeinfo         },
  {"tables"            ,  cnn_gettables           },
  {"tabletypes"        ,  cnn_gettabletypes       },
  {"schemas"           ,  cnn_getschemas          },
  {"catalogs"          ,  cnn_getcatalogs         },
  {"statistics"        ,  cnn_getstatistics       },
  {"columns"           ,  cnn_getcolumns          },
  {"tableprivileges"   ,  cnn_gettableprivileges  },
  {"columnprivileges"  ,  cnn_getcolumnprivileges },
  {"primarykeys"       ,  cnn_getprimarykeys      },
  {"indexinfo"         ,  cnn_getindexinfo        },
  {"crossreference"    ,  cnn_crossreference      },
  {"procedures"        ,  cnn_getprocedures       },
  {"procedurecolumns"  ,  cnn_getprocedurecolumns },
  {"specialcolumns"    ,  cnn_getspecialcolumns   },

  {"getuintattr",   cnn_get_uint_attr},
  {"getstrattr",    cnn_get_str_attr},
  {"setuintattr",   cnn_set_uint_attr},
  {"setstrattr",    cnn_set_str_attr},

  {"uint32info", cnn_get_uint32_info},
  {"uint16info", cnn_get_uint16_info},
  {"strinfo",    cnn_get_str_info},

  {"supportsAsync",                  cnn_supportsAsync},
  {"supportsAsyncConnection",        cnn_supportsAsyncConnection},
  {"supportsAsyncStatement",         cnn_supportsAsyncStatement},

  {"identifierQuoteString",                  cnn_getIdentifierQuoteString},
  {"catalogTerm",                            cnn_getCatalogTerm},
  {"schemaTerm",                             cnn_getSchemaTerm},
  {"tableTerm",                              cnn_getTableTerm},
  {"procedureTerm",                          cnn_getProcedureTerm},
  {"userName",                               cnn_getUserName},
  {"catalogSeparator",                       cnn_getCatalogSeparator},
  {"isCatalogName",                          cnn_isCatalogName},
  {"isCatalogAtStart",                       cnn_isCatalogAtStart},
  {"SQLKeywords",                            cnn_getSQLKeywords},
  // {"supportsTransactions",                      cnn_supportsTransactions}, // used cnn_has_txn
  {"supportsDataDefinitionAndDataManipulationTransactions", cnn_supportsDataDefinitionAndDataManipulationTransactions},
  {"supportsDataManipulationTransactionsOnly",  cnn_supportsDataManipulationTransactionsOnly},
  {"dataDefinitionCausesTransactionCommit",     cnn_dataDefinitionCausesTransactionCommit},
  {"dataDefinitionIgnoredInTransactions",       cnn_dataDefinitionIgnoredInTransactions},
  {"defaultTransactionIsolation",               cnn_getDefaultTransactionIsolation},
  {"supportsTransactionIsolationLevel",         cnn_supportsTransactionIsolationLevel},
  {"supportsOpenCursorsAcrossCommit",           cnn_supportsOpenCursorsAcrossCommit},
  {"supportsOpenStatementsAcrossCommit",        cnn_supportsOpenStatementsAcrossCommit},
  {"supportsOpenCursorsAcrossRollback",         cnn_supportsOpenCursorsAcrossRollback },
  {"supportsOpenStatementsAcrossRollback",      cnn_supportsOpenStatementsAcrossRollback },
#ifdef LUASQL_USE_DRIVERINFO
  {"supportsResultSetType",                     cnn_supportsResultSetType},
  {"supportsResultSetConcurrency",              cnn_supportsResultSetConcurrency},
#endif
  {"nullPlusNonNullIsNull",                     cnn_nullPlusNonNullIsNull},
  {"supportsColumnAliasing",                    cnn_supportsColumnAliasing},
  {"supportsAlterTableWithAddColumn",           cnn_supportsAlterTableWithAddColumn},
  {"supportsAlterTableWithDropColumn",          cnn_supportsAlterTableWithDropColumn},
  {"extraNameCharacters",                       cnn_getExtraNameCharacters},
  {"searchStringEscape",                        cnn_getSearchStringEscape},
  {"numericFunctions",                          cnn_getNumericFunctions},
  {"stringFunctions",                           cnn_getStringFunctions},
  {"systemFunctions",                           cnn_getSystemFunctions},
  {"timeDateFunctions",                         cnn_getTimeDateFunctions},
  {"supportsCatalogsInDataManipulation",        cnn_supportsCatalogsInDataManipulation},
  {"supportsCatalogsInProcedureCalls",          cnn_supportsCatalogsInProcedureCalls},
  {"supportsCatalogsInTableDefinitions",        cnn_supportsCatalogsInTableDefinitions},
  {"supportsCatalogsInPrivilegeDefinitions",    cnn_supportsCatalogsInIndexDefinitions},
  {"supportsCatalogsInIndexDefinitions",        cnn_supportsCatalogsInIndexDefinitions},
  {"supportsSchemasInDataManipulation",         cnn_supportsSchemasInDataManipulation},
  {"supportsSchemasInProcedureCalls",           cnn_supportsSchemasInProcedureCalls},
  {"supportsSchemasInTableDefinitions",         cnn_supportsSchemasInTableDefinitions},
  {"supportsSchemasInIndexDefinitions",         cnn_supportsSchemasInIndexDefinitions},
  {"supportsSchemasInPrivilegeDefinitions",     cnn_supportsSchemasInPrivilegeDefinitions},
  {"supportsGroupBy",                           cnn_supportsGroupBy},
  {"supportsGroupByUnrelated",                  cnn_supportsGroupByUnrelated},
  {"supportsGroupByBeyondSelect",               cnn_supportsGroupByBeyondSelect},
  {"supportsUnion",                             cnn_supportsUnion},
  {"supportsUnionAll",                          cnn_supportsUnionAll},
  {"supportsOuterJoins",                        cnn_supportsOuterJoins},
  {"supportsFullOuterJoins",                    cnn_supportsFullOuterJoins},
  {"supportsLimitedOuterJoins",                 cnn_supportsLimitedOuterJoins},
  {"usesLocalFilePerTable",                     cnn_usesLocalFilePerTable},
  {"usesLocalFiles",                            cnn_usesLocalFiles},
  {"nullsAreSortedHigh",                        cnn_nullsAreSortedHigh},
  {"nullsAreSortedLow",                         cnn_nullsAreSortedLow},
  {"nullsAreSortedAtStart",                     cnn_nullsAreSortedAtStart},
  {"nullsAreSortedAtEnd",                       cnn_nullsAreSortedAtEnd},
  {"allProceduresAreCallable",                  cnn_allProceduresAreCallable},
  {"allTablesAreSelectable",                    cnn_allTablesAreSelectable},
  {"isReadOnly",                                cnn_isReadOnly},
  {"supportsTableCorrelationNames",             cnn_supportsTableCorrelationNames},
  {"supportsCorrelatedSubqueries",              cnn_supportsCorrelatedSubqueries},
  {"supportsSubqueriesInComparisons",           cnn_supportsSubqueriesInComparisons},
  {"supportsSubqueriesInExists",                cnn_supportsSubqueriesInExists},
  {"supportsSubqueriesInIns",                   cnn_supportsSubqueriesInIns},
  {"supportsSubqueriesInQuantifieds",           cnn_supportsSubqueriesInQuantifieds},
  {"supportsExpressionsInOrderBy",              cnn_supportsExpressionsInOrderBy},
  {"supportsLikeEscapeClause",                  cnn_supportsLikeEscapeClause},
  {"supportsMultipleResultSets",                cnn_supportsMultipleResultSets},
  {"supportsNonNullableColumns",                cnn_supportsNonNullableColumns},
  {"supportsMinimumSQLGrammar",                 cnn_supportsMinimumSQLGrammar},
  {"supportsCoreSQLGrammar",                    cnn_supportsCoreSQLGrammar},
  {"supportsExtendedSQLGrammar",                cnn_supportsExtendedSQLGrammar},
  {"supportsANSI92EntryLevelSQL",               cnn_supportsANSI92EntryLevelSQL},
  {"supportsANSI92FullSQL",                     cnn_supportsANSI92FullSQL},
  {"supportsANSI92IntermediateSQL",             cnn_supportsANSI92IntermediateSQL},
  {"maxBinaryLiteralLength",                 cnn_getMaxBinaryLiteralLength},
  {"maxCharLiteralLength",                   cnn_getMaxCharLiteralLength},
  {"maxColumnNameLength",                    cnn_getMaxColumnNameLength},
  {"maxColumnsInGroupBy",                    cnn_getMaxColumnsInGroupBy},
  {"maxColumnsInIndex",                      cnn_getMaxColumnsInIndex},
  {"maxColumnsInOrderBy",                    cnn_getMaxColumnsInOrderBy},
  {"maxColumnsInSelect",                     cnn_getMaxColumnsInSelect},
  {"maxColumnsInTable",                      cnn_getMaxColumnsInTable},
  {"maxCursorNameLength",                    cnn_getMaxCursorNameLength},
  {"maxIndexLength",                         cnn_getMaxIndexLength},
  {"maxSchemaNameLength",                    cnn_getMaxSchemaNameLength},
  {"maxProcedureNameLength",                 cnn_getMaxProcedureNameLength},
  {"maxCatalogNameLength",                   cnn_getMaxCatalogNameLength},
  {"maxRowSize",                             cnn_getMaxRowSize},
  {"maxStatementLength",                     cnn_getMaxStatementLength},
  {"maxTableNameLength",                     cnn_getMaxTableNameLength},
  {"maxTablesInSelect",                      cnn_getMaxTablesInSelect},
  {"maxUserNameLength",                      cnn_getMaxUserNameLength},
  {"maxConnections",                         cnn_getMaxConnections},
  {"maxStatements",                          cnn_getMaxStatements},
  {"doesMaxRowSizeIncludeBlobs",                cnn_doesMaxRowSizeIncludeBlobs},
  {"supportsMultipleTransactions",              cnn_supportsMultipleTransactions},
  {"supportsOrderByUnrelated",                  cnn_supportsOrderByUnrelated},
  {"supportsDifferentTableCorrelationNames",    cnn_supportsDifferentTableCorrelationNames},
  {"supportsConvertFn",                         cnn_supportsConvertFn},
  {"supportsConvert",                           cnn_supportsConvert},
  {"storesLowerCaseIdentifiers",                cnn_storesLowerCaseIdentifiers},
  {"storesLowerCaseQuotedIdentifiers",          cnn_storesLowerCaseQuotedIdentifiers},
  {"storesMixedCaseIdentifiers",                cnn_storesMixedCaseIdentifiers},
  {"storesMixedCaseQuotedIdentifiers",          cnn_storesMixedCaseQuotedIdentifiers},
  {"storesUpperCaseIdentifiers",                cnn_storesUpperCaseIdentifiers},
  {"storesUpperCaseQuotedIdentifiers",          cnn_storesUpperCaseQuotedIdentifiers},
  {"supportsMixedCaseIdentifiers",              cnn_supportsMixedCaseIdentifiers},
  {"supportsMixedCaseQuotedIdentifiers",        cnn_supportsMixedCaseQuotedIdentifiers},
  {"supportsStoredProcedures",                  cnn_supportsStoredProcedures},
#ifdef LODBC_USE_DRIVERINFO
  {"ownUpdatesAreVisible",                      cnn_ownUpdatesAreVisible},
  {"ownDeletesAreVisible",                      cnn_ownDeletesAreVisible},
  {"ownInsertsAreVisible",                      cnn_ownInsertsAreVisible},
  {"othersUpdatesAreVisible",                   cnn_othersUpdatesAreVisible},
  {"othersInsertsAreVisible",                   cnn_othersInsertsAreVisible},
  {"othersDeletesAreVisible",                   cnn_othersDeletesAreVisible},
  {"deletesAreDetected",                        cnn_deletesAreDetected},
  {"insertsAreDetected",                        cnn_insertsAreDetected},
#endif
  {"updatesAreDetected",                        cnn_updatesAreDetected},

  {NULL, NULL},
};

void lodbc_cnn_initlib (lua_State *L, int nup){
  lutil_createmetap(L, LODBC_CNN, lodbc_cnn_methods, nup);
  lua_pushstring(L,LODBC_CNN);
  lua_setfield(L,-2,"__metatable");
  lua_pop(L, 1);
}

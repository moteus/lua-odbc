#include "lodbc.h"
#include "utils.h"
#include "l52util.h"
#include "lenv.h"
#include "lcnn.h"
#include "lerr.h"
#include "luaodbc.h"

const char *LODBC_ENV = LODBC_PREFIX "Environment";

static int env_get_uint_attr_(lua_State*L, lodbc_env *env, SQLINTEGER optnum);

static int env_get_str_attr_(lua_State*L, lodbc_env *env, SQLINTEGER optnum);

static int env_set_uint_attr_(lua_State*L, lodbc_env *env, SQLINTEGER optnum, SQLUINTEGER value);

static int env_set_str_attr_(lua_State*L, lodbc_env *env, SQLINTEGER optnum, 
                             const char* value, size_t len);

lodbc_env *lodbc_getenv_at (lua_State *L, int i) {
  lodbc_env * env= (lodbc_env *)lutil_checkudatap (L, i, LODBC_ENV);
  luaL_argcheck (L, env != NULL, 1, LODBC_PREFIX "environment expected");
  luaL_argcheck (L, !(env->flags & LODBC_FLAG_DESTROYED), 1, LODBC_PREFIX "environment is closed");
  return env;
}

int lodbc_environment_create(lua_State *L, SQLHENV henv, uchar own){
  lodbc_env *env;
  uchar is_new = 0;
  if(henv == SQL_NULL_HANDLE){
    SQLRETURN ret = SQLAllocHandle(hENV, SQL_NULL_HANDLE, &henv);
    if (lodbc_iserror(ret))
      return lodbc_faildirect(L, "error creating environment.");
    own = 1;
    is_new = 1;
  }

  env = lutil_newudatap(L, lodbc_env, LODBC_ENV);
  memset(env, 0, sizeof(lodbc_env));
  if(!own)env->flags |= LODBC_FLAG_DONT_DESTROY;
  env->handle = henv;

  if(is_new){
    int top = lua_gettop(L);
    SQLRETURN ret = env_set_uint_attr_(L, env, SQL_ATTR_ODBC_VERSION, LODBC_ODBC3_C(SQL_OV_ODBC3, SQL_OV_ODBC2) );
    if(lodbc_is_fail(L,ret)){
      SQLFreeHandle (hENV, henv);
      env->flags &= ~LODBC_FLAG_DESTROYED;
      return ret;
    }
    lua_settop(L, top);
  }

  return 1;
}

static int env_destroy (lua_State *L) {
  lodbc_env *env = (lodbc_env *)lutil_checkudatap (L, 1, LODBC_ENV);
  luaL_argcheck (L, env != NULL, 1, LODBC_PREFIX "environment expected");

  if(!(env->flags & LODBC_FLAG_DESTROYED)){
    //! @todo autoclose connections

    if (env->conn_counter > 0)
      return luaL_error (L, LODBC_PREFIX"there are open connections");

    if(!(env->flags & LODBC_FLAG_DONT_DESTROY)){
#ifdef LODBC_CHECK_ERROR_ON_DESTROY
      SQLRETURN ret =
#endif
      SQLFreeHandle (hENV, env->handle);

#ifdef LODBC_CHECK_ERROR_ON_DESTROY
      if (lodbc_iserror(ret)) return lodbc_fail(L, hENV, env->handle);
#endif

      env->handle = SQL_NULL_HANDLE;
    }
    env->flags |= LODBC_FLAG_DESTROYED;
  }

  lua_pushnil(L);
  lodbc_set_user_value(L, 1);
  return lodbc_pass(L);
}

static int env_closed (lua_State *L) {
  lodbc_env *env = (lodbc_env *)lutil_checkudatap (L, 1, LODBC_ENV);
  luaL_argcheck (L, env != NULL, 1, LODBC_PREFIX "environment expected");
  lua_pushboolean(L, env->flags & LODBC_FLAG_DESTROYED);
  return 1;
}

//{ DSN and drivers info

static int env_getdatasources (lua_State *L) {
  lodbc_env *env = lodbc_getenv (L);
  SQLRETURN ret;
  SQLSMALLINT dsnlen,desclen;
  char dsn[SQL_MAX_DSN_LENGTH+1];
  char desc[256];
  int i = 1;
  int is_cb = lua_isfunction(L,2);
  int top = lua_gettop(L);

  ret = SQLDataSources(
    env->handle,SQL_FETCH_FIRST,
    (SQLPOINTER)dsn, SQL_MAX_DSN_LENGTH+1,&dsnlen,
    (SQLPOINTER)desc,sizeof(desc),&desclen
  );
  if(!is_cb) top++, lua_newtable(L);
  if(LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND) == ret) return is_cb ? 0 : 1;
  while(!lodbc_iserror(ret)){
    assert(top == lua_gettop(L));
    if(is_cb) lua_pushvalue(L, 2);
    if(!is_cb){
      lua_newtable(L);
      lua_pushstring(L,dsn);  lua_rawseti(L,-2,1);
      lua_pushstring(L,desc); lua_rawseti(L,-2,2);
      lua_rawseti(L,-2,i++);
    }
    else{
      int ret;
      lua_pushstring(L,dsn);
      lua_pushstring(L,desc);

      lua_call(L,2,LUA_MULTRET);
      ret = lua_gettop(L) - top;
      assert(ret >= 0);
      if(ret) return ret;
    }

    ret = SQLDataSources(
      env->handle,SQL_FETCH_NEXT,
      (SQLPOINTER)dsn, SQL_MAX_DSN_LENGTH+1,&dsnlen,
      (SQLPOINTER)desc,256,&desclen
      );
    if(LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND) == ret) return is_cb ? 0 : 1;
  }
  return lodbc_fail(L, hENV, env->handle);
}

#define MAX_DESC_LEN 256
#define MAX_ATTR_LEN 1024
static int env_getdrivers (lua_State *L) {
  lodbc_env *env = (lodbc_env *) lodbc_getenv (L);
  SQLRETURN ret;
  SQLSMALLINT attrlen,desclen;
  int i = 1;
  char desc[MAX_DESC_LEN];
  char *attr = malloc(MAX_ATTR_LEN+1);
  int is_cb = lua_isfunction(L,2);
  int top = lua_gettop(L);

  if(!attr)
  return LODBC_ALLOCATE_ERROR(L);

  ret = SQLDrivers(env->handle,SQL_FETCH_FIRST,
    (SQLPOINTER)desc,MAX_DESC_LEN,&desclen,
    (SQLPOINTER)attr,MAX_ATTR_LEN,&attrlen);
  if(!is_cb) top++,lua_newtable(L);
  if(LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND) == ret){
    free(attr);
    return is_cb ? 0 : 1;
  }
  while(!lodbc_iserror(ret)){
    assert(top == lua_gettop(L));
    if(is_cb) lua_pushvalue(L, 2);

    //find our attributes
    if(attr[0]!=0) {
      size_t i=0, last=0, n=1;
      lua_newtable(L);
      do {
        char *p,*a;
        while(attr[++i] != 0);
        a = &(attr[last]);
        p = strchr(a,'=');
        if(p){
          lua_pushlstring(L, a, p-a);
          lua_pushlstring(L, p + 1, (i-last)-(p-a)-1);
          lua_settable(L,-3);
        }
        else{
          lua_pushlstring(L,a,(i-last)); lua_rawseti(L,-2,n++);
        }
        last=i+1;
      } while(attr[last]!=0);
    }
    else lua_pushnil(L);

    if(!is_cb){
      lua_newtable(L);
      lua_insert(L,-2);lua_rawseti(L,-2,2);
      lua_pushstring(L,desc); lua_rawseti(L,-2,1);
      lua_rawseti(L,-2,i++);
    }
    else{
      int ret;
      lua_pushstring(L,desc);
      lua_insert(L,-2);

      lua_call(L,2,LUA_MULTRET);
      ret = lua_gettop(L) - top;
      assert(ret >= 0);
      if(ret){
        free(attr);
        return ret;
      }
    }

    ret = SQLDrivers(env->handle,SQL_FETCH_NEXT,
      (SQLPOINTER)desc,MAX_DESC_LEN,&desclen,
      (SQLPOINTER)attr,MAX_ATTR_LEN,&attrlen);

    if(LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND) == ret){
      free(attr);
      return is_cb ? 0 : 1;
    }
  }
  free(attr);
  return lodbc_fail(L, hENV, env->handle);
}
#undef MAX_DESC_LEN
#undef MAX_ATTR_LEN

//}

//{ get / set attr

static int env_get_uint_attr_(lua_State*L, lodbc_env *env, SQLINTEGER optnum){
  return lodbc_get_uint_attr_(L, hENV, env->handle, optnum);
}

static int env_get_str_attr_(lua_State*L, lodbc_env *env, SQLINTEGER optnum){
  return lodbc_get_str_attr_(L, hENV, env->handle, optnum);
}

static int env_set_uint_attr_(lua_State*L, lodbc_env *env, SQLINTEGER optnum, SQLUINTEGER value){
  return lodbc_set_uint_attr_(L, hENV, env->handle, optnum, value);
}

static int env_set_str_attr_(lua_State*L, lodbc_env *env, SQLINTEGER optnum, 
  const char* value, size_t len)
{
  return lodbc_set_str_attr_(L, hENV, env->handle, optnum, value, len);
}

static int env_get_uint_attr(lua_State*L){
  lodbc_env *env = lodbc_getenv(L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return env_get_uint_attr_(L, env, optnum);
}

static int env_get_str_attr(lua_State*L){
  lodbc_env *env = lodbc_getenv(L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return env_get_str_attr_(L, env, optnum);
}

static int env_set_uint_attr(lua_State*L){
  lodbc_env *env = lodbc_getenv(L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return env_set_uint_attr_(L, env, optnum,luaL_checkinteger(L,3));
}

static int env_set_str_attr(lua_State*L){
  lodbc_env *env = lodbc_getenv(L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  size_t len;
  const char *str = luaL_checklstring(L,3,&len);
  return env_set_str_attr_(L, env, optnum, str, len);
}

static int env_setv2(lua_State *L){
  lodbc_env *env = lodbc_getenv(L);
  return env_set_uint_attr_(L, env, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC2);
}

static int env_setv3(lua_State *L){
  lodbc_env *env = lodbc_getenv(L);
  return env_set_uint_attr_(L, env, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3);
}

//}

static int env_connection(lua_State *L){
  lodbc_env *env = lodbc_getenv(L);
  SQLHDBC hdbc;
  SQLRETURN ret = SQLAllocHandle (hDBC, env->handle, &hdbc);
  if(lodbc_iserror(ret)) return lodbc_fail(L, hENV, env->handle);

  return lodbc_connection_create(L, hdbc, env, 1, 1);
}

static int env_getuservalue(lua_State *L){
  lodbc_getenv(L);
  lodbc_get_user_value(L, 1);
  return 1;
}

static int env_setuservalue(lua_State *L){
  lodbc_getenv(L);lua_settop(L, 2);
  lodbc_set_user_value(L, 1);
  return 1;
}

static const struct luaL_Reg lodbc_env_methods[] = {
  {"__gc",       env_destroy},
  {"destroy",    env_destroy},
  {"destroyed",  env_closed},

  {"drivers",     env_getdrivers},
  {"datasources", env_getdatasources},

  {"connection",  env_connection},

  {"getuservalue", env_getuservalue},
  {"setuservalue", env_setuservalue},

  {"getuintattr", env_get_uint_attr},
  {"getstrattr",  env_get_str_attr},
  {"setuintattr", env_set_uint_attr},
  {"setstrattr",  env_set_str_attr},

  {NULL, NULL},
};

void lodbc_env_initlib (lua_State *L, int nup){
  lutil_createmetap(L, LODBC_ENV, lodbc_env_methods, nup);
  lua_pushstring(L,LODBC_ENV);
  lua_setfield(L,-2,"__metatable");
  lua_pop(L, 1);
}

#include "lval.h"
#include "lualib.h"
#include "utils.h"
#include "l52util.h"
#include "lstmt.h"
#include "lerr.h"
#include <string.h>
#include <malloc.h>

#define lodbc_newval(L, T) lutil_newudatap(L, T, T##_NAME)

static void *lodbc_value_at_impl (lua_State *L, const char*NAME, int i) {
  void *val = lutil_checkudatap(L, i, NAME);
  if(val) return val;

  lua_pushliteral(L, LODBC_PREFIX);
  lua_pushstring(L, NAME);
  lua_pushstring(L, " value expected");
  lua_concat(L,3);
  lua_error(L);
  return NULL;
}

#define lodbc_value_at(L, T, I) (T *)lodbc_value_at_impl(L, T##_NAME, I)
#define lodbc_value(L, T) lodbc_value_at(L, T, 1)
#define create_meta(L, T) lutil_createmetap(L, T##_NAME, T##_methods, 0)

//{ numeric

#define lodbc_utinyint_CTYPE SQL_C_UTINYINT
#define lodbc_stinyint_CTYPE SQL_C_STINYINT
#define lodbc_ushort_CTYPE   SQL_C_USHORT
#define lodbc_sshort_CTYPE   SQL_C_SSHORT
#define lodbc_ulong_CTYPE    SQL_C_ULONG
#define lodbc_slong_CTYPE    SQL_C_SLONG
#define lodbc_ubigint_CTYPE  SQL_C_UBIGINT
#define lodbc_sbigint_CTYPE  SQL_C_SBIGINT
#define lodbc_float_CTYPE    SQL_C_FLOAT
#define lodbc_double_CTYPE   SQL_C_DOUBLE

#define lodbc_utinyint_STYPE SQL_TINYINT
#define lodbc_stinyint_STYPE SQL_TINYINT
#define lodbc_ushort_STYPE   SQL_SMALLINT
#define lodbc_sshort_STYPE   SQL_SMALLINT
#define lodbc_ulong_STYPE    SQL_INTEGER
#define lodbc_slong_STYPE    SQL_INTEGER
#define lodbc_ubigint_STYPE  SQL_BIGINT
#define lodbc_sbigint_STYPE  SQL_BIGINT
#define lodbc_float_STYPE    SQL_FLOAT
#define lodbc_double_STYPE   SQL_DOUBLE



#define make_numeric_T(T, CT)                                       \
                                                                    \
static const char* lodbc_##T##_NAME = LODBC_PREFIX#T;               \
typedef struct lodbc_##T{                                           \
  SQLLEN ind;                                                       \
  CT     data;                                                      \
}lodbc_##T;                                                         \
                                                                    \
int lodbc_##T##_create(lua_State *L){                               \
  lodbc_##T *val = lodbc_newval(L, lodbc_##T);                      \
  val->ind = 0;                                                     \
  if(lua_isnumber(L,1))                                             \
    val->data = (CT)lua_tonumber(L, 1);                             \
  return 1;                                                         \
}                                                                   \
                                                                    \
int lodbc_##T##_get_value(lua_State *L){                            \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  if(val->ind == SQL_NULL_DATA) lua_pushnil(L);                     \
  else lua_pushnumber(L, val->data);                                \
  return 1;                                                         \
}                                                                   \
                                                                    \
int lodbc_##T##_set_value(lua_State *L){                            \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  val->data = (CT)luaL_checknumber(L, 2);                           \
  val->ind  = 0;                                                    \
  return 0;                                                         \
}                                                                   \
                                                                    \
int lodbc_##T##_set_null(lua_State *L){                             \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  val->ind = SQL_NULL_DATA;                                         \
  return 0;                                                         \
}                                                                   \
                                                                    \
int lodbc_##T##_set_default(lua_State *L){                          \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  val->ind = SQL_DEFAULT_PARAM;                                     \
  return 0;                                                         \
}                                                                   \
                                                                    \
int lodbc_##T##_is_null(lua_State *L){                              \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  lua_pushboolean(L, (val->ind == SQL_NULL_DATA)?1:0);              \
  return 1;                                                         \
}                                                                   \
                                                                    \
int lodbc_##T##_is_default(lua_State *L){                           \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  lua_pushboolean(L, (val->ind == SQL_DEFAULT_PARAM)?1:0);          \
  return 1;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_bind_col(lua_State *L){                      \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);                       \
  SQLSMALLINT i = luaL_checkint(L, 3);                              \
  SQLRETURN ret = SQLBindCol(stmt->handle, i,                       \
    lodbc_##T##_CTYPE, &val->data, 0, &val->ind);                   \
  if (lodbc_iserror(ret))                                           \
    return lodbc_fail(L, hSTMT, stmt->handle);                      \
  return lodbc_pass(L);                                             \
}                                                                   \
                                                                    \
static int lodbc_##T##_bind_param(lua_State *L){                    \
  lodbc_##T   *val = lodbc_value(L, lodbc_##T);                     \
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);                       \
  SQLSMALLINT i = luaL_checkint(L, 3);                              \
  SQLSMALLINT sqltype = luaL_optint(L, 4, lodbc_##T##_STYPE);       \
  SQLULEN     len     = luaL_optint(L, 4, 0);                       \
  SQLSMALLINT scale   = luaL_optint(L, 4, 0);                       \
                                                                    \
  SQLRETURN ret = SQLBindParameter(stmt->handle, i,SQL_PARAM_INPUT, \
                  lodbc_##T##_CTYPE, sqltype, len, scale,           \
                  &val->data, sizeof(val->data), &val->ind);        \
                                                                    \
  if (lodbc_iserror(ret))                                           \
    return lodbc_fail(L, hSTMT, stmt->handle);                      \
  return lodbc_pass(L);                                             \
}                                                                   \
                                                                    \
static const struct luaL_Reg lodbc_##T##_methods[] = {              \
  {"set_null",   lodbc_##T##_set_null},                             \
  {"set_default",lodbc_##T##_set_default},                          \
  {"is_null",    lodbc_##T##_is_null},                              \
  {"is_default", lodbc_##T##_is_default},                           \
  {"set",        lodbc_##T##_set_value},                            \
  {"get",        lodbc_##T##_get_value},                            \
  {"bind_col",   lodbc_##T##_bind_col},                             \
  {"bind_param", lodbc_##T##_bind_param},                           \
                                                                    \
  {NULL, NULL},                                                     \
};                                                                  \

make_numeric_T(ubigint,  SQLUBIGINT     )
make_numeric_T(sbigint,  SQLBIGINT      )
make_numeric_T(utinyint, unsigned char  )
make_numeric_T(stinyint, signed   char  )
make_numeric_T(ushort  , SQLUSMALLINT   )
make_numeric_T(sshort  , SQLSMALLINT    )
make_numeric_T(ulong   , SQLUINTEGER    )
make_numeric_T(slong   , SQLINTEGER     )
make_numeric_T(float   , SQLFLOAT       )
make_numeric_T(double  , SQLDOUBLE      )

//}

//{ char

#define lodbc_char_CTYPE  SQL_C_CHAR
#define lodbc_char_STYPE  SQL_CHAR
static const char* lodbc_char_NAME = LODBC_PREFIX"char";
typedef struct lodbc_char{
  SQLLEN  ind;
  SQLULEN size;
  SQLCHAR data[1]; // size + '\0'
}lodbc_char;

/*
 * значение val->ind необходимо корректировать после выполнени€ запроса в него записываетс€ 
 * реальна€ длина строки в Ѕƒ. Ёто значение может превышать  размер буфера. » если это значение 
 * не изменть перед следующем запросом можно получить AV.
 * перед запросом (выходной параметр / столбец):
 * val->size == 10 val->ind  == 0
 * после:
 * val->size == 10 val->ind  == 20 (скопировано только 10)
 * и если это значение использовать как входной параметр то мы получим AV.
 * ƒл€ избежани€ этого необходимо произвести какие либо дестви€ со значением, чтобы дать библиотеки 
 * скорректировать val->ind
 */

static int lodbc_char_create(lua_State *L){
  lodbc_char *val;
  SQLULEN len = 1;
  size_t sz;
  const char *data = NULL;
  if(LUA_TNUMBER == lua_type(L, 1)){
    len = lua_tointeger(L,1);
    lua_remove(L,1);
  }

  if(LUA_TSTRING == lua_type(L, 1)){
    data = lua_tolstring(L, 1, &sz);
    if(sz > len)len = sz; 
  }

  if(0 == len) len = 1;
  val = lutil_newudatap_impl(L, sizeof(lodbc_char) + len, lodbc_char_NAME);
  val->size = len;
  val->data[ val->size ] = '\0';
  if(data){
    val->ind = sz;
    memcpy(val->data, data, val->ind);
  }
  else val->ind = 0;
  return 1;
}

// correct and return ind 
SQLLEN lodbc_char_ind(lodbc_char *val){
  if(val->ind < 0) // SQL_NULL_DATA or SQL_DEFAULT
    return val->ind;
  if((SQLULEN)val->ind > val->size) val->ind = val->size;
  return val->ind;
}

int lodbc_char_get_value(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  SQLULEN ind = lodbc_char_ind(val);
  if   (ind == SQL_NULL_DATA) lua_pushnil(L);
  else if(ind == SQL_DEFAULT) lua_pushnil(L);
  else lua_pushlstring(L, val->data, val->ind);
  return 1;
}

int lodbc_char_set_value(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  size_t len;
  const char *data = luaL_checklstring(L,2,&len);
  val->ind = (val->size > len)?len:val->size;
  memcpy(val->data, data, val->ind);
  return 0;
}

int lodbc_char_set_null(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  val->ind = SQL_NULL_DATA;
  return 0;
}

int lodbc_char_set_default(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  val->ind = SQL_DEFAULT_PARAM;
  return 0;
}

int lodbc_char_size(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  lua_pushnumber(L, val->size);
  return 1;
}

int lodbc_char_length(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  SQLULEN ind = lodbc_char_ind(val);
  lua_pushnumber(L, ind<0?0:ind);
  return 1;
}

int lodbc_char_is_null(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  SQLULEN ind = lodbc_char_ind(val);
  lua_pushboolean(L, (ind == SQL_NULL_DATA)?1:0);
  return 1;
}

int lodbc_char_is_default(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  SQLULEN ind = lodbc_char_ind(val);
  lua_pushboolean(L, (ind == SQL_DEFAULT_PARAM)?1:0);
  return 1;
}

static int lodbc_char_bind_col(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i = luaL_checkint(L, 3);
  SQLRETURN ret;
  ret = SQLBindCol(stmt->handle, i,
    lodbc_char_CTYPE, &val->data[0], val->size + 1, &val->ind);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return lodbc_pass(L);
}

static int lodbc_char_bind_param(lua_State *L){
  lodbc_char   *val = lodbc_value(L, lodbc_char);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i = luaL_checkint(L, 3);
  SQLSMALLINT sqltype = luaL_optint(L, 4, lodbc_char_STYPE);
  SQLULEN     len     = luaL_optint(L, 4, val->size);
  SQLSMALLINT scale   = luaL_optint(L, 4, 0);

  SQLRETURN ret = SQLBindParameter(stmt->handle, i,SQL_PARAM_INPUT,
    lodbc_char_CTYPE, sqltype, len, scale,
    &val->data, val->size + 1, &val->ind);

  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return lodbc_pass(L);
}

static const struct luaL_Reg lodbc_char_methods[] = { 
  {"set_null",   lodbc_char_set_null},
  {"set_default",lodbc_char_set_default},
  {"set",        lodbc_char_set_value},
  {"get",        lodbc_char_get_value},
  {"size",       lodbc_char_size},
  {"length",     lodbc_char_length},
  {"is_null",    lodbc_char_is_null},
  {"is_default", lodbc_char_is_default},
  {"bind_col",   lodbc_char_bind_col},
  {"bind_param", lodbc_char_bind_param},

  {NULL, NULL}
};

//}

//{ binary

#define lodbc_binary_CTYPE  SQL_C_BINARY
#define lodbc_binary_STYPE  SQL_BINARY
static const char* lodbc_binary_NAME = LODBC_PREFIX"binary";
typedef struct lodbc_binary{
  SQLLEN  ind;
  SQLULEN size;
  SQLCHAR data[1]; // size
}lodbc_binary;

static int lodbc_binary_create(lua_State *L){
  lodbc_char *val;
  SQLULEN len = 1;
  size_t sz;
  const char *data = NULL;
  if(LUA_TNUMBER == lua_type(L, 1)){
    len = lua_tointeger(L,1);
    lua_remove(L,1);
  }

  if(LUA_TSTRING == lua_type(L, 1)){
    data = lua_tolstring(L, 1, &sz);
    if(sz > len)len = sz; 
  }

  if(0 == len) len = 1;
  val = lutil_newudatap_impl(L, sizeof(lodbc_char) + len - 1, lodbc_binary_NAME);
  val->size = len;
  if(data){
    val->ind = sz;
    memcpy(val->data, data, val->ind);
  }
  else val->ind = 0;
  return 1;
}

// correct and return ind 
SQLLEN lodbc_binary_ind(lodbc_binary *val){
  if(val->ind < 0) // SQL_NULL_DATA or SQL_DEFAULT
    return val->ind;
  if((SQLULEN)val->ind > val->size) val->ind = val->size;
  return val->ind;
}

int lodbc_binary_get_value(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  SQLULEN ind = lodbc_binary_ind(val);
  if   (ind == SQL_NULL_DATA) lua_pushnil(L);
  else if(ind == SQL_DEFAULT) lua_pushnil(L);
  else lua_pushlstring(L, val->data, val->ind);
  return 1;
}

int lodbc_binary_set_value(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  size_t len;
  const char *data = luaL_checklstring(L,2,&len);
  val->ind = (val->size > len)?len:val->size;
  memcpy(val->data, data, val->ind);
  return 0;
}

int lodbc_binary_set_null(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  val->ind = SQL_NULL_DATA;
  return 0;
}

int lodbc_binary_set_default(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  val->ind = SQL_DEFAULT_PARAM;
  return 0;
}

int lodbc_binary_size(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  lua_pushnumber(L, val->size);
  return 1;
}

int lodbc_binary_length(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  SQLULEN ind = lodbc_binary_ind(val);
  lua_pushnumber(L, ind<0?0:ind);
  return 1;
}

int lodbc_binary_is_null(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  SQLULEN ind = lodbc_binary_ind(val);
  lua_pushboolean(L, (ind == SQL_NULL_DATA)?1:0);
  return 1;
}

int lodbc_binary_is_default(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  SQLULEN ind = lodbc_binary_ind(val);
  lua_pushboolean(L, (ind == SQL_DEFAULT_PARAM)?1:0);
  return 1;
}

static int lodbc_binary_bind_col(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i = luaL_checkint(L, 3);
  SQLRETURN ret;
  ret = SQLBindCol(stmt->handle, i,
    lodbc_binary_CTYPE, &val->data[0], val->size, &val->ind);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return lodbc_pass(L);
}

static int lodbc_binary_bind_param(lua_State *L){
  lodbc_binary   *val = lodbc_value(L, lodbc_binary);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i = luaL_checkint(L, 3);
  SQLSMALLINT sqltype = luaL_optint(L, 4, lodbc_binary_STYPE);
  SQLULEN     len     = luaL_optint(L, 4, val->size);
  SQLSMALLINT scale   = luaL_optint(L, 4, 0);

  SQLRETURN ret = SQLBindParameter(stmt->handle, i,SQL_PARAM_INPUT,
    lodbc_binary_CTYPE, sqltype, len, scale,
    &val->data, val->size, &val->ind);

  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return lodbc_pass(L);
}

static const struct luaL_Reg lodbc_binary_methods[] = { 
  {"set_null",   lodbc_binary_set_null},
  {"set_default",lodbc_binary_set_default},
  {"set",        lodbc_binary_set_value},
  {"get",        lodbc_binary_get_value},
  {"size",       lodbc_binary_size},
  {"length",     lodbc_binary_length},
  {"is_null",    lodbc_binary_is_null},
  {"is_default", lodbc_binary_is_default},
  {"bind_col",   lodbc_binary_bind_col},
  {"bind_param", lodbc_binary_bind_param},

  {NULL, NULL}
};

//}

#define ctor_record(T) {#T, lodbc_##T##_create} 
#define reg_type(T) create_meta(L, lodbc_##T); lua_pop(L, 1)

static const struct luaL_Reg lodbc_val_func[] = {
  ctor_record( ubigint  ),
  ctor_record( sbigint  ),
  ctor_record( utinyint ),
  ctor_record( stinyint ),
  ctor_record( ushort   ),
  ctor_record( sshort   ),
  ctor_record( ulong    ),
  ctor_record( slong    ),
  ctor_record( float    ),
  ctor_record( double   ),
  ctor_record( char     ),
  ctor_record( binary   ),

  {NULL, NULL}
};

void lodbc_val_initlib (lua_State *L, int nup){
  reg_type( ubigint  );
  reg_type( sbigint  );
  reg_type( utinyint );
  reg_type( stinyint );
  reg_type( ushort   );
  reg_type( sshort   );
  reg_type( ulong    );
  reg_type( slong    );
  reg_type( float    );
  reg_type( double   );
  reg_type( char     );
  reg_type( binary   );

  lua_pop(L, nup);
  lua_pushvalue(L, -1 - nup);
  luaL_setfuncs(L, lodbc_val_func, 0);
  lua_pop(L,1);
}

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

static int optpartype(lua_State *L, int idx){
  int par_type;
  if(LUA_TNONE == lua_type(L, idx)) return SQL_PARAM_INPUT;
  par_type = luaL_checkint(L,idx);
  luaL_argcheck(L,
    (par_type == SQL_PARAM_INPUT)||
    (par_type == SQL_PARAM_OUTPUT)||
    (par_type == SQL_PARAM_INPUT_OUTPUT)||
    (par_type == SQL_RETURN_VALUE),
    idx, "invalid parameter type"
  );
  return par_type;
}

#define lodbc_value_at(L, T, I) (T *)lodbc_value_at_impl(L, T##_NAME, I)
#define lodbc_value(L, T) lodbc_value_at(L, T, 1)
#define create_meta(L, T) lutil_createmetap(L, T##_NAME, T##_methods, 0)

//{ make_fixsize_T

#define make_fixsize_T(T, CT)                                       \
                                                                    \
static const char* lodbc_##T##_NAME = LODBC_PREFIX#T;               \
typedef struct lodbc_##T{                                           \
  SQLLEN ind;                                                       \
  CT     data;                                                      \
}lodbc_##T;                                                         \
                                                                    \
static int lodbc_##T##_get(lua_State *L, lodbc_##T *val);           \
static int lodbc_##T##_set(lua_State *L, lodbc_##T *val, int i, int opt);\
                                                                    \
static int lodbc_##T##_create(lua_State *L){                        \
  lodbc_##T *val = lodbc_newval(L, lodbc_##T);                      \
  val->ind = SQL_NULL_DATA;                                         \
  if(lua_gettop(L) > 1) lodbc_##T##_set(L,val,1,1);                 \
  return 1;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_get_value(lua_State *L){                     \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  if(val->ind == SQL_NULL_DATA) lodbc_pushnull(L);                  \
  else if(val->ind == SQL_DEFAULT) lua_pushnil(L);                  \
  else return lodbc_##T##_get(L,val);                               \
  return 1;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_set_value(lua_State *L){                     \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  lodbc_##T##_set(L,val,2,0);                                       \
  return 0;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_size(lua_State *L){                          \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  lua_pushnumber(L, sizeof(val->data));                             \
  return 1;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_set_null(lua_State *L){                      \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  val->ind = SQL_NULL_DATA;                                         \
  return 0;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_set_default(lua_State *L){                   \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  val->ind = SQL_DEFAULT_PARAM;                                     \
  return 0;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_is_null(lua_State *L){                       \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  lua_pushboolean(L, (val->ind == SQL_NULL_DATA)?1:0);              \
  return 1;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_is_default(lua_State *L){                    \
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
  lua_settop(L, 1);                                                 \
  return 1;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_bind_param(lua_State *L){                    \
  lodbc_##T   *val = lodbc_value(L, lodbc_##T);                     \
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);                       \
  SQLSMALLINT i = luaL_checkint(L, 3);                              \
  int par_type        = optpartype(L,4);                            \
  SQLSMALLINT sqltype = luaL_optint(L, 5, lodbc_##T##_STYPE);       \
  SQLULEN     len     = luaL_optint(L, 6, 0);                       \
  SQLSMALLINT scale   = luaL_optint(L, 7, 0);                       \
                                                                    \
  SQLRETURN ret = SQLBindParameter(stmt->handle, i,par_type,        \
                  lodbc_##T##_CTYPE, sqltype, len, scale,           \
                  &val->data, sizeof(val->data), &val->ind);        \
                                                                    \
  if (lodbc_iserror(ret))                                           \
    return lodbc_fail(L, hSTMT, stmt->handle);                      \
  lua_settop(L, 1);                                                 \
  return 1;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_get_data(lua_State *L){                      \
  lodbc_##T *val = lodbc_value(L, lodbc_##T);                       \
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);                       \
  SQLSMALLINT i = luaL_checkint(L, 3);                              \
  SQLRETURN ret = SQLGetData(stmt->handle, i,                       \
    lodbc_##T##_CTYPE, &val->data, 0, &val->ind);                   \
  if (lodbc_iserror(ret))                                           \
    return lodbc_fail(L, hSTMT, stmt->handle);                      \
                                                                    \
  if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)){          \
    lua_pushboolean(L, 0);                                          \
    return 1;                                                       \
  }                                                                 \
                                                                    \
  lua_settop(L, 1);                                                 \
  return 1;                                                         \
}                                                                   \
                                                                    \
static const struct luaL_Reg lodbc_##T##_methods[] = {              \
  {"set_null",   lodbc_##T##_set_null},                             \
  {"set_default",lodbc_##T##_set_default},                          \
  {"is_null",    lodbc_##T##_is_null},                              \
  {"is_default", lodbc_##T##_is_default},                           \
  {"set",        lodbc_##T##_set_value},                            \
  {"get",        lodbc_##T##_get_value},                            \
  {"size",       lodbc_##T##_size},                                 \
  {"bind_col",   lodbc_##T##_bind_col},                             \
  {"bind_param", lodbc_##T##_bind_param},                           \
  {"get_data",   lodbc_##T##_get_data},                             \
                                                                    \
  {NULL, NULL},                                                     \
};                                                                  \
//}

//{ numeric

//{ make_numeric_get_set
#define make_numeric_get_set(T, CT)                                 \
                                                                    \
static int lodbc_##T##_get(lua_State *L, lodbc_##T *val){           \
  lua_pushnumber(L, (CT)val->data);                                 \
  return 1;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_set(lua_State *L, lodbc_##T *val, int i, int opt){\
  if(!opt){                                                         \
    val->data = (CT)luaL_checknumber(L, i);                         \
    val->ind  = 0;                                                  \
  }else if(lua_isnumber(L,i)){                                      \
    val->data = (CT)lua_tonumber(L, 1);                             \
    val->ind  = 0;                                                  \
  }                                                                 \
  return 0;                                                         \
}
//}

//{ make_integer_get_set
#define make_integer_get_set(T, CT)                                 \
                                                                    \
static int lodbc_##T##_get(lua_State *L, lodbc_##T *val){           \
  lua_pushinteger(L, (CT)val->data);                                \
  return 1;                                                         \
}                                                                   \
                                                                    \
static int lodbc_##T##_set(lua_State *L, lodbc_##T *val, int i, int opt){\
  if(!opt){                                                         \
    val->data = (CT)luaL_checkinteger(L, i);                        \
    val->ind  = 0;                                                  \
  }else if(lua_isnumber(L,i)){                                      \
    val->data = (CT)lua_tointeger(L, 1);                            \
    val->ind  = 0;                                                  \
  }                                                                 \
  return 0;                                                         \
}
//}

//{ make_numeric_T
#define make_numeric_T(T,CT) \
  make_fixsize_T(T,CT) \
  make_numeric_get_set(T,CT)
//}

//{ make_integer_T
#ifdef LODBC_USE_INTEGER
#  define make_integer_T(T,CT) \
    make_fixsize_T(T,CT) \
    make_integer_get_set(T,CT)
#else
#  define make_integer_T(T,CT) make_numeric_T(T,CT)
#endif
//}

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

#if defined(LODBC_INT_SIZE_64)
make_integer_T(ubigint,  SQLUBIGINT     )
make_integer_T(sbigint,  SQLBIGINT      )
#else
make_numeric_T(ubigint,  SQLUBIGINT     )
make_numeric_T(sbigint,  SQLBIGINT      )
#endif

#if defined(LODBC_INT_SIZE_32)
make_integer_T(ulong   , SQLUINTEGER    )
make_integer_T(slong   , SQLINTEGER     )
#else
make_numeric_T(ulong   , SQLUINTEGER    )
make_numeric_T(slong   , SQLINTEGER     )
#endif

make_integer_T(ushort  , SQLUSMALLINT   )
make_integer_T(sshort  , SQLSMALLINT    )
make_integer_T(utinyint, SQLCHAR        )
make_integer_T(stinyint, SQLSCHAR       )
make_numeric_T(float   , SQLFLOAT       )
make_numeric_T(double  , SQLDOUBLE      )

//}

//{ bit

#define lodbc_bit_CTYPE  SQL_C_BIT
#define lodbc_bit_STYPE  SQL_BIT

make_fixsize_T(bit, SQLCHAR)

static int lodbc_bit_set(lua_State *L, lodbc_bit *val, int i, int opt){
  if(opt){
    if(LUA_TNONE == lua_type(L, i)) return 0;
    if(LUA_TNIL == lua_type(L, i)) return 0;
  }

  if(lua_type(L,i) == LUA_TNUMBER)
    val->data = lua_tointeger(L, i)?1:0;
  else 
    val->data = lua_toboolean(L,i);
  val->ind  = 0;

  return 0;
}

static int lodbc_bit_get(lua_State *L, lodbc_bit *val){
  lua_pushboolean(L, val->data);
  return 1;
}

//}

//{ date

#if (LODBC_ODBCVER >= 0x0300)
#  define lodbc_date_CTYPE  SQL_C_TYPE_DATE
#  define lodbc_date_STYPE  SQL_TYPE_DATE
#else
#  define lodbc_date_CTYPE  SQL_C_DATE
#  define lodbc_date_STYPE  SQL_DATE
#endif

make_fixsize_T(date, SQL_DATE_STRUCT)

static int lodbc_date_set(lua_State *L, lodbc_date *val, int idx, int opt){
  int y, m, d;
  const char *str;
  if(opt && !lua_isstring(L,idx))
    return 0;
  str = luaL_checkstring(L,idx);
  if(3 == 
#ifdef _MSC_VER
    sscanf_s
#else
    sscanf
#endif
    (str, "%d-%d-%d", &y, &m, &d)
  ){
    val->data.year  = y;
    val->data.month = m;
    val->data.day   = d;
    val->ind        = 0;
  }
  return 0;
}

static int lodbc_date_get(lua_State *L, lodbc_date *val){
  char str[128];
#ifdef _MSC_VER
  sprintf_s(str,sizeof(str),
#else
  sprintf(str,
#endif
  "%.4d-%.2d-%.2d", val->data.year, val->data.month, val->data.day);
  lua_pushstring(L, str);
  return 1;
}

//}

//{ time

#if (LODBC_ODBCVER >= 0x0300)
#  define lodbc_time_CTYPE  SQL_C_TYPE_TIME
#  define lodbc_time_STYPE  SQL_TYPE_TIME
#else
#  define lodbc_time_CTYPE  SQL_C_TIME
#  define lodbc_time_STYPE  SQL_TIME
#endif

make_fixsize_T(time, SQL_TIME_STRUCT)

static int lodbc_time_set(lua_State *L, lodbc_time *val, int idx, int opt){
  int h, m, s;
  const char *str;
  if(opt && !lua_isstring(L,idx))
    return 0;
  str = luaL_checkstring(L,idx);
  if(3 == 
#ifdef _MSC_VER
    sscanf_s
#else
    sscanf
#endif
    (str, "%d:%d:%d", &h, &m, &s)
  ){
    val->data.hour   = h;
    val->data.minute = m;
    val->data.second = s;
    val->ind         = 0;
  }
  return 0;
}

static int lodbc_time_get(lua_State *L, lodbc_time *val){
  char str[128];
#ifdef _MSC_VER
  sprintf_s(str,sizeof(str),
#else
  sprintf(str,
#endif
  "%.2d:%.2d:%.2d", val->data.hour, val->data.minute, val->data.second);
  lua_pushstring(L, str);
  return 1;
}

//}

//{ timestamp

#if (LODBC_ODBCVER >= 0x0300)
#  define lodbc_timestamp_CTYPE  SQL_C_TYPE_TIMESTAMP
#  define lodbc_timestamp_STYPE  SQL_TYPE_TIMESTAMP
#else
#  define lodbc_timestamp_CTYPE  SQL_C_TIMESTAMP
#  define lodbc_timestamp_STYPE  SQL_TIMESTAMP
#endif

make_fixsize_T(timestamp, SQL_TIMESTAMP_STRUCT)

static int lodbc_timestamp_set(lua_State *L, lodbc_timestamp *val, int idx, int opt){
  int dy, dm, dd;
  int th, tm, ts, tf;

  const char *str;
  if(opt && !lua_isstring(L,idx))
    return 0;
  str = luaL_checkstring(L,idx);
  if(7 == 
#ifdef _MSC_VER
    sscanf_s
#else
    sscanf
#endif
    (str, "%d-%d-%d %d:%d:%d.%d", &dy, &dm, &dd, &th, &tm, &ts, &tf)
  ){
    val->data.year   = dy;
    val->data.month  = dm;
    val->data.day    = dd;
    val->data.hour   = th;
    val->data.minute = tm;
    val->data.second = ts;
    val->data.fraction = tf;
    val->ind         = 0;
  }
  else if(6 == 
#ifdef _MSC_VER
    sscanf_s
#else
    sscanf
#endif
    (str, "%d-%d-%d %d:%d:%d", &dy, &dm, &dd, &th, &tm, &ts)
  ){
    val->data.year   = dy;
    val->data.month  = dm;
    val->data.day    = dd;
    val->data.hour   = th;
    val->data.minute = tm;
    val->data.second = ts;
    val->data.fraction = 0;
    val->ind         = 0;
  }

  return 0;
}

static int lodbc_timestamp_get(lua_State *L, lodbc_timestamp *val){
  char str[128];
  if(val->data.fraction){
#ifdef _MSC_VER
    sprintf_s(str,sizeof(str),
#else
    sprintf(str,
#endif
    "%.4d-%.2d-%.2d %.2d:%.2d:%.2d.%d", val->data.year, val->data.month, val->data.day, val->data.hour, val->data.minute, val->data.second, val->data.fraction);
  }
  else{
#ifdef _MSC_VER
    sprintf_s(str,sizeof(str),
#else
    sprintf(str,
#endif
    "%.4d-%.2d-%.2d %.2d:%.2d:%.2d", val->data.year, val->data.month, val->data.day, val->data.hour, val->data.minute, val->data.second);
  }
  lua_pushstring(L, str);
  return 1;
}

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
 * значение val->ind необходимо корректировать. После выполнения запроса в него записывается 
 * реальная длина строки в БД(PARAM_OUTPUT). Это значение может превышать размер буфера. 
 * И если это значение не изменть перед следующем запросом (PARAM_INPUT) можно получить AV.
 * Пример. Перед запросом (выходной параметр / столбец):
 * val->size == 10 val->ind  == 0
 * после:
 * val->size == 10 val->ind  == 20 (скопировано только 10)
 * и если это значение использовать как входной параметр то мы получим AV.
 * Для избежания этого необходимо произвести какие либо дествия со значением, чтобы дать библиотеки 
 * скорректировать val->ind
 * Это актуально только если исползовать одно значение как входной параметр после выполнения запроса 
 * где это значение использовалось как выходной параметр.
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
  else val->ind = SQL_NULL_DATA;
  return 1;
}

// correct and return ind 
static SQLLEN lodbc_char_ind(lodbc_char *val){
  if(val->ind < 0) // SQL_NULL_DATA or SQL_DEFAULT
    return val->ind;
  if((SQLULEN)val->ind > val->size) val->ind = val->size;
  return val->ind;
}

static int lodbc_char_get_value(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  SQLULEN ind = lodbc_char_ind(val);
  if   (ind == SQL_NULL_DATA) lodbc_pushnull(L);
  else if(ind == SQL_DEFAULT) lua_pushnil(L);
  else lua_pushlstring(L, (char*)val->data, val->ind);
  return 1;
}

static int lodbc_char_set_value(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  size_t len;
  const char *data = luaL_checklstring(L,2,&len);
  val->ind = (val->size > len)?len:val->size;
  memcpy(val->data, data, val->ind);
  return 0;
}

static int lodbc_char_set_null(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  val->ind = SQL_NULL_DATA;
  return 0;
}

static int lodbc_char_set_default(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  val->ind = SQL_DEFAULT_PARAM;
  return 0;
}

static int lodbc_char_size(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  lua_pushnumber(L, val->size);
  return 1;
}

static int lodbc_char_length(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  SQLLEN ind = lodbc_char_ind(val);
  lua_pushnumber(L, ind<0?0:ind);
  return 1;
}

static int lodbc_char_is_null(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  SQLULEN ind = lodbc_char_ind(val);
  lua_pushboolean(L, (ind == SQL_NULL_DATA)?1:0);
  return 1;
}

static int lodbc_char_is_default(lua_State *L){
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
  lua_settop(L, 1);
  return 1;
}

static int lodbc_char_bind_param(lua_State *L){
  lodbc_char   *val = lodbc_value(L, lodbc_char);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i       = luaL_checkint(L, 3);
  int par_type        = optpartype(L,4);
  SQLSMALLINT sqltype = luaL_optint(L, 5, lodbc_char_STYPE);
  SQLULEN     len     = luaL_optint(L, 6, val->size);
  SQLSMALLINT scale   = luaL_optint(L, 7, 0);
  SQLRETURN ret;

  ret = SQLBindParameter(stmt->handle, i,par_type,
    lodbc_char_CTYPE, sqltype, len, scale,
    &val->data, val->size + 1, &val->ind);

  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  lua_settop(L, 1);
  return 1;
}

static int lodbc_char_get_data(lua_State *L){
  lodbc_char *val = lodbc_value(L, lodbc_char);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i = luaL_checkint(L, 3);
  SQLRETURN ret;
  ret = SQLGetData(stmt->handle, i,
    lodbc_char_CTYPE, &val->data[0], val->size + 1, &val->ind);

  if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)){
    lua_pushboolean(L, 0);
    return 1;
  }

  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  lua_settop(L, 1);
  return 1;
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
  {"get_data",   lodbc_char_get_data},

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
  lodbc_binary *val;
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
  val = lutil_newudatap_impl(L, sizeof(lodbc_binary) + len - 1, lodbc_binary_NAME);
  val->size = len;
  if(data){
    val->ind = sz;
    memcpy(val->data, data, val->ind);
  }
  else val->ind = SQL_NULL_DATA;
  return 1;
}

// correct and return ind 
static SQLLEN lodbc_binary_ind(lodbc_binary *val){
  if(val->ind < 0) // SQL_NULL_DATA or SQL_DEFAULT
    return val->ind;
  if((SQLULEN)val->ind > val->size) val->ind = val->size;
  return val->ind;
}

static int lodbc_binary_get_value(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  SQLULEN ind = lodbc_binary_ind(val);
  if   (ind == SQL_NULL_DATA) lodbc_pushnull(L);
  else if(ind == SQL_DEFAULT) lua_pushnil(L);
  else lua_pushlstring(L, (char*)val->data, val->ind);
  return 1;
}

static int lodbc_binary_set_value(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  size_t len;
  const char *data = luaL_checklstring(L,2,&len);
  val->ind = (val->size > len)?len:val->size;
  memcpy(val->data, data, val->ind);
  return 0;
}

static int lodbc_binary_set_null(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  val->ind = SQL_NULL_DATA;
  return 0;
}

static int lodbc_binary_set_default(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  val->ind = SQL_DEFAULT_PARAM;
  return 0;
}

static int lodbc_binary_size(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  lua_pushnumber(L, val->size);
  return 1;
}

static int lodbc_binary_length(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  SQLLEN ind = lodbc_binary_ind(val);
  lua_pushnumber(L, ind<0?0:ind);
  return 1;
}

static int lodbc_binary_is_null(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  SQLULEN ind = lodbc_binary_ind(val);
  lua_pushboolean(L, (ind == SQL_NULL_DATA)?1:0);
  return 1;
}

static int lodbc_binary_is_default(lua_State *L){
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
  lua_settop(L, 1);
  return 1;
}

static int lodbc_binary_bind_param(lua_State *L){
  lodbc_binary   *val = lodbc_value(L, lodbc_binary);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i = luaL_checkint(L, 3);
  int par_type        = optpartype(L,4);
  SQLSMALLINT sqltype = luaL_optint(L, 5, lodbc_binary_STYPE);
  SQLULEN     len     = luaL_optint(L, 6, val->size);
  SQLSMALLINT scale   = luaL_optint(L, 7, 0);

  SQLRETURN ret = SQLBindParameter(stmt->handle, i, par_type,
    lodbc_binary_CTYPE, sqltype, len, scale,
    &val->data[0], val->size, &val->ind);

  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  lua_settop(L, 1);
  return 1;
}

static int lodbc_binary_get_data(lua_State *L){
  lodbc_binary *val = lodbc_value(L, lodbc_binary);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i = luaL_checkint(L, 3);
  SQLRETURN ret;
  ret = SQLGetData(stmt->handle, i,
    lodbc_binary_CTYPE, &val->data[0], val->size, &val->ind);

  if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)){
    lua_pushboolean(L, 0);
    return 1;
  }

  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  lua_settop(L, 1);
  return 1;
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
  {"get_data",   lodbc_binary_get_data},

  {NULL, NULL}
};

//}

//{ wchar

#define lodbc_wchar_CTYPE  SQL_C_WCHAR
#define lodbc_wchar_STYPE  SQL_WCHAR
static const char* lodbc_wchar_NAME = LODBC_PREFIX"wchar";
typedef struct lodbc_wchar{
  SQLLEN   ind;
  SQLULEN  size; // in bytes
  SQLWCHAR data[1]; // size + 1 
}lodbc_wchar;

static int lodbc_wchar_create(lua_State *L){
  lodbc_wchar *val;
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
  len *= sizeof(val->data[0]);

  val = lutil_newudatap_impl(L, sizeof(lodbc_wchar) + len, lodbc_wchar_NAME);
  val->size = len;
  if(data){
    val->ind = sz;
    memcpy(val->data, data, val->ind);
  }
  else val->ind = SQL_NULL_DATA;

  val->data[ val->size ] = '\0';

  return 1;
}

// correct and return ind 
static SQLLEN lodbc_wchar_ind(lodbc_wchar *val){
  if(val->ind < 0) // SQL_NULL_DATA or SQL_DEFAULT
    return val->ind;
  if((SQLULEN)val->ind > val->size) val->ind = val->size;
  return val->ind;
}

static int lodbc_wchar_get_value(lua_State *L){
  lodbc_wchar *val = lodbc_value(L, lodbc_wchar);
  SQLULEN ind = lodbc_wchar_ind(val);
  if   (ind == SQL_NULL_DATA) lodbc_pushnull(L);
  else if(ind == SQL_DEFAULT) lua_pushnil(L);
  else lua_pushlstring(L, (const char*)&val->data[0], val->ind);
  return 1;
}

static int lodbc_wchar_set_value(lua_State *L){
  lodbc_wchar *val = lodbc_value(L, lodbc_wchar);
  size_t len;
  const char *data = luaL_checklstring(L,2,&len);
  val->ind = (val->size > len)?len:val->size;
  memcpy(val->data, data, val->ind);
  return 0;
}

static int lodbc_wchar_set_null(lua_State *L){
  lodbc_wchar *val = lodbc_value(L, lodbc_wchar);
  val->ind = SQL_NULL_DATA;
  return 0;
}

static int lodbc_wchar_set_default(lua_State *L){
  lodbc_wchar *val = lodbc_value(L, lodbc_wchar);
  val->ind = SQL_DEFAULT_PARAM;
  return 0;
}

static int lodbc_wchar_size(lua_State *L){
  lodbc_wchar *val = lodbc_value(L, lodbc_wchar);
  size_t sz = val->size / sizeof(val->data[0]);
  lua_pushnumber(L, sz);
  return 1;
}

static int lodbc_wchar_length(lua_State *L){
  lodbc_wchar *val = lodbc_value(L, lodbc_wchar);
  SQLLEN ind = lodbc_wchar_ind(val);
  size_t sz = ind<0?0:ind;
  sz /= sizeof(val->data[0]);
  lua_pushnumber(L, sz);
  return 1;
}

static int lodbc_wchar_is_null(lua_State *L){
  lodbc_wchar *val = lodbc_value(L, lodbc_wchar);
  SQLULEN ind = lodbc_wchar_ind(val);
  lua_pushboolean(L, (ind == SQL_NULL_DATA)?1:0);
  return 1;
}

static int lodbc_wchar_is_default(lua_State *L){
  lodbc_wchar *val = lodbc_value(L, lodbc_wchar);
  SQLULEN ind = lodbc_wchar_ind(val);
  lua_pushboolean(L, (ind == SQL_DEFAULT_PARAM)?1:0);
  return 1;
}

static int lodbc_wchar_bind_col(lua_State *L){
  lodbc_wchar *val = lodbc_value(L, lodbc_wchar);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i = luaL_checkint(L, 3);
  SQLRETURN ret;
  ret = SQLBindCol(stmt->handle, i,
    lodbc_wchar_CTYPE, &val->data[0], val->size + sizeof(val->data[0]), &val->ind);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  lua_settop(L, 1);
  return 1;
}

static int lodbc_wchar_bind_param(lua_State *L){
  lodbc_wchar   *val = lodbc_value(L, lodbc_wchar);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i = luaL_checkint(L, 3);
  int par_type        = optpartype(L,4);
  SQLSMALLINT sqltype = luaL_optint(L, 5, lodbc_wchar_STYPE);
  SQLULEN     len     = luaL_optint(L, 6, val->size);
  SQLSMALLINT scale   = luaL_optint(L, 7, 0);

  SQLRETURN ret = SQLBindParameter(stmt->handle, i, par_type,
    lodbc_wchar_CTYPE, sqltype, len, scale,
    &val->data[0], val->size + sizeof(val->data[0]), &val->ind);

  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  lua_settop(L, 1);
  return 1;
}

static int lodbc_wchar_get_data(lua_State *L){
  lodbc_wchar *val = lodbc_value(L, lodbc_wchar);
  lodbc_stmt  *stmt = lodbc_getstmt_at(L, 2);
  SQLSMALLINT i = luaL_checkint(L, 3);
  SQLRETURN ret;
  ret = SQLGetData(stmt->handle, i,
    lodbc_wchar_CTYPE, &val->data[0], val->size + sizeof(val->data[0]), &val->ind);

  if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)){
    lua_pushboolean(L, 0);
    return 1;
  }

  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  lua_settop(L, 1);
  return 1;
}

static const struct luaL_Reg lodbc_wchar_methods[] = { 
  {"set_null",   lodbc_wchar_set_null},
  {"set_default",lodbc_wchar_set_default},
  {"set",        lodbc_wchar_set_value},
  {"get",        lodbc_wchar_get_value},
  {"size",       lodbc_wchar_size},
  {"length",     lodbc_wchar_length},
  {"is_null",    lodbc_wchar_is_null},
  {"is_default", lodbc_wchar_is_default},
  {"bind_col",   lodbc_wchar_bind_col},
  {"bind_param", lodbc_wchar_bind_param},
  {"get_data",   lodbc_wchar_get_data},

  {NULL, NULL}
};

//}

#define ODBC_CONST(N) {#N, SQL_##N}

struct INT_CONST{
  char *name;
  int  value;
} lodbc_val_const[] = {
  ODBC_CONST( PARAM_INPUT               ),
  ODBC_CONST( PARAM_OUTPUT              ),
  ODBC_CONST( PARAM_INPUT_OUTPUT        ),

  ODBC_CONST( BIGINT                    ),
  ODBC_CONST( INTEGER                   ),
  ODBC_CONST( SMALLINT                  ),
  ODBC_CONST( TINYINT                   ),
  ODBC_CONST( BIT                       ),

  ODBC_CONST( NUMERIC                   ),
  ODBC_CONST( DECIMAL                   ),
  ODBC_CONST( FLOAT                     ),
  ODBC_CONST( REAL                      ),
  ODBC_CONST( DOUBLE                    ),

  ODBC_CONST( CHAR                      ),
  ODBC_CONST( VARCHAR                   ),
  ODBC_CONST( LONGVARCHAR               ),
  ODBC_CONST( BINARY                    ),
  ODBC_CONST( VARBINARY                 ),
  ODBC_CONST( LONGVARBINARY             ),
  ODBC_CONST( WCHAR                     ),
  ODBC_CONST( WVARCHAR                  ),
  ODBC_CONST( WLONGVARCHAR              ),

  ODBC_CONST( GUID                      ),

  ODBC_CONST( DATE                      ),
  ODBC_CONST( TIME                      ),
  ODBC_CONST( DATETIME                  ),
  ODBC_CONST( TIMESTAMP                 ),

  ODBC_CONST( TYPE_DATE                 ),
  ODBC_CONST( TYPE_TIME                 ),
  ODBC_CONST( TYPE_TIMESTAMP            ),

  ODBC_CONST( INTERVAL                  ),
  ODBC_CONST( INTERVAL_YEAR             ),
  ODBC_CONST( INTERVAL_MONTH            ),
  ODBC_CONST( INTERVAL_DAY              ),
  ODBC_CONST( INTERVAL_HOUR             ),
  ODBC_CONST( INTERVAL_MINUTE           ),
  ODBC_CONST( INTERVAL_SECOND           ),
  ODBC_CONST( INTERVAL_YEAR_TO_MONTH    ),
  ODBC_CONST( INTERVAL_DAY_TO_HOUR      ),
  ODBC_CONST( INTERVAL_DAY_TO_MINUTE    ),
  ODBC_CONST( INTERVAL_DAY_TO_SECOND    ),
  ODBC_CONST( INTERVAL_HOUR_TO_MINUTE   ),
  ODBC_CONST( INTERVAL_HOUR_TO_SECOND   ),
  ODBC_CONST( INTERVAL_MINUTE_TO_SECOND ),

  {0,0}
};

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
  ctor_record( wchar    ),
  ctor_record( date     ),
  ctor_record( time     ),
  ctor_record( timestamp),
  ctor_record( bit      ),

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
  reg_type( wchar    );
  reg_type( date     );
  reg_type( time     );
  reg_type( timestamp);
  reg_type( bit      );

  lua_pop(L, nup);
  lua_pushvalue(L, -1 - nup);
  luaL_setfuncs(L, lodbc_val_func, 0);
  {
    struct INT_CONST *p = lodbc_val_const;
    while(p->name){
      lua_pushinteger(L, p->value);
      lua_setfield(L, -2, p->name);
      p++;
    }
  }
  lua_pop(L,1);
}

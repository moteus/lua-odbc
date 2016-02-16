#ifndef _LODBC_H_94E57C49_1405_4ED4_845F_F6B4AC30D1AE_
#define _LODBC_H_94E57C49_1405_4ED4_845F_F6B4AC30D1AE_

#if defined(_WIN32)
#include <windows.h>
#include <sqlext.h>
#elif defined(INFORMIX)
#include "infxcli.h"
#elif defined(UNIXODBC)
#include "sql.h"
#include "sqltypes.h"
#include "sqlext.h"
#endif

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "lua.h"
#include "lauxlib.h"

#ifndef LODBC_INTERNAL
//! @todo implement
#  define LODBC_INTERNAL
#endif

#ifndef LODBC_MIN_PAR_BUFSIZE 
#  define LODBC_MIN_PAR_BUFSIZE 64
#endif

// #define LODBC_FREE_PAR_AT_CLEAR

#define LODBC_PREFIX "Lua-ODBC: "

#define LODBC_ODBCVER ODBCVER

#if LUA_VERSION_NUM >= 503 /* Lua 5.3 */

/*! @fixme detect real types (e.g. float/int32_t) */

#  ifndef LODBC_C_NUMBER
#    define LODBC_C_NUMBER       SQL_C_DOUBLE
#    define LODBC_NUMBER         SQL_DOUBLE
#    define LODBC_NUMBER_SIZE    0
#    define LODBC_NUMBER_DIGEST  0
#  endif

#  ifndef LODBC_C_INTEGER
#    define LODBC_C_INTEGER      SQL_C_SBIGINT
#    define LODBC_INTEGER        SQL_BIGINT
#    define LODBC_INTEGER_SIZE   0
#    define LODBC_INTEGER_DIGEST 0
#  endif

#  define LODBC_USE_INTEGER

#endif

#ifdef LODBC_USE_INTEGER
#  ifdef LUA_32BITS
#    define LODBC_INT_SIZE_16
#    define LODBC_INT_SIZE_32
#  else
#    define LODBC_INT_SIZE_16
#    define LODBC_INT_SIZE_32
#    define LODBC_INT_SIZE_64
#  endif
#endif

#ifndef LODBC_C_NUMBER
#  if defined LUA_NUMBER_DOUBLE
#    define LODBC_C_NUMBER SQL_C_DOUBLE
#    define LODBC_NUMBER SQL_DOUBLE
#    define LODBC_NUMBER_SIZE 0
#    define LODBC_NUMBER_DIGEST 0
#  elif defined LUA_NUMBER_INEGER
#    define LODBC_C_NUMBER SQL_C_INT
#    define LODBC_NUMBER SQL_INTEGER
#    define LODBC_NUMBER_SIZE 0
#    define LODBC_NUMBER_DIGEST 0
#  else
#    error need to specify LODBC_C_NUMBER
#  endif
#endif

#define hENV  SQL_HANDLE_ENV
#define hSTMT SQL_HANDLE_STMT
#define hDBC  SQL_HANDLE_DBC

typedef unsigned char uchar;

// объект уничтожен
#define LODBC_FLAG_DESTROYED        (uchar)0x01 
// при унчтожении не освобождать ресурсы
#define LODBC_FLAG_DONT_DESTROY     (uchar)0x02 
// объект открыт
#define LODBC_FLAG_OPENED           (uchar)0x04 

#define LODBC_FLAG_PREPARED         (uchar)0x08 

#define LODBC_FLAG_DESTROYONCLOSE   (uchar)0x10

#define LODBC_ASTATE_NONE            (uchar)0

// Выполняется асинхронный execute
#define LODBC_ASTATE_EXECUTE         (uchar)1

// Выполняется асинхронный ParamData
#define LODBC_ASTATE_PARAMDATA       (uchar)2
#define LODBC_ASTATE_FETCH           (uchar)3
#define LODBC_ASTATE_NEXTRS          (uchar)4

#ifdef LODBC_USE_LUA_REGISTRY
#  define LODBC_LUA_REGISTRY LUA_REGISTRYINDEX
#else
#  define LODBC_LUA_REGISTRY lua_upvalueindex(1)
#endif

#endif
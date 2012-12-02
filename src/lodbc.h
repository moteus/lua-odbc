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

#ifndef LODBC_MIN_PAR_BUFSIZE 
#  define LODBC_MIN_PAR_BUFSIZE 64
#endif

// #define LODBC_FREE_PAR_AT_CLEAR

#define LODBC_PREFIX "Lua-ODBC: "

#define LODBC_ODBCVER ODBCVER

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



#define LODBC_LUA_REGISTRY lua_upvalueindex(1)

#endif
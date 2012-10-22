#ifdef _MSC_VER
#  pragma once
#endif

#ifndef _LUAODBC_H_25621A6F_D9AD_47EB_85DB_06AD52493CD7_
#define _LUAODBC_H_25621A6F_D9AD_47EB_85DB_06AD52493CD7_

#ifdef _MSC_VER
#  ifdef LUAODBC_EXPORTS
#    define LODBC_EXPORT __declspec(dllexport)
#  else
#    define LODBC_EXPORT __declspec(dllimport)
#  endif
#else
#  define LODBC_EXPORT
#endif

typedef int (*lodbc_free_fn) (lua_State *, SQLHANDLE, void *);

LODBC_EXPORT extern const char *LODBC_ENV;
LODBC_EXPORT extern const char *LODBC_CNN;
LODBC_EXPORT extern const char *LODBC_STMT;

LODBC_EXPORT unsigned int lodbc_odbcver();

LODBC_EXPORT int luaopen_lodbc (lua_State *L);

LODBC_EXPORT int lodbc_environment(lua_State *L, SQLHENV henv, unsigned char own);

LODBC_EXPORT int lodbc_connection(lua_State *L, SQLHDBC hdbc,  unsigned char own);

LODBC_EXPORT int lodbc_statement(lua_State *L, SQLHSTMT hstmt, unsigned char own);


#endif
#ifdef _MSC_VER
#  pragma once
#endif

#ifndef _LUAODBC_H_25621A6F_D9AD_47EB_85DB_06AD52493CD7_
#define _LUAODBC_H_25621A6F_D9AD_47EB_85DB_06AD52493CD7_

#ifdef __cplusplus
extern "C" {
#endif

#ifndef LODBC_EXPORT
#  if defined(_WIN32)
#    ifdef LUAODBC_EXPORTS
#      define LODBC_EXPORT __declspec(dllexport) extern
#    else
#      define LODBC_EXPORT __declspec(dllimport) extern
#    endif
#  else
#    define LODBC_EXPORT extern
#  endif
#endif

#define LODBC_VERSION_MAJOR 0
#define LODBC_VERSION_MINOR 3
#define LODBC_VERSION_PATCH 1
#define LODBC_VERSION_COMMENT "dev"

LODBC_EXPORT const char *LODBC_ENV;
LODBC_EXPORT const char *LODBC_CNN;
LODBC_EXPORT const char *LODBC_STMT;

#ifndef LODBC_USE_NULL_AS_NIL

LODBC_EXPORT const int  *LODBC_NULL;

#endif

LODBC_EXPORT void lodbc_version(int *major, int *minor, int *patch);

LODBC_EXPORT unsigned int lodbc_odbcver();

LODBC_EXPORT int luaopen_lodbc (lua_State *L);

LODBC_EXPORT int lodbc_environment(lua_State *L, SQLHENV henv, unsigned char own);

LODBC_EXPORT int lodbc_connection(lua_State *L, SQLHDBC hdbc,  unsigned char own);

LODBC_EXPORT int lodbc_statement(lua_State *L, SQLHSTMT hstmt, unsigned char own);

#ifdef __cplusplus
}
#endif

#endif
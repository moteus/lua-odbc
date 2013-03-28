#ifndef _UTILS_H_4F4D6EFB_A532_424D_B1F8_0F46B5AEFF16_
#define _UTILS_H_4F4D6EFB_A532_424D_B1F8_0F46B5AEFF16_

#include "lodbc.h"

extern const char 
  *LT_STRING,
  *LT_NUMBER,
  *LT_BOOLEAN,
  *LT_BINARY;

#if LODBC_ODBCVER >= 0x0300
#   define LODBC_ODBC3_C(odbc3_value,old_value) odbc3_value
#else
#   define LODBC_ODBC3_C(odbc3_value,old_value) old_value
#endif

#define lodbc_iserror(a) (!SQL_SUCCEEDED((a)))

int lodbc_is_fail(lua_State *L, int nresult);

// no result or error
int lodbc_is_unknown(lua_State *L, int nresult);

/*
** Check if is SQL type.
*/
int lodbc_issqltype (const SQLSMALLINT type);

/*
** Returns the name of an equivalent lua type for a SQL type.
*/
const char *lodbc_sqltypetolua (const SQLSMALLINT type);

int lodbc_push_column_value(lua_State *L, SQLHSTMT hstmt, SQLUSMALLINT i, const char type);

/*
** Retrieves data from the i_th column in the current row
** Input
**   types: index in sack of column types table
**   hstmt: statement handle
**   i: column number
** Returns:
**   0 if successfull, non-zero otherwise;
*/
int lodbc_push_column(lua_State *L, int coltypes, const SQLHSTMT hstmt, SQLUSMALLINT i);

int lodbc_get_uint_attr_(lua_State*L, SQLSMALLINT HandleType, SQLHANDLE Handle, SQLINTEGER optnum);

int lodbc_get_str_attr_(lua_State*L, SQLSMALLINT HandleType, SQLHANDLE Handle, SQLINTEGER optnum);

int lodbc_set_uint_attr_(lua_State*L, SQLSMALLINT HandleType, SQLHANDLE Handle, 
    SQLINTEGER optnum, SQLUINTEGER value);

int lodbc_set_str_attr_(lua_State*L, SQLSMALLINT HandleType, SQLHANDLE Handle, 
    SQLINTEGER optnum, const char* value, size_t len);

int lodbc_iscallable(lua_State*L, int idx);

void lodbc_pushnull(lua_State*L);

int lodbc_isnull(lua_State*L, int idx);

void lodbc_init_user_value(lua_State*L);

void lodbc_get_user_value(lua_State*L, int keyindex);

void lodbc_set_user_value(lua_State*L, int keyindex);

#endif
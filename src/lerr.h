#ifndef _LERR_H_CF1547C4_0EC0_460F_9273_29FAC1C3F5D1_
#define _LERR_H_CF1547C4_0EC0_460F_9273_29FAC1C3F5D1_

#include "lodbc.h"

#ifdef LODBC_ERROR_AS_OBJECT
#  define lodbc_fail       lodbc_fail_obj
#  define lodbc_faildirect lodbc_faildirect_obj
#else
#  define lodbc_fail       lodbc_fail_str
#  define lodbc_faildirect lodbc_faildirect_str
#endif

LODBC_INTERNAL int lodbc_pass(lua_State *L);

LODBC_INTERNAL int lodbc_fail_str(lua_State *L, const SQLSMALLINT type, const SQLHANDLE handle);

LODBC_INTERNAL int lodbc_fail_obj(lua_State *L, const SQLSMALLINT type, const SQLHANDLE handle);

LODBC_INTERNAL int lodbc_faildirect_obj(lua_State *L, const char *err);

LODBC_INTERNAL int lodbc_faildirect_str(lua_State *L, const char *err);

#define LODBC_ALLOCATE_ERROR(L) luaL_error((L), LODBC_PREFIX"memory allocation error.")

LODBC_INTERNAL void lodbc_err_initlib (lua_State *L, int nup);

#endif
#ifndef _PARLIST_H_A582D4E6_E956_4290_997F_692458C8FAC6_
#define _PARLIST_H_A582D4E6_E956_4290_997F_692458C8FAC6_

#include "lodbc.h"

typedef struct par_data_tag{
  union{
    struct{
      SQLPOINTER           buf;
      SQLULEN              bufsize;
    }          strval;
    lua_Number numval;
    char       boolval;
  }                    value;
  SQLSMALLINT          sqltype;
  SQLULEN              parsize;
  SQLSMALLINT          digest;
  SQLLEN               ind;
  int                  get_cb;   /* reference to callback */
  struct par_data_tag* next;
} par_data;

LODBC_INTERNAL int par_data_setparinfo(par_data* par, lua_State *L, SQLHSTMT hstmt, SQLSMALLINT i);

/* Create params
** only memory allocation error
*/
LODBC_INTERNAL int par_data_create_unknown(par_data** ptr, lua_State *L);

/*
** Ensure that in list at least n params
** only memory allocation error
*/
LODBC_INTERNAL int par_data_ensure_nth (par_data **par, lua_State *L, int n, par_data **res);

LODBC_INTERNAL void par_data_free(par_data* next, lua_State *L);

/*
** assign new type to par_data.
** if there error while allocate memory then strval.buf will be NULL
*/
LODBC_INTERNAL void par_data_settype(par_data* par, SQLSMALLINT sqltype, SQLULEN parsize, SQLSMALLINT digest, SQLULEN bufsize);

LODBC_INTERNAL par_data* par_data_nth(par_data* p, int n);

LODBC_INTERNAL int par_init_cb(par_data *par, lua_State *L, SQLUSMALLINT sqltype);

LODBC_INTERNAL int par_call_cb(par_data *par, lua_State *L, int nparam);


#endif
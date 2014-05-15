#ifndef _LSTMT_H_667A2063_E838_4674_BB89_519BB67FCF41_
#define _LSTMT_H_667A2063_E838_4674_BB89_519BB67FCF41_

#include "lodbc.h"

struct par_data;
struct lodbc_cnn;

typedef struct lodbc_stmt{
  uchar     flags;
  uchar     aflags;
  SQLHSTMT  handle;
  int       coltypes, colnames;
  int       numcols, numpars;
  int       resultsetno;
  uchar     autoclose;
  struct par_data  *par;
  struct lodbc_cnn *cnn;
  int       cnn_ref;
  SQLRETURN last_exec_ret;
} lodbc_stmt;

LODBC_INTERNAL lodbc_stmt *lodbc_getstmt_at (lua_State *L, int i);
#define lodbc_getstmt(L) lodbc_getstmt_at((L),1)

LODBC_INTERNAL void lodbc_stmt_initlib (lua_State *L, int nup);

LODBC_INTERNAL int lodbc_statement_create (lua_State *L, SQLHSTMT hstmt, struct lodbc_cnn *cnn, int cnn_idx, uchar own, int ncols, uchar opened);

#endif
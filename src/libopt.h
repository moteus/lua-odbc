#ifndef _LIBOPT_H_CDD76C30_6410_4748_8DB7_C8EE28B3DD23_
#define _LIBOPT_H_CDD76C30_6410_4748_8DB7_C8EE28B3DD23_

#include "lodbc.h"

//! @todo implemnt config this options form Lua

// options ids
#define LODBC_OPT_STMT_DESTROYONCLOSE 1
#define LODBC_OPT_CNN_AUTOCLOSESTMT   2
#define LODBC_OPT_ENV_AUTOCLOSECNN    3

LODBC_INTERNAL int lodbc_opt_get_int(int optid);

#define LODBC_OPT_INT(NAME) lodbc_opt_get_int(LODBC_OPT_##NAME)

#endif


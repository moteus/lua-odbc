#include "libopt.h"
#include <assert.h>

int lodbc_opt_get_int(int optid){
  switch(optid){
    case LODBC_OPT_STMT_DESTROYONCLOSE : return 0;
    case LODBC_OPT_CNN_AUTOCLOSESTMT   : return 0;
  }
  assert(0); // unknown optid
  return 0;
}


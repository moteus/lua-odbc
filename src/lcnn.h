#ifndef _LCNN_H_5007C764_4F0B_4BC0_BE65_4BC8AD9AC0F9_
#define _LCNN_H_5007C764_4F0B_4BC0_BE65_4BC8AD9AC0F9_

#include "lodbc.h"

typedef struct lodbc_env lodbc_env;

#define LODBC_CNN_SUPPORT_INIT      0
#define LODBC_CNN_SUPPORT_TXN       1
#define LODBC_CNN_SUPPORT_PREPARE   2
#define LODBC_CNN_SUPPORT_BINDPARAM 3
#define LODBC_CNN_SUPPORT_NUMPARAMS 4
#define LODBC_CNN_SUPPORT_MAX       5

//{ libodbc  enum TransactionIsolation 
/** The data source does not support transactions */
#define TRANSACTION_NONE ((SQLUINTEGER)1)
/** Dirty reads, non-repeatable reads and phantom reads can occur. */
#define TRANSACTION_READ_UNCOMMITTED ((SQLUINTEGER)2)
/** Non-repeatable and phantom reads can occur */
#define TRANSACTION_READ_COMMITTED ((SQLUINTEGER)3)
/** Phantom reads can occur */
#define TRANSACTION_REPEATABLE_READ ((SQLUINTEGER)4)
/** Simply no problems */
#define TRANSACTION_SERIALIZABLE ((SQLUINTEGER)5)
//}

typedef struct lodbc_cnn{
  uchar    flags;
  SQLHDBC  handle;
  int      stmt_counter;
  signed char supports[LODBC_CNN_SUPPORT_MAX];

  lodbc_env *env;
  int       env_ref;

  int       stmt_list_ref; // list for autoclose statements
} lodbc_cnn;

LODBC_INTERNAL lodbc_cnn *lodbc_getcnn_at (lua_State *L, int i);
#define lodbc_getcnn(L) lodbc_getcnn_at((L),1)

LODBC_INTERNAL void lodbc_cnn_initlib (lua_State *L, int nup);

// hdbc - валидный описатель
// env - кто распределил (opt)
// env_idx - индекс объекта env на стеке (opt)
// own - true - передается право владения(объект ответственен за закрытие описателя)
LODBC_INTERNAL int lodbc_connection_create(lua_State *L, SQLHDBC hdbc, lodbc_env *env, int env_idx, uchar own);

LODBC_INTERNAL int lodbc_cnn_init_support(lua_State *L);

#endif 
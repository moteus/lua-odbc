#ifndef _LCNN_H_5007C764_4F0B_4BC0_BE65_4BC8AD9AC0F9_
#define _LCNN_H_5007C764_4F0B_4BC0_BE65_4BC8AD9AC0F9_

#include "luaodbc.h"
#include "lodbc.h"

typedef struct lodbc_env lodbc_env;

#define LODBC_CNN_SUPPORT_TXN       0
#define LODBC_CNN_SUPPORT_PREPARE   1
#define LODBC_CNN_SUPPORT_BINDPARAM 2
#define LODBC_CNN_SUPPORT_NUMPARAMS 3
#define LODBC_CNN_SUPPORT_MAX       4

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

  int       stmt_ref; // list for autoclose statements
} lodbc_cnn;

lodbc_cnn *lodbc_getcnn_at (lua_State *L, int i);
#define lodbc_getcnn(L) lodbc_getcnn_at((L),1)

void lodbc_cnn_initlib (lua_State *L, int nup);

// hdbc - валидный описатель
// env - кто распределил (opt)
// env_idx - индекс объекта env на стеке (opt)
// own - true - передается право владения(объект ответственен за закрытие описателя)
int lodbc_connection_create(lua_State *L, SQLHDBC hdbc, lodbc_env *env, int env_idx, uchar own);

#endif 
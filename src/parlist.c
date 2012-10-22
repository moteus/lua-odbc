#include "lodbc.h"
#include "utils.h"
#include "parlist.h"

int par_data_setparinfo(par_data* par, lua_State *L, SQLHSTMT hstmt, SQLSMALLINT i){

  SQLRETURN ret = SQLDescribeParam(hstmt, i, &par->sqltype, &par->parsize, &par->digest, NULL);
  // Sybase ODBC driver 9.0.2.3951
  // if use in select/update and so on query - always return SQL_CHAR(32567)
  // so we do not allocate this buffer yet
  // if use CALL SOME_PROC(...)
  // for unsigned int i get -18(SQL_C_ULONG?) from SQLDescribeParam!!!
  // So it's not useful function

  if(lodbc_iserror(ret)){
    par->sqltype = SQL_UNKNOWN_TYPE;
    return lodbc_fail(L,hSTMT,hstmt);
  }
  else{
    if(!lodbc_issqltype(par->sqltype)){ // SQL_C_ULONG for example
      par->sqltype = SQL_UNKNOWN_TYPE;
      par->parsize = par->digest = 0;
    }
    else{
      // and what now?
    }
  }

  return 0;
}

/* Create params
** only memory allocation error
*/
int par_data_create_unknown(par_data** ptr, lua_State *L){
  par_data* par = (par_data*)malloc(sizeof(par_data));
  assert(*ptr == NULL);
  if(!par) return 1;
  *ptr = par;

  memset(par,0,sizeof(par_data));
  par->get_cb  = LUA_NOREF;
  par->sqltype = SQL_UNKNOWN_TYPE;
  return 0;
}

/*
** Ensure that in list at least n params
** only memory allocation error
*/
int par_data_ensure_nth (par_data **par, lua_State *L, int n, par_data **res){
  assert(n > 0);
  *res = 0;
  if(!(*par)){
    int ret = par_data_create_unknown(par, L);
    if(ret){
      return ret;
    }
  }

  while(--n>0){
    par = &((*par)->next);
    if(!(*par)){
      int ret = par_data_create_unknown(par, L);
      if(ret){
        *res = *par;
        return ret;
      }
    }
  }
  *res = *par;
  return 0;
}

void par_data_free(par_data* next, lua_State *L){
  while(next){
    const char *luatype = lodbc_sqltypetolua(next->sqltype);
    par_data* p = next->next;
    luaL_unref (L, LODBC_LUA_REGISTRY, next->get_cb);
    if(((luatype == LT_STRING)||(luatype == LT_BINARY))&&(next->value.strval.buf))
      free(next->value.strval.buf);
    free(next);
    next = p;
  }
}

/*
** assign new type to par_data.
** if there error while allocate memory then strval.buf will be NULL
*/
void par_data_settype(par_data* par, SQLSMALLINT sqltype, SQLULEN parsize, SQLSMALLINT digest, SQLULEN bufsize){
  const char* oldtype = lodbc_sqltypetolua(par->sqltype);
  const char* newtype = lodbc_sqltypetolua(sqltype);
  const int old_isstring = (oldtype == LT_BINARY)||(oldtype == LT_STRING);
  const int new_isstring = (newtype == LT_BINARY)||(newtype == LT_STRING);
  par->parsize = parsize;
  par->digest  = digest;
  if(old_isstring){
    if(new_isstring){
      par->sqltype = sqltype;
      if(par->value.strval.bufsize < bufsize){
        par->value.strval.bufsize = bufsize > LODBC_MIN_PAR_BUFSIZE?bufsize:LODBC_MIN_PAR_BUFSIZE;
        if(par->value.strval.buf) par->value.strval.buf = realloc(par->value.strval.buf,par->value.strval.bufsize);
        else par->value.strval.buf = malloc(par->value.strval.bufsize);
        if(!par->value.strval.buf)
          par->value.strval.bufsize = 0;
      }
      return;
    }
    if(par->value.strval.buf){
      free(par->value.strval.buf);
      par->value.strval.buf = NULL;
      par->value.strval.bufsize = 0;
    }
  }
  else{
    if(new_isstring){
      par->sqltype = sqltype;
      if(bufsize){
        par->value.strval.bufsize = bufsize > LODBC_MIN_PAR_BUFSIZE?bufsize:LODBC_MIN_PAR_BUFSIZE;
        par->value.strval.buf = malloc(par->value.strval.bufsize);
        if(!par->value.strval.buf)
          par->value.strval.bufsize = 0;
      }
      else{
        par->value.strval.bufsize = 0;
        par->value.strval.buf = NULL;
      }
      return;
    }
  }
  par->sqltype = sqltype;
}

par_data* par_data_nth(par_data* p, int n){
  while(p && --n>0){
    p = p->next;
  }
  return p;
}

int par_init_cb(par_data *par, lua_State *L, SQLUSMALLINT sqltype){
  int data_len=0;
  assert(lua_isfunction(L,3));
  luaL_unref(L, LODBC_LUA_REGISTRY, par->get_cb);
  lua_pushvalue(L,3);
  par->get_cb = luaL_ref(L, LODBC_LUA_REGISTRY);
  if(LUA_REFNIL == par->get_cb)
    return luaL_error(L, LODBC_PREFIX"can not register CallBack (expire resource).");
  // may be should check sqltype in (SQL_BINARY/SQL_CHAR)?
  if(lua_isnumber(L,4))
    data_len = luaL_checkint(L,4);
  par->parsize = data_len;
  par->ind = SQL_LEN_DATA_AT_EXEC(data_len);
  par_data_settype(par, sqltype, data_len, 0, 0);
  return 0;
}

int par_call_cb(par_data *par, lua_State *L, int nparam){
  assert(LUA_NOREF != par->get_cb);
  lua_rawgeti(L, LODBC_LUA_REGISTRY, par->get_cb);
  assert(lua_isfunction(L,-1));
  lua_insert(L,-(nparam+1));
  if(lua_pcall(L,nparam,1,0)){
    lua_pushnil(L);
    lua_insert(L,-2);
    return 2;
  }
  return 0;
}


#include "utils.h"
#include "l52util.h"
#include "lstmt.h"
#include "lcnn.h"
#include "lerr.h"
#include "parlist.h"
#include "libopt.h"
#include "lodbc.h"
#include "luaodbc.h"

const char *LODBC_STMT = LODBC_PREFIX "Statement";

static const char *CLOSE_CURSOR_STATES[] = {
  "24000", // Invalid cursor state.
  "34000", // Cursor not open
};
static const int CLOSE_CURSOR_STATES_N = 2;

#define IS_CURSOR_CLOSED(T,H) (lodbc_test_state(T, H, CLOSE_CURSOR_STATES, CLOSE_CURSOR_STATES_N) >= 0)

//-----------------------------------------------------------------------------
// declaretions
//{----------------------------------------------------------------------------
static void create_colinfo (lua_State *L, lodbc_stmt *stmt);
static int stmt_destroy (lua_State *L);
//}----------------------------------------------------------------------------

lodbc_stmt *lodbc_getstmt_at (lua_State *L, int i) {
  lodbc_stmt * stmt= (lodbc_stmt *)lutil_checkudatap (L, i, LODBC_STMT);
  luaL_argcheck (L, stmt != NULL, 1, LODBC_PREFIX "statement expected");
  luaL_argcheck (L, !(stmt->flags & LODBC_FLAG_DESTROYED), 1, LODBC_PREFIX "statement is closed");
  return stmt;
}

//{ ctor/dtor/close

int lodbc_statement_create (lua_State *L, SQLHSTMT hstmt, lodbc_cnn *cnn, int cnn_idx, uchar own, int ncols, uchar opened){
  lodbc_stmt *stmt;
  if((!cnn) && cnn_idx) cnn = lodbc_getcnn_at(L, cnn_idx);
  stmt = lutil_newudatap(L, lodbc_stmt, LODBC_STMT);
  memset(stmt, 0, sizeof(lodbc_stmt));
  if(!own) stmt->flags |= LODBC_FLAG_DONT_DESTROY;
  if(LODBC_OPT_INT(STMT_DESTROYONCLOSE)) stmt->flags |= LODBC_FLAG_DESTROYONCLOSE;

  stmt->handle   = hstmt;
  stmt->colnames = stmt->coltypes = LUA_NOREF;
  stmt->numcols  = ncols;
  stmt->numpars  = -1;
  stmt->autoclose = 1;

  if(cnn){
    stmt->cnn = cnn;
    cnn->stmt_counter++;
    if(cnn->stmt_list_ref != LUA_NOREF){
      lua_rawgeti(L, LODBC_LUA_REGISTRY, cnn->stmt_list_ref);
      lua_pushvalue(L, -2);
      lua_pushboolean(L, 1);
      lua_rawset(L, -3);
      lua_pop(L,1);
    }
  }

  stmt->cnn_ref = LUA_NOREF;
  if(cnn_idx){
    lua_pushvalue(L, cnn_idx);
    stmt->cnn_ref = luaL_ref(L, LODBC_LUA_REGISTRY);
  }

  if(ncols){
    create_colinfo(L, stmt);
    if(opened) stmt->flags |= LODBC_FLAG_OPENED;
  }

  return 1;
}

static int stmt_destroy (lua_State *L) {
  lodbc_stmt * stmt= (lodbc_stmt *)lutil_checkudatap (L, 1, LODBC_STMT);
  luaL_argcheck (L, stmt != NULL, 1, LODBC_PREFIX "statement expected");

  if(!(stmt->flags & LODBC_FLAG_DESTROYED)){

    if (stmt->flags & LODBC_FLAG_OPENED){
#ifdef LODBC_CHECK_ERROR_ON_DESTROY
      SQLRETURN ret =
#endif

        SQLCloseCursor(stmt->handle);

#ifdef LODBC_CHECK_ERROR_ON_DESTROY
      if (lodbc_iserror(ret)){
        if(!IS_CURSOR_CLOSED(hSTMT, stmt->handle)){
          return lodbc_fail(L, hSTMT, stmt->handle);
        }
      }
#endif
      stmt->flags &= ~LODBC_FLAG_OPENED;
    }

    if(!(stmt->flags & LODBC_FLAG_DONT_DESTROY)){
#ifdef LODBC_CHECK_ERROR_ON_DESTROY
      SQLRETURN ret =
#endif
        SQLFreeHandle (hSTMT, stmt->handle);

#ifdef LODBC_CHECK_ERROR_ON_DESTROY
      if (lodbc_iserror(ret)) return lodbc_fail(L, hSTMT, stmt->handle);
#endif

      stmt->handle = SQL_NULL_HANDLE;
    }
    if(stmt->cnn){
      stmt->cnn->stmt_counter--;
      assert(stmt->cnn->stmt_counter >= 0);
      if(stmt->cnn->stmt_list_ref != LUA_NOREF){
        lua_rawgeti(L, LODBC_LUA_REGISTRY, stmt->cnn->stmt_list_ref);
        lua_pushvalue(L, -2);
        lua_pushnil(L);
        lua_rawset(L, -3);
        lua_pop(L,1);
      }
    }

    luaL_unref (L, LODBC_LUA_REGISTRY, stmt->colnames);
    luaL_unref (L, LODBC_LUA_REGISTRY, stmt->coltypes);
    luaL_unref (L, LODBC_LUA_REGISTRY, stmt->cnn_ref);
    stmt->cnn_ref = stmt->colnames = stmt->coltypes = LUA_NOREF;

    par_data_free(stmt->par, L);
    stmt->par = NULL;

    stmt->flags |= LODBC_FLAG_DESTROYED;
  }

  lua_pushnil(L);
  // lua_rawsetp(L, LODBC_LUA_REGISTRY, (void*)stmt);
  lodbc_set_user_value(L, 1);
  return lodbc_pass(L);
}

static int stmt_destroyed (lua_State *L) {
  lodbc_stmt *stmt = (lodbc_stmt *)lutil_checkudatap (L, 1, LODBC_STMT);
  luaL_argcheck (L, stmt != NULL, 1, LODBC_PREFIX "statement expected");
  lua_pushboolean(L, stmt->flags & LODBC_FLAG_DESTROYED);
  return 1;
}

static int stmt_close(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  if(stmt->flags & LODBC_FLAG_DESTROYONCLOSE){
    return stmt_destroy(L);
  }

  if(stmt->flags & LODBC_FLAG_OPENED){
#ifdef LODBC_CHECK_ERROR_ON_DESTROY
    SQLRETURN ret =
#endif

      SQLCloseCursor(stmt->handle);

#ifdef LODBC_CHECK_ERROR_ON_DESTROY
    if (lodbc_iserror(ret)){
      if(!IS_CURSOR_CLOSED(hSTMT, stmt->handle)){
        return lodbc_fail(L, hSTMT, stmt->handle);
      }
    }
#endif
    stmt->flags &= ~LODBC_FLAG_OPENED;
  }
  stmt->aflags = LODBC_ASTATE_NONE;
  return lodbc_pass(L);
}

static int stmt_closed (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  lua_pushboolean(L, !(stmt->flags & LODBC_FLAG_OPENED));
  return 1;
}

static int stmt_connection(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  lua_rawgeti(L, LODBC_LUA_REGISTRY, stmt->cnn_ref);
  return 1;
}

static int stmt_getuservalue(lua_State *L){
  lodbc_getstmt(L);
  lodbc_get_user_value(L, 1);
  return 1;
}

static int stmt_setuservalue(lua_State *L){
  lodbc_getstmt(L);lua_settop(L, 2);
  lodbc_set_user_value(L, 1);
  return 1;
}

//}

//{

static int create_parinfo(lua_State *L, lodbc_stmt *stmt){
  par_data **par = &stmt->par;
  int top = lua_gettop(L);
  int i;
  for(i = 1; i <= stmt->numpars; i++){
    if(!(*par)){
      int ret = par_data_create_unknown(par, L);
      if(ret){
        par_data_free(stmt->par, L);
        stmt->par = NULL;
        if(!(*par)) return LODBC_ALLOCATE_ERROR(L);
        return ret;
      }
    }
    par = &((*par)->next);
  }

  assert(top == lua_gettop(L));
  return 0;
}

/*
** Clear addition info about stmt.
*/
static void stmt_clear_info_ (lua_State *L, lodbc_stmt *stmt){
  int top = lua_gettop(L);

  assert(!(stmt->flags & LODBC_FLAG_OPENED));

  luaL_unref (L, LODBC_LUA_REGISTRY, stmt->colnames);
  luaL_unref (L, LODBC_LUA_REGISTRY, stmt->coltypes);

  stmt->aflags = LODBC_ASTATE_NONE;

  // don't need check error
  SQLFreeStmt(stmt->handle, SQL_RESET_PARAMS);

  {par_data *par;for(par = stmt->par;par;par=par->next){
    luaL_unref(L, LODBC_LUA_REGISTRY, par->get_cb);
    par->get_cb = LUA_NOREF;
  }}

  if(stmt->numcols) // if we use bindcol
    SQLFreeStmt(stmt->handle, SQL_UNBIND);

#ifdef LODBC_FREE_PAR_AT_CLEAR
  par_data_free(stmt->par, L);
  stmt->par      = NULL;
#endif

  stmt->numpars     = -1;
  stmt->flags       &= ~LODBC_FLAG_PREPARED;
  stmt->resultsetno = 0;
  stmt->colnames    = LUA_NOREF;
  stmt->coltypes    = LUA_NOREF;
  stmt->numcols     = 0;

  assert(top == lua_gettop(L));
}

/*
** Creates two tables with the names and the types of the columns.
*/
static void create_colinfo (lua_State *L, lodbc_stmt *stmt){
  SQLCHAR buffer[256];
  SQLSMALLINT namelen, datatype, i;
  int types, names;

  lua_newtable(L);
  types = lua_gettop (L);
  lua_newtable(L);
  names = lua_gettop (L);
  for (i = 1; i <= stmt->numcols; i++){
    /*SQLRETURN ret = */SQLDescribeCol(stmt->handle, i, buffer, sizeof(buffer), 
      &namelen, &datatype, NULL, NULL, NULL);

    // if (lodbc_iserror(ret)) return lodbc_fail(L, hSTMT, stmt->handle);

    lua_pushstring (L, (char*)buffer);
    lua_rawseti (L, names, i);
    lua_pushstring(L, lodbc_sqltypetolua(datatype));
    lua_rawseti (L, types, i);
  }
  stmt->colnames = luaL_ref (L, LODBC_LUA_REGISTRY);
  stmt->coltypes = luaL_ref (L, LODBC_LUA_REGISTRY);
}

/*
** Prepare a SQL statement.
** init params and cols
** return 0 if success
*/
static int stmt_prepare_ (lua_State *L, lodbc_stmt *stmt, const char *statement){
  SQLSMALLINT numcols;
  SQLRETURN ret;

  if(stmt->flags & LODBC_FLAG_OPENED)
    return lodbc_faildirect(L, "can not prepare opened statement.");

  stmt_clear_info_(L, stmt);

  ret = SQLPrepare(stmt->handle, (SQLCHAR *) statement, SQL_NTS);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  stmt->flags |= LODBC_FLAG_PREPARED;

  /* determine the number of results */
  ret = SQLNumResultCols (stmt->handle, &numcols);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);

  stmt->numcols  = numcols;

  stmt->numpars = -1;
  if((stmt->cnn)&&(stmt->cnn->supports[LODBC_CNN_SUPPORT_NUMPARAMS])){
    SQLSMALLINT numpars;
    ret = SQLNumParams(stmt->handle, &numpars);
    if (lodbc_iserror(ret)) return 0;
    stmt->numpars = numpars;
  }
  return 0;
}

//}

//{ bind CallBack

static int stmt_bind_number_cb_pre_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret = par_init_cb(par, L, LODBC_NUMBER);
  if(ret) return ret;

  par_data_settype(par, LODBC_NUMBER, LODBC_NUMBER_SIZE, LODBC_NUMBER_DIGEST, 0);
  return 0;
}

#ifdef LODBC_USE_INTEGER

static int stmt_bind_integer_cb_pre_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret = par_init_cb(par, L, LODBC_INTEGER);
  if(ret) return ret;

  par_data_settype(par, LODBC_INTEGER, LODBC_INTEGER_SIZE, LODBC_INTEGER_DIGEST, 0);
  return 0;
}

#else
#  define stmt_bind_integer_cb_pre_ stmt_bind_number_cb_pre_
#endif

static int stmt_bind_number_cb_post_(lua_State *L, SQLSMALLINT ct, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret = SQLBindParameter(stmt->handle, i, SQL_PARAM_INPUT, ct, par->sqltype, par->parsize, par->digest, (SQLPOINTER)par, 0, &par->ind);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return lodbc_pass(L);
}

static int stmt_bind_number_cb_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret = stmt_bind_number_cb_pre_(L, stmt, i, par);
  if(ret)return ret;
  return stmt_bind_number_cb_post_(L, LODBC_C_NUMBER, stmt, i, par);
}

#ifdef LODBC_USE_INTEGER

static int stmt_bind_integer_cb_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret = stmt_bind_integer_cb_pre_(L, stmt, i, par);
  if(ret)return ret;
  return stmt_bind_number_cb_post_(L, LODBC_C_INTEGER, stmt, i, par);
}

#else
#  define stmt_bind_integer_cb_ stmt_bind_number_cb_
#endif


static int stmt_bind_bool_cb_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret = par_init_cb(par, L, SQL_BIT);
  if(ret)
    return ret;
  ret = SQLBindParameter(stmt->handle, i, SQL_PARAM_INPUT, SQL_C_BIT, par->sqltype, 0, 0, (SQLPOINTER)par, 0, &par->ind);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return lodbc_pass(L);
}

static int stmt_bind_string_cb_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret = par_init_cb(par, L, SQL_CHAR);
  if(ret)
    return ret;
  ret = SQLBindParameter(stmt->handle, i, SQL_PARAM_INPUT, SQL_C_CHAR, par->sqltype, 0, 0, (SQLPOINTER)par, 0, &par->ind);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return lodbc_pass(L);
}

static int stmt_bind_binary_cb_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret = par_init_cb(par, L, SQL_BINARY);
  if(ret)
    return ret;
  ret = SQLBindParameter(stmt->handle, i, SQL_PARAM_INPUT, SQL_C_BINARY, par->sqltype, 0, 0, (SQLPOINTER)par, 0, &par->ind);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return lodbc_pass(L);
}

//}

//{ bind Data

static int stmt_bind_number_post_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret;

  #ifdef LODBC_USE_INTEGER
  if(lua_isinteger(L, 3)){
    par_data_settype(par, LODBC_INTEGER, LODBC_INTEGER_SIZE, LODBC_INTEGER_DIGEST, 0);
    par->value.intval = lua_tointeger(L,3);
    ret = SQLBindParameter(stmt->handle, i, SQL_PARAM_INPUT, LODBC_C_INTEGER, par->sqltype, par->parsize, par->digest, &par->value.intval, 0, NULL);
  }
  else
  #endif
  {
    par_data_settype(par,LODBC_NUMBER,LODBC_NUMBER_SIZE, LODBC_NUMBER_DIGEST, 0);
    par->value.numval = luaL_checknumber(L,3);
    ret = SQLBindParameter(stmt->handle, i, SQL_PARAM_INPUT, LODBC_C_NUMBER, par->sqltype, par->parsize, par->digest, &par->value.numval, 0, NULL);
  }

  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  
  return lodbc_pass(L);
}

static int stmt_bind_number_impl_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  if(lua_isfunction(L,3))
    return stmt_bind_number_cb_(L,stmt, i, par);

  return stmt_bind_number_post_(L, stmt, i, par);
}

static int stmt_bind_integer_impl_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  if(lua_isfunction(L,3))
    return stmt_bind_integer_cb_(L, stmt, i, par);

  return stmt_bind_number_post_(L, stmt, i, par);
}

static int stmt_bind_number_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  return stmt_bind_number_impl_(L, stmt, i, par);
}

static int stmt_bind_integer_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  return stmt_bind_integer_impl_(L, stmt, i, par);
}

static int stmt_bind_string_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret;
  size_t buffer_len;
  if(lua_isfunction(L,3))
    return stmt_bind_string_cb_(L,stmt,i,par);
  lua_tolstring(L,3,&buffer_len);
  buffer_len++;

  par_data_settype(par,SQL_CHAR, 0, 0, buffer_len);
  if(!par->value.strval.buf)
    return LODBC_ALLOCATE_ERROR(L);
  par->ind = SQL_NTS;
  ret = SQLBindParameter(stmt->handle, i, SQL_PARAM_INPUT, SQL_C_CHAR, par->sqltype, par->value.strval.bufsize, 0, par->value.strval.buf, par->value.strval.bufsize, &par->ind);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);

  memcpy(par->value.strval.buf, lua_tostring(L, 3), buffer_len-1);
  ((char*)par->value.strval.buf)[buffer_len-1] = '\0';
  return lodbc_pass(L);
}

static int stmt_bind_binary_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret;
  size_t buffer_len;
  if(lua_isfunction(L,3))
    return stmt_bind_binary_cb_(L,stmt,i,par);
  lua_tolstring(L,3,&buffer_len);
  par_data_settype(par,SQL_BINARY, 0, 0, buffer_len?buffer_len:1);
  if(!par->value.strval.buf)
    return LODBC_ALLOCATE_ERROR(L);
  par->ind = buffer_len;
  ret = SQLBindParameter(stmt->handle, i, SQL_PARAM_INPUT, SQL_C_BINARY, par->sqltype, par->value.strval.bufsize, 0, par->value.strval.buf, par->value.strval.bufsize, &par->ind);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);

  memcpy(par->value.strval.buf, lua_tostring(L, 3), buffer_len);
  return lodbc_pass(L);
}

static int stmt_bind_bool_(lua_State *L, lodbc_stmt *stmt, SQLUSMALLINT i, par_data *par){
  SQLRETURN ret;
  if(lua_isfunction(L,3))
    return stmt_bind_bool_cb_(L,stmt,i,par);
  par_data_settype(par,SQL_BIT, 0, 0, 0);
  ret = SQLBindParameter(stmt->handle, i, SQL_PARAM_INPUT, SQL_C_BIT, par->sqltype, 0, 0, &par->value.boolval, 0, NULL);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  par->value.boolval = lua_isboolean(L,3) ? lua_toboolean(L,3) : luaL_checkinteger(L,3);
  return lodbc_pass(L);
}

//}

//{ bind lua interface

#define CHECK_BIND_PARAM() \
  lodbc_stmt *stmt = lodbc_getstmt(L);\
  par_data     *par = NULL;\
  SQLUSMALLINT    i = luaL_checkinteger(L,2);\
  if(i <= 0) return lodbc_faildirect(L, "invalid param index");\
  if((stmt->numpars>0) && (!stmt->par)){\
    int ret = create_parinfo(L, stmt);\
    if(ret){\
      stmt_clear_info_(L, stmt);\
      return ret;\
    }\
  }\
  if(stmt->numpars >= 0){\
    assert(stmt->flags & LODBC_FLAG_PREPARED);\
    if(i > stmt->numpars) return lodbc_faildirect(L, "invalid param index");\
    par = par_data_nth(stmt->par, i);\
  }\
  else{\
    int ret = par_data_ensure_nth(&stmt->par, L, i, &par);\
    if(ret){\
      if(!par) return LODBC_ALLOCATE_ERROR(L);\
      return ret;\
    }\
  }\
  assert(par);\
  luaL_unref (L, LODBC_LUA_REGISTRY, par->get_cb);\
  par->get_cb = LUA_NOREF;


static int stmt_bind_ind(lua_State *L, SQLINTEGER ind){
  SQLRETURN ret;
  CHECK_BIND_PARAM();

  par->ind = ind;
  ret = SQLBindParameter(stmt->handle, i, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, par->value.strval.buf, 0, &par->ind);
  if (lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return lodbc_pass(L);
}

static int stmt_bind_null(lua_State *L){
  return stmt_bind_ind(L,SQL_NULL_DATA);
}

static int stmt_bind_default(lua_State *L){
  return stmt_bind_ind(L,SQL_DEFAULT_PARAM);
}

static int stmt_bind(lua_State *L){
  if( lodbc_isnull(L, 2) ) return stmt_bind_null(L);
  else{
    const char* type;
    CHECK_BIND_PARAM();
    type = lodbc_sqltypetolua(par->sqltype);

    /* deal with data according to type */
    switch (type[1]) {
      /* nUmber iNteger*/
      case 'u': case 'n': return stmt_bind_number_(L,stmt,i,par);
      /* bOol */
      case 'o': return stmt_bind_bool_(L,stmt,i,par);
      /* sTring */ 
      case 't': return stmt_bind_string_(L,stmt,i,par);
      /* bInary */
      case 'i': return stmt_bind_binary_(L,stmt,i,par);
    }
    return lodbc_faildirect(L,"unknown param type.");
  }
}

static int stmt_bind_number(lua_State *L){
  CHECK_BIND_PARAM();
  return stmt_bind_number_(L,stmt,i,par);
}

static int stmt_bind_integer(lua_State *L){
  CHECK_BIND_PARAM();
  return stmt_bind_integer_(L,stmt,i,par);
}

static int stmt_bind_bool(lua_State *L){
  CHECK_BIND_PARAM();
  return stmt_bind_bool_(L,stmt,i,par);
}

static int stmt_bind_string(lua_State *L){
  CHECK_BIND_PARAM();
  return stmt_bind_string_(L,stmt,i,par);
}

static int stmt_bind_binary(lua_State *L){
  CHECK_BIND_PARAM();
  return stmt_bind_binary_(L,stmt,i,par);
}

#undef CHECK_BIND_PARAM
//}

//{ putparam for CallBack

static int stmt_putparam_binary_(lua_State *L, lodbc_stmt *stmt, par_data *par){
  SQLRETURN ret;

  size_t lbytes = par->parsize;
  while((par->parsize==0)||(lbytes > 0)){
    int top = lua_gettop(L);
    const char *data = NULL;
    size_t data_len = 0;

    if(par->parsize){
      lua_pushnumber(L,lbytes);
      ret = par_call_cb(par,L,1); 
    }
    else{
      ret = par_call_cb(par,L,0);
    }

    if(ret)
      return ret;

    data = luaL_checklstring(L,-1,&data_len);
    if(!data_len){
      lua_settop(L,top);
      break;
    }
    if(par->parsize){
      if(lbytes < data_len)
        data_len = lbytes;
      lbytes -= data_len;
      assert(lbytes < par->parsize);// assert overflow
    }

    ret = SQLPutData(stmt->handle, (SQLPOINTER*)data, data_len);// is it safe const cast?
    if(lodbc_iserror(ret))
      return lodbc_fail(L, hSTMT, stmt->handle);
    lua_settop(L,top);
  }
  return 0;
}

static int stmt_putparam_number_(lua_State *L, lodbc_stmt *stmt, par_data *par){
  SQLRETURN ret;
  int top = lua_gettop(L);
  if((ret = par_call_cb(par,L,0)))
    return ret;
  #ifdef LODBC_USE_INTEGER
  if(LT_INTEGER == lodbc_sqltypetolua(par->sqltype)){
    par->value.intval = luaL_checkinteger(L,-1);
    ret = SQLPutData(stmt->handle, &par->value.intval, sizeof(par->value.intval));
  }
  else
  #endif
  {
    par->value.numval = luaL_checknumber(L,-1);
    ret = SQLPutData(stmt->handle, &par->value.numval, sizeof(par->value.numval));
  }

  lua_settop(L,top);
  if(lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return 0;
}

static int stmt_putparam_bool_(lua_State *L, lodbc_stmt *stmt, par_data *par){
  SQLRETURN ret;
  int top = lua_gettop(L);
  if((ret = par_call_cb(par,L,0)))
    return ret;
  par->value.boolval = lua_isboolean(L,-1) ? lua_toboolean(L,-1) : luaL_checkinteger(L,-1);
  lua_settop(L,top);
  ret = SQLPutData(stmt->handle, &par->value.boolval, sizeof(par->value.boolval));
  if(lodbc_iserror(ret))
    return lodbc_fail(L, hSTMT, stmt->handle);
  return 0;
}

static int stmt_putparam(lua_State *L, lodbc_stmt *stmt, par_data *par){
  const char *type = lodbc_sqltypetolua(par->sqltype);
  /* deal with data according to type */
  switch (type[1]) {
    /* nUmber, iNteger */
    case 'u': case 'n': return stmt_putparam_number_(L,stmt,par);
    /* bOol */
    case 'o': return stmt_putparam_bool_(L,stmt,par);
    /* sTring */ 
    case 't': 
    /* bInary */
    case 'i': return stmt_putparam_binary_(L,stmt,par);
  }
  return 0;
}

//}

//{ fetch

static int stmt_vfetch (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  SQLHSTMT hstmt = stmt->handle;
  SQLRETURN rc;

  if(!(stmt->flags & LODBC_FLAG_OPENED)){
    return luaL_error (L, LODBC_PREFIX"there are no open cursor");
  }

  rc = SQLFetch(hstmt);
  if (rc == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) {
    stmt->aflags = LODBC_ASTATE_NONE;
    if(stmt->autoclose){
      stmt_close(L);
    }
    lua_pushboolean(L,0);
    return 1;
  } else if(rc == SQL_STILL_EXECUTING){
    lua_pushliteral(L, "timeout");
    stmt->aflags = LODBC_ASTATE_FETCH;
    return 1 + lodbc_push_diagnostics(L, hSTMT, hstmt);
  }else if(lodbc_iserror(rc)) return lodbc_fail(L, hSTMT, hstmt);

  lua_pushboolean(L,1);
  return 1;
}

static int stmt_fetch (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  SQLHSTMT hstmt = stmt->handle;
  int ret, i, alpha_mode = -1,digit_mode = -1; 
  SQLRETURN rc;

  if(!(stmt->flags & LODBC_FLAG_OPENED)){
    return luaL_error (L, LODBC_PREFIX"there are no open cursor");
  }

  rc = SQLFetch(hstmt);
  if (rc == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) {
    if(stmt->autoclose){
      stmt_close(L);
    }
    return 0;
  } else if (lodbc_iserror(rc)) return lodbc_fail(L, hSTMT, hstmt);

  if(stmt->coltypes == LUA_NOREF){
    create_colinfo(L, stmt);
  }

  if (lua_istable (L, 2)) {// stack: cur, row, fetch_mode?
    alpha_mode = 0;
    if(lua_gettop(L) == 3){
      if(!lua_isnil(L,3)){
        const char *opt = luaL_checkstring(L, 3);
        alpha_mode = strchr(opt,'a')?1:0;
        digit_mode = strchr(opt,'n')?1:0;
      }
      lua_remove(L,3);
    }
    if (lua_gettop(L) > 2){
      luaL_error(L, LODBC_PREFIX"too many arguments");
    }
  }
  else if (lua_gettop(L) > 1){ // stack: cur
    luaL_error(L, LODBC_PREFIX"too many arguments");
  }

  lua_rawgeti (L, LODBC_LUA_REGISTRY, stmt->coltypes); // stack: cur, row?, coltypes

  if(alpha_mode >= 0){ // stack: cur, row, coltypes
    if(alpha_mode){
      lua_rawgeti (L, LODBC_LUA_REGISTRY, stmt->colnames); // stack: cur, row, coltypes, colnames
      lua_insert  (L, -2);                               // stack: cur, row, colnames, coltypes
    }

    for (i = 1; i <= stmt->numcols; i++) { // fill the row
      // stack: cur, row, colnames?, coltypes
      if((ret = lodbc_push_column (L, -1, hstmt, i)))
        return ret;
      if (alpha_mode) {// stack: cur, row, colnames, coltypes, value
        if (digit_mode){
          lua_pushvalue(L,-1);    // stack: cur, row, colnames, coltypes, value, value
          lua_rawseti (L, -5, i); // stack: cur, row, colnames, coltypes, value
        }
        lua_rawgeti(L, -3, i);  // stack: cur, row, colnames, coltypes, value, colname
        lua_insert(L, -2);      // stack: cur, row, colnames, coltypes, colname, value
        lua_rawset(L, -5);      /* table[name] = value */
        // stack: cur, row, colnames
      }
      else{            // stack: cur, row, coltypes, value
        lua_rawseti (L, -3, i);
      }
      // stack: cur, row, colnames?, coltypes
    }
    lua_pushvalue(L,2);
    return 1;
  }

  // stack: cur, coltypes
  luaL_checkstack (L, stmt->numcols, LODBC_PREFIX"too many columns");
  for (i = 1; i <= stmt->numcols; i++) { 
    // stack: cur, coltypes, ...
    if((ret = lodbc_push_column (L, 2, hstmt, i)))
      return ret;
  }
  return stmt->numcols;
}

static int stmt_foreach_(lua_State *L, lodbc_stmt *stmt, lua_CFunction close_fn){
#define FOREACH_RETURN(N) {\
  if(autoclose){\
    int top = lua_gettop(L);\
    close_fn(L);\
    lua_settop(L, top);\
  }\
  return (N);}


  signed char alpha_mode = -1,digit_mode = -1; 
  unsigned char autoclose = 1;
  int ct_idx, top = lua_gettop(L);

  if(4 == top){ // self, fetch_mode, autoclose, fn
    if(!lua_isnil(L, 2)){ // fetch_mode
      const char *opt = luaL_checkstring(L,2);
      alpha_mode = strchr(opt,'a')?1:0;
      digit_mode = strchr(opt,'n')?1:0;
      if (!(alpha_mode || digit_mode)) digit_mode = 1;
    }

    if(!lua_isnil(L, 3)){ // autoclose
      autoclose = lua_toboolean(L,3)?1:0;
    }
    lua_remove(L,2); // fetch_mode
    lua_remove(L,2); // autoclose
  }
  else if(3 == top){ // self, fetch_mode/autoclose, fn
    if(!lua_isnil(L, 2)){
      if(lua_type(L,2) == LUA_TSTRING){
        const char *opt = lua_tostring(L,2);
        alpha_mode = strchr(opt,'a')?1:0;
        digit_mode = strchr(opt,'n')?1:0;
        if (!(alpha_mode || digit_mode)) digit_mode = 1;
      }
      else{
        autoclose = lua_toboolean(L,2)?1:0;
      }
    }
    lua_remove(L,2); // fetch_mode/autoclose
  }
  else if (2 != top ){
    if (1 == top) return luaL_error(L, LODBC_PREFIX"no function was provided");
    return luaL_error(L, LODBC_PREFIX"too many arguments");
  }

  assert(lua_gettop(L) == 2);                          // stack: cur, func
  luaL_argcheck(L, lodbc_iscallable(L,-1), top, "must be callable");

  if(stmt->coltypes == LUA_NOREF){
    create_colinfo(L, stmt);
  }

  if(alpha_mode >= 0){                                     // stack: cur, func
    if(alpha_mode){
      lua_rawgeti (L, LODBC_LUA_REGISTRY, stmt->colnames); // stack: cur, func, colnames
      lua_insert(L,-2);                                    // stack: cur, colnames, func
    }
    lua_newtable(L);                                       // stack: cur, colnames?, func, row
    lua_rawgeti (L, LODBC_LUA_REGISTRY, stmt->coltypes);   // stack: cur, colnames?, func, row, coltypes
    lua_insert  (L, -3);                                   // stack: cur, colnames?, coltypes, func, row

    top = lua_gettop(L);
    ct_idx = lua_absindex(L, -3);
    while(1){
      SQLRETURN rc = SQLFetch(stmt->handle);
      int ret,i;

      assert(top == lua_gettop(L));

      if (rc == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)){
        FOREACH_RETURN(0);
      } else if (lodbc_iserror(rc)){
        FOREACH_RETURN(lodbc_fail(L, hSTMT, stmt->handle));
      }

      for (i = 1; i <= stmt->numcols; i++) { // fill the row
                                                    // stack: cur, colnames?, coltypes, func, row
        if((ret = lodbc_push_column (L, ct_idx, stmt->handle, i)))
          FOREACH_RETURN(ret);
        if (alpha_mode) {                                  // stack: cur, colnames, coltypes, func, row, value
          if (digit_mode){
            lua_pushvalue(L,-1);                           // stack: cur, colnames, coltypes, func, row, value, value
            lua_rawseti (L, -3, i);                        // stack: cur, colnames, coltypes, func, row, value
          }
          lua_rawgeti(L, -5, i);                           // stack: cur, colnames, coltypes, func, row, value, colname
          lua_insert(L, -2);                               // stack: cur, colnames, coltypes, func, row, colname, value
          lua_rawset(L, -3);                               /* table[name] = value */
        }
        else{                                              // stack: cur, colnames, coltypes, func, row, value
          lua_rawseti (L, -2, i);
        }
      }

      assert(lodbc_iscallable(L,-2));                      // stack: cur, colnames?, coltypes, func, row
      lua_pushvalue(L, -2);                                // stack: cur, colnames?, coltypes, func, row, func
      lua_pushvalue(L, -2);                                // stack: cur, colnames?, coltypes, func, row, func, row
      if(autoclose) ret = lua_pcall(L,1,LUA_MULTRET,0);
      else {ret = 0;lua_call(L,1,LUA_MULTRET);}

                                                           // stack: cur, colnames?, coltypes, func, row, ...
      assert(lua_gettop(L) >= top);
      if(ret){ // error
        int top = lua_gettop(L);
        assert(autoclose);
        close_fn(L);
        lua_settop(L, top);
        return lua_error(L);
      }
      else if(lua_gettop(L) > top){ // func return value
        ret = lua_gettop(L) - top;
        FOREACH_RETURN(ret);
      }
    }
  }
  assert((alpha_mode == digit_mode) && (alpha_mode == -1));
                                                       // stack: cur, func
  lua_rawgeti (L, LODBC_LUA_REGISTRY, stmt->coltypes); // stack: cur, func, coltypes
  luaL_checkstack (L, stmt->numcols, LODBC_PREFIX"too many columns");
  top = lua_gettop(L);
  ct_idx = lua_absindex(L, -1);
  while(1){
    SQLRETURN rc = SQLFetch(stmt->handle);
    int ret,i;

    assert(top == lua_gettop(L));

    if (rc == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)){
      FOREACH_RETURN(0);
    } else if (lodbc_iserror(rc)){
      FOREACH_RETURN(lodbc_fail(L, hSTMT, stmt->handle));
    }

    lua_pushvalue(L, -2);                             // stack: cur, ..., func, coltypes, func
    assert(lodbc_iscallable(L,-1));

    for (i = 1; i <= stmt->numcols; i++){
      if((ret = lodbc_push_column (L, ct_idx, stmt->handle, i)))
        FOREACH_RETURN(ret);
    }
                                                      // stack: cur, ..., func, coltypes, func, vals[numcols]
    if(autoclose) ret = lua_pcall(L,stmt->numcols,LUA_MULTRET,0);
    else {ret = 0;lua_call(L,stmt->numcols,LUA_MULTRET);}

                                                      // stack: cur, ..., func, coltypes, res[?]
    assert(lua_gettop(L) >= top);
    if(ret){ // error
      int top = lua_gettop(L);
      assert(autoclose);
      close_fn(L);
      lua_settop(L, top);
      return lua_error(L);
    }
    else if(lua_gettop(L) > top){
      FOREACH_RETURN(lua_gettop(L) - top);
    }
  }


  assert(0); // we never get here
  return 0;
#undef FOREACH_RETURN
}

static int stmt_foreach(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  return stmt_foreach_(L, stmt, stmt_close);
}

//}

static int stmt_execute(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  int top = lua_gettop(L);
  SQLRETURN ret;
  SQLRETURN exec_ret;

  if(stmt->flags & LODBC_FLAG_OPENED){
    return luaL_error (L, LODBC_PREFIX"there are open cursor");
  }

  stmt->last_exec_ret = 0;
  if((!stmt->aflags) || (stmt->aflags == LODBC_ASTATE_EXECUTE)){
    stmt->aflags = LODBC_ASTATE_NONE;
    if(stmt->flags & LODBC_FLAG_PREPARED){
      ret = SQLExecute (stmt->handle);
      if(stmt->resultsetno != 0){// cols are not valid
        luaL_unref (L, LODBC_LUA_REGISTRY, stmt->colnames);
        luaL_unref (L, LODBC_LUA_REGISTRY, stmt->coltypes);
        stmt->colnames    = LUA_NOREF;
        stmt->coltypes    = LUA_NOREF;
        stmt->numcols     = 0;
        stmt->resultsetno = 0;
      }
    }
    else{
      const char *statement = luaL_checkstring(L, 2);
      ret = SQLExecDirect (stmt->handle, (SQLCHAR *) statement, SQL_NTS);
      luaL_unref (L, LODBC_LUA_REGISTRY, stmt->colnames);
      luaL_unref (L, LODBC_LUA_REGISTRY, stmt->coltypes);
      stmt->colnames    = LUA_NOREF;
      stmt->coltypes    = LUA_NOREF;
      stmt->numcols     = 0;
      stmt->resultsetno = 0;
    }
    if(ret == SQL_STILL_EXECUTING){
      stmt->aflags = LODBC_ASTATE_EXECUTE;
      lua_pushliteral(L,"timeout");
      return 1 + lodbc_push_diagnostics(L, hSTMT, stmt->handle);
    }

    if(
      (lodbc_iserror(ret))&&
      (ret != SQL_NEED_DATA)&&
      (ret != LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND))
      ){
        return lodbc_fail(L, hSTMT, stmt->handle);
    }
  }
  else ret = SQL_SUCCESS;


  if((ret == SQL_NEED_DATA)||(stmt->aflags == LODBC_ASTATE_PARAMDATA)){
    stmt->aflags = LODBC_ASTATE_NONE;
    while(1){
      par_data *par;
      ret = SQLParamData(stmt->handle, (void**)&par); // if done then this call execute statement
      if(ret == SQL_NEED_DATA){
        if((ret = stmt_putparam(L, stmt, par)))
          return ret;
      }
      else{
         break;
      }
    }
    if(ret == SQL_STILL_EXECUTING){
      stmt->aflags = LODBC_ASTATE_PARAMDATA;
      lua_pushliteral(L,"timeout");
      return 1 + lodbc_push_diagnostics(L, hSTMT, stmt->handle);
    }
    if(
      (lodbc_iserror(ret))&&
      (ret != LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND))
    ){
      return lodbc_fail(L, hSTMT, stmt->handle);
    }
  }

  assert(lua_gettop(L) == top);
  stmt->last_exec_ret = exec_ret = ret;

  if(!stmt->numcols){
    SQLSMALLINT numcols;
    if(!(stmt->flags & LODBC_FLAG_PREPARED)){// remove SQL text for SQLExecDirect
      assert(lua_isstring(L,-1));
      lua_pop(L, 1); 
    }

    ret = SQLNumResultCols (stmt->handle, &numcols);
    if (lodbc_iserror(ret))
        return lodbc_fail(L, hSTMT, stmt->handle);
    stmt->numcols = numcols;
  }

  lua_settop(L,1);
  if(stmt->numcols > 0){
    stmt->flags |= LODBC_FLAG_OPENED;
  }
  else{
    // For ODBC3, if the last call to SQLExecute or SQLExecDirect
    // returned SQL_NO_DATA, a call to SQLRowCount can cause a
    // function sequence error. Therefore, if the last result is
    // SQL_NO_DATA, we simply return 0

    SQLLEN numrows = 0;
    if(exec_ret != LODBC_ODBC3_C(SQL_NO_DATA, SQL_NO_DATA_FOUND)){
      /* if action has no results (e.g., UPDATE) */
      ret = SQLRowCount(stmt->handle, &numrows);
      if(lodbc_iserror(ret)){
        return lodbc_fail(L, hSTMT, stmt->handle);
      }
    }
    lua_pushnumber(L, numrows);
  }

  return 1; // rowsaffected or self
}

static int stmt_cancel(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  SQLRETURN ret = SQLCancel(stmt->handle);
  stmt->aflags = LODBC_ASTATE_NONE;
  if(lodbc_iserror(ret)){
    return lodbc_fail(L, hSTMT, stmt->handle);
  }
  return lodbc_pass(L);
}

static int stmt_rowcount(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  SQLLEN numrows = 0;
  // For ODBC3, if the last call to SQLExecute or SQLExecDirect
  // returned SQL_NO_DATA, a call to SQLRowCount can cause a
  // function sequence error. Therefore, if the last result is
  // SQL_NO_DATA, we simply return 0

  if(stmt->last_exec_ret != LODBC_ODBC3_C(SQL_NO_DATA, SQL_NO_DATA_FOUND)){
    SQLRETURN ret = SQLRowCount(stmt->handle, &numrows);
    if(lodbc_iserror(ret)){
      return lodbc_fail(L, hSTMT, stmt->handle);
    }
  }
  lua_pushnumber(L, numrows);
  return 1;
}

static int stmt_reset_colinfo (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  luaL_unref (L, LODBC_LUA_REGISTRY, stmt->colnames);
  luaL_unref (L, LODBC_LUA_REGISTRY, stmt->coltypes);
  stmt->colnames  = LUA_NOREF;
  stmt->coltypes  = LUA_NOREF;
  stmt->numcols   = 0;
  return lodbc_pass(L);
}

static int stmt_moreresults(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  SQLRETURN ret;
  SQLSMALLINT numcols;

  if(!(stmt->flags & LODBC_FLAG_OPENED))
    return luaL_error (L, LODBC_PREFIX"there are no open cursor");

  ret = SQLMoreResults(stmt->handle);
  stmt->aflags = LODBC_ASTATE_NONE;
  if(ret == LODBC_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)){
    lua_pushboolean(L, 0);
    return 1;
  }
  if(ret == SQL_STILL_EXECUTING){
    stmt->aflags = LODBC_ASTATE_NEXTRS;
    lua_pushliteral(L, "timeout");
    return 1 + lodbc_push_diagnostics(L, hSTMT, stmt->handle);
  }
  if(lodbc_iserror(ret)) return lodbc_fail(L, hSTMT, stmt->handle);

  luaL_unref (L, LODBC_LUA_REGISTRY, stmt->colnames);
  luaL_unref (L, LODBC_LUA_REGISTRY, stmt->coltypes);
  stmt->colnames  = LUA_NOREF;
  stmt->coltypes  = LUA_NOREF;
  stmt->numcols   = 0;
  ret = SQLNumResultCols (stmt->handle, &numcols);
  if(lodbc_iserror(ret)) return lodbc_fail(L, hSTMT, stmt->handle);
  stmt->numcols = numcols;
  stmt->resultsetno++;
  if(numcols) create_colinfo(L, stmt);

  return 1;
}

/*
** Unbind all params and columns from statement
** after call statement steel alive
*/
static int stmt_reset (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  stmt_clear_info_(L, stmt);
  return lodbc_pass(L);
}

/*
** Prepare statement
*/
static int stmt_prepare (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  const char *statement = luaL_checkstring(L, 2);
  int ret = stmt_prepare_(L, stmt, statement);
  if(ret) return ret;
  return lodbc_pass(L);
}

/*
** is statement prepare 
*/
static int stmt_prepared (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  lua_pushboolean(L, (stmt->flags & LODBC_FLAG_PREPARED));
  return 1;
}

/*
** 
*/
static int stmt_parcount (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  lua_pushnumber(L, stmt->numpars);
  return 1;
}

//{ Params

static int stmt_get_uint_attr_(lua_State*L, lodbc_stmt *stmt, SQLINTEGER optnum){
  return lodbc_get_uint_attr_(L, hSTMT, stmt->handle, optnum);
}

static int stmt_get_str_attr_(lua_State*L, lodbc_stmt *stmt, SQLINTEGER optnum){
  return lodbc_get_str_attr_(L, hSTMT, stmt->handle, optnum);
}

static int stmt_set_uint_attr_(lua_State*L, lodbc_stmt *stmt, SQLINTEGER optnum, SQLINTEGER value){
  return lodbc_set_uint_attr_(L, hSTMT, stmt->handle, optnum, value);
}

static int stmt_set_str_attr_(lua_State*L, lodbc_stmt *stmt, SQLINTEGER optnum, const char* value, size_t len){
  return lodbc_set_str_attr_(L, hSTMT, stmt->handle, optnum, value, len);
}


// libodbc++
#define DEFINE_GET_UINT_ATTR(NAME, WHAT) \
static int stmt_get_##NAME(lua_State*L){\
  lodbc_stmt *stmt = lodbc_getstmt(L);\
  return stmt_get_uint_attr_(L, stmt, (WHAT));\
}

#define DEFINE_SET_UINT_ATTR(NAME,WHAT) \
static int stmt_set_##NAME(lua_State*L){\
  lodbc_stmt *stmt = lodbc_getstmt(L);\
  return stmt_set_uint_attr_(L, stmt, (WHAT), luaL_checkinteger(L,2));\
}

DEFINE_GET_UINT_ATTR(querytimeout, LODBC_ODBC3_C(SQL_ATTR_QUERY_TIMEOUT,SQL_QUERY_TIMEOUT));
DEFINE_SET_UINT_ATTR(querytimeout, LODBC_ODBC3_C(SQL_ATTR_QUERY_TIMEOUT,SQL_QUERY_TIMEOUT));
DEFINE_GET_UINT_ATTR(maxrows,      LODBC_ODBC3_C(SQL_ATTR_MAX_ROWS,SQL_MAX_ROWS));
DEFINE_SET_UINT_ATTR(maxrows,      LODBC_ODBC3_C(SQL_ATTR_MAX_ROWS,SQL_MAX_ROWS));
DEFINE_GET_UINT_ATTR(maxfieldsize, LODBC_ODBC3_C(SQL_ATTR_MAX_LENGTH,SQL_MAX_LENGTH));
DEFINE_SET_UINT_ATTR(maxfieldsize, LODBC_ODBC3_C(SQL_ATTR_MAX_LENGTH,SQL_MAX_LENGTH));


static int stmt_set_escapeprocessing(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  int on = lua_toboolean(L,2);
  return stmt_set_uint_attr_(L, stmt,
    (LODBC_ODBC3_C(SQL_ATTR_NOSCAN,SQL_NOSCAN)),
    on?SQL_NOSCAN_OFF:SQL_NOSCAN_ON
  );
}

static int stmt_get_escapeprocessing(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  int ret = stmt_get_uint_attr_(L, stmt, (LODBC_ODBC3_C(SQL_ATTR_NOSCAN,SQL_NOSCAN)));
  if(ret != 1) return ret;
  lua_pushboolean(L, lua_tonumber(L,-1) == SQL_NOSCAN_OFF);
  return 1;
}

#undef DEFINE_GET_UINT_ATTR
#undef DEFINE_SET_UINT_ATTR

static int stmt_get_autoclose(lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  lua_pushboolean(L, stmt->autoclose?1:0);
  return 1;
}

static int stmt_set_autoclose(lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  stmt->autoclose = lua_toboolean(L,2)?1:0;
  return 1;
}

static int stmt_set_destroyonclose(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  if(lua_toboolean(L, 2)) stmt->flags |= LODBC_FLAG_DESTROYONCLOSE;
  else  stmt->flags &= ~LODBC_FLAG_DESTROYONCLOSE;
  return lodbc_pass(L);
}

static int stmt_get_destroyonclose (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  lua_pushboolean(L, (stmt->flags & LODBC_FLAG_DESTROYONCLOSE));
  return 1;
}

//}

/*
** Returns the table with column names.
*/
static int stmt_colnames (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  if((stmt->numcols > 0) && (stmt->coltypes == LUA_NOREF)){
    create_colinfo(L, stmt);
  }
  lua_rawgeti (L, LODBC_LUA_REGISTRY, stmt->colnames);
  return 1;
}

/*
** Returns the table with column types.
*/
static int stmt_coltypes (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt(L);
  if((stmt->numcols > 0) && (stmt->coltypes == LUA_NOREF)){
    create_colinfo(L, stmt);
  }
  lua_rawgeti (L, LODBC_LUA_REGISTRY, stmt->coltypes);
  return 1;
}

//{ get/set attr

static int stmt_get_uint_attr(lua_State*L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return stmt_get_uint_attr_(L, stmt, optnum);
}

static int stmt_get_str_attr(lua_State*L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return stmt_get_str_attr_(L, stmt, optnum);
}

static int stmt_set_uint_attr(lua_State*L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return stmt_set_uint_attr_(L, stmt, optnum,luaL_checkinteger(L,3));
}

static int stmt_set_str_attr(lua_State*L){
  lodbc_stmt *stmt = lodbc_getstmt(L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  size_t len;
  const char *str = luaL_checklstring(L,3,&len);
  return stmt_set_str_attr_(L, stmt, optnum, str, len);
}

//}

//{ some attributes

static int stmt_set_asyncmode (lua_State *L) {
  lodbc_stmt *stmt = lodbc_getstmt (L);
  return stmt_set_uint_attr_(L,stmt,SQL_ATTR_ASYNC_ENABLE,
    lua_toboolean (L, 2)?SQL_ASYNC_ENABLE_ON:SQL_ASYNC_ENABLE_OFF
  );
}

static int stmt_get_asyncmode(lua_State *L){
  lodbc_stmt *stmt = lodbc_getstmt (L);
  int ret = stmt_get_uint_attr_(L,stmt,SQL_ATTR_ASYNC_ENABLE);
  if(lodbc_is_fail(L,ret)) return ret;
  if(0 == ret){
      lua_pushboolean(L, SQL_ASYNC_ENABLE_ON == SQL_ASYNC_ENABLE_DEFAULT);
      return 1;
  }
  assert(1 == ret);
  lua_pushboolean(L, SQL_ASYNC_ENABLE_ON == lua_tointeger(L,-1));
  lua_remove(L,-2);
  return 1;
}

//}

static int stmt_tostring (lua_State *L) {
  char status[16];
  char self[65];

  lodbc_stmt * stmt= (lodbc_stmt *)lutil_checkudatap (L, 1, LODBC_STMT);
  luaL_argcheck (L, stmt != NULL, 1, LODBC_PREFIX "statement expected");

  if(stmt->flags & LODBC_FLAG_DESTROYED){
    strcpy (status, "[closed] ");
  }
  else{
    status[0] = '\0';
  }

  sprintf (self, "%p", (void *)stmt);

  lua_pushfstring (L, "%s %s(%s)", LODBC_STMT, status, self);
  return 1;
}

static const struct luaL_Reg lodbc_stmt_methods[] = {
  {"__tostring", stmt_tostring},

  {"__gc",      stmt_destroy},
  {"destroy",   stmt_destroy},
  {"destroyed", stmt_destroyed},
  {"close",     stmt_close},
  {"closed",    stmt_closed},
  {"reset",     stmt_reset},
  {"resetcolinfo", stmt_reset_colinfo},
  {"connection", stmt_connection},

  {"getuservalue",  stmt_getuservalue},
  {"setuservalue",  stmt_setuservalue},

  {"rowcount",  stmt_rowcount},
  {"vfetch",    stmt_vfetch},
  {"fetch",     stmt_fetch},
  {"execute",   stmt_execute},
  {"prepare",   stmt_prepare},
  {"prepared",  stmt_prepared},
  {"cancel",    stmt_cancel},

  {"bind",        stmt_bind},
  {"bindnum",     stmt_bind_number},
  {"bindint",     stmt_bind_integer},
  {"bindstr",     stmt_bind_string},
  {"bindbin",     stmt_bind_binary},
  {"bindbool",    stmt_bind_bool},
  {"bindnull",    stmt_bind_null},
  {"binddefault", stmt_bind_default},

  {"setasyncmode",stmt_set_asyncmode},
  {"getasyncmode",stmt_get_asyncmode},

  {"parcount",      stmt_parcount},
  {"nextresultset", stmt_moreresults},
  {"foreach",       stmt_foreach},

  {"coltypes", stmt_coltypes},
  {"colnames", stmt_colnames},


  {"getuintattr", stmt_get_uint_attr},
  {"getstrattr",  stmt_get_str_attr},
  {"setuintattr", stmt_set_uint_attr},
  {"setstrattr",  stmt_set_str_attr},

  {"getquerytimeout",     stmt_get_querytimeout},
  {"setquerytimeout",     stmt_set_querytimeout},
  {"getmaxrows"     ,     stmt_get_maxrows},
  {"setmaxrows"     ,     stmt_set_maxrows},
  {"getmaxfieldsize",     stmt_get_maxfieldsize},
  {"setmaxfieldsize",     stmt_set_maxfieldsize},
  {"getescapeprocessing", stmt_get_escapeprocessing},
  {"setescapeprocessing", stmt_set_escapeprocessing},
  {"getautoclose",        stmt_get_autoclose},
  {"setautoclose",        stmt_set_autoclose},
  {"getdestroyonclose",   stmt_get_destroyonclose},
  {"setdestroyonclose",   stmt_set_destroyonclose},

  {NULL, NULL},
};

void lodbc_stmt_initlib (lua_State *L, int nup){
  lutil_createmetap(L, LODBC_STMT, lodbc_stmt_methods, nup);
  lua_pushstring(L,LODBC_STMT);
  lua_setfield(L,-2,"__metatable");
  lua_pop(L, 1);
}


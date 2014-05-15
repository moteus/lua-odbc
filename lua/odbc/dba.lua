---
-- implement lua-dba interface
--

-- local odbc = require "odbc.core"
local odbc  = require "odbc.proxy"
local utils = require "odbc.dba.utils"

local unpack       = utils.unpack;
local pack_n       = utils.pack_n;
local unpack_n     = utils.unpack_n;
local ifind        = utils.ifind;
local collect      = utils.collect;
local make_cahe    = utils.make_cahe;
local throw        = utils.throw;
local E            = utils.E;
local ERROR        = utils.ERROR;
local param_utils  = utils.param_utils;
local user_val     = utils.user_val;
local set_user_val = utils.set_user_val;

local Environment = odbc.getenvmeta()
local Connection  = odbc.getcnnmeta()
local Statement   = odbc.getstmtmeta()

local environment = odbc.environment
local function Environment_new(...)
  local env, err = environment(...)
  if not env then return nil, err end

  set_user_val(env, {})

  return env
end

local function Connection_new(env, ...)
  local cnn, err = env:connection_impl()
  if not cnn then return nil, err end
  cnn:setautoclosestmt(true)

  set_user_val(cnn, {
    params = pack_n(...);
  })

  return cnn
end

local init_connection = odbc.init_connection
local function Connection_wrap(hcnn, ...)
  local cnn, err = init_connection(hcnn)
  if not cnn then return nil, err end
  cnn:setautoclosestmt(true)

  set_user_val(cnn, {
    params = pack_n(...);
  })

  return cnn
end

local function Statement_new(cnn, sql)
  local stmt, err = cnn:statement_impl()
  if not stmt then return nil, err end

  set_user_val(stmt,{})

  if sql then 
    local ok, err = stmt:set_sql(sql)
    if not ok then
      stmt:destroy()
      return nil, err
    end
  end

  return stmt
end

local DBA_OPTIONS = {
  -- всегда заменять параметры подстановкой строки
  FORCE_REPLACE_PARAMS = false;

  -- не пытатся преобразовывать именованные параметры
  -- это необходимо для предотвращения изменения текста SQL перед выполнением
  -- при этом параметры будут поддерживатся только если проддерживается bind(будут использоваться только '?')
  IGNORE_NAMED_PARAMS = false;
};

local function bind_param(stmt, i, val, ...)
  if val == odbc.PARAM_NULL    then return stmt:bindnull(i)    end
  if val == odbc.PARAM_DEFAULT then return stmt:binddefault(i) end
  if type(val) == 'userdata' then
    return stmt:vbind_param(i, val, ...)
  end
  return stmt:bind_impl(i, val, ...)
end

local function rows(cur, fetch_mode)
  local res = {}
  if fetch_mode then
    return function ()
      local res, err = cur:fetch(res, fetch_mode)
      if res then return res end
      if err then throw(err) end
    end
  end

  local n = #cur:colnames()
  return function ()
    local res, err = cur:fetch(res, 'n')
    if res then return unpack(res, 1, n) end
    if err then throw(err) end
  end
end

-------------------------------------------------------------------------------
do -- odbc

odbc.PARAM_NULL    = odbc.NULL or {}

odbc.PARAM_DEFAULT = odbc.DEFAULT or {}

odbc.TRANSACTION   = {} -- set below

---
--
function odbc.Connect(...)
  local env, err = odbc.Environment()
  if not env then return nil, err end
  local cnn, err = env:connect(...)
  if not cnn then 
    env:destroy()
    return nil, err
  end
  return cnn, env
end

function odbc.Connection(...)
  local env, err = odbc.Environment()
  if not env then return nil, err end
  local cnn, err = env:connection(...)
  if not cnn then 
    env:destroy()
    return nil, err
  end
  return cnn, env
end

function odbc.Environment(...)
  return Environment_new(...)
end

odbc.connect = odbc.Connect

odbc.init_connection = Connection_wrap

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
if not Environment.connection_impl then

Environment.connection_impl = Environment.connection

function Environment:set_login_timeout(ms)
  assert((ms == nil) or (type(ms) == "number"))
  user_val(self).login_timeout = ms
end

function Environment:get_login_timeout()
  return user_val(self).login_timeout
end

function Environment:connection(...)
  return Connection_new(self, ...)
end

function Environment:connect(...)
  local cnn, err = self:connection(...)
  if not cnn then return nil, err end
  local ms = self:get_login_timeout()
  if ms then cnn:set_login_timeout(ms) end
  local ok, err = cnn:connect()
  if not ok then cnn:destroy() return nil, err end
  return cnn
end

function Environment:handle()
  return self
end

function Environment:get_config(name)
  local private_ = user_val(self)
  if private_.lib_opt then
    local val = private_.lib_opt[name]
    if val ~= nil then return val end
  end
  return DBA_OPTIONS[name]
end

function Environment:set_config(name, value)
  local private_ = user_val(self)
  if not private_.lib_opt then private_.lib_opt = {} end
  private_.lib_opt[name] = value
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
if not Connection.connect_impl then

Connection.connect_impl   = Connection.connect
Connection.statement_impl = Connection.statement

local function connect(obj, ...)
  local dsn, lgn, pwd, autocommit = ...
  local cnndrv_params
  if type(dsn) == 'table' then
    cnndrv_params, autocommit = ...
  else
    if type(lgn) == 'boolean' then
      assert(pwd == nil)
      assert(autocommit == nil)
      autocommit = lgn
      lgn = nil
    elseif type(pwd) == 'boolean' then
      assert(autocommit == nil)
      autocommit = pwd
      pwd = nil
    end
  end

  if autocommit == nil then autocommit = true end

  local cnn, err
  if cnndrv_params then cnn, err = obj:driverconnect(cnndrv_params)
  else cnn, err  = obj:connect_impl(dsn, lgn or "", pwd or "") end

  if not cnn then return nil, err end
  cnn:setautocommit(autocommit)

  return cnn, err
end

-- function Connection:handle()
--   return self
-- end

function Connection:connect(...)
  self:disconnect()
  local private_ = user_val(self)

  if select('#', ...) > 0 then 
    private_.params = pack_n(...)
  end

  local ok, err = connect(self, unpack_n(private_.params))
  if not ok then return nil, err end

  assert(self == ok)
  return self, err
end

function Connection:statement(sql)
  return Statement_new(self, sql)
end

function Connection:prepare(sql)
  local stmt, err = self:statement(sql)
  if not stmt then return nil, err end
  local ok, err = stmt:prepare()
  if not ok then 
    stmt:destroy()
    return nil, err
  end
  return stmt
end

function Connection:apply_params(sql, params)
  assert(type(sql)    == "string")
  assert(type(params) == "table" )
  return param_utils.apply_params(self, sql, params)
end

function Connection:execute(sql, params)
  assert(type(sql) == "string")
  assert((params == nil) or (type(params) == "table"))
  local stmt, err = self:statement_impl()
  if not stmt then return nil, err end
  if params then
    assert(type(params) == 'table')
    --[[if not self:supports_bind_param() or self:get_config"FORCE_REPLACE_PARAMS" then
      sql, err = self:apply_params(sql, params)
      if not sql then return nil, err end
    else--]]
    if type((next(params))) == 'string' then -- named parameters
      local parnames
      sql, parnames = param_utils.translate_params(sql, true)
      if not sql then
        stmt:destroy()
        return nil, parnames
      end
      if parnames then -- parameters found in sql
        for paramNo, paramName in ipairs(parnames) do
          local value = params[paramName]
          if value ~= nil then
            local ok, err = bind_param(stmt, paramNo, value)
            if not ok then
              stmt:destroy()
              return nil, err, paramNo
            end
          else
            stmt:destroy()
            return nil, ERROR.unknown_parameter .. paramName
          end
        end
      end
    else
      for i, v in ipairs(params) do
        local ok, err = bind_param(stmt, i, v)
        if not ok then
          stmt:destroy()
          return nil, err, i
        end
      end
    end
  end
  local ok, err = stmt:execute_impl(sql)
  if not ok then 
    stmt:destroy()
    return nil, err
  end
  if ok == stmt then
    return stmt
  end
  stmt:destroy()
  return ok
end

function Connection:exec(...)
  local res, err = self:execute(...)
  if not res then return nil, err end
  if type(res) ~= 'number' then 
    res:destroy()
    return nil, ERROR.ret_cursor
  end
  return res
end

function Connection:first_row(...)
  local stmt, err = self:execute(...)
  if not stmt then return nil, err end
  if type(stmt) == 'number' then return nil, ERROR.no_cursor end
  local args = pack_n(stmt:fetch())
  stmt:destroy()
  return unpack_n(args)
end

local function Connection_first_Xrow(self, fetch_mode, ...)
  local stmt, err = self:execute(...)
  if not stmt then return nil, err end
  if type(stmt) == 'number' then return nil, ERROR.no_cursor end
  local row, err = stmt:fetch({}, fetch_mode)
  stmt:destroy()
  if not row then return nil, err end
  return row
end

function Connection:first_irow(...) return Connection_first_Xrow(self, 'n', ...) end

function Connection:first_nrow(...) return Connection_first_Xrow(self, 'a', ...) end

function Connection:first_trow(...) return Connection_first_Xrow(self, 'an', ...) end

function Connection:first_value(...)
  local ok, err = self:first_irow(...)
  if not ok then return nil, err end
  return ok[1]
end

local function Connection_Xeach(self, fetch_mode, sql, ...)
  assert(type(sql) == 'string')
  local n, params = 2, ...
  if type(params) ~= 'table' then n, params = 1, nil end
  local callback = select(n, ...)

  local stmt, err = self:execute(sql, params)
  if not stmt then return nil, err end
  if type(stmt) == 'number' then return nil, ERROR.no_cursor end
  stmt:setdestroyonclose(true)
  return stmt:foreach(fetch_mode, true, callback)
end

function Connection:each(...)  return Connection_Xeach(self, nil,  ...) end

function Connection:ieach(...) return Connection_Xeach(self, 'n',  ...) end

function Connection:neach(...) return Connection_Xeach(self, 'a',  ...) end

function Connection:teach(...) return Connection_Xeach(self, 'an', ...) end

local function Connection_Xrows(self, fetch_mode, sql, params)
  local stmt, err = self:execute(sql, params)
  if not stmt then throw(err, 2) end
  if type(stmt) == 'number' then throw(ERROR.no_cursor, 2) end
  stmt:setdestroyonclose(true)

  return rows(stmt, fetch_mode)
end

function Connection:rows(...)  return Connection_Xrows(self, nil,  ...) end

function Connection:irows(...) return Connection_Xrows(self, 'n',  ...) end

function Connection:nrows(...) return Connection_Xrows(self, 'a',  ...) end

function Connection:trows(...) return Connection_Xrows(self, 'an', ...) end

function Connection:fetch_all(fetch_mode, sql, param)
  assert(type(fetch_mode) == 'string')
  assert(type(sql) == 'string')
  local result = {}
  local ok, err
  if param then
    ok, err = Connection_Xeach(self, fetch_mode, sql, param, collect(result))
  else
    ok, err = Connection_Xeach(self, fetch_mode, sql, collect(result))
  end
  if err == nil then return result end
  return nil, err
end

function Connection:set_config(name, value)
  local private_ = user_val(self)
  if not private_.lib_opt then private_.lib_opt = {} end
  private_.lib_opt[name] = value
end

function Connection:get_config(name )
  local private_ = user_val(self)
  if private_.lib_opt then
    local val = private_.lib_opt[name]
    if val ~= nil then return val end
  end
  return self:environment():get_config(name)
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
if not Statement.prepare_impl then

Statement.prepare_impl = Statement.prepare
Statement.bind_impl    = Statement.bind
Statement.execute_impl = Statement.execute

function Statement:vbind_col(i, val, ...)
  --- @todo better detect of using proxy module
  return val:bind_col(self._self or self, i, ...)
end

function Statement:vbind_param(i, val, ...)
  --- @todo better detect of using proxy module
  return val:bind_param(self._self or self, i, ...)
end

local fix_types = {
  'ubigint', 'sbigint', 'utinyint', 'stinyint', 'ushort', 'sshort', 
  'ulong', 'slong', 'float', 'double', 'date', 'time', 'bit'
}

local buf_types = {'char', 'binary', 'wchar'}

for _, tname in ipairs(fix_types) do
  Statement["vbind_col_" .. tname] = function(self, ...)
    return odbc[tname]():bind_col(self._self or self, ...)
  end

  Statement["vbind_param_" .. tname] = function(self, i, val, ...)
    return odbc[tname](val):bind_param(self._self or self, i, ...)
  end
end

for _, tname in ipairs(buf_types) do
  Statement["vbind_col_" .. tname] = function(self, i, size, ...)
    return odbc[tname](size):bind_col(self._self or self, i, ...)
  end

  Statement["vbind_param_" .. tname] = function(self, i, size, ...)
    if type(size) == 'string' then
      return odbc[tname](size):bind_param(self._self or self, i, ...)
    end
    assert(type(size) == 'number')
    local val = ...
    if type(val) == 'string' then
      return odbc[tname](size, val):bind_param(self._self or self, i, select(2, ...))
    end
    return odbc[tname](size):bind_param(self._self or self, i, ...)
  end
end

function Statement:prepare(sql)
  if sql then
    local ok, err = self:set_sql(sql)
    if not ok then return nil, err end
  else
    sql = user_val(self).sql
  end
  return self:prepare_impl(sql)
end

function Statement:bind(paramID, val, ...)
  local paramID_type = type(paramID)
  assert((paramID_type == 'string')or(paramID_type == 'number')or(paramID_type == 'table'))

  if self:opened() then return nil, ERROR.query_opened end

  if paramID_type == "number" then
    return bind_param(self, paramID, val)
  end

  local private_ = user_val(self)

  if paramID_type == "string" then
    local flag
    if private_.parnames then
      for paramNo, paramName in ipairs(private_.parnames) do
        if paramName == paramID then
          local ok, err = bind_param(self, paramNo, val, ...)
          if not ok then return nil, err, paramNo end
          flag = true
        end
      end
    end
    if not flag then return nil, ERROR.unknown_parameter .. paramID end
    return true
  end

  if paramID_type == "table" then
    if type(next(paramID)) == 'string' then
      if not private_.parnames then
        return nil, ERROR.unknown_parameter .. tostring(next(paramID))
      end
      for paramName, paramValue in pairs(paramID) do
        local flag
        for paramNo, paramName2 in ipairs(private_.parnames) do
          if paramName == paramName2 then
            local ok, err = bind_param(self, paramNo, paramValue, ...)
            if not ok then return nil, err, paramNo end
            flag = true
          end
        end
        if not flag then return nil, ERROR.unknown_parameter .. paramName end
      end
      return true
    end

    for i, v in pairs(paramID) do
      local ok, err = bind_param(self, i, v)
      if not ok then
        return nil, err, i
      end
    end
    return true
  end

  return self:bind_impl(paramID, val, ...)
end

function Statement:execute(sql, params)
  if not self:closed() then return nil, ERROR.query_opened end

  if not params and type(sql) == 'table' then
    params, sql = sql, nil
  end

  if sql ~= nil then
    local ok, err = self:set_sql(sql)
    if not ok then return nil, err end
  end
  sql = user_val(self).sql

  if not sql then return nil, ERROR.no_sql_text end

  if params ~= nil then
    assert(type(params) == 'table')
    local ok, err, i = self:bind(params)
    if not ok then return nil, err, i end
  end

  if self:prepared() then return self:execute_impl() end

  return self:execute_impl(sql)
end

function Statement:open(...)
  local ok, err = self:execute(...)
  if not ok then return nil, err end
  if ok ~= self then return nil, ERROR.no_cursor end
  return ok
end

function Statement:opened()
  return not self:closed()
end

function Statement:exec(...)
  if not self:closed() then return nil, ERROR.query_opened end

  local ok, err = self:execute(...)
  if not ok then return nil, err end
  if ok == self then
    self:close()
    return nil, "no resultset"
  end
  return ok
end

function Statement:first_row(...)
  local stmt, err = self:execute(...)
  if not stmt then return nil, err end
  if stmt ~= self then return nil, ERROR.no_cursor end
  local args = pack_n(stmt:fetch())
  stmt:close()
  return unpack_n(args)
end

local function Statement_first_Xrow(self, fetch_mode, ...)
  local stmt, err = self:execute(...)
  if not stmt then return nil, err end
  if stmt ~= self then return nil, ERROR.no_cursor end
  local row, err = stmt:fetch({}, fetch_mode)
  stmt:close()
  if not row then return nil, err end
  return row
end

function Statement:first_irow(...) return Statement_first_Xrow(self, 'n', ...) end

function Statement:first_nrow(...) return Statement_first_Xrow(self, 'a', ...) end

function Statement:first_trow(...) return Statement_first_Xrow(self, 'an', ...) end

function Statement:first_value(...)
  local ok, err = self:first_irow(...)
  if not ok then return nil, err end
  return ok[1]
end

-- fetch_mode, [sql,] [params,] [autoclose,] fn
local function Statement_Xeach(self, fetch_mode, ...)
  local n = 1
  local sql, params  = ...
  if type(sql) == 'string' then
    n = n + 1
    if type(params) == 'table' then 
      n = n + 1
    else 
      params = nil
    end
  elseif type(sql) == 'table' then
    params = nil
    n = n + 1
  else 
    sql, params = nil
  end

  if not self:closed() then 
    if sql or params then return nil, ERROR.query_opened end
    return self:foreach(fetch_mode, select(n, ...))
  end

  local ok, err = self:open(sql, params)
  if not ok then return nil, err end

  return self:foreach(fetch_mode, select(n, ...))
end

function Statement:each(...)  return Statement_Xeach(self, nil,  ...) end

function Statement:ieach(...) return Statement_Xeach(self, 'n',  ...) end

function Statement:neach(...) return Statement_Xeach(self, 'a',  ...) end

function Statement:teach(...) return Statement_Xeach(self, 'an', ...) end

local function Statement_Xrows(self, fetch_mode, sql, params)
  local stmt, err = self:open(sql, params)
  if not stmt then throw(err, 2) end

  return rows(self, fetch_mode)
end

function Statement:rows(...)  return Statement_Xrows(self, nil,  ...) end

function Statement:irows(...) return Statement_Xrows(self, 'n',  ...) end

function Statement:nrows(...) return Statement_Xrows(self, 'a',  ...) end

function Statement:trows(...) return Statement_Xrows(self, 'an', ...) end

function Statement:fetch_all(fetch_mode, ...)
  assert(type(fetch_mode) == 'string')
  local result = {}
  local args = {...};
  table.insert(args, collect(result))
  local ok, err = Statement_Xeach(self, fetch_mode, unpack(args))
  if err == nil then return result end
  return nil, err
end

function Statement:supports_prepare()
  return self:connection():supports_prepare()
end

function Statement:set_sql(sql)
  assert(type(sql) == "string")

  if self:prepared()   then return nil, ERROR.query_prepared end
  if not self:closed() then return nil, ERROR.query_opened   end

  self:reset()
  local private_ = user_val(self)
  private_.sql   = sql

  local sql, parnames = param_utils.translate_params(sql)
  if sql and parnames then
    private_.sql, private_.parnames = sql, parnames
  else 
    private_.parnames = nil
  end

  return true
end

function Statement:handle()
  return self
end

function Statement:set_config(name, value)
  local private_ = user_val(self)
  if not private_.lib_opt then private_.lib_opt = {} end
  private_.lib_opt[name] = value
end

function Statement:get_config(name )
  local private_ = user_val(self)
  if private_.lib_opt then
    local val = private_.lib_opt[name]
    if val ~= nil then return val end
  end
  return self:connection():get_config(name)
end

end
-------------------------------------------------------------------------------

local CONNECTION_RENAME = {
  set_autocommit       = "setautocommit";
  get_autocommit       = "getautocommit";
  query                = "statement";
  username             = "userName";
  set_catalog          = "setcatalog";
  get_catalog          = "getcatalog";
  set_readonly         = "setreadonly";
  get_readonly         = "getreadonly";
  set_trace            = "settrace";
  get_trace            = "gettrace";
  set_trace_file       = "settracefile";
  get_trace_file       = "gettracefile";
  set_login_timeout    = "setlogintimeout";
  get_login_timeout    = "getlogintimeout";
  supports_catalg_name = "isCatalogName";
  get_async_mode       = "getasyncmode";
  set_async_mode       = "setasyncmode";
  supports_async_statement  = "supportsAsyncStatement";
  supports_async_query      = "supportsAsyncStatement";
  supports_async_connection = "supportsAsyncConnection";
  supports_async_mode       = "supportsAsyncConnection"
}

local STATEMENT_RENAME = {
  set_autoclose       = "setautoclose";
  get_autoclose       = "getautoclose";
  get_async_mode      = "getasyncmode";
  set_async_mode      = "setasyncmode";
  unprepare           = "reset";
}

for new, old in pairs(CONNECTION_RENAME) do
  assert(nil == Connection[new])
  assert(nil ~= Connection[old])
  Connection[new] = Connection[old]
end

for new, old in pairs(STATEMENT_RENAME) do
  assert(nil == Statement[new])
  assert(nil ~= Statement[old])
  Statement[new] = Statement[old]
end

------------------------------------------------------------------
do -- async

function Connection:supports_async_mode()
  return self:supportsAsyncStatement()
end

function Statement:supports_async_mode()
  return self:connection():supportsAsyncStatement()
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do

local TRANSACTION_LEVEL = {
  "NONE","READ_UNCOMMITTED","READ_COMMITTED",
  "REPEATABLE_READ","SERIALIZABLE"
}

for i = 1, #TRANSACTION_LEVEL do
  odbc.TRANSACTION [ TRANSACTION_LEVEL[i] ] = i
  TRANSACTION_LEVEL[ TRANSACTION_LEVEL[i] ] = i
end

function Connection:supports_transaction(lvl)
  if not self:connected() then return nil, ERROR.cnn_not_opened end
  if lvl == nil then return self:supportsTransactions() end
  if type(lvl) == 'string' then 
    local lvl_n = TRANSACTION_LEVEL[lvl] 
    if not lvl_n then return nil, ERROR.unknown_txn_lvl .. lvl end
    lvl = lvl_n
  end

  assert(type(lvl) == 'number')
  return self:supportsTransactionIsolationLevel(lvl)
end

function Connection:default_transaction()
  if not self:connected() then return nil, ERROR.cnn_not_opened end
  local lvl, err = self:getDefaultTransactionIsolation()
  if not lvl then return nil, err end
  return lvl, TRANSACTION_LEVEL[lvl]
end

function Connection:set_transaction_level(lvl)
  if not self:connected() then return nil, ERROR.cnn_not_opened end

  local err 
  if lvl == nil then
    lvl, err = self:default_transaction()
    if not lvl then return nil, err end;
  elseif type(lvl) == 'string' then 
    local lvl_n = TRANSACTION_LEVEL[lvl] 
    if not lvl_n then return nil, ERROR.unknown_txn_lvl .. lvl end
    lvl = lvl_n
  end

  assert(type(lvl) == 'number')
  return self:settransactionisolation(lvl)
end

function Connection:get_transaction_level()
  if not self:connected() then return nil, ERROR.cnn_not_opened end

  local lvl, err = self:gettransactionisolation()
  if not lvl then return nil, err end
  return lvl, TRANSACTION_LEVEL[lvl]
end

function Connection:supports_bind_param()
  if not self:connected() then return nil, ERROR.cnn_not_opened end
  return self:supportsBindParam()
end

function Connection:supports_prepare()
  if not self:connected() then return nil, ERROR.cnn_not_opened end
  return self:supportsPrepare()
end

function Connection:set_login_timeout(ms)
  assert((ms == nil) or (type(ms) == 'number'))
  self:setlogintimeout(ms or -1)
  return true
end

function Connection:get_login_timeout()
  local ms = self:getlogintimeout()
  if ms == -1 then return nil end
  return ms
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection catalog

local function callable(fn)
  if not fn then return false end
  local t = type(fn)
  if t == 'function' then return true  end
  if t == 'number'   then return false end
  if t == 'boolean'  then return false end
  if t == 'string'   then return false end
  return true
end

local function implement(name, newname)
  local impl_name  = name .. "_impl"
  assert(     Connection[name]      )
  assert( not Connection[impl_name] )
  assert( not Connection[newname]   )
  local impl = function (self, fetch_mode, ...)
    local arg = pack_n(...)
    local fn = arg[arg.n]
    if callable(fn) then -- assume this callback
      arg[arg.n] = nil
      arg.n = arg.n - 1
    else fn = nil end

    local stmt, err = self[impl_name](self, unpack_n(arg))
    if not stmt then return nil, err end
    stmt:setdestroyonclose(true)

    if fn then return stmt:foreach(fetch_mode, true, fn) end
    return stmt:fetch_all(fetch_mode or 'a', true)
  end

  newname = newname or name
  Connection[ name ..'_impl' ] = Connection[ name ]
  Connection[         newname   ] = function(self, ...) return impl(self, nil,  ...) end
  Connection[ 'i'  .. newname   ] = function(self, ...) return impl(self, 'n',  ...) end
  Connection[ 'n'  .. newname   ] = function(self, ...) return impl(self, 'a',  ...) end
  Connection[ 't'  .. newname   ] = function(self, ...) return impl(self, 'an', ...) end
end

implement('typeinfo')
implement('tabletypes')
implement('schemas')
implement('catalogs')
implement('statistics')
implement('tables')
implement('tableprivileges', 'table_privileges')
implement('primarykeys', 'primary_keys')
implement('indexinfo', 'index_info')
implement('crossreference')
implement('columns')
implement('specialcolumns', 'special_columns')
implement('procedures')
implement('procedurecolumns', 'procedure_columns')
implement('columnprivileges', 'column_privileges')
end
------------------------------------------------------------------

return odbc
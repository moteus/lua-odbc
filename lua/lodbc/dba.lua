---
-- implement lua-dba interface
--

local odbc = require "odbc.core"

local throw
if _G._VERSION >= 'Lua 5.2' then
  throw = error
else
  throw = function(err, lvl)
    return error(tostring(err), (lvl or 1)+1)
  end
end

local ERROR = {
  unsolved_parameter   = 'unsolved name of parameter: ';
  unknown_parameter    = 'unknown parameter: ';
  no_cursor            = 'query has not returned a cursor';
  ret_cursor           = 'query has returned a cursor';
  query_opened         = 'query is already opened';
  cnn_not_opened       = 'connection is not opened';
  query_not_opened     = 'query is not opened';
  query_prepared       = 'query is already prepared';
  deny_named_params    = 'named parameters are denied';
  no_sql_text          = 'SQL text was not set';
  pos_params_unsupport = 'positional parameters are not supported';
  not_support          = 'not supported';
  unknown_txn_lvl      = 'unknown transaction level: '; 
};

local Environment = odbc.getenvmeta()
local Connection  = odbc.getcnnmeta()
local Statement   = odbc.getstmtmeta()

local function make_cahe()
  return setmetatable({}, {__mode="k"})
end

local user_values = make_cahe()

local function user_val(ud)
  return user_values[ud]
end

local function set_user_val(ud, val)
  user_values[ud] = val
end

local unpack = unpack or table.unpack

local function pack_n(...)
  return {n = select('#', ...), ...}
end

local function unpack_n(t, s)
  return unpack(t, s or 1, t.n or #t)
end

local function clone(t)
  local o = {}
  for k, v in pairs(t) do o[k] = v end
  return o
end

--
-- возвращает индекс значения val в массиве t
--
local function ifind(val,t)
  for i,v in ipairs(t) do
    if v == val then
      return i
    end
  end
end

local function collect(t)
  return function(row)
    table.insert(t, clone(row))
  end
end

local function stringQuoteChar(cnn)
  return cnn:identifierQuoteString() == "'" and '"' or "'"
end

local function bind_param(stmt, i, val, ...)
  if val == odbc.NULL    then return stmt:bindnull(i)    end
  if val == odbc.DEFAULT then return stmt:binddefault(i) end
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

local function Connection_new(env, ...)
  local cnn, err = env:connection_impl()
  if not cnn then return nil, err end

  set_user_val(cnn, {
    params = pack_n(...);
  })

  return cnn
end

local Statement_set_sql
local function Statement_new(cnn, sql)
  assert(cnn)
  local stmt, err = cnn:statement_impl()
  if not stmt then return nil, err end

  set_user_val(stmt,{})

  if sql then 
    local ok, err = Statement_set_sql(stmt, sql)
    if not ok then
      stmt:destroy()
      return nil, err
    end
  end

  return stmt
end

-------------------------------------------------------------------------------
local param_utils = {} do
--
-- Используется для реализации именованных параметров
--

--
-- паттерн для происка именованных параметров в запросе
--
param_utils.param_pattern = "[:]([^%d%s][%a%d_]+)"

--
-- заключает строку в ковычки
--
function param_utils.quoted (s,q) return (q .. string.gsub(s, q, q..q) .. q) end

--
--
--
function param_utils.bool2sql(v) return v and 1 or 0 end

--
-- 
--
function param_utils.num2sql(v)  return tostring(v) end

--
-- 
--
function param_utils.str2sql(v, q) return param_utils.quoted(v, q or "'") end

--
-- Подставляет именованные параметры
--
-- @param sql      - текст запроса
-- @param params   - таблица значений параметров
-- @return         - новый текст запроса
--
function param_utils.apply_params(cnn, sql, params)
  params = params or {}
  local q = cnn and stringQuoteChar(cnn) or "'"

  local err
  local str = string.gsub(sql,param_utils.param_pattern,function(param)
    local v = params[param]
    local tv = type(v)
    if    ("number"      == tv)then return param_utils.num2sql (v)
    elseif("string"      == tv)then return param_utils.str2sql (v, q)
    elseif("boolean"     == tv)then return param_utils.bool2sql(v)
    elseif(PARAM_NULL    ==  v)then return 'NULL'
    elseif(PARAM_DEFAULT ==  v)then return 'DEFAULT'
    end
    err = ERR_MSGS.unknown_parameter .. param
  end)
  if err then return nil, err end
  return str
end

--
-- Преобразует именованные параметры в ?
-- 
-- @param sql      - текст запроса
-- @param parnames - таблица разрешонных параметров
--                 - true - разрешены все имена
-- @return  новый текст запроса
-- @return  массив имен параметров. Индекс - номер по порядку данного параметра
--
function param_utils.translate_params(sql,parnames)
  if parnames == nil then parnames = true end
  assert(type(parnames) == 'table' or (parnames == true))
  local param_list={}
  local err
  local function replace()
    local function t1(param) 
      -- assert(type(parnames) == 'table')
      if not ifind(param, parnames) then
        err = ERR_MSGS.unsolved_parameter .. param
        return
      end
      table.insert(param_list, param)
      return '?'
    end

    local function t2(param)
      -- assert(parnames == true)
      table.insert(param_list, param)
      return '?'
    end

    return (parnames == true) and t2 or t1
  end

  local str = string.gsub(sql,param_utils.param_pattern,replace())
  if err then return nil, err end
  if #param_list == 0 then return sql end
  return str, param_list
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
do -- odbc

odbc.NULL    = odbc.NULL or {}

odbc.DEFAULT = odbc.DEFAULT or {}

---
--
function odbc.connect(...)
  local env, err = odbc.environment()
  if not env then return nil, err end
  local cnn, err = env:connect(...)
  if not cnn then 
    env:destroy()
    return nil, err
  end
  assert(cnn:environment() == env)
  return cnn, env
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
if not Environment.connection_impl then

Environment.connection_impl = Environment.connection

function Environment:connection(...)
  return Connection_new(self, ...)
end

function Environment:connect(...)
  local cnn, err = self:connection(...)
  if not cnn then return nil, err end
  local ok, err = cnn:connect()
  if not ok then cnn:destroy() return nil, err end
  return cnn
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

function Connection:handle()
  return self
end

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

function Connection:execute(sql, params)
  assert(type(sql) == "string")
  assert((params == nil) or (type(params) == "table"))
  local stmt, err = self:statement_impl()
  if not stmt then return nil, err end
  if params then
    assert(type(params) == 'table')
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
              return nil, err, i
            end
          else
            stmt:destroy()
            return nil, ERR_MSGS.unknown_parameter .. paramName
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
  if type(res) == 'userdata' then 
    res:destroy()
    return nil, ERROR.ret_cursor
  end
  return res
end

function Connection:first_row(...)
  local stmt, err = self:execute(...)
  if not stmt then return nil, err end
  if type(stmt) ~= 'userdata' then return nil, ERROR.no_cursor end
  local args = pack_n(stmt:fetch())
  stmt:destroy()
  return unpack_n(args)
end

local function Connection_first_Xrow(self, fetch_mode, ...)
  local stmt, err = self:execute(...)
  if not stmt then return nil, err end
  if type(stmt) ~= 'userdata' then return nil, ERROR.no_cursor end
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
  if type(stmt) ~= 'userdata' then return nil, ERROR.no_cursor end
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
  if type(stmt) ~= 'userdata' then throw(ERROR.no_cursor, 2) end
  stmt:setdestroyonclose(true)

  return rows(stmt, fetch_mode)
end

function Connection:rows(...)  return Connection_Xrows(self, nil,  ...) end

function Connection:irows(...) return Connection_Xrows(self, 'n',  ...) end

function Connection:nrows(...) return Connection_Xrows(self, 'a',  ...) end

function Connection:trows(...) return Connection_Xrows(self, 'an', ...) end

function Connection:fetch_all(fetch_mode, sql, param)
  assert(type(fetch_mode) == 'string')
  local result = {}
  local ok, err = Connection_Xeach(self, fetch_mode, sql, param, collect(result))
  if err == nil then return result end
  return nil, err
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
if not Statement.prepare_impl then

Statement.prepare_impl = Statement.prepare
Statement.bind_impl    = Statement.bind
Statement.execute_impl = Statement.execute

function Statement:vbind_col(i, val, ...)
  return val:bind_col(self, i, ...)
end

function Statement:vbind_param(i, val, ...)
  return val:bind_param(self, i, ...)
end

local fix_types = {
  'ubigint', 'sbigint', 'utinyint', 'stinyint', 'ushort', 'sshort', 
  'ulong', 'slong', 'float', 'double', 'date', 'time', 'bit'
}

local buf_types = {'char', 'binary', 'wchar'}

for _, tname in ipairs(fix_types) do
  Statement["vbind_col_" .. tname] = function(self, ...)
    return odbc[tname]():bind_col(self, ...)
  end

  Statement["vbind_param_" .. tname] = function(self, i, val, ...)
    return odbc[tname](val):bind_param(self, i, ...)
  end
end

for _, tname in ipairs(buf_types) do
  Statement["vbind_col_" .. tname] = function(self, i, size, ...)
    return odbc[tname](size):bind_col(self, i, ...)
  end

  Statement["vbind_param_" .. tname] = function(self, i, size, ...)
    if type(size) == 'string' then
      return odbc[tname](size):bind_param(self, i, ...)
    end
    assert(type(size) == 'number')
    local val = ...
    if type(val) == 'string' then
      return odbc[tname](size, val):bind_param(self, i, select(2, ...))
    end
    return odbc[tname](size):bind_param(self, i, ...)
  end
end

Statement_set_sql = function (self, sql)
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

function Statement:prepare(sql)
  if sql then
    local ok, err = Statement_set_sql(self, sql)
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
    local ok, err = Statement_set_sql(self, sql)
    if not ok then return nil, err end
  else
    local private_ = user_val(self)
    sql = private_.psql or private_.sql
  end
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
  if not self:closed() then 
    if (type(...) == 'string') or (type(...) == 'table') then
      return nil, ERROR.query_opened
    end
    return self:foreach(fetch_mode, ...)
  end

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

function Statement:fetch_all(fetch_mode, sql, param)
  assert(type(fetch_mode) == 'string')
  local result = {}
  local ok, err = Statement_Xeach(self, fetch_mode, sql, param, collect(result))
  if err == nil then return result end
  return nil, err
end

end
-------------------------------------------------------------------------------

return odbc
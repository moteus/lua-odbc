local throw
if _G._VERSION >= 'Lua 5.2' then
  throw = error
else
  throw = function(err, lvl)
    return error(tostring(err), (lvl or 1)+1)
  end
end

local function clone(t)
  local o = {}
  for k, v in pairs(t) do o[k] = v end
  return o
end

local unpack = unpack or table.unpack

local function pack_n(...)
  return {n = select('#', ...), ...}
end

local function unpack_n(t, s)
  return unpack(t, s or 1, t.n or #t)
end

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

local function make_cahe()
  return setmetatable({}, {__mode="k"})
end

local ErrorMeta ErrorMeta = {
  __index = function (self, key)
    assert(#self > 0)
    local row = rawget(self, 1)
    return row and row[key]
  end;

  __concat = function (self, rhs)
    assert(type(self) == 'table')
    assert(#self > 0)

    local t = {}
    for _,v in ipairs(self)  do table.insert(t, clone(v)) end
    if type(rhs) == 'table' then
      for _,v in ipairs(rhs) do table.insert(t, clone(v)) end
    else
      assert(type(rhs) == 'string')
      t[1].message = t[1].message .. rhs
    end
    return setmetatable(t, ErrorMeta)
  end;

  __tostring = function(self)
    assert(#self > 0)
    local res = ""
    for i,t in ipairs(self) do
      if t.message then
        if #res == 0 then res = t.message
        else res = res .. "\n" .. t.message end
      end
    end
    return res
  end;
}

local function E(msg,state,code)
  assert(type(msg) == "string")
  return setmetatable({{message=msg;state=state;code=code}}, ErrorMeta)
end

local function test_error()
  local err = E"some error"
  assert(#err == 1)
  assert(err.message == "some error")
  assert(err[1].message == "some error")
  assert(tostring(err) == "some error")

  local er2 = err.." value"
  assert(#er2 == 1)
  assert(er2.message == "some error value")
  assert(er2[1].message == "some error value")
  assert(tostring(er2) == "some error value")

  local er2 = err..E"other error"
  assert(#er2 == 2)
  assert(er2.message == "some error")
  assert(er2[1].message == "some error")
  assert(er2[2].message == "other error")
  assert(tostring(er2) == "some error\nother error")
end

local function user_val(ud)
  return ud:getuservalue()
end

local function set_user_val(ud, val)
  ud:setuservalue(val)
end

local function stringQuoteChar(cnn)
  return cnn:identifierQuoteString() == "'" and '"' or "'"
end

local ERROR = {
  unsolved_parameter   = E'unsolved name of parameter: ';
  unknown_parameter    = E'unknown parameter: ';
  no_cursor            = E'query has not returned a cursor';
  ret_cursor           = E'query has returned a cursor';
  query_opened         = E'query is already opened';
  cnn_not_opened       = E'connection is not opened';
  query_not_opened     = E'query is not opened';
  query_prepared       = E'query is already prepared';
  deny_named_params    = E'named parameters are denied';
  no_sql_text          = E'SQL text was not set';
  pos_params_unsupport = E'positional parameters are not supported';
  not_support          = E'not supported';
  unknown_txn_lvl      = E'unknown transaction level: '; 
};

test_error()

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
-- @param cnn      - `Connection`
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
    err = ERROR.unknown_parameter .. param
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
        err = ERROR.unsolved_parameter .. param
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

return {
  unpack       = unpack;
  pack_n       = pack_n;
  unpack_n     = unpack_n;
  ifind        = ifind;
  collect      = collect;
  make_cahe    = make_cahe;
  E            = E;
  ERROR        = ERROR;
  param_utils  = param_utils;
  user_val     = user_val;
  set_user_val = set_user_val;
}
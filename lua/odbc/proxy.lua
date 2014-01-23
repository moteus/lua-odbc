
local setmeta = setmetatable

local function make_cahe()
  return setmeta({}, {__mode="k"})
end

local USER_VALUES = make_cahe()

local function user_val(ud)
  return USER_VALUES[ud]
end

local function set_user_val(ud, val)
  USER_VALUES[ud] = val
end

local function make_proxy(t)
  local proxy = {}
  setmeta(proxy,{__index = function(self, key)
    local fun = rawget(t, key)
    if not fun then return nil end
    if type(fun) == "function" then
      proxy[key] = function(self, ...) return fun(self._self, ...) end
    else 
      proxy[key] = fun
    end
    return proxy[key]
  end})
  proxy.__index = proxy
  return proxy
end

local odbc = require "odbc.core"

local Environment = make_proxy(odbc.getenvmeta() )
local Connection  = make_proxy(odbc.getcnnmeta() )
local Statement   = make_proxy(odbc.getstmtmeta())

local _M = setmeta({},{__index = odbc})

function _M.getenvmeta()  return Environment end

function _M.getcnnmeta()  return Connection  end

function _M.getstmtmeta() return Statement   end

local init_connection = odbc.init_connection
function _M.init_connection(...)
  local cnn, err = init_connection(...)
  if not cnn then return nil, err end
  local obj = setmeta({_self=cnn}, Connection)
  set_user_val(cnn, obj)
  return obj
end

local environment = odbc.environment
function _M.environment(...)
  local env, err = environment(...)
  if not env then return nil, err end
  local obj = setmeta({_self=env}, Environment)
  set_user_val(env, obj)
  return obj
end

local connection = Environment.connection
function Environment:connection(...)
  local cnn, err = connection(self, ...)
  if not cnn then return nil, err end
  local obj = setmeta({_self=cnn}, Connection)
  set_user_val(cnn, obj)
  return obj
end

local destroy = Environment.destroy
function Environment:destroy(...)
  set_user_val(self._self, nil)
  return destroy(self, ...)
end

local statement = Connection.statement
function Connection:statement(...)
  local stmt, err = statement(self, ...)
  if not stmt then return nil, err end
  local obj = setmeta({_self=stmt}, Statement)
  set_user_val(stmt, obj)
  return obj
end

local destroy = Connection.destroy
function Connection:destroy(...)
  set_user_val(self._self, nil)
  return destroy(self, ...)
end

local environment = Connection.environment
function Connection:environment(...)
  local env, err = environment(self, ...)
  if not env then return nil, err end
  return user_val(env)
end

local connect = Connection.connect
function Connection:connect(...)
  local ok, err = connect(self, ...)
  if not ok then return nil, err end
  assert(ok == self._self)
  return self
end

local driverconnect = Connection.driverconnect
function Connection:driverconnect(...)
  local ok, err = driverconnect(self, ...)
  if not ok then return nil, err end
  assert(ok == self._self)
  return self
end

local connection = Statement.connection
function Statement:connection(...)
  local cnn, err = connection(self, ...)
  if not cnn then return nil, err end
  return user_val(cnn)
end

local destroy = Statement.destroy
function Statement:destroy(...)
  set_user_val(self._self, nil)
  return destroy(self, ...)
end

local execute = Statement.execute
function Statement:execute(...)
  local ok, err = execute(self, ...)
  if not ok then return nil, err end
  if ok == self._self then return self end
  return ok
end

return _M
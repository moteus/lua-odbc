local odbc = require "odbc.core"
local Environment = odbc.getenvmeta()
local Connection  = odbc.getcnnmeta()
local Statement   = odbc.getstmtmeta()

-------------------------------------------------------------------------------
do -- odbc

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

function odbc.driverconnect(...)
  local env, err = odbc.environment()
  if not env then return nil, err end
  local cnn, err = env:driverconnect(...)
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
do -- Environment

function Environment:driverconnect(...)
  local cnn, err = self:connection()
  if not cnn then return nil, err end
  local ok, err = cnn:driverconnect(...)
  if not ok then cnn:destroy() return nil, err end
  return cnn
end

function Environment:connect(...)
  if type((...)) == 'table' then
    return self:driverconnect(...)
  end
  local cnn, err = self:connection()
  if not cnn then return nil, err end
  local ok, err = cnn:connect(...)
  if not ok then cnn:destroy() return nil, err end
  return cnn
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
do -- Connection

function Connection:prepare(sql)
  local stmt, err = self:statement()
  if not stmt then return nil, err end
  local ok, err = stmt:prepare(sql)
  if not ok then 
    stmt:destroy()
    return nil, err
  end
  return stmt
end

function Connection:execute(sql)
  local stmt, err = self:statement()
  if not stmt then return nil, err end
  local ok, err = stmt:execute(sql)
  if not ok then 
    stmt:destroy()
    return nil, err
  end
  return stmt
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
do -- Statement

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

local buf_types = {'char', 'binary'}

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

end
-------------------------------------------------------------------------------

return odbc
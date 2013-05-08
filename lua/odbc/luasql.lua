local odbc = require "odbc.core"

luasql = (type(luasql) == 'table') and luasql or {
  _COPYRIGHT   = "Copyright (C) 2006-2012 Kepler Project";
  _DESCRIPTION = "LuaSQL is a simple interface from Lua to a DBMS";
  _VERSION     = "LuaSQL 2.2.1";
}

local Environment = {__metatable = "LuaSQL: you're not allowed to get this metatable"} Environment.__index = Environment
local Connection  = {__metatable = "LuaSQL: you're not allowed to get this metatable"} Connection.__index = Connection
local Cursor      = {__metatable = "LuaSQL: you're not allowed to get this metatable"} Cursor.__index = Cursor

local unpack = unpack or table.unpack

local conv_null

if odbc.NULL ~= nil then

  local ok, va = pcall(require, "vararg")
  if not ok then va = nil end

  local function null2nil(v)
    if v == odbc.NULL then return nil end
    return v
  end

  conv_null = function (...)
    if type((...)) == "table" then
      local t = (...)
      for k, v in pairs(t) do
        t[k] = null2nil(t[k])
      end
      return t
    end

    if va then return va.map(null2nil, ...) end

    local t, n = {...}, select('#', ...)
    for i = 1, n do
      if t[i] == odbc.NULL then t[i] = nil end
    end
    return unpack(t, 1, n)
  end

else conv_null = function (...) return ... end end

local Environment_new, Connection_new, Cursor_new

function Environment_new()
  local env, err = odbc.environment()
  if not env then return nil, err end
  return setmetatable({private_={env = env}}, Environment)
end

function Environment:close()
  if not self.private_.env:destroyed() then
    return self.private_.env:destroy()
  end
  return false
end

function Environment:connect(...)
  local cnn, err = self.private_.env:connection()
  if not cnn then return nil, err end
  local ok ok, err = cnn:connect(...)
  if not ok then cnn:destroy() return nil, err end
  return Connection_new(cnn, self)
end

function Connection_new(cnn, env)
  assert(cnn)
  assert(cnn:environment() == env.private_.env)
  return setmetatable({private_={cnn = cnn;env=env}}, Connection)
end

function Connection:close()
  if not self.private_.cnn:destroyed() then
    return self.private_.cnn:destroy()
  end
  return false
end

function Connection:commit()
  return self.private_.cnn:commit()
end

function Connection:rollback()
  return self.private_.cnn:rollback()
end

function Connection:setautocommit(val)
  return self.private_.cnn:setautocommit(val)
end

function Connection:execute(sql)
  local stmt, err = self.private_.cnn:statement()
  if not stmt then return nil, err end
  local ok ok,err = stmt:execute(sql)
  if not ok then return nil, err end
  if stmt:closed() then
    stmt:destroy()
    return ok
  end
  return Cursor_new(stmt, self)
end

function Cursor_new(stmt, cnn)
  assert(stmt)
  assert(stmt:connection() == cnn.private_.cnn)
  stmt:setautoclose(true)
  stmt:setdestroyonclose(true)
  return setmetatable({private_={stmt=stmt,cnn=cnn}}, Cursor)
end

function Cursor:close()
  if not self.private_.stmt:destroyed() then
    local ret = self.private_.stmt:closed()
    ret = self.private_.stmt:destroy() and (not ret)
    return ret
  end
  return false
end

function Cursor:fetch(...)
  return conv_null(self.private_.stmt:fetch(...))
end

function Cursor:getcolnames()
  return self.private_.stmt:colnames()
end

function Cursor:getcoltypes()
  return self.private_.stmt:coltypes()
end

luasql.odbc = function()
  return Environment_new()
end
 
return luasql
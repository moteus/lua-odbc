odbc = require "odbc"
require "config"

IS_LUA52 = _VERSION >= 'Lua 5.2'

function run_test(arg)
  local _, emsg = xpcall(function()
    lunit.main(arg)
  end, debug.traceback)
  if emsg then
    print(emsg)
    os.exit(1)
  end
  if lunit.stats.failed > 0 then
    os.exit(1)
  end
end

function is_dsn_exists(env, dsn_name)
  local cnt, d = return_count(env:datasources(function(dsn) 
    if dsn:upper() == dsn_name:upper() then return dsn end
  end))
  assert((cnt == 0) or ( d:upper() == dsn_name:upper() ))
  return d
end

function return_count(...)
  return select('#', ...), ...
end

unpack = unpack or table.unpack

function do_connect()
  local env, cnn
  local err
  env,err = odbc.environment()
  if env then
    cnn,err = env:connection()
    if cnn then
      local ok ok, err = cnn:driverconnect(CNN_DRV)
      if ok then return env, cnn end
    end
  end
  if cnn then cnn:destroy() end
  if env then env:destroy() end
  return nil, err
end

function exec_ddl(cnn, cmd)
  local stmt, err = cnn:statement()
  if not stmt then return nil, err end
  local ok, err = stmt:execute (cmd)
  stmt:destroy()
  return ok, err 
end

function define_table (n)
  local t = {}
  for i = 1, n do
    table.insert (t, "f"..i.." "..DEFINITION_STRING_TYPE_NAME)
  end
  return "create table " .. TEST_TABLE_NAME .. " ("..table.concat (t, ',')..")"
end

function create_table (cnn)
  return exec_ddl(cnn, define_table(TOTAL_FIELDS))
end

function drop_table(cnn)
  return exec_ddl(cnn, 'drop table ' .. TEST_TABLE_NAME)
end

function table_exists(cnn)
  local stmt, err = cnn:tables()
  if not stmt then return nil, err end
  local found, err = stmt:foreach(function(_,_,t)
    if t:lower() == TEST_TABLE_NAME:lower() then return true end
  end)
  stmt:destroy()
  if (not found) and (not err) then found = false end
  return found, err
end

function ensure_table(cnn)
  if table_exists(cnn) then drop_table(cnn) end
  return create_table(cnn)
end

function proc_exists(cnn)
  local stmt, err = cnn:procedures()
  if not stmt then return nil, err end
  local found, err = stmt:foreach(function(_,_,t)
    if t:lower() == TEST_PROC_NAME:lower() then return true end
  end)
  stmt:destroy()
  if (not found) and (not err) then found = false end
  return found, err
end

function create_proc (cnn)
  return exec_ddl(cnn, TEST_PROC_CREATE)
end

function drop_proc(cnn)
  return exec_ddl(cnn, TEST_PROC_DROP)
end

function ensure_proc(cnn)
  if proc_exists(cnn) then drop_proc(cnn) end
  return create_proc(cnn)
end

function TEST_CASE (name)
  local lunit = require"lunit"
  if not IS_LUA52 then
    module(name, package.seeall, lunit.testcase)
    setfenv(2, _M)
  else
    return lunit.module(name, 'seeall')
  end
end

function weak_ptr(val)
  return setmetatable({value = val},{__mode = 'v'})
end

function gc_collect()
  collectgarbage("collect")
  collectgarbage("collect")
end


require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local arg = {...}

local _ENV = TEST_CASE'Environment test'

local function assert_noret(fn, ...)
  local n = return_count(fn())
  assert_equal(0, n, ...)
end

local env

function setup()
  env = odbc.environment()
end

function teardown()
  if env then env:destroy() end
end

local function check_dsn(name, desc)
  assert_string(name)
  assert_string(desc)
end

local function check_drv(name, params)
  assert_string(name)
  if(params ~= nil) then assert_table(params) end
end

function test_drivers()
  assert_noret(function() return env:drivers(check_drv) end)
  local t = assert_table(env:drivers())
  for _, v in ipairs(t) do check_drv((unpack or table.unpack)(v))end
end

function test_datasources()
  assert_noret(function() return env:datasources(check_dsn) end)
  local t = assert_table(env:datasources())
  for _, v in ipairs(t) do check_dsn((unpack or table.unpack)(v))end
end

function test_exists_dsn()
-- we can find 
-- @see is_dsn_exists
  assert_not_nil(is_dsn_exists(env, CNN_DSN[1]), 'Can not find test dsn')
end

function test_destroy()
  assert_not_nil(env)
  assert_false(env:destroyed())
  assert_true(env:destroy())
  assert_true(env:destroyed())
  assert_error(function() env:drivers() end)
  assert_true(env:destroy())
end

local_run_test(arg)
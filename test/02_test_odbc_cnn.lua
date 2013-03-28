require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local arg = {...}

local TEST_NAME = 'Connection test'
if _VERSION >= 'Lua 5.2' then  _ENV = lunit.module(TEST_NAME,'seeall')
else module( TEST_NAME, package.seeall, lunit.testcase ) end

local env, cnn

function teardown()
  if cnn then cnn:destroy() end
  if env then env:destroy() end
  cnn = nil
  env = nil
end

function setup()
  env, cnn = do_connect()
  assert_not_nil(env, cnn)
end

function test_basic()
  assert_equal(env, cnn:environment())
end

function test_destroy()
  assert_not_nil(env)
  assert_not_nil(cnn)
  assert_false(cnn:destroyed())
  assert_pass(function() cnn:drvname() end)
  assert_true(cnn:destroy())
  assert_true(cnn:destroyed())
  assert_error(function() cnn:drvname() end)
  assert_true(cnn:destroy())
end

function test_driverconnect()
  assert_true(cnn:connected())
  assert_true(cnn:disconnect())
  assert_true(not cnn:connected())
  assert_false(cnn:destroyed())
  local c, str = cnn:driverconnect(CNN_DRV)
  assert_equal(cnn, c)
  assert_string(str)
  assert_true(cnn:connected())
  assert_true(cnn:disconnect())
  assert_equal(cnn, cnn:driverconnect(str))
end

function test_connect()
  assert_true(cnn:connected())
  assert_true(cnn:disconnect())
  assert_true(not cnn:connected())
  assert_false(cnn:destroyed())
  assert_equal(cnn, cnn:connect(unpack(CNN_DSN)))
  assert_true(cnn:disconnect())
  assert_true(not cnn:connected())
end

function test_uservalue()
  assert_nil(cnn:getuservalue())
  assert_equal(cnn, cnn:setuservalue(123))
  assert_equal(123, cnn:getuservalue())
  assert_equal(cnn, cnn:setuservalue())
  assert_nil(cnn:getuservalue())
  local ptr = weak_ptr{}
  assert_equal(cnn, cnn:setuservalue(ptr.value))
  assert_equal(ptr.value, cnn:getuservalue())
  gc_collect()
  assert_table(ptr.value)
  cnn:destroy()
  gc_collect()
  assert_nil(ptr.value)
end

local_run_test(arg)
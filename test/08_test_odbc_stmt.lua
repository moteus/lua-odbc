require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local arg = {...}

local TEST_NAME = 'Statement test'
if _VERSION >= 'Lua 5.2' then  _ENV = lunit.module(TEST_NAME,'seeall')
else module( TEST_NAME, package.seeall, lunit.testcase ) end

local env, cnn, stmt

function teardown()
  if stmt then stmt:destroy() end
  if cnn then cnn:destroy() end
  if env then env:destroy() end
  stmt = nil
  cnn = nil
  env = nil
end

function setup()
  env, cnn = do_connect()
  assert_not_nil(env, cnn)
end

function test_destroy()
  assert_not_nil(env)
  assert_not_nil(cnn)
  assert_false(cnn:destroyed())
  stmt = assert_userdata( cnn:tables() )
  assert_error(function() cnn:destroy() end)
  assert_false(stmt:destroyed())
  assert_boolean(cnn:getautoclosestmt())
  assert_true(cnn:setautoclosestmt(true))
  assert_error(function() cnn:destroy() end) -- autoclosestmt works only for statement created after is set
  assert_true(stmt:destroy())
  assert_true(stmt:destroyed())
  stmt = assert_userdata( cnn:tables() )
  assert_pass(function() cnn:destroy() end)
  assert_true(stmt:destroyed())
end

local function weak_ptr(val)
  return setmetatable({value = val},{__mode = 'v'})
end

local function gc_collect()
  collectgarbage("collect")
  collectgarbage("collect")
end

function test_weak()
  local function test()
    local ptr
    do 
      local stmt = cnn:tables()
      ptr = weak_ptr(stmt)
    end
    gc_collect()
    if ptr.value then stmt = ptr.value end -- for destroy in teardown
    assert_nil(ptr.value)
  end

  assert_true(cnn:setautoclosestmt(false))
  test()
  assert_true(cnn:setautoclosestmt(true))
  test()
end

local_run_test(arg)
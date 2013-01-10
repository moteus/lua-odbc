require "config"
require "tools"

local lunit = require "lunitx"

local TEST_NAME = 'Statement test'
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

function test_destroy()
  assert_not_nil(env)
  assert_not_nil(cnn)
  assert_false(cnn:destroyed())
  local stmt = assert_userdata( cnn:tables() )
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


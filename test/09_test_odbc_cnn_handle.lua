require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local arg = {...}

local _ENV = TEST_CASE'Connection reset handle test'

local env, cnn, cnn2, stmt

function teardown()
  if stmt then stmt:destroy() end
  if cnn  then cnn:destroy()  end
  if env  then env:destroy()  end
  env, cnn, stmt = nil
end

function setup()
  env, cnn = do_connect()
  assert_not_nil(env, cnn)
end

local function clone_cnn(cnn)
  local h = assert_userdata(cnn:handle())
  cnn2 = assert_userdata(odbc.init_connection(h))
  assert_equal(h, cnn2:handle())
end

function test_clone()
  clone_cnn(cnn)
  assert_true(cnn2:connected())
  assert_true(cnn2:disconnect())
  assert(not cnn2:connected())
  assert(not cnn:connected())
end

function test_clone_destroy()
  -- destroy does not disconnect handle if it does not own them
  clone_cnn(cnn)
  cnn2:destroy()
  assert_error(function() cnn2:connected() end)
  assert_pass(function() cnn:connected() end)
  assert_true(cnn:connected())
end

function test_clone_close_stmt()
  clone_cnn(cnn)
  assert_true(cnn2:setautoclosestmt(true))
  stmt = assert_userdata(cnn2:statement())
  assert_false(stmt:destroyed())
  cnn2:reset_handle((cnn:handle()))
  assert_true(stmt:destroyed())
end

function test_clone_cnn_info()
  clone_cnn(cnn)
  assert_true(cnn2:connected())
  local flag = assert_boolean(cnn:supportsTransactions())
  assert_equal(flag, cnn2:supportsTransactions())
end

local_run_test(arg)
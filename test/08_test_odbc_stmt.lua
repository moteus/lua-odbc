require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local arg = {...}

local _ENV = TEST_CASE'Statement test'

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

function test_uservalue()
  stmt = assert_userdata( cnn:statement() )
  assert_nil(stmt:getuservalue())
  assert_equal(stmt, stmt:setuservalue(123))
  assert_equal(123, stmt:getuservalue())
  assert_equal(stmt, stmt:setuservalue())
  assert_nil(stmt:getuservalue())
  local ptr = weak_ptr{}
  assert_equal(stmt, stmt:setuservalue(ptr.value))
  assert_equal(ptr.value, stmt:getuservalue())
  gc_collect()
  assert_table(ptr.value)
  assert_equal(ptr.value, stmt:getuservalue())
  stmt:destroy()
  gc_collect()
  assert_nil(ptr.value)
end

function test_statement_counter()
  local function test()
    assert_equal(1, cnn:statement_count())
    local stmt2 = assert_not_nil(cnn:statement())
    local n, err = cnn:statement_count()
    stmt2:destroy()
    assert_equal(2, n, err)
    assert_equal(1, cnn:statement_count())
    stmt:destroy()
    assert_equal(0, cnn:statement_count())
  end

  cnn:setautoclosestmt(false)
  stmt = cnn:statement()
  test()

  cnn:setautoclosestmt(true)
  stmt = cnn:statement()
  test()
end

function test_tostring()
  stmt = cnn:statement()
  assert_match("Statement", tostring(stmt))
  assert_not_match("closed", tostring(stmt))
  stmt:destroy()
  assert_match("Statement", tostring(stmt))
  assert_match("closed", tostring(stmt))
end

local_run_test(arg)
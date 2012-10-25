require "config"
require "tools"
local lunit = require "lunitx"

local TEST_NAME = 'Statement foreach'
if _VERSION >= 'Lua 5.2' then  _ENV = lunit.module(TEST_NAME,'seeall')
else module( TEST_NAME, package.seeall, lunit.testcase ) end

local function assert_equal3(a,b,c, ra,rb,rc)
  assert_equal(a,ra)
  assert_equal(b,rb)
  assert_equal(c,rc)
end

local function assert_equal2(a,b, ra,rb)
  assert_equal(a,ra)
  assert_equal(b,rb)
end

local env, cnn, stmt

local function init_table()
  assert_boolean(table_exists(cnn))
  assert_equal(CREATE_TABLE_RETURN_VALUE, ensure_table(cnn))
  assert_true(table_exists(cnn))
  stmt = assert(cnn:statement())
  assert_true(cnn:setautocommit(false))
  assert_true(stmt:prepare("insert into " .. TEST_TABLE_NAME .. "(f1) values(?)"))
  for i = 1, 100 do
    assert_true(stmt:bindnum(1, i))
    assert_equal(1, stmt:execute())
    assert_true(stmt:closed())
  end
  assert_true( cnn:commit()   )
  assert_true( stmt:reset()   )
  assert_true(cnn:setautocommit(true))
  assert_equal(100, stmt:execute("select count(*) from " .. TEST_TABLE_NAME):fetch() )
  assert_true( stmt:destroy() )
end

local function fin_table()
  assert_equal(DROP_TABLE_RETURN_VALUE, drop_table(cnn))
  assert_false(table_exists(cnn))
end

function teardown()
  if cnn and cnn:connected() then drop_table(cnn) end
  if stmt then stmt:destroy() end
  if cnn then cnn:destroy() end
  if env then env:destroy() end
  cnn = nil
  env = nil
end

function setup()
  env, cnn = do_connect()
  assert_not_nil(env, cnn)
end

local sql = "select f1 from " .. TEST_TABLE_NAME

local function open_stmt(autodestroy)
  if stmt:destroyed() then stmt = cnn:statement() end
  stmt:setdestroyonclose(false)
  if not stmt:closed() then stmt:close() end
  stmt:setdestroyonclose(autodestroy)

  assert_equal(stmt, stmt:execute(sql))
  assert_false(stmt:closed())
  return stmt
end

local function make_fn(fn)
  return setmetatable({},{__call = function(self, ...) return fn(...) end})
end

function test_call()
  local function assert_table_(v) 
    assert_table(v)
    return 1
  end

  local function assert_string_(v) 
    assert_string(v)
    return 1
  end

  local function rise_error() error('some error') end

  local function inner_test (assert_table_, assert_string_)

    open_stmt():foreach(assert_string_)             assert_true (stmt:closed())

    open_stmt():foreach(false, assert_string_)      assert_false(stmt:closed())
    stmt:foreach(nil, assert_string_)               assert_true (stmt:closed())

    open_stmt():foreach('', assert_table_)          assert_true (stmt:closed())
    open_stmt():foreach('', assert_table_)          assert_true (stmt:closed())

    open_stmt():foreach(nil, nil, assert_string_)   assert_true (stmt:closed())
    open_stmt():foreach('', nil, assert_table_)     assert_true (stmt:closed())
    open_stmt():foreach(nil, false, assert_string_) assert_false(stmt:closed())

    assert_error_match('some error', function() open_stmt():foreach(rise_error) end)
    assert_true (stmt:closed())
    assert_error_match('some error', function() open_stmt():foreach(false, rise_error) end)
    assert_false (stmt:closed())

    assert_true(stmt:close())
  end

  local function inner_test2 (assert_table_, assert_string_)

    open_stmt(true):foreach(assert_string_)             assert_true (stmt:destroyed())

    open_stmt(true):foreach(false, assert_string_)      assert_false(stmt:destroyed()) assert_false(stmt:closed())
    stmt:foreach(nil, assert_string_)               assert_true (stmt:destroyed())

    open_stmt(true):foreach('', assert_table_)          assert_true (stmt:destroyed())
    open_stmt(true):foreach('', assert_table_)          assert_true (stmt:destroyed())

    open_stmt(true):foreach(nil, nil, assert_string_)   assert_true (stmt:destroyed())
    open_stmt(true):foreach('', nil, assert_table_)     assert_true (stmt:destroyed())
    open_stmt(true):foreach(nil, false, assert_string_) assert_false(stmt:destroyed()) assert_false(stmt:closed())

    assert_error_match('some error', function() open_stmt(true):foreach(rise_error) end)
    assert_true (stmt:destroyed())
    assert_error_match('some error', function() open_stmt(true):foreach(false, rise_error) end)
    assert_false(stmt:destroyed())
    assert_false(stmt:closed())

    assert_true(stmt:close())
  end

  init_table()
  stmt = assert(cnn:statement())
  inner_test (        assert_table_,          assert_string_)
  inner_test (make_fn(assert_table_), make_fn(assert_string_))
  inner_test2(        assert_table_,          assert_string_)
  inner_test2(make_fn(assert_table_), make_fn(assert_string_))
  assert_true(stmt:destroy())
  fin_table()
end

function test_cover()
  init_table()
  stmt = assert(cnn:statement())
  stmt = open_stmt()
  assert_error(function() stmt:execute(sql) end)

  local t = {}
  local cnt = return_count(stmt:foreach(function(f1)
    local n = tonumber(f1)
    assert_number(n)
    t[n] = true
  end))

  assert_equal(100, #t)
  assert_true(stmt:closed())
  assert_equal(0, cnt)
  assert_true(stmt:destroy())
  fin_table()
end

function test_return()
  init_table()
  stmt = assert(cnn:statement())

  assert_equal3(2, true, 'match',
    return_count(open_stmt():foreach(function(f1)
      if f1 == '50' then return true, 'match' end
    end))
  )
  assert_true(stmt:closed())

  assert_equal2(1, nil, 
    return_count(open_stmt():foreach(function(f1)
      if f1 == '50' then return nil end
    end))
  )
  assert_true(stmt:closed())

  assert_true(stmt:destroy())
  fin_table()
end

function test_error()
  init_table()
  stmt = assert(cnn:statement())

  assert_error(function() open_stmt():foreach(function() error('some error') end) end)
  assert_true(stmt:closed())

  assert_true(stmt:destroy())
  fin_table()
end



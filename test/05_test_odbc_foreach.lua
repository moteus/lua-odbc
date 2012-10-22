require "config"
local lunit = require "lunitx"

local TEST_NAME = 'Statement foreach'
if _VERSION >= 'Lua 5.2' then  _ENV = lunit.module(TEST_NAME,'seeall')
else module( TEST_NAME, package.seeall, lunit.testcase ) end

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
function test_1()
  init_table()
  stmt = assert(cnn:statement())
  assert_equal(stmt, stmt:execute(sql))
  assert_false(stmt:closed())

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

function test_2()
  init_table()
  stmt = assert(cnn:statement())
  assert_equal(stmt, stmt:execute(sql))
  assert_false(stmt:closed())

  local cnt, flag, str = return_count(stmt:foreach(function(f1)
    if f1 == '50' then return true, 'match' end
  end))
  assert_true(stmt:closed())
  assert_equal(2, cnt)
  assert_true(flag)
  assert_equal('match', str)

  assert_equal(stmt, stmt:execute(sql))
  assert_false(stmt:closed())

  local cnt, flag = return_count(stmt:foreach(function(f1)
    if f1 == '50' then return nil end
  end))
  assert_true(stmt:closed())
  assert_equal(1, cnt)
  assert_nil(flag)


  assert_true(stmt:destroy())
  fin_table()
end

function test_3()
  init_table()
  stmt = assert(cnn:statement())
  assert_equal(stmt, stmt:execute(sql))
  assert_false(stmt:closed())

  assert_error(function() stmt:foreach(function() error('some error') end) end)
  assert_true(stmt:closed())


  assert_equal(stmt, stmt:execute(sql))
  assert_false(stmt:closed())
  local cnt, flag = return_count(stmt:foreach(false, function() return true end))
  assert_false(stmt:closed())
  assert_equal(1, cnt)
  assert_true(flag)

  assert_true(stmt:destroy())
  fin_table()
end



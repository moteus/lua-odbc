require "config"
require "tools"

local lunit = require "lunitx"

local TEST_NAME = 'Statement multi resultset'
if _VERSION >= 'Lua 5.2' then  _ENV = lunit.module(TEST_NAME,'seeall')
else module( TEST_NAME, package.seeall, lunit.testcase ) end

local env, cnn, stmt

function teardown()
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

function FETCH_AND_ASSERT(cur)
  local cols = assert_table(cur:colnames())
  assert_equal(2, #cols)
  assert_equal('INTVAL1', cols[1]:upper())
  assert_equal('INTVAL2', cols[2]:upper())

  local t = {}
  assert_equal(t, cur:fetch(t,"n"))
  assert_equal(1, t[1])
  assert_equal(2, t[2])

  local a, b = cur:fetch()
  assert_equal(11, a)
  assert_equal(12, b)

  local c2 = cur:nextresultset()
  assert_equal(cur, c2)

  cols = assert_table(cur:colnames())
  assert_equal(3, #cols)
  assert_equal('STRVAL1', cols[1]:upper())
  assert_equal('STRVAL2', cols[2]:upper())
  assert_equal('STRVAL3', cols[3]:upper())

  assert_equal(t, cur:fetch(t,"n"))
  assert_equal('hello', t[1])
  assert_equal('world', t[2])
  assert_equal('!!!',   t[3])

  local a,b,c = cur:fetch()
  assert_equal('some',  a)
  assert_equal('other', b)
  assert_equal('row',   c)

  assert_false(cur:nextresultset())
  assert_true(cur:close())
end

local sql = [[begin 
  select 1 as IntVal1, 2 as IntVal2
  union all
  select 11, 12;
  select 'hello' as StrVal1, 'world' as StrVal2, '!!!' as StrVal3
  union all
  select 'some', 'other', 'row';
end]]

function test_1()
  stmt = assert(cnn:statement())
  FETCH_AND_ASSERT( assert(stmt:execute(sql)) )
  assert_true(stmt:prepare(sql))
  FETCH_AND_ASSERT( assert(stmt:execute()) )
  stmt:destroy()
end


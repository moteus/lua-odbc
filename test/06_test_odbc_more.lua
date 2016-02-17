require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local function SKIP(msg) return function() lunit.skip(msg) end end

local arg = {...}

local _ENV = TEST_CASE'Statement multi resultset'
if DBMS == 'PgSQL' then test = SKIP(DBMS .. " does not support batch query")
else

local env, cnn, stmt

function teardown()
  if stmt then stmt:destroy() end

  drop_proc(cnn)

  if cnn then cnn:destroy() end
  if env then env:destroy() end
  cnn = nil
  env = nil
end

function setup()
  env, cnn = do_connect()
  assert_not_nil(env, cnn)
  if TEST_PROC_CREATE_MULTI_RS then
    drop_proc(cnn)
    assert(exec_ddl(cnn, TEST_PROC_CREATE_MULTI_RS))
  end
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

  if DBMS ~= 'MySQL' then
    assert_false(cur:nextresultset())
    assert_true(cur:close())
    return
  end

  local res = cur:nextresultset()
  if res == false then
    assert_true(cur:close())
    return;
  end
  assert_false(cur:nextresultset())
  assert_true(cur:close())
  skip("FIXME :MySQL nextresultset return one more reusltset")
end

local sql = [[
begin
  select 1 as IntVal1, 2 as IntVal2
  union all
  select 11, 12;

  select 'hello' as StrVal1, 'world' as StrVal2, '!!!' as StrVal3
  union all
  select 'some', 'other', 'row';
end
]]

function test_exec()
  stmt = assert(cnn:statement())
  if TEST_PROC_CREATE_MULTI_RS then
    sql = assert(TEST_PROC_CALL_MULTI_RS)
  end
  FETCH_AND_ASSERT( assert(stmt:execute(sql)) )
  stmt:destroy()
end

function test_prepared()
  stmt = assert(cnn:statement())
  if TEST_PROC_CREATE_MULTI_RS then
    sql = assert(TEST_PROC_CALL_MULTI_RS)
  end
  assert_true(stmt:prepare(sql))
  FETCH_AND_ASSERT( assert(stmt:execute()) )
  stmt:destroy()
end

end

local_run_test(arg)
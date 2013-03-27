local CreateConnect = {
  ["odbc.dba"] = function()
    local dba = require "odbc.dba"
    local cnn, err = dba.connect{
      Driver   = "SQLite3 ODBC Driver";
      Database = ":memory:";
    }
    if cnn then cnn:setautoclosestmt(true) end
    return cnn, err
  end;

  ["lsql"] = function()
    local dba = require "dba.luasql".load('sqlite3')
    return dba.Connect(":memory:")
  end;

  ["odbc.luasql"] = function()
    local dba = require "dba"
    local luasql = require "odbc.luasql"
    dba = dba.load(luasql.odbc)
    return dba.Connect("SQLite3memory")
  end
}

local CNN_TYPE = 'lsql'
local CNN_ROWS = 10
local function init_db(cnn)
  local fmt = string.format
  assert(cnn:exec"create table Agent(ID INTEGER PRIMARY KEY, Name char(32))")
  for i = 1, CNN_ROWS do
    assert(cnn:exec(fmt("insert into Agent(ID,NAME)values(%d, 'Agent#%d')", i, i)))
  end
end

local function pack_n(...)
  return { n = select("#", ...), ... }
end

local to_n = tonumber

local lunit = require "lunit"

local TEST_NAME = 'Connection'
if _VERSION >= 'Lua 5.2' then  _ENV = lunit.module(TEST_NAME,'seeall')
else module( TEST_NAME, package.seeall, lunit.testcase ) end

local cnn

function setup()
  cnn = assert(CreateConnect[CNN_TYPE]())
  init_db(cnn)
end

function teardown()
  if cnn then cnn:destroy() end
end

function test_reconnect()
  assert_true(cnn:connected())
  assert_true(cnn:disconnect())
  assert_false(not not cnn:connected())
  assert_true(not not cnn:connect())
end

function test_each()
  local sql = "select ID, Name from Agent order by ID"
  local n = 0
  cnn:each(sql, function(ID, Name) 
    n = n + 1
    assert_equal(n, to_n(ID))
  end)
  assert_equal(CNN_ROWS, n)

  n = 0
  cnn:ieach(sql, function(row) 
    n = n + 1
    assert_equal(n, to_n(row[1]))
  end)
  assert_equal(CNN_ROWS, n)

  n = 0
  cnn:neach(sql, function(row) 
    n = n + 1
    assert_equal(n, to_n(row.ID))
  end)
  assert_equal(CNN_ROWS, n)

  n = 0
  cnn:teach(sql, function(row) 
    n = n + 1
    assert_equal(n, to_n(row.ID))
    assert_equal(n, to_n(row[1]))
  end)
  assert_equal(CNN_ROWS, n)

  n = 0
  local args = pack_n(cnn:each(sql, function(ID, Name) 
    n = n + 1
    return nil, 1, nil, 2
  end))
  assert_equal(1, n)
  assert_equal(4, args.n)
  assert_equal(1, args[2])
  assert_equal(2, args[4])
  assert_nil(args[1])
  assert_nil(args[3])

  n = 0
  sql = "select ID, Name from Agent where ID > :ID order by ID"
  local par = {ID = 1}
  assert_true(cnn:each(sql, par, function(ID)
    n = n + 1
    assert_equal(par.ID + 1, to_n(ID))
    return true
  end))
  assert_equal(1, n)
end

function test_rows()
  local sql = "select ID, Name from Agent order by ID"
  local n = 0
  for ID, Name in cnn:rows(sql) do
    n = n + 1
    assert_equal(n, to_n(ID))
  end
  assert_equal(CNN_ROWS, n)

  n = 0
  for row in cnn:irows(sql) do
    n = n + 1
    assert_equal(n, to_n(row[1]))
  end
  assert_equal(CNN_ROWS, n)

  n = 0
  for row in cnn:nrows(sql) do
    n = n + 1
    assert_equal(n, to_n(row.ID))
  end
  assert_equal(CNN_ROWS, n)

  n = 0
  for row in cnn:trows(sql) do
    n = n + 1
    assert_equal(n, to_n(row.ID))
    assert_equal(n, to_n(row[1]))
  end
  assert_equal(CNN_ROWS, n)

  n = 0
  sql = "select ID, Name from Agent where ID > :ID order by ID"
  local par = {ID = 1}
  for ID in cnn:rows(sql, par) do
    n = n + 1
    assert_equal(par.ID + 1, to_n(ID))
    break
  end
  assert_equal(1, n)
end

function test_first()
  local sql = "select ID, Name from Agent order by ID"
  local ID, Name = cnn:first_row(sql)
  assert_equal(1, to_n(ID))
  assert_equal("Agent#1", Name)

  local row
  row = cnn:first_nrow(sql)
  assert_equal(1, to_n(row.ID))
  assert_equal("Agent#1", row.Name)

  row = cnn:first_irow(sql)
  assert_equal(1, to_n(row[1]))
  assert_equal("Agent#1", row[2])

  row = cnn:first_trow(sql)
  assert_equal(1, to_n(row[1]))
  assert_equal(1, to_n(row.ID))
  assert_equal("Agent#1", row[2])
  assert_equal("Agent#1", row.Name)

  assert_equal(CNN_ROWS, to_n(cnn:first_value("select count(*) from Agent")))
  assert_equal(CNN_ROWS, to_n(cnn:first_value("select ID from Agent where ID=:ID",{ID=CNN_ROWS})))
end

function test_txn()
  assert_equal(CNN_ROWS, to_n(cnn:first_value("select count(*) from Agent")))
  cnn:set_autocommit(false)
  assert_number(cnn:exec("delete from Agent"))
  assert_equal(0, to_n(cnn:first_value("select count(*) from Agent")))
  cnn:rollback()
  assert_equal(CNN_ROWS, to_n(cnn:first_value("select count(*) from Agent")))
end

function test_rowsaffected()
  assert_equal(CNN_ROWS, to_n(cnn:first_value("select count(*) from Agent")))
end

function test_exec()
  assert_nil(cnn:exec("select ID, Name from Agent order by ID"))
  assert_number(cnn:exec("update Agent set ID=ID"))
end

local TEST_NAME = 'Query'
if _VERSION >= 'Lua 5.2' then  _ENV = lunit.module(TEST_NAME,'seeall')
else module( TEST_NAME, package.seeall, lunit.testcase ) end

local cnn, qry

function setup()
  cnn = assert(CreateConnect[CNN_TYPE]())
  init_db(cnn)
end

function teardown()
  if qry then qry:destroy() end
  if cnn then cnn:destroy() end
end

function test_create()
  local sql = "select ID, Name from Agent order by ID"
  local n
  local function do_test(ID, Name) 
    n = n + 1
    assert_equal(n, to_n(ID))
  end

  n = 0
  qry = assert(cnn:query())
  qry:each(sql, do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  qry:each(do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  sql = "select ID, Name from Agent where 555=cast(:ID as INTEGER) order by ID"
  local par = {ID = 555}

  n = 0
  qry = assert(cnn:query())
  qry:each(sql, par, do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  qry:each(par, do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  assert_true(qry:bind(par))
  qry:each(do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  --------------------------------------------------------
  sql = "select ID, Name from Agent order by ID"
  local function do_test(row) 
    n = n + 1
    assert_equal(n, to_n(row.ID))
  end

  n = 0
  qry = assert(cnn:query())
  qry:neach(sql, do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  qry:neach(do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  sql = "select ID, Name from Agent where 555=cast(:ID as INTEGER) order by ID"
  local par = {ID = 555}

  n = 0
  qry = assert(cnn:query())
  qry:neach(sql, par, do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  qry:neach(par, do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  assert_true(qry:bind(par))
  qry:neach(do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

end

function test_rows()
  local sql = "select ID, Name from Agent order by ID"
  local n

  n = 0
  qry = assert(cnn:query())
  for ID, Name in qry:rows(sql) do
    n = n + 1 assert_equal(n, to_n(ID))
  end
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  for ID, Name in qry:rows() do
    n = n + 1 assert_equal(n, to_n(ID))
  end
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  sql = "select ID, Name from Agent where 555=cast(:ID as INTEGER) order by ID"
  local par = {ID = 555}

  n = 0
  qry = assert(cnn:query())
  for ID, Name in qry:rows(sql, par) do
    n = n + 1 assert_equal(n, to_n(ID))
  end
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  for ID, Name in qry:rows(par) do
    n = n + 1 assert_equal(n, to_n(ID))
  end
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  assert_true(qry:bind(par))
  for ID, Name in qry:rows() do
    n = n + 1 assert_equal(n, to_n(ID))
  end
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  --------------------------------------------------

  sql = "select ID, Name from Agent order by ID"

  n = 0
  qry = assert(cnn:query())
  for row in qry:nrows(sql) do
    n = n + 1 assert_equal(n, to_n(row.ID))
  end
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  for row in qry:nrows() do
    n = n + 1 assert_equal(n, to_n(row.ID))
  end
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  sql = "select ID, Name from Agent where 555=cast(:ID as INTEGER) order by ID"
  local par = {ID = 555}

  n = 0
  qry = assert(cnn:query())
  for row in qry:nrows(sql, par) do
    n = n + 1 assert_equal(n, to_n(row.ID))
  end
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  for row in qry:nrows(par) do
    n = n + 1 assert_equal(n, to_n(row.ID))
  end
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:query(sql))
  assert_true(qry:bind(par))
  for row in qry:nrows() do
    n = n + 1 assert_equal(n, to_n(row.ID))
  end
  assert_equal(CNN_ROWS, n)
  qry:destroy()

end

function test_prepare()
  local sql = "select ID, Name from Agent order by ID"
  local n
  local function do_test(ID, Name) 
    n = n + 1
    assert_equal(n, to_n(ID))
  end

  n = 0
  qry = assert(cnn:prepare(sql))
  qry:each(do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  sql = "select ID, Name from Agent where 555 = cast(:ID as INTEGER) order by ID"
  local par = {ID = 555}

  n = 0
  qry = assert(cnn:prepare(sql))
  qry:each(par, do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()

  n = 0
  qry = assert(cnn:prepare(sql))
  assert_true(qry:bind(par))
  qry:each(do_test)
  assert_equal(CNN_ROWS, n)
  qry:destroy()
end

function test_destroy()
  qry = assert(cnn:query())
  assert_true(qry:closed())
  assert_false(qry:destroyed())
  qry:open("select ID, Name from Agent order by ID")
  assert_false(qry:closed())
  assert_false(qry:destroyed())
  assert_pass(function() cnn:destroy() end)
  assert_pass(function() qry:closed()  end)
  assert_true(qry:closed())
  assert_true(qry:destroyed())
  assert_pass(function() qry:destroy() end)
end

function test_first()
  local sql = "select ID, Name from Agent order by ID"
  qry = cnn:query()
  local ID, Name = qry:first_row(sql)
  assert_equal(1, to_n(ID))
  assert_equal("Agent#1", Name)

  local row
  row = qry:first_nrow(sql)
  assert_equal(1, to_n(row.ID))
  assert_equal("Agent#1", row.Name)

  row = qry:first_irow(sql)
  assert_equal(1, to_n(row[1]))
  assert_equal("Agent#1", row[2])

  row = qry:first_trow(sql)
  assert_equal(1, to_n(row[1]))
  assert_equal(1, to_n(row.ID))
  assert_equal("Agent#1", row[2])
  assert_equal("Agent#1", row.Name)

  local v = assert(qry:first_value("select count(*) from Agent"))
  assert_equal(CNN_ROWS, to_n(v))
  local v = assert(qry:first_value("select ID from Agent where ID=:ID",{ID=CNN_ROWS}))
  assert_equal(CNN_ROWS, to_n(v))
  qry:destroy()

  sql = "select ID, Name from Agent where ID=:ID"
  local par = {ID=CNN_ROWS}
  local Agent = "Agent#" .. CNN_ROWS

  qry = cnn:prepare(sql)

  ID, Name = qry:first_row(par)
  assert_equal(CNN_ROWS, to_n(ID))
  assert_equal(Agent, Name)

  row = qry:first_nrow(par)
  assert_equal(CNN_ROWS, to_n(row.ID))
  assert_equal(Agent, row.Name)

  row = qry:first_irow(par)
  assert_equal(CNN_ROWS, to_n(row[1]))
  assert_equal(Agent, row[2])

  row = qry:first_trow(par)
  assert_equal(CNN_ROWS, to_n(row[1]))
  assert_equal(CNN_ROWS, to_n(row.ID))
  assert_equal(Agent, row[2])
  assert_equal(Agent, row.Name)

  qry:destroy()

  qry = cnn:prepare(sql)
  assert_true(qry:bind(par))

  ID, Name = qry:first_row()
  assert_equal(CNN_ROWS, to_n(ID))
  assert_equal(Agent, Name)

  row = qry:first_nrow()
  assert_equal(CNN_ROWS, to_n(row.ID))
  assert_equal(Agent, row.Name)

  row = qry:first_irow()
  assert_equal(CNN_ROWS, to_n(row[1]))
  assert_equal(Agent, row[2])

  row = qry:first_trow()
  assert_equal(CNN_ROWS, to_n(row[1]))
  assert_equal(CNN_ROWS, to_n(row.ID))
  assert_equal(Agent, row[2])
  assert_equal(Agent, row.Name)

end

for _, str in ipairs{
  "odbc.dba",
  -- "lsql",
  -- "odbc.luasql",
} do 
  print()
  print("---------------- TEST " .. str)
  CNN_TYPE = str
  lunit.run()
end

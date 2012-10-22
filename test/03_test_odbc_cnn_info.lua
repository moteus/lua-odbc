require "config"
local lunit = require "lunitx"

local TEST_NAME = 'Connection test info'
if _VERSION >= 'Lua 5.2' then  _ENV = lunit.module(TEST_NAME,'seeall')
else module( TEST_NAME, package.seeall, lunit.testcase ) end

local function assert_opt_string(v, ...)
  if v == nil then return v, ... end
  return assert_string(v, ...)
end

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

function test_info()
  local SQL_KEYWORDS = 89
  local SQL_TXN_CAPABLE = 46
  local str = cnn:strinfo(SQL_KEYWORDS)
  assert_string(str)
  assert_equal(str, cnn:SQLKeywords())
  assert_number(cnn:uint16info(SQL_TXN_CAPABLE))
  assert_opt_string(cnn:getcatalog())
  assert_boolean(cnn:gettrace())
  assert_opt_string(cnn:gettracefile())
end

function test_catalog()
  stmt = assert(cnn:catalogs())
  stmt:foreach(function(catalog, schema, table, type)
    assert_string(catalog)
    assert_nil(schema)
    assert_nil(table)
    assert_nil(type)
  end)
  stmt:destroy()

  stmt = assert(cnn:schemas())
  stmt:foreach(function(catalog, schema, table, type)
    assert_nil(catalog)
    assert_string(schema)
    assert_nil(table)
    assert_nil(type)
  end)
  stmt:destroy()

  stmt = assert(cnn:tabletypes())
  stmt:foreach(function(catalog, schema, table, type)
    assert_nil(catalog)
    assert_nil(schema)
    assert_nil(table)
    assert_string(type)
  end)
  stmt:destroy()

  stmt = assert(cnn:tables())
  stmt:foreach(function(catalog, schema, table, type)
    assert_opt_string(catalog)
    assert_opt_string(schema)
    assert_string(table)
    assert_opt_string(type)
  end)
  stmt:destroy()
end

function test_create_table()
  assert_boolean(table_exists(cnn))
  assert_equal(CREATE_TABLE_RETURN_VALUE, ensure_table(cnn))
  assert_true(table_exists(cnn))
  assert_equal(DROP_TABLE_RETURN_VALUE, drop_table(cnn))
  assert_false(table_exists(cnn))
end


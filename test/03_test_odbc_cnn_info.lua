require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local arg = {...}

local _ENV = TEST_CASE'Connection test info'

local function assert_null_or_string(v, ...)
  if v == odbc.NULL then return v, ... end
  return assert_string(v, ...)
end

local function assert_opt_string(v, ...)
  if v == nil then return v, ... end
  return assert_string(v, ...)
end

local function assert_null(v, ...)
  return assert_equal(odbc.NULL, v, ...)
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
    assert_null(schema)
    assert_null(table)
    assert_null(type)
  end)
  stmt:destroy()

  stmt = assert(cnn:schemas())
  stmt:foreach(function(catalog, schema, table, type)
    assert_null(catalog)
    assert_string(schema)
    assert_null(table)
    assert_null(type)
  end)
  stmt:destroy()

  stmt = assert(cnn:tabletypes())
  stmt:foreach(function(catalog, schema, table, type)
    assert_null(catalog)
    assert_null(schema)
    assert_null(table)
    assert_string(type)
  end)
  stmt:destroy()

  stmt = assert(cnn:tables())
  stmt:foreach(function(catalog, schema, table, type)
    assert_null_or_string(catalog)
    assert_null_or_string(schema)
    assert_string(table)
    assert_null_or_string(type)
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

local_run_test(arg)
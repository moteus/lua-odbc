require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local arg = {...}

local _ENV = TEST_CASE'Library test'

function test_version()
  local n, major, minor, patch, comment = return_count(odbc.version())
  assert_number(major)
  assert_number(minor)
  assert_number(patch)

  local ver = table.concat({major, minor, patch}, '.')
  if n ~= 3 then
    assert_equal(4, n)
    assert_string(comment)
    ver = ver .. "-" .. comment
  end

  assert_equal(ver, odbc._VERSION)
end

local_run_test(arg)
require "config"
require "tools"

local lunit = require "lunitx"

local TEST_NAME = 'Value test'
if _VERSION >= 'Lua 5.2' then  _ENV = lunit.module(TEST_NAME,'seeall')
else module( TEST_NAME, package.seeall, lunit.testcase ) end

function setup()
end

function teardown()
end

local types = {
  'ubigint', 'sbigint', 'utinyint', 'stinyint', 'ushort', 'sshort', 
  'ulong', 'slong', 'float', 'double', 'date', 'time', 'bit',
  'char', 'binary',
}

function test_ctor()
  local val;
  for i, tname in ipairs(types)do 
    val = odbc[tname]()    assert_true(val:is_null())
    val = odbc[tname](nil) assert_true(val:is_null())
    if(tname ~= 'bit')then
      assert_error('fail empty assign to ' .. tname, function() val:set() end)
      assert_error('fail nil   assign to ' .. tname, function() val:set(nil) end)
    else
      val:set(true) val:set()    assert_false(val:get())
      val:set(true) val:set(nil) assert_false(val:get())
    end
  end
end


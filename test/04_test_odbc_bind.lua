require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local arg = {...}

local TEST_NAME = 'Statement bind'
if _VERSION >= 'Lua 5.2' then  _ENV = lunit.module(TEST_NAME,'seeall')
else module( TEST_NAME, package.seeall, lunit.testcase ) end

local function assert_opt_string(v, ...)
  if v == nil then return v, ... end
  return assert_string(v, ...)
end

local env, cnn, stmt

function teardown()
  if cnn and cnn:connected() then drop_proc(cnn) end
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

local x = function(b)
  return(string.gsub(b, "([a-fA-F0-9][a-fA-F0-9])", function(v)
    return string.char( tonumber(v, 16) )
  end))
end


local inIntVal     = -0x7FFFFFFF
local inUIntVal    = 0xFFFFFFFF
local inDoubleVal  = 1234.235664879123456
local inStringVal  = "Hello world"
local inBinaryVal  = "\000\001\002\003"
local inDateVal    = "2011-01-01"
local inNullVal    = nil
local inDefaultVal = 1234
local inBoolVal    = true
local inGuidVal    = 'B1BB49A2B4014413BEBB7ACD10399875'

local function get_int()
  return inIntVal;
end
local function get_uint()
  return inUIntVal
end
local function get_double()
  return inDoubleVal
end
local function get_date()
  return inDateVal
end
local function get_bool()
  return inBoolVal
end
local function create_get_bin_by(str,n)
  local pos = 1
  return function()
    local data = str:sub(pos,pos+n-1)
    pos = pos + n
    return data
  end
end
local function get_uuid()
  return inGuidVal
end

local function EXEC_AND_ASSERT(qrySQL)
  if qrySQL then assert_equal(stmt, stmt:execute(qrySQL))
  else assert_equal(stmt, stmt:execute()) end

  assert_equal(1, stmt:rowcount())
  local outIntVal, outUIntVal, outDoubleVal, outStringVal,
  outBinaryVal, outDateVal, outNullVal, outDefaultVal,
  outBoolVal,outGuidVal = stmt:fetch()

  stmt:close()

  -- print()
  -- print('IntVal     =', outIntVal      )
  -- print('UIntVal    =', outUIntVal     )
  -- print('DoubleVal  =', outDoubleVal   )
  -- print('StringVal  =', outStringVal   )
  -- print('BinaryVal  =', outBinaryVal   )
  -- print('DateVal    =', outDateVal     )
  -- print('NullVal    =', outNullVal     )
  -- print('DefaultVal =', outDefaultVal  )
  -- print('BoolVal    =', outBoolVal     )
  -- print('GuidVal    =', outGuidVal     )
  -- print"================================="

  assert_equal(inIntVal    , outIntVal      )
  assert_equal(inUIntVal   , outUIntVal     )
  assert_equal(inDoubleVal , outDoubleVal   )
  assert_equal(inStringVal , outStringVal   )
  assert_equal(inBinaryVal , outBinaryVal   )
  assert_equal(inDateVal   , outDateVal     )
  assert_equal(inNullVal   , outNullVal     )
  assert_equal(inDefaultVal, outDefaultVal  )
  assert_equal(inBoolVal   , outBoolVal     )
  assert_equal(inGuidVal   , outGuidVal     )
end

local function VEXEC_AND_ASSERT(qrySQL)
  if qrySQL then assert_equal(stmt, stmt:execute(qrySQL))
  else assert_equal(stmt, stmt:execute()) end

  local outIntVal     = assert( odbc.slong()              :bind_col(stmt, 1  ) )
  local outUIntVal    = assert( odbc.ulong()              :bind_col(stmt, 2  ) )
  local outDoubleVal  = assert( odbc.double()             :bind_col(stmt, 3  ) )
  local outStringVal  = assert( odbc.char(#inStringVal)   :bind_col(stmt, 4  ) )
  local outBinaryVal  = assert( odbc.binary(#inBinaryVal) :bind_col(stmt, 5  ) )
  local outDateVal    = assert( odbc.date()               :bind_col(stmt, 6  ) )
  local outNullVal    = assert( odbc.utinyint()           :bind_col(stmt, 7  ) )
  local outDefaultVal = assert( odbc.ulong()              :bind_col(stmt, 8  ) )
  local outBoolVal    = assert( odbc.bit()                :bind_col(stmt, 9  ) )
  local outGuidVal    = assert( odbc.binary(#inGuidVal)   :bind_col(stmt, 10 ) )

  assert_true(stmt:vfetch())
  stmt:close()

  -- print()
  -- print('IntVal     =', outIntVal      :get())
  -- print('UIntVal    =', outUIntVal     :get())
  -- print('DoubleVal  =', outDoubleVal   :get())
  -- print('StringVal  =', outStringVal   :get())
  -- print('BinaryVal  =', outBinaryVal   :get())
  -- print('DateVal    =', outDateVal     :get())
  -- print('NullVal    =', outNullVal     :get())
  -- print('DefaultVal =', outDefaultVal  :get())
  -- print('BoolVal    =', outBoolVal     :get())
  -- print('GuidVal    =', outGuidVal     :get())
  -- print"================================="

  assert_equal(inIntVal    , outIntVal      :get())
  assert_equal(inUIntVal   , outUIntVal     :get())
  assert_equal(inDoubleVal , outDoubleVal   :get())
  assert_equal(inStringVal , outStringVal   :get())
  assert_equal(inBinaryVal , outBinaryVal   :get())
  assert_equal(inDateVal   , outDateVal     :get())
  assert_equal(inNullVal   , outNullVal     :get())
  assert_equal(inDefaultVal, outDefaultVal  :get())
  assert_equal(inBoolVal   , outBoolVal     :get())
  assert_equal(x(inGuidVal), outGuidVal     :get())
end


local function BIND(stmt)
  assert_true(stmt:bindnum    (1,inIntVal    ))
  assert_true(stmt:bindnum    (2,inUIntVal   ))
  assert_true(stmt:bindnum    (3,inDoubleVal ))
  assert_true(stmt:bindstr    (4,inStringVal ))
  assert_true(stmt:bindbin    (5,inBinaryVal ))
  assert_true(stmt:bindstr    (6,inDateVal   ))
  assert_true(stmt:bindnull   (7             ))
  assert_true(stmt:binddefault(8             ))
  assert_true(stmt:bindbool   (9,inBoolVal   ))
  assert_true(stmt:bindstr   (10,inGuidVal   ))
end

local function BIND_CB(stmt)
  assert_true(stmt:bindnum    (1, get_int    ))
  assert_true(stmt:bindnum    (2, get_uint   ))
  assert_true(stmt:bindnum    (3, get_double ))
  assert_true(stmt:bindstr    (4, create_get_bin_by(inStringVal,10)))
  assert_true(stmt:bindbin    (5, create_get_bin_by(inBinaryVal,10)))
  assert_true(stmt:bindstr    (6, get_date, #inDateVal   ))
  assert_true(stmt:bindnull   (7             ))
  assert_true(stmt:binddefault(8             ))
  assert_true(stmt:bindbool   (9, get_bool   ))
  assert_true(stmt:bindstr   (10, get_uuid   ,#inGuidVal))
end

function test_1()
  assert_boolean(proc_exists(cnn))
  assert(ensure_proc(cnn))
  assert_true(proc_exists(cnn))

  stmt = cnn:statement()
  assert_false(stmt:prepared())
  assert_nil(stmt:colnames())
  assert_nil(stmt:coltypes())
  assert_equal(-1, stmt:parcount())

  BIND(stmt)

  assert_equal(-1, stmt:parcount())
  EXEC_AND_ASSERT(TEST_PROC_CALL)
end

function test_2()
  assert_boolean(proc_exists(cnn))
  assert(ensure_proc(cnn))
  assert_true(proc_exists(cnn))

  stmt = cnn:statement()
  assert_true(stmt:prepare(TEST_PROC_CALL))
  assert_true(stmt:prepared())
  local col = assert_table(stmt:colnames())
  local typ = assert_table(stmt:coltypes())
  assert((stmt:parcount() == 10) or (stmt:parcount() == -1))

  BIND(stmt)

  EXEC_AND_ASSERT()
  
  assert_equal(col, stmt:colnames())
  assert_equal(typ, stmt:coltypes())

  assert_true(stmt:reset())
  assert_false(stmt:prepared())
  assert_nil(stmt:colnames())
  assert_nil(stmt:coltypes())
  assert_equal(-1, stmt:parcount())

  assert_true(stmt:destroy())
end

function test_3()
  assert_boolean(proc_exists(cnn))
  assert(ensure_proc(cnn))
  assert_true(proc_exists(cnn))

  stmt = cnn:statement()
  BIND_CB(stmt)
  EXEC_AND_ASSERT(TEST_PROC_CALL)
  assert_true(stmt:destroy())
end

function test_4()
  assert_boolean(proc_exists(cnn))
  assert(ensure_proc(cnn))
  assert_true(proc_exists(cnn))

  stmt = cnn:statement()
  assert_true(stmt:prepare(TEST_PROC_CALL))
  
  BIND_CB(stmt)
  EXEC_AND_ASSERT()
  assert_true(stmt:destroy())
end

function test_bind_value()
  local vIntVal     = odbc.slong(-0x7FFFFFFF)
  local vUIntVal    = odbc.ulong(0xFFFFFFFF)
  local vDoubleVal  = odbc.double(1234.235664879123456)
  local vStringVal  = odbc.char("Hello world")
  local vBinaryVal  = odbc.binary("\000\001\002\003")
  local vDateVal    = odbc.char("2011-01-01") -- sybase has error. for date : Cannot convert SQLDATETIME to a date
  local vNullVal    = odbc.utinyint()
  local vDefaultVal = odbc.ulong(1234)
  local vBoolVal    = odbc.bit(true)
  local vGuidVal    = odbc.binary(x'B1BB49A2B4014413BEBB7ACD10399875')

  assert_boolean(proc_exists(cnn))
  assert(ensure_proc(cnn))
  assert_true(proc_exists(cnn))

  stmt = cnn:statement()

  assert_equal(vIntVal     , vIntVal     :bind_param(stmt, 1  ))
  assert_equal(vUIntVal    , vUIntVal    :bind_param(stmt, 2  ))
  assert_equal(vDoubleVal  , vDoubleVal  :bind_param(stmt, 3  ))
  assert_equal(vStringVal  , vStringVal  :bind_param(stmt, 4  ))
  assert_equal(vBinaryVal  , vBinaryVal  :bind_param(stmt, 5  ))
  assert_equal(vDateVal    , vDateVal    :bind_param(stmt, 6, odbc.PARAM_INPUT, odbc.DATE))
  assert_equal(vNullVal    , vNullVal    :bind_param(stmt, 7  ))
  assert_equal(vDefaultVal , vDefaultVal :bind_param(stmt, 8  ))
  assert_equal(vBoolVal    , vBoolVal    :bind_param(stmt, 9  ))
  assert_equal(vGuidVal    , vGuidVal    :bind_param(stmt, 10 ))

  EXEC_AND_ASSERT(TEST_PROC_CALL)
  VEXEC_AND_ASSERT(TEST_PROC_CALL)

  stmt:prepare(TEST_PROC_CALL)
  assert_equal(vIntVal     , vIntVal     :bind_param(stmt, 1  ))
  assert_equal(vUIntVal    , vUIntVal    :bind_param(stmt, 2  ))
  assert_equal(vDoubleVal  , vDoubleVal  :bind_param(stmt, 3  ))
  assert_equal(vStringVal  , vStringVal  :bind_param(stmt, 4  ))
  assert_equal(vBinaryVal  , vBinaryVal  :bind_param(stmt, 5  ))
  assert_equal(vDateVal    , vDateVal    :bind_param(stmt, 6, odbc.PARAM_INPUT, odbc.DATE))
  assert_equal(vNullVal    , vNullVal    :bind_param(stmt, 7  ))
  assert_equal(vDefaultVal , vDefaultVal :bind_param(stmt, 8  ))
  assert_equal(vBoolVal    , vBoolVal    :bind_param(stmt, 9  ))
  assert_equal(vGuidVal    , vGuidVal    :bind_param(stmt, 10 ))

  EXEC_AND_ASSERT()
  VEXEC_AND_ASSERT()

  assert_true(stmt:destroy())
end

local_run_test(arg)
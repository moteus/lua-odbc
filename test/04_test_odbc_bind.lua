require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local skip      = assert(lunit.skip)
local TEST_CASE = assert(lunit.TEST_CASE)
local function SKIP(msg) return function() lunit.skip(msg) end end

local arg = {...}

local _ENV = TEST_CASE 'Statement bind'
if not TEST_PROC_CREATE then test = SKIP(DBMS .. " does not support SP with resultset")
else

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
local inNullVal    = odbc.NULL
local inDefaultVal = 1234
local inBoolVal    = true
local inGuidVal    = 'B1BB49A2B4014413BEBB7ACD10399875'
local inBigIntVal  = -0x7FFFFFFFFFFFFFFF

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
local function get_bigint()
  return inBigIntVal;
end

local function EXEC_AND_ASSERT(qrySQL)
  if qrySQL then assert_equal(stmt, stmt:execute(qrySQL))
  else assert_equal(stmt, stmt:execute()) end

  assert_equal(1, stmt:rowcount())
  local outIntVal, outUIntVal, outDoubleVal, outStringVal,
  outBinaryVal, outDateVal, outNullVal, outDefaultVal,
  outBoolVal,outGuidVal,outBigIntVal = stmt:fetch()

  if not PROC_SUPPORT_DEFAULT then
    outBoolVal, outGuidVal = outDefaultVal, outBoolVal, outGuidVal 
    outDefaultVal = "----"
  end

  local test_bin_val = inBinaryVal
  if DBMS == 'MySQL' then
    test_bin_val = inBinaryVal .. ('\0'):rep(#outBinaryVal - #inBinaryVal)
  end


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
  assert_equal(test_bin_val, outBinaryVal   )
  assert_equal(inDateVal   , outDateVal     )
  assert_equal(inNullVal   , outNullVal     )
  if PROC_SUPPORT_DEFAULT then
    assert_equal(inDefaultVal, outDefaultVal  )
  end
  assert_equal(inBoolVal   , outBoolVal     )
  if HAS_GUID_TYPE then
    assert_equal(inGuidVal   , outGuidVal     )
  end
  assert_equal(inBigIntVal   , outBigIntVal     )
end

local function VEXEC_AND_ASSERT(qrySQL)
  if qrySQL then assert_equal(stmt, stmt:execute(qrySQL))
  else assert_equal(stmt, stmt:execute()) end

  local i = 1
  local outIntVal     = assert( odbc.slong()              :bind_col(stmt, i ) ) i = i + 1
  local outUIntVal    = assert( odbc.ulong()              :bind_col(stmt, i ) ) i = i + 1
  local outDoubleVal  = assert( odbc.double()             :bind_col(stmt, i ) ) i = i + 1
  local outStringVal  = assert( odbc.char(#inStringVal)   :bind_col(stmt, i ) ) i = i + 1
  local outBinaryVal  = assert( odbc.binary(#inBinaryVal) :bind_col(stmt, i ) ) i = i + 1
  local outDateVal    = assert( odbc.date()               :bind_col(stmt, i ) ) i = i + 1
  local outNullVal    = assert( odbc.utinyint()           :bind_col(stmt, i ) ) i = i + 1
  local outDefaultVal if PROC_SUPPORT_DEFAULT then
    outDefaultVal = assert( odbc.ulong()              :bind_col(stmt, i ) ) i = i + 1
  end
  local outBoolVal    = assert( odbc.bit()                :bind_col(stmt, i ) ) i = i + 1
  local outGuidVal    if HAS_GUID_TYPE then
    outGuidVal    = assert( odbc.binary(#inGuidVal)   :bind_col(stmt, i ) ) i = i + 1
  end
  local outBigIntVal  = assert( odbc.sbigint()            :bind_col(stmt, i ) ) i = i + 1

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
  if PROC_SUPPORT_DEFAULT then
    assert_equal(inDefaultVal, outDefaultVal  :get())
  end
  assert_equal(inBoolVal   , outBoolVal     :get())
  if HAS_GUID_TYPE then
    assert_equal(x(inGuidVal), outGuidVal     :get())
  end
  assert_equal(inBigIntVal   , outBigIntVal :get())
end

local function BIND(stmt)
  local i = 1
  assert_true(stmt:bindnum    (i,inIntVal    )) i = i + 1
  assert_true(stmt:bindnum    (i,inUIntVal   )) i = i + 1
  assert_true(stmt:bindnum    (i,inDoubleVal )) i = i + 1
  assert_true(stmt:bindstr    (i,inStringVal )) i = i + 1
  assert_true(stmt:bindbin    (i,inBinaryVal )) i = i + 1
  assert_true(stmt:bindstr    (i,inDateVal   )) i = i + 1
  assert_true(stmt:bindnull   (i             )) i = i + 1
  if PROC_SUPPORT_DEFAULT then
    assert_true(stmt:binddefault(i           )) i = i + 1
  end
  assert_true(stmt:bindbool   (i,inBoolVal   )) i = i + 1
  assert_true(stmt:bindstr    (i,inGuidVal   )) i = i + 1
  assert_true(stmt:bindnum    (i,inBigIntVal )) i = i + 1
end

local function BIND_CB(stmt)
  local i = 1
  assert_true(stmt:bindnum    (i, get_int    ))                       i = i + 1
  assert_true(stmt:bindnum    (i, get_uint   ))                       i = i + 1
  assert_true(stmt:bindnum    (i, get_double ))                       i = i + 1
  assert_true(stmt:bindstr    (i, create_get_bin_by(inStringVal,10))) i = i + 1
  assert_true(stmt:bindbin    (i, create_get_bin_by(inBinaryVal,10))) i = i + 1
  assert_true(stmt:bindstr    (i, get_date, #inDateVal   ))           i = i + 1
  assert_true(stmt:bindnull   (i             ))                       i = i + 1
  if PROC_SUPPORT_DEFAULT then
    assert_true(stmt:binddefault(i             ))                     i = i + 1
  end
  assert_true(stmt:bindbool   (i, get_bool   ))                       i = i + 1
  if HAS_GUID_TYPE then
    assert_true(stmt:bindstr    (i, get_uuid   ,#inGuidVal))          i = i + 1
  end

  -- Use `bindint` instead of `bindnum` because there no way to detect value type
  -- before execute so with callback we have to point value type explicity
  assert_true(stmt:bindint    (i, get_bigint   ))                     i = i + 1
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
  if DBMS == 'MySQL' then return skip'MySQL does not supported' end

  assert_boolean(proc_exists(cnn))
  assert(ensure_proc(cnn))
  assert_true(proc_exists(cnn))

  stmt = cnn:statement()
  assert_true(stmt:prepare(TEST_PROC_CALL))
  assert_true(stmt:prepared())
  local col = assert_table(stmt:colnames())
  local typ = assert_table(stmt:coltypes())
  assert((stmt:parcount() == 11) or (stmt:parcount() == -1))

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
  local vBigIntVal  = odbc.sbigint(-0x7FFFFFFFFFFFFFFF)

  assert_boolean(proc_exists(cnn))
  assert(ensure_proc(cnn))
  assert_true(proc_exists(cnn))

  stmt = cnn:statement()

  local i = 1
  assert_equal(vIntVal     , vIntVal     :bind_param(stmt, i  )) i = i + 1
  assert_equal(vUIntVal    , vUIntVal    :bind_param(stmt, i  )) i = i + 1
  assert_equal(vDoubleVal  , vDoubleVal  :bind_param(stmt, i  )) i = i + 1
  assert_equal(vStringVal  , vStringVal  :bind_param(stmt, i  )) i = i + 1
  assert_equal(vBinaryVal  , vBinaryVal  :bind_param(stmt, i  )) i = i + 1
  assert_equal(vDateVal    , vDateVal    :bind_param(stmt, i, odbc.PARAM_INPUT, odbc.DATE)) i = i + 1
  assert_equal(vNullVal    , vNullVal    :bind_param(stmt, i  )) i = i + 1
  if PROC_SUPPORT_DEFAULT then
    assert_equal(vDefaultVal , vDefaultVal :bind_param(stmt, i  )) i = i + 1
  end
  assert_equal(vBoolVal    , vBoolVal    :bind_param(stmt, i  )) i = i + 1
  assert_equal(vGuidVal    , vGuidVal    :bind_param(stmt, i  )) i = i + 1
  assert_equal(vBigIntVal  , vBigIntVal  :bind_param(stmt, i  )) i = i + 1

  EXEC_AND_ASSERT(TEST_PROC_CALL)
  VEXEC_AND_ASSERT(TEST_PROC_CALL)

  stmt:prepare(TEST_PROC_CALL)

  local i = 1
  assert_equal(vIntVal     , vIntVal     :bind_param(stmt, i  )) i = i + 1
  assert_equal(vUIntVal    , vUIntVal    :bind_param(stmt, i  )) i = i + 1
  assert_equal(vDoubleVal  , vDoubleVal  :bind_param(stmt, i  )) i = i + 1
  assert_equal(vStringVal  , vStringVal  :bind_param(stmt, i  )) i = i + 1
  assert_equal(vBinaryVal  , vBinaryVal  :bind_param(stmt, i  )) i = i + 1
  assert_equal(vDateVal    , vDateVal    :bind_param(stmt, i, odbc.PARAM_INPUT, odbc.DATE)) i = i + 1
  assert_equal(vNullVal    , vNullVal    :bind_param(stmt, i  )) i = i + 1
  if PROC_SUPPORT_DEFAULT then
    assert_equal(vDefaultVal , vDefaultVal :bind_param(stmt, i  )) i = i + 1
  end
  assert_equal(vBoolVal    , vBoolVal    :bind_param(stmt, i  )) i = i + 1
  assert_equal(vGuidVal    , vGuidVal    :bind_param(stmt, i  )) i = i + 1
  assert_equal(vBigIntVal  , vBigIntVal  :bind_param(stmt, i  )) i = i + 1

  EXEC_AND_ASSERT()
  VEXEC_AND_ASSERT()

  assert_true(stmt:destroy())
end

end

local_run_test(arg)
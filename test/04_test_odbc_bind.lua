require "config"
local lunit = require "lunitx"

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


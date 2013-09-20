require "config"
require "tools"

local local_run_test = lunit and function() end or run_test
local lunit = require "lunit"
local arg = {...}

local env, cnn, stmt

local _ENV = TEST_CASE'Library test'

local odbc_types = {
  'ubigint', 'sbigint', 'utinyint', 'stinyint', 'ushort', 'sshort', 
  'ulong', 'slong', 'float', 'double', 'date', 'time', 'timestamp',
  'bit', 'char', 'binary', 'wchar'
}

function teardown()
  if stmt then stmt:destroy() end
  if cnn  then cnn:destroy()  end
  if env  then env:destroy()  end
  stmt,cnn,env = nil
end

function test_library_interface()
  assert_function(odbc.version)
  assert_string(odbc._VERSION)
  assert_function(odbc.environment)
  assert_function(odbc.connect)
  assert_function(odbc.driverconnect)

  for _, tname in ipairs(odbc_types) do
    assert_function(odbc[tname], "Unknown ODBC type: " .. tname)
  end

  assert_function(odbc.getenvmeta)
  assert_function(odbc.getcnnmeta)
  assert_function(odbc.getstmtmeta)
end

function test_environment_interface()
  env = assert_not_nil(odbc.environment())

  assert_function(env.connection)
  assert_function(env.driverconnect)
  assert_function(env.connect)

  assert_function(env.destroy)
  assert_function(env.destroyed)

  assert_function(env.drivers)
  assert_function(env.datasources)

  assert_function(env.getuservalue)
  assert_function(env.setuservalue)
  
  assert_function(env.getuintattr)
  assert_function(env.getstrattr)
  assert_function(env.setuintattr)
  assert_function(env.setstrattr)
  
  assert_function(env.setautoclosecnn)
  assert_function(env.getautoclosecnn)
  
  assert_function(env.connection_count)
end

function test_connection_interface()
  env = assert_not_nil(odbc.environment())
  cnn = assert_not_nil(env:connection())

  assert_function(cnn.destroy)
  assert_function(cnn.destroyed)

  assert_function(cnn.environment)

  assert_function(cnn.driverconnect)
  assert_function(cnn.connect)
  assert_function(cnn.disconnect)
  assert_function(cnn.connected)

  assert_function(cnn.commit)
  assert_function(cnn.rollback)

  assert_function(cnn.statement)

  assert_function(cnn.getuservalue)
  assert_function(cnn.setuservalue)

  assert_function(cnn.statement_count)

  assert_function(cnn.setautocommit)
  assert_function(cnn.getautocommit)
  assert_function(cnn.setcatalog)
  assert_function(cnn.getcatalog)
  assert_function(cnn.setreadonly)
  assert_function(cnn.getreadonly)
  assert_function(cnn.settracefile)
  assert_function(cnn.gettracefile)
  assert_function(cnn.settrace)
  assert_function(cnn.gettrace)
  assert_function(cnn.gettransactionisolation)
  assert_function(cnn.settransactionisolation)
  assert_function(cnn.setlogintimeout)
  assert_function(cnn.getlogintimeout)
  assert_function(cnn.setasyncmode)
  assert_function(cnn.getasyncmode)

  assert_function(cnn.setautoclosestmt)
  assert_function(cnn.getautoclosestmt)

  assert_function(cnn.dbmsname)
  assert_function(cnn.dbmsver)
  assert_function(cnn.drvname)
  assert_function(cnn.drvver)
  assert_function(cnn.odbcver)
  assert_function(cnn.odbcvermm)

  assert_function(cnn.typeinfo)
  assert_function(cnn.tables)
  assert_function(cnn.tabletypes)
  assert_function(cnn.schemas)
  assert_function(cnn.catalogs)
  assert_function(cnn.statistics)
  assert_function(cnn.columns)
  assert_function(cnn.tableprivileges)
  assert_function(cnn.columnprivileges)
  assert_function(cnn.primarykeys)
  assert_function(cnn.indexinfo)
  assert_function(cnn.crossreference)
  assert_function(cnn.procedures)
  assert_function(cnn.procedurecolumns)
  assert_function(cnn.specialcolumns)

  assert_function(cnn.getuintattr)
  assert_function(cnn.getstrattr)
  assert_function(cnn.setuintattr)
  assert_function(cnn.setstrattr)

  assert_function(cnn.uint32info)
  assert_function(cnn.uint16info)
  assert_function(cnn.strinfo)

  assert_function(cnn.supportsPrepare)
  assert_function(cnn.supportsBindParam)
  assert_function(cnn.supportsTransactions)
  assert_function(cnn.supportsAsync)
  assert_function(cnn.supportsAsyncConnection)
  assert_function(cnn.supportsAsyncStatement)

end

function test_statement_interface()
  env, cnn = assert_not_nil(do_connect())
  stmt = assert_not_nil(cnn:statement())

  assert_function(stmt.destroy)
  assert_function(stmt.destroyed)
  assert_function(stmt.close)
  assert_function(stmt.closed)
  assert_function(stmt.reset)
  assert_function(stmt.resetcolinfo)
  assert_function(stmt.connection)

  assert_function(stmt.getuservalue)
  assert_function(stmt.setuservalue)

  assert_function(stmt.rowcount)
  assert_function(stmt.vfetch)
  assert_function(stmt.fetch)
  assert_function(stmt.execute)
  assert_function(stmt.prepare)
  assert_function(stmt.prepared)
  assert_function(stmt.cancel)

  assert_function(stmt.bind)
  assert_function(stmt.bindnum)
  assert_function(stmt.bindstr)
  assert_function(stmt.bindbin)
  assert_function(stmt.bindbool)
  assert_function(stmt.bindnull)
  assert_function(stmt.binddefault)

  assert_function(stmt.setasyncmode)
  assert_function(stmt.getasyncmode)

  assert_function(stmt.parcount)
  assert_function(stmt.nextresultset)
  assert_function(stmt.foreach)

  assert_function(stmt.coltypes)
  assert_function(stmt.colnames)


  assert_function(stmt.getuintattr)
  assert_function(stmt.getstrattr)
  assert_function(stmt.setuintattr)
  assert_function(stmt.setstrattr)

  assert_function(stmt.getquerytimeout)
  assert_function(stmt.setquerytimeout)
  assert_function(stmt.getmaxrows)
  assert_function(stmt.setmaxrows)
  assert_function(stmt.getmaxfieldsize)
  assert_function(stmt.setmaxfieldsize)
  assert_function(stmt.getescapeprocessing)
  assert_function(stmt.setescapeprocessing)
  assert_function(stmt.getautoclose)
  assert_function(stmt.setautoclose)
  assert_function(stmt.getdestroyonclose)
  assert_function(stmt.setdestroyonclose)

  assert_function(stmt.vbind_col)
  assert_function(stmt.vbind_param)

  for _, tname in ipairs(odbc_types) do
    assert_function(stmt["vbind_col_"   .. tname], "Unknown ODBC type: " .. tname)
    assert_function(stmt["vbind_param_" .. tname], "Unknown ODBC type: " .. tname)
  end
end

function test_default_options()
  env = assert_not_nil(odbc.environment())
  cnn = assert_not_nil(env:connection())

  assert_true(env:getautoclosecnn())
  assert_true(cnn:getautoclosestmt())
end

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
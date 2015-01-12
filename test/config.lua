IS_WINDOWS = (require"package".config:sub(1,1) == '\\')

local function export(t, d)
  d = d or _G
  for k,v in pairs(t) do d[k] = v end
  return d
end

DEFINITION_STRING_TYPE_NAME = 'varchar(50)'

QUERYING_STRING_TYPE_NAME = 'string'

TEST_TABLE_NAME = 'odbc_test_dbt'

TEST_PROC_NAME = 'odbc_test_dbsp'

TOTAL_FIELDS = 40

local SYBASE9 = {
  DBMS = "ASA";

  CNN_DRV = {
    {Driver='{Adaptive Server Anywhere 9.0}'};
    {UID='TestUser'};
    {PWD='sql'};
    {EngineName='DevelopServer'};
    {DatabaseName='EmptyDB'};
    {CommLinks='tcpip{host=127.0.0.1}'};
  };

  CNN_DSN = {'emptydb', 'TestUser', 'sql'};

  CREATE_TABLE_RETURN_VALUE = -1;

  DROP_TABLE_RETURN_VALUE = -1;

  UPDATE_RETURN_ROWS = true;

  PROC_SUPPORT_DEFAULT = true;

  HAS_GUID_TYPE = true;

  TEST_PROC_CREATE = [[CREATE PROCEDURE ]] .. TEST_PROC_NAME .. [[(
    in inIntVal     integer,
    in inUIntVal    unsigned integer,
    in inDoubleVal  double,
    in inStringVal  char(50),
    in inBinaryVal  binary(50),
    in inDateVal    date,
    in inNullVal    integer,
    in inDefaultVal integer default 1234,
    in inBitVal     bit,
    in inGuidVal    uniqueidentifier,
    in inBigInt     bigint
  )
  BEGIN
    select 
      inIntVal,
      inUIntVal,
      inDoubleVal,
      inStringVal,
      inBinaryVal,
      inDateVal,
      inNullVal,
      inDefaultVal,
      inBitVal,
      inGuidVal,
      inBigInt
  END]];

  TEST_PROC_DROP = "DROP PROCEDURE " .. TEST_PROC_NAME;

  TEST_PROC_CALL = [[CALL ]] .. TEST_PROC_NAME .. [[(
    inIntVal=?,
    inUIntVal=?,
    inDoubleVal=?,
    inStringVal=?,
    inBinaryVal=?,
    inDateVal=?,
    inNullVal=?,
    inDefaultVal=?,
    inBitVal=?,
    inGuidVal=?,
    inBigInt=?
  )]];
}

local SYBASE12 = export(SYBASE9, {}) do

SYBASE12.CNN_DRV = {
  {Driver='{SQL Anywhere 12}'};
  {UID='DBA'};
  {PWD='sql'};
  {EngineName='EmptyDB'};
  {DatabaseName='EmptyDB'};
  {CommLinks='tcpip{host=127.0.0.1}'};
}

SYBASE12.CNN_DSN = {'emptydb', 'DBA', 'sql'}

end

local MSSQL = {
  DBMS = "MSSQL";

  CNN_DRV = {
    {Driver='{SQL Server Native Client 10.0}'};
    {Server='127.0.0.1,1443'};
    {Database='testdb'};
    {Uid='sa'};
    {Pwd='sql'};
  };

  CNN_DSN = {'luasql-test', 'sa', 'sql'};

  CREATE_TABLE_RETURN_VALUE = -1;

  DROP_TABLE_RETURN_VALUE = -1;

  UPDATE_RETURN_ROWS = true;

}

local MySQL = {
  DBMS = "MySQL";

  CNN_DRV = {
    {Driver = IS_WINDOWS and '{MySQL ODBC 5.2 ANSI Driver}' or 'MySQL'};
    {db='test'};
    {uid='root'};
  };

  CNN_DSN = {'MySQL-test', 'root', ''};

  CREATE_TABLE_RETURN_VALUE = 0;

  DROP_TABLE_RETURN_VALUE = 0;

  UPDATE_RETURN_ROWS = false;

  PROC_SUPPORT_DEFAULT = false;

  HAS_GUID_TYPE = false;

  TEST_PROC_CREATE = [[CREATE DEFINER=CURRENT_USER PROCEDURE ]] .. TEST_PROC_NAME .. [[(
    in inIntVal     integer,
    in inUIntVal    int unsigned,
    in inDoubleVal  double,
    in inStringVal  char(50),
    in inBinaryVal  binary(50),
    in inDateVal    date,
    in inNullVal    integer,
    in inBitVal     bit(1),
    in inBigInt     bigint
  )
  BEGIN
    select 
      inIntVal,
      inUIntVal,
      inDoubleVal,
      inStringVal,
      inBinaryVal,
      inDateVal,
      inNullVal,
      inBitVal,
      inBigInt;
  END]];

  TEST_PROC_DROP = 'DROP PROCEDURE ' .. TEST_PROC_NAME;

  TEST_PROC_CALL = 'CALL ' .. TEST_PROC_NAME .. '(?, ?, ?, ?, ?, ?, ?, ?, ?)';

  TEST_PROC_CREATE_MULTI_RS = [[CREATE DEFINER=CURRENT_USER PROCEDURE ]] .. TEST_PROC_NAME .. [[()
  BEGIN 
    select 1 as IntVal1, 2 as IntVal2
    union all
    select 11, 12;
    select 'hello' as StrVal1, 'world' as StrVal2, '!!!' as StrVal3
    union all
    select 'some', 'other', 'row';
  END]];
  
  TEST_PROC_CALL_MULTI_RS = 'CALL ' .. TEST_PROC_NAME .. '()'
}

local VARS = {
  mysql  = MySQL;
  asa9   = SYBASE9;
  asa12  = SYBASE12;
  mssql  = MSSQL;
}

local v = (require"os".getenv("LUAODBC_TEST_DBMS") or 'asa9'):lower()

export(
  (assert(VARS[v], 'unknown DBMS: ' .. v))
)

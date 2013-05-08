CNN_DRV = {
  {Driver='{Adaptive Server Anywhere 9.0}'};
  {UID='TestUser'};
  {PWD='sql'};
  {EngineName='DevelopServer'};
  {DatabaseName='EmptyDB'};
  {CommLinks='tcpip{host=127.0.0.1}'};
}

CNN_DSN = {'emptydb', 'TestUser', 'sql'}

if false then

CNN_DRV = {
  {Driver='{SQL Server Native Client 10.0}'};
  {Server='127.0.0.1,1443'};
  {Database='testdb'};
  {Uid='sa'};
  {Pwd='sql'};
}

CNN_DSN = {'luasql-test', 'sa', 'sql'}

end

CREATE_TABLE_RETURN_VALUE = -1

DROP_TABLE_RETURN_VALUE = -1

DEFINITION_STRING_TYPE_NAME = 'varchar(50)'

QUERYING_STRING_TYPE_NAME = 'string'

TEST_TABLE_NAME = 'odbc_test_dbt'

TOTAL_FIELDS = 40

TEST_PROC_NAME = 'odbc_test_dbsp'

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
  in inGuidVal    uniqueidentifier
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
    inGuidVal
END]]

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
  inGuidVal=?
)]]


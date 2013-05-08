local odbc = require "odbc.dba"

local CNN_ROWS = 10
local function init_db(cnn)
  local fmt = string.format
  assert(cnn:exec"create table Agent(ID INTEGER PRIMARY KEY, Name char(32))")
  for i = 1, CNN_ROWS do
    assert(cnn:exec(fmt("insert into Agent(ID,NAME)values(%d, 'Agent#%d')", i, i)))
  end
end

local cnn = odbc.Connect{
  Driver   = "SQLite3 ODBC Driver";
  Database = ":memory:";
}
init_db(cnn)
local timer = require "lzmq.timer".monotonic()
timer:start()
for i = 1, 10000 do
  cnn:exec("update Agent set ID=ID where ID=:ID", {ID = 1})
end
print(timer:stop())


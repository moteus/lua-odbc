# ODBC library for Lua 5.1/5.2 #

[![Build Status](https://travis-ci.org/moteus/lua-odbc.png?branch=master)](https://travis-ci.org/moteus/lua-odbc)
[![Coverage Status](https://img.shields.io/coveralls/moteus/lua-odbc.svg)](https://coveralls.io/r/moteus/lua-odbc?branch=master)

## Supports ##
- prepared query
- bind parameters (input/output)
- bind column
- async mode for statements
- catalog functions
- luasql compatable module
- C API to create Lua object based on existed ODBC handle

## Usage ##

Insert multiple rows
```lua
odbc = require "odbc"
dbassert = odbc.assert

cnn = odbc.connect('EmptyDB')
cnn:setautocommit(false)
stmt = cnn:prepare("insert into test_table(f1, f2) values(?, ?)")
f1 = stmt:vbind_param_ulong(1)
for i = 1, 5 do
  f1:set(i)          -- just set data
  stmt:bindnum(2, i) -- each time call SQLBindParameter
  dbassert(stmt:execute())
end
dbassert( cnn:commit() )
```

Select resultset
```lua
odbc = require "odbc"
dbassert = odbc.assert

cnn = odbc.connect('EmptyDB')

stmt = cnn:execute('select f1, f2 from test_table order by 1')
stmt:foreach(function(f1, f2)
  if f1 == 2 then return true end
end)
assert(stmt:closed()) -- foreach close cursor 
assert(not stmt:destroyed()) -- statement valid

stmt:execute('select f1, f2 from test_table order by 1')
f1, f2 = stmt:fetch()
```

Input/Output parameters
```lua
odbc = require "odbc"
dbassert = odbc.assert
cnn = odbc.connect('EmptyDB')

stmt = dbassert(cnn:prepare("{?= call dba.fn_test(?)}"))
ret  = stmt:vbind_param_ulong(1, ret, odbc.PARAM_OUTPUT)
val  = stmt:vbind_param_char(2, "hello") -- default odbc.PARAM_INPUT
dbassert(stmt:execute())
print(ret:get())

stmt:reset()
stmt:vbind_col(1, ret)
dbassert(stmt:execute('select 321'))
stmt:vfetch()
print(ret:get())
```

Use async mode
```lua
local odbc = require "odbc"
local dbassert = odbc.assert

cnn = odbc.connect('EmptyDB')

stmt = cnn:prepare('select f1, f2 from test_table order by 1')
local f1 = stmt:vbind_col_ulong(1)
local f2 = stmt:vbind_col_ulong(2)

stmt:setasyncmode(true)

local COUNTER = 0

function async(stmt, f, ...)
  while true do
    local ok, err = f(stmt, ...)
    if ok ~= 'timeout' then return ok, err end
    COUNTER = COUNTER + 1
  end
end

dbassert(async(stmt, stmt.execute))
while(async(stmt, stmt.vfetch))do
  print(f1:get(),f2:get())
end

print("execute counter:", COUNTER)
```

Use C API
```C
static void luaX_call_method(lua_State *L, const char *name, int nargs, int nresults){
  int obj_index = -nargs - 1;
  lua_getfield(L, obj_index, name);
  lua_insert(L, obj_index - 1);
  return lua_call(L, nargs + 1, nresults);
}

static int odbc_first_row_impl(lua_State *L){
  SQLHSTMT *pstmt = (SQLHSTMT *)lua_touserdata(L, lua_upvalueindex(1));

  lua_settop(L, 1); // sql

  // wrap existed handle to lua object
  lodbc_statement(L, *pstmt, 0);      // sql, stmt
  lua_insert(L, 1); lua_insert(L, 1); // stmt, sql

  luaX_call_method(L, "execute", 1, 2); // [nil/stmt], [err/nil]

  // here we should check error and ether execute return recordset

  lua_pop(L, 1); //stmt
  luaX_call_method(L, "fetch", 0, LUA_MULTRET);

  return lua_gettop(L);
}

static int odbc_first_row(lua_State *L){
  int res;
  { // this is may be cpp scope (we can use RAII)
    // get existed handle from somewhere(e.g. prepared pool)
    SQLHSTMT hstmt = ... 

    lua_pushlightuserdata(L, &hstmt);
    lua_pushcclosure(L, odbc_first_row_impl, 1);
    lua_insert(L, 1);

    res = lua_pcall(L, lua_gettop(L)-1, LUA_MULTRET, 0);

    // cleanup
  }
  if(res) return lua_error(L);
  return lua_gettop(L);
}
```
And from Lua we can call this function just like `... = odbc.first_row("select ...")`

## odbc.dba module ##
Implementaion of [dba](http://moteus.github.io/dba/index.html) API


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/moteus/lua-odbc/trend.png)](https://bitdeli.com/free "Bitdeli Badge")


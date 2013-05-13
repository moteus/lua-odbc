ODBC library for Lua 5.1/5.2

[![Build Status](https://travis-ci.org/moteus/lua-odbc.png?branch=travis-test)](https://travis-ci.org/moteus/lua-odbc)

## Supports ##
- prepared query
- bind parameters (input/output)
- bind column
- async mode for statements
- catalog functions
- luasql compatable module

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

## odbc.dba module ##
Implementaion of [dba](http://moteus.github.io/dba/index.html) API

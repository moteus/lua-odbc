package = "odbc"
version = "scm.dba-1"
source = {
  url = "https://github.com/moteus/lua-odbc/archive/dba.zip",
  dir = "lua-odbc-dba",
}

description = {
  summary = "ODBC library",
  detailed = [[
  ]],
  homepage = "https://github.com/moteus/lua-odbc",
  -- license = ""
}

dependencies = {
  "lua >= 5.1",
}

build = {
  type = "builtin",
  copy_directories = {"test"},
  modules = {
    [ "odbc.core"    ] = {
      sources = {
        'src/l52util.c', 'src/lcnn.c',
        'src/lenv.c',    'src/lerr.c',
        'src/libopt.c',  'src/lodbc.c',
        'src/lstmt.c',   'src/lval.c',
        'src/parlist.c', 'src/utils.c',
        -- 'src/driverinfo.c',
      };
      defines = {
        'LUAODBC_EXPORTS';
        'LODBC_ERROR_AS_OBJECT';
        'LODBC_MIN_PAR_BUFSIZE=64';
        -- 'LODBC_FREE_PAR_AT_CLEAR';
        -- 'LODBC_USE_LUA_REGISTRY';
      };
      libraries = {'odbc32', 'odbccp32'};
      incdirs = {"./include"},
    };
    [ "odbc"           ] = "lua/odbc.lua";
    [ "odbc.luasql"    ] = "lua/odbc/luasql.lua";
    [ "odbc.dba"       ] = "lua/odbc/dba.lua";
    [ "odbc.dba.utils" ] = "lua/odbc/dba/utils.lua";
    [ "odbc.proxy"     ] = "lua/odbc/proxy.lua";
  }
}

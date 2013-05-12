package = "odbc"
version = "scm-0"
source = {
  url = "https://github.com/moteus/lua-odbc/archive/master.zip",
  dir = "lua-odbc-master",
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

  platforms = {
    windows = { modules = {
      [ "odbc.core"    ] = {
        libraries = {'odbc32', 'odbccp32'};
      }
    }},
  },

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
        "UNIXODBC";
        'LUAODBC_EXPORTS';
        'LODBC_ERROR_AS_OBJECT';
        'LODBC_MIN_PAR_BUFSIZE=64';
        -- 'LODBC_FREE_PAR_AT_CLEAR';
        -- 'LODBC_USE_LUA_REGISTRY';
        -- 'LODBC_USE_UDPTR_AS_KEY';
        -- 'LODBC_USE_NULL_AS_NIL';
      };
      incdirs = {"./include"},
    };
    [ "odbc"           ] = "lua/odbc.lua";
    [ "odbc.luasql"    ] = "lua/odbc/luasql.lua";
    [ "odbc.dba"       ] = "lua/odbc/dba.lua";
    [ "odbc.dba.utils" ] = "lua/odbc/dba/utils.lua";
    [ "odbc.proxy"     ] = "lua/odbc/proxy.lua";
  }
}

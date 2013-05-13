package = "odbc"
version = "0.1.0-1"
source = {
  url = "https://github.com/moteus/lua-odbc/archive/v0.1.0.zip",
  dir = "lua-odbc-0.1.0",
}

description = {
  summary = "ODBC library for Lua",
  detailed = [[
  ]],
  homepage = "https://github.com/moteus/lua-odbc",
  license  = "MIT/X11",
}

dependencies = {
  "lua >= 5.1",
}

external_dependencies = {
  platforms = { 
    unix = {
      ODBC = {
        header  = 'sql.h',
        -- library = 'odbc', -- does not work !?
      }
    };
    windows = {
      ODBC = {}
    };
  }
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
    unix = { modules = {
      [ "odbc.core"    ] = {
        libraries = {'odbc'};
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
        'UNIXODBC';
        'LUAODBC_EXPORTS';
        'LODBC_ERROR_AS_OBJECT';
        'LODBC_MIN_PAR_BUFSIZE=64';
        -- 'LODBC_FREE_PAR_AT_CLEAR';
        -- 'LODBC_USE_LUA_REGISTRY';
        -- 'LODBC_USE_UDPTR_AS_KEY';
        -- 'LODBC_USE_NULL_AS_NIL';
      };
      incdirs = {"./include","$(ODBC_INCDIR)"},
    };
    [ "odbc"           ] = "lua/odbc.lua";
    [ "odbc.luasql"    ] = "lua/odbc/luasql.lua";
    [ "odbc.dba"       ] = "lua/odbc/dba.lua";
    [ "odbc.dba.utils" ] = "lua/odbc/dba/utils.lua";
    [ "odbc.proxy"     ] = "lua/odbc/proxy.lua";
  }
}

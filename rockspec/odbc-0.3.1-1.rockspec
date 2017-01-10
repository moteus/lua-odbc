package = "odbc"
version = "0.3.1-1"
source = {
  url = "https://github.com/moteus/lua-odbc/archive/v0.3.1.zip",
  dir = "lua-odbc-0.3.1",
}

description = {
  summary = "ODBC library for Lua",
  detailed = [[
  ]],
  homepage = "https://github.com/moteus/lua-odbc",
  license  = "MIT/X11",
}

dependencies = {
  "lua >= 5.1, < 5.4",
}

external_dependencies = {
  platforms = { 
    unix = {
      ODBC = {
        header  = 'sql.h',
        -- library = 'odbc', -- does not work !?
      }
    };
    windows = { ODBC = {} };
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
    unix    = { modules = {
      [ "odbc.core"    ] = {
        libraries = {'odbc'};
      }
    }},
  },

  modules = {
    [ "odbc.core"      ] = {
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

        -- Configuration behavior
        -- return object as error instead of just string
        'LODBC_ERROR_AS_OBJECT';

        -- odbc.assert call tostring function to second arg before raise error
        -- it may sense most for Lua 5.1 because it does not produce correct message.
        -- Lua > 5.1 call tostring in case for error byself
        -- 'LODBC_ASSERT_TOSTRING';

        -- return NULL value as `nil` instead of odbc.NULL
        -- 'LODBC_USE_NULL_AS_NIL';

        -- check return codes from SQLClose.
        -- It really not very useful because usially we really can not do anything.
        -- 'LODBC_CHECK_ERROR_ON_DESTROY';

        -- Perfomance/memory usage options
        'LODBC_MIN_PAR_BUFSIZE=64';
        -- 'LODBC_FREE_PAR_AT_CLEAR';
        -- 'LODBC_USE_LUA_REGISTRY';

        -- Use pointer to userdata as lightuserdata as key for `uservalue`.
        -- In other case use userdata as key and we have to create separate
        -- weak table to allow remove value by gc and do one additional table lookup
        -- It is not documented that Lua preserve constant pointer 
        -- for userdata but it is true for all current implementation 
        -- And Lua developers do not going to implement movable GC
        -- I test it with (Lua 5.1, 5.2, 5.3, JIT)
        -- 'LODBC_USE_UDPTR_AS_KEY';
      };
      incdirs = {"./include","$(ODBC_INCDIR)"},
      libdirs = {"$(ODBC_LIBDIR)"},
    };
    [ "odbc"           ] = "lua/odbc.lua";
    [ "odbc.luasql"    ] = "lua/odbc/luasql.lua";
    [ "odbc.dba"       ] = "lua/odbc/dba.lua";
    [ "odbc.dba.utils" ] = "lua/odbc/dba/utils.lua";
    [ "odbc.proxy"     ] = "lua/odbc/proxy.lua";
  }
}

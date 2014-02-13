cmake_minimum_required ( VERSION 2.8 )

project ( odbc C )
include ( cmake/dist.cmake )
include ( lua )

include_directories ( ${CMAKE_CURRENT_SOURCE_DIR}/include )
include_directories ( ${CMAKE_CURRENT_SOURCE_DIR}/src )

find_package ( ODBC REQUIRED )

set ( LODBC_SOURCE 
  src/l52util.c  src/lcnn.c
  src/lenv.c     src/lerr.c
  src/libopt.c   src/lodbc.c
  src/lstmt.c    src/lval.c
  src/parlist.c  src/utils.c
)

if ( UNIX )
  set ( LODBC_DEFINITIONS UNIXODBC LUAODBC_EXPORTS LODBC_ERROR_AS_OBJECT LODBC_MIN_PAR_BUFSIZE=64)
else ( )
  set ( LODBC_DEFINITIONS LUAODBC_EXPORTS LODBC_ERROR_AS_OBJECT LODBC_MIN_PAR_BUFSIZE=64 )
endif ( )

install_lua_module ( odbc.core ${LODBC_SOURCE} LINK ${ODBC_LIBRARIES} )

set_target_properties ( odbc_core PROPERTIES COMPILE_DEFINITIONS "${LODBC_DEFINITIONS}" )

install_lua_module ( odbc           lua/odbc.lua           )
install_lua_module ( odbc.luasql    lua/odbc/luasql.lua    )
install_lua_module ( odbc.dba       lua/odbc/dba.lua       )
install_lua_module ( odbc.dba.utils lua/odbc/dba/utils.lua )
install_lua_module ( odbc.proxy     lua/odbc/proxy.lua     )


install_data ( README.md LICENCE.txt )

install_test( test/ )


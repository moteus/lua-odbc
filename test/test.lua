print("------------------------------------")
print("Lua version: " .. (_G.jit and _G.jit.version or _G._VERSION))
print("------------------------------------")
print("") 
local HAS_RUNNER = not not lunit
lunit = require "lunit"
require "00_test_odbc"
require "01_test_odbc_env"
require "02_test_odbc_cnn"
require "03_test_odbc_cnn_info"
require "04_test_odbc_bind"
require "05_test_odbc_foreach"
require "06_test_odbc_more"
require "07_test_odbc_val"
require "08_test_odbc_stmt"
if not HAS_RUNNER then run_test{...} end
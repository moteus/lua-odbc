J = path.join

lake.define_need('lua52', function()
  return {
    incdir = J(ENV.LUA_DIR_5_2, 'include');
    libdir = J(ENV.LUA_DIR_5_2, 'lib');
    libs   = {'lua52'};
  }
end)

lake.define_need('lua51', function()
  return {
    incdir = J(ENV.LUA_DIR, 'include');
    libdir = J(ENV.LUA_DIR, 'lib');
    libs   = {'lua5.1'};
  }
end)

lake.define_need('odbc', function()
  return {
    libs   = {'odbc32', 'odbccp32'};
  }
end)


#include "lodbc.h"
#include "driverinfo.h"
// port DriverInfo from libodbc++ Manush Dodunekov <manush@stendahls.net>

#if LODBC_ODBCVER >= 0x0300

#define DI_IMPLEMENT_SUPPORTS(SUFFIX,VALUE_3,VALUE_2) \
int di_supports_##SUFFIX(const drvinfo_data *di, int ct){   \
    int r = 0;                                        \
    assert(di != NULL);                               \
    if(di->majorVersion_>=3) {                        \
        switch(ct) {                                  \
        case SQL_CURSOR_FORWARD_ONLY:                 \
            r=(di->forwardOnlyA2_&VALUE_3)?1:0;       \
            break;                                    \
                                                      \
        case SQL_CURSOR_STATIC:                       \
            r=(di->staticA2_&VALUE_3)?1:0;            \
            break;                                    \
                                                      \
        case SQL_CURSOR_KEYSET_DRIVEN:                \
            r=(di->keysetA2_&VALUE_3)?1:0;            \
            break;                                    \
                                                      \
        case SQL_CURSOR_DYNAMIC:                      \
            r=(di->dynamicA2_&VALUE_3)?1:0;           \
            break;                                    \
                                                      \
        default:                                      \
            assert(0);                                \
            break;                                    \
        }                                             \
    } else {                                          \
        r=(di->concurMask_&VALUE_2)?1:0;              \
    }                                                 \
    return r;                                         \
}                                                   

#else

#define DI_IMPLEMENT_SUPPORTS(SUFFIX,VALUE_3,VALUE_2) \
int di_supports_##SUFFIX(const drvinfo_data *di, int ct){   \
    return (di->concurMask_&VALUE_2)?1:0;             \
}

#endif

DI_IMPLEMENT_SUPPORTS(readonly,SQL_CA2_READ_ONLY_CONCURRENCY,SQL_SCCO_READ_ONLY)
DI_IMPLEMENT_SUPPORTS(lock,SQL_CA2_LOCK_CONCURRENCY,SQL_SCCO_LOCK)
DI_IMPLEMENT_SUPPORTS(rowver,SQL_CA2_OPT_ROWVER_CONCURRENCY,SQL_SCCO_OPT_ROWVER)
DI_IMPLEMENT_SUPPORTS(values,SQL_CA2_OPT_VALUES_CONCURRENCY,SQL_SCCO_OPT_VALUES)

#undef DI_IMPLEMENT_SUPPORTS

int di_supports_forwardonly(const drvinfo_data *di){
    return (di->cursorMask_&SQL_SO_FORWARD_ONLY)?1:0;
}

int di_supports_static(const drvinfo_data *di){
    return (di->cursorMask_&SQL_SO_STATIC)?1:0;
}

int di_supports_keyset(const drvinfo_data *di){
    return (di->cursorMask_&SQL_SO_KEYSET_DRIVEN)?1:0;
}

int di_supports_dynamic(const drvinfo_data *di){
    return (di->cursorMask_&SQL_SO_DYNAMIC)?1:0;
}

int di_supports_scrollsensitive(const drvinfo_data *di){
    return
        di_supports_dynamic(di) || 
        di_supports_keyset(di);
}

// assumes that di_supportsScrollSensitive(di)==true
int di_getscrollsensitive(const drvinfo_data *di){
    if(di_supports_dynamic(di)){
        return SQL_CURSOR_DYNAMIC;
    } else {
        return SQL_CURSOR_KEYSET_DRIVEN;
    }
}

int di_supports_updatable(const drvinfo_data *di, int ct){
    return
        di_supports_lock  (di, ct) ||
        di_supports_rowver(di, ct) ||
        di_supports_values(di, ct);
}

int di_getupdatable(const drvinfo_data *di, int ct){
    // assumes supportsUpdatable(ct) returns true
    if(di_supports_rowver(di, ct)) {
        return SQL_CONCUR_ROWVER;
    } else if(di_supports_values(di, ct)) {
        return SQL_CONCUR_VALUES;
    } else if(di_supports_lock(di, ct)) {
        return SQL_CONCUR_LOCK;
    }
    return SQL_CONCUR_READ_ONLY;
}

int di_supports_getdata_anyorder(const drvinfo_data *di){
    return (di->getdataExt_ & SQL_GD_ANY_ORDER)?1:0;
};

int di_supports_getdata_anycolumn(const drvinfo_data *di){
    return (di->getdataExt_ & SQL_GD_ANY_COLUMN)?1:0;
};

int di_supports_getdata_block(const drvinfo_data *di){
    return (di->getdataExt_ & SQL_GD_BLOCK)?1:0;
};

int di_supports_getdata_bound(const drvinfo_data *di){
    return (di->getdataExt_ & SQL_GD_BOUND)?1:0;
};

#ifdef LODBC_USE_DRIVERINFO_SUPPORTED_FUNCTIONS
int di_supports_function(const drvinfo_data *di, int funcId){
    return SQL_TRUE == LODBC_ODBC3_C(
        SQL_FUNC_EXISTS(di->supportedFunctions_,funcId),
        di->supportedFunctions_[funcId]
    );
}
#endif

int di_getODBCCursorTypeFor(const drvinfo_data* di, int rsType){
  int r;
  assert(IS_VALID_RS_TYPE(rsType));
  switch(rsType) {
    case RS_TYPE_FORWARD_ONLY:
      r = SQL_CURSOR_FORWARD_ONLY;
      break;
    case RS_TYPE_SCROLL_INSENSITIVE:
      r = SQL_CURSOR_STATIC;
      break;
    case RS_TYPE_SCROLL_SENSITIVE:
      if(di->cursorMask_  & SQL_SO_DYNAMIC){
        r = SQL_CURSOR_DYNAMIC;
      } else {
        r = SQL_CURSOR_KEYSET_DRIVEN;
      }
      break;
    default:
      assert(0);
  }
  return r;
}


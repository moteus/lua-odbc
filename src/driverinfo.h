#ifndef _DRIVERINFO_H_976EAE8E_CC7E_4234_91C5_A49CB4198556_
#define _DRIVERINFO_H_976EAE8E_CC7E_4234_91C5_A49CB4198556_

//{ libodbc enum ResultSet type constants
/** The result set only goes forward. */
#define RS_TYPE_FORWARD_ONLY ((SQLUINTEGER)1)
/** The result set is scrollable, but the data in it is not
* affected by changes in the database.
*/
#define RS_TYPE_SCROLL_INSENSITIVE ((SQLUINTEGER)2)
/** The result set is scrollable and sensitive to database changes */
#define RS_TYPE_SCROLL_SENSITIVE ((SQLUINTEGER)3)
//}

#define IS_VALID_RS_TYPE(RS) (((RS) == RS_TYPE_FORWARD_ONLY) || \
                             ((RS) == RS_TYPE_SCROLL_INSENSITIVE) || \
                             ((RS) == RS_TYPE_SCROLL_SENSITIVE))

#define TYPE_BIGINT        1
#define TYPE_BINARY        2
#define TYPE_BIT           3
#define TYPE_CHAR          4
#define TYPE_DATE          5
#define TYPE_DECIMAL       6
#define TYPE_DOUBLE        7
#define TYPE_FLOAT         8
#define TYPE_INTEGER       9
#define TYPE_LONGVARBINARY 10
#define TYPE_LONGVARCHAR   11
#define TYPE_NUMERIC       12
#define TYPE_REAL          13
#define TYPE_SMALLINT      14
#define TYPE_TIME          15
#define TYPE_TIMESTAMP     16
#define TYPE_TINYINT       17
#define TYPE_VARBINARY     18
#define TYPE_VARCHAR       19
#if (LODBC_ODBCVER >= 0x0300)
#define TYPE_WCHAR         20
#define TYPE_WLONGVARCHAR  21
#define TYPE_WVARCHAR      22
#define TYPE_GUID          23
#define TYPES_COUNT        23
#else
#define TYPES_COUNT        19
#endif


#define di_supportedFunctionsSize (sizeof(SQLUSMALLINT) * LODBC_ODBC3_C(SQL_API_ODBC3_ALL_FUNCTIONS_SIZE,100))

typedef struct drvinfo_data{
    // odbc version
    unsigned char majorVersion_;
    unsigned char minorVersion_;

    SQLUINTEGER getdataExt_;
    SQLUINTEGER cursorMask_;

#if LODBC_ODBCVER >= 0x0300
    SQLUINTEGER forwardOnlyA2_;
    SQLUINTEGER staticA2_;
    SQLUINTEGER keysetA2_;
    SQLUINTEGER dynamicA2_;
#endif

    SQLUINTEGER  concurMask_;

#ifdef LODBC_USE_DRIVERINFO_SUPPORTED_FUNCTIONS
    SQLUSMALLINT supportedFunctions_[di_supportedFunctionsSize];
#endif

} drvinfo_data;

int di_supports_readonly(const drvinfo_data *di, int ct);
int di_supports_lock(const drvinfo_data *di, int ct);
int di_supports_rowver(const drvinfo_data *di, int ct);
int di_supports_values(const drvinfo_data *di, int ct);
int di_supports_forwardonly(const drvinfo_data *di);
int di_supports_static(const drvinfo_data *di);
int di_supports_keyset(const drvinfo_data *di);
int di_supports_dynamic(const drvinfo_data *di);
int di_supports_scrollsensitive(const drvinfo_data *di);
// assumes that di_supportsScrollSensitive(di)==true
int di_getscrollsensitive(const drvinfo_data *di);
int di_supports_updatable(const drvinfo_data *di, int ct);
int di_getupdatable(const drvinfo_data *di, int ct);
int di_supports_getdata_anyorder(const drvinfo_data *di);
int di_supports_getdata_anycolumn(const drvinfo_data *di);
int di_supports_getdata_block(const drvinfo_data *di);
int di_supports_getdata_bound(const drvinfo_data *di);

#ifdef LODBC_USE_DRIVERINFO_SUPPORTED_FUNCTIONS
int di_supports_function(const drvinfo_data *di, int funcId);
#endif

// returns the actual ODBC cursor type this datasource would
// use for a given ResultSet type
int di_getODBCCursorTypeFor(const drvinfo_data* di, int rsType);

#endif
#include "Vertica.h"
#include "StringParsers.h"

using namespace Vertica;
using namespace std;

#include <sql.h>
#include <time.h>
#include <sqlext.h>
#include <iostream>
#include <fstream>
#include <memory>
#include <cstdlib>
#ifdef DBLINK_DEBUG
#include <malloc.h>
#endif

#define DBLINK_CIDS "/usr/local/etc/dblink.cids" // Default Connection identifiers config file
#define MAXCNAMELEN 128                          // Max column name length
#define DEF_ROWSET 100                           // Default rowset
#define MAX_ROWSET 1000                          // Default rowset
#define MAX_NUMERIC_CHARLEN 128                  // Max NUMERIC size in characters
#define MAX_CHAR_LEN 65000                       // Max [W]CHAR length
#define MAX_LONGCHAR_LEN 32000000                // Max LONG[W]VARCHAR length
#define MAX_BINARY_LEN 65000                     // Max [VAR]BINARY length
#define MAX_LONGBINARY_LEN 32000000              // Max LONGVARBINARY length
#define MAX_ODBC_ERROR_LEN 1024                  // Max ODBC Error length

namespace DBLINK
{
    enum DBs
    {
        GENERIC = 0,
        POSTGRES,
        VERTICA,
        SQLSERVER,
        TERADATA,
        ORACLE,
        MYSQL
    };

    void getCidValue(ServerInterface &srvInterface, std::string &cidValue)
    {
        std::string cid = "";
        std::string cid_env = "";
        std::string cid_file = DBLINK_CIDS;
        std::string cid_name = "";
        std::string cid_value = "";
        bool connect = false;

        // Read Params:
        ParamReader params = srvInterface.getParamReader();
        if (params.containsParameter("cidfile"))
        { // Start checking "cidfile" param
            cid_file = params.getStringRef("cidfile").str();
#ifdef DBLINK_DEBUG
            srvInterface.log("DEBUG DBLINK read param cidfile=<%s>", cid_file.c_str());
#endif
        }

        if (params.containsParameter("cid"))
        { // Start checking "cid" param
            cid = params.getStringRef("cid").str();
#ifdef DBLINK_DEBUG
            srvInterface.log("DEBUG DBLINK read param cid=<%s>", cid.c_str());
#endif
        }
        else if (params.containsParameter("connect_secret"))
        { // if "cid" is undef try with "connect_secret"
            connect = true;
            cid = params.getStringRef("connect_secret").str();
        }
        else if (params.containsParameter("connect"))
        { // if "cid" is undef try with "connect"
            connect = true;
            cid = params.getStringRef("connect").str();
        }
        else if (srvInterface.getUDSessionParamReader("library").containsParameter("dblink_secret"))
        {
            // if "cid", "connect_secret" and "connect" are not defined try "dblink_secret" session param
            connect = true;
            cid = srvInterface.getUDSessionParamReader("library").getStringRef("dblink_secret").str();
        }
        else
        {
            vt_report_error(101, "DBLINK. Missing connection parameters");
        }

        // Check connection parameters
        if (connect)
        { // old VFQ connect style: connect='@/tmp/file.txt' will read CIDs from a different file
            if (cid[0] == '@')
            {
                std::ifstream cids(cid.substr(1));
                if (cids.is_open())
                {
                    std::stringstream ssFile;
                    ssFile << cids.rdbuf();
                    cid_value = ssFile.str();
                    cid_value.erase(std::remove(cid_value.begin(), cid_value.end(), '\n'), cid_value.end());
                }
                else
                {
                    vt_report_error(103, "DBLINK. Error reading <%s>", cid.substr(1).c_str());
                }
            }
            else
            {
                cid_value = cid;
            }
        }
        else
        { // new CID connect style:
            std::ifstream cids(cid_file);
            if (cids.is_open())
            {
                std::string cline;
                size_t pos;
                while (getline(cids, cline))
                {
                    if (cline[0] == '#' || cline.empty())
                    {
                        continue; // skip empty lines & comments
                    }
                    if ((pos = cline.find(":")) != std::string::npos)
                    {
                        cid_name = cline.substr(0, pos);
                        if (cid_name == cid)
                        {
                            cid_value = cline.substr(pos + 1, std::string::npos);
                        }
                        else if (cid_name == cid + "$")
                        {
                            cid_env = cline.substr(pos + 1, std::string::npos);
                            std::stringstream se_stream(cid_env);
                            std::string token;
                            while (std::getline(se_stream, token, ';'))
                            {
                                size_t pos = 0;
                                if ((pos = token.find('=')) && pos != std::string::npos)
                                {
#ifdef DBLINK_DEBUG
                                    srvInterface.log("DEBUG DBLINK setting <%s> to <%s>", token.substr(0, pos).c_str(), token.substr(pos + 1).c_str());
#endif
                                    setenv(token.substr(0, pos).c_str(), token.substr(pos + 1).c_str(), 1);
                                }
                            }
                        }
                    }
                    else
                    {
                        continue; // skip malformed lines
                    }
                }
                cids.close();
            }
            else
            {
                vt_report_error(104, "DBLINK. Error reading <%s>", cid_file.c_str());
            }

            if (cid_value.empty())
            {
                vt_report_error(105, "DBLINK. Error finding CID <%s> in <%s>", cid.c_str(), DBLINK_CIDS);
            }
        }
        cidValue = cid_value;
    }

    void getQuery(ServerInterface &srvInterface, std::string &query, bool &isSelect)
    {
        std::string queryString = "";

        // Read Params:
        ParamReader params = srvInterface.getParamReader();

        if (params.containsParameter("query"))
        {
            queryString = params.getStringRef("query").str();
#ifdef DBLINK_DEBUG
            srvInterface.log("DEBUG DBLINK read param query=<%s>", queryString.c_str());
#endif
        }
        else
        {
            vt_report_error(102, "DBLINK. Missing query parameter");
        }

        // Check if "query" is a script file name:
        if (queryString[0] == '@')
        {
            std::ifstream qscript(queryString.substr(1));
            if (qscript.is_open())
            {
                std::stringstream ssFile;
                ssFile << qscript.rdbuf();
                queryString = ssFile.str();
            }
            else
            {
                vt_report_error(106, "DBLINK. Error reading query from <%s>", queryString.substr(1).c_str());
            }
        }

        // Determine Statement type:
        queryString.erase(0, queryString.find_first_not_of(" \n\t\r"));
        if (!strncasecmp(queryString.c_str(), "SELECT", 6))
        {
            isSelect = true;
        }
        query = queryString;
    }

    void clean(SQLHSTMT Ost, SQLHDBC Ocon, SQLHENV Oenv)
    {
        if (Ost)
        {
            (void)SQLFreeHandle(SQL_HANDLE_STMT, Ost);
            Ost = nullptr;
        }
        if (Ocon)
        {
            (void)SQLDisconnect(Ocon);
            (void)SQLFreeHandle(SQL_HANDLE_DBC, Ocon);
            Ocon = nullptr;
        }
        if (Oenv)
        {
            (void)SQLFreeHandle(SQL_HANDLE_ENV, Oenv);
            Oenv = nullptr;
        }
    }

    void ex_err(SQLSMALLINT htype, SQLHANDLE Oh, int loc, const char *vtext, SQLHSTMT Ost, SQLHDBC Ocon, SQLHENV Oenv)
    {
        SQLCHAR Oerr_state[6];                 // ODBC Error State
        SQLINTEGER Oerr_native = 0;            // ODBC Error Native Code
        SQLCHAR Oerr_text[MAX_ODBC_ERROR_LEN]; // ODBC Error Text
        SQLRETURN Oret = 0;
        SQLSMALLINT Oln = 0;

        Oerr_state[0] = Oerr_text[0] = '\0';

        if (htype == 0)
        {
            vt_report_error(loc, "DBLINK. %s", vtext);
        }
        else if ((Oret = SQLGetDiagRec(htype, Oh, 1, Oerr_state, &Oerr_native, Oerr_text,
                                       (SQLSMALLINT)MAX_ODBC_ERROR_LEN, &Oln)) != SQL_SUCCESS)
        {
            clean(Ost, Ocon, Oenv);
            vt_report_error(loc, "DBLINK. %s. Unable to display ODBC error message", vtext);
        }
        else
        {
            clean(Ost, Ocon, Oenv);
            vt_report_error(loc, "DBLINK. %s. State %s. Native Code %d. Error text: %s%c",
                            vtext, (char *)Oerr_state, (int)Oerr_native, (char *)Oerr_text,
                            (Oln > MAX_ODBC_ERROR_LEN) ? '>' : '.');
        }
    }

    class DBLink : public TransformFunction
    {
        SQLHENV Oenv = nullptr;
        SQLHDBC Ocon = nullptr;
        SQLHSTMT Ost = nullptr;

        std::string cid_value = "";
        std::string query = "";
        bool is_select = false;
        size_t rowset;

        void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
        {
            getCidValue(srvInterface, cid_value);
            getQuery(srvInterface, query, is_select);

            // Read/Set rowset Param:
            ParamReader params = srvInterface.getParamReader();
            if (params.containsParameter("rowset"))
            {
                vint rowset_param = params.getIntRef("rowset");
                if (rowset_param < 1 || rowset_param > MAX_ROWSET)
                {
                    ex_err(0, 0, 203, "DBLink. Error rowset out of range", Ost, Ocon, Oenv);
                }
                else
                {
                    rowset = (size_t)rowset_param;
                }
            }
            else
            {
                rowset = DEF_ROWSET;
            }
        }

        void cancel(ServerInterface &srvInterface)
        {
            SQLRETURN Oret = 0;
            if (Ost)
            {
                if (!SQL_SUCCEEDED(Oret = SQLCancel(Ost)))
                {
                    ex_err(SQL_HANDLE_STMT, Ost, 301, "Error canceling SQL statement", Ost, Ocon, Oenv);
                }
            }
            clean(Ost, Ocon, Oenv);
        }

        void destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
        {
#ifdef DBLINK_DEBUG
            srvInterface.log("DEBUG DBLink clean called in DBLink::destory");
#endif
            clean(Ost, Ocon, Oenv);
        }

        void processPartition(ServerInterface &srvInterface,
                              PartitionReader &inputReader,
                              PartitionWriter &outputWriter)
        {
            SQLCHAR Obuff[64];
            DBs dbt;

            SQLSMALLINT Onamel = 0;
            SQLSMALLINT Onull = 0;
            SQLCHAR Ocname[MAXCNAMELEN];
            SQLUSMALLINT Oncol = 0;
            SQLPOINTER *Ores;
            SQLLEN **Olen;
            StringParsers parser;

            SQLRETURN Oret = 0;
            SQLPOINTER Odp = nullptr;
            SQLULEN Odl = 0;
            SQLULEN nfr = 0;

            // ODBC Connection:
            if (!SQL_SUCCEEDED(Oret = SQLAllocHandle(SQL_HANDLE_ENV, (SQLHANDLE)SQL_NULL_HANDLE, &Oenv)))
            {
                ex_err(0, 0, 107, "Error allocating Environment Handle", Ost, Ocon, Oenv);
            }
            if (!SQL_SUCCEEDED(Oret = SQLSetEnvAttr(Oenv, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0)))
            {
                ex_err(0, 0, 108, "Error setting SQL_OV_ODBC3", Ost, Ocon, Oenv);
            }
            if (!SQL_SUCCEEDED(Oret = SQLAllocHandle(SQL_HANDLE_DBC, Oenv, &Ocon)))
            {
                ex_err(0, 0, 109, "Error allocating Connection Handle", Ost, Ocon, Oenv);
            }
            if (!SQL_SUCCEEDED(Oret = SQLDriverConnect(Ocon, (SQLHWND)NULL, (SQLCHAR *)cid_value.c_str(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT)))
            {
                ex_err(SQL_HANDLE_DBC, Ocon, 110, "Error connecting to target database", Ost, Ocon, Oenv);
            }

            // ODBC Statement preparation:
            if (!SQL_SUCCEEDED(Oret = SQLAllocHandle(SQL_HANDLE_STMT, Ocon, &Ost)))
            {
                ex_err(SQL_HANDLE_DBC, Ocon, 111, "Error allocating Statement Handle", Ost, Ocon, Oenv);
            }

            // Check the DBMS we are connecting to:
            if (!SQL_SUCCEEDED(Oret = SQLGetInfo(Ocon, SQL_DBMS_NAME,
                                                 (SQLPOINTER)Obuff, (SQLSMALLINT)sizeof(Obuff), NULL)))
            {
                ex_err(SQL_HANDLE_DBC, Ocon, 202, "Error getting remote DBMS Name", Ost, Ocon, Oenv);
            }
            if (!strcmp((char *)Obuff, "Oracle"))
            {
                dbt = ORACLE;
            }
            else
            {
                dbt = GENERIC;
            }
            memset(&Obuff[0], 0, sizeof(Obuff));

            try
            {
                if (is_select)
                {
                    if (!SQL_SUCCEEDED(Oret = SQLPrepare(Ost, (SQLCHAR *)query.c_str(), SQL_NTS)))
                    {
                        ex_err(SQL_HANDLE_STMT, Ost, 112, "Error preparing the statement", Ost, Ocon, Oenv);
                    }
                    if (!SQL_SUCCEEDED(Oret = SQLNumResultCols(Ost, (SQLSMALLINT *)&Oncol)))
                    {
                        ex_err(SQL_HANDLE_STMT, Ost, 115, "Error finding the number of resulting columns", Ost, Ocon, Oenv);
                    }

                    std::unique_ptr<SQLSMALLINT[], decltype(&free)> Odt(static_cast<SQLSMALLINT *>(calloc((size_t)Oncol, sizeof(SQLSMALLINT))), std::free);
                    if (Odt.get() == nullptr)
                    {
                        ex_err(0, 0, 116, "Error allocating data types array", Ost, Ocon, Oenv);
                    }

                    std::unique_ptr<SQLULEN[], decltype(&free)> Ors(static_cast<SQLULEN *>(calloc((size_t)Oncol, sizeof(SQLULEN))), std::free);
                    if (Ors.get() == nullptr)
                    {
                        ex_err(0, 0, 117, "Error allocating result set columns size array", Ost, Ocon, Oenv);
                    }

                    std::unique_ptr<size_t[], decltype(&free)> desz(static_cast<size_t *>(calloc((size_t)Oncol, sizeof(size_t))), std::free);
                    if (desz.get() == nullptr)
                    {
                        ex_err(0, 0, 118, "Error allocating data element size array", Ost, Ocon, Oenv);
                    }

                    std::unique_ptr<SQLSMALLINT[], decltype(&free)> Odd(static_cast<SQLSMALLINT *>(calloc((size_t)Oncol, sizeof(SQLSMALLINT))), std::free);
                    if (Odd.get() == nullptr)
                    {
                        ex_err(0, 0, 119, "Error allocating result set decimal size array", Ost, Ocon, Oenv);
                    }

                    // Allocate memory for Result Set and length array pointers:
                    Ores = (SQLPOINTER *)srvInterface.allocator->alloc(Oncol * sizeof(SQLPOINTER));
                    Olen = (SQLLEN **)srvInterface.allocator->alloc(Oncol * sizeof(SQLLEN *));
#ifdef DBLINK_DEBUG
                    srvInterface.log("DEBUG DBLink num_col=%d allocated size Ores=%lu Olen=%lu", Oncol, malloc_usable_size(Ores), malloc_usable_size(Olen));
#endif

                    // Allocate space for each column and bind it:
                    for (unsigned int j = 0; j < Oncol; j++)
                    {
                        SQLLEN Ool = 0;
                        if (!SQL_SUCCEEDED(Oret = SQLDescribeCol(Ost, (SQLUSMALLINT)(j + 1),
                                                                 Ocname, (SQLSMALLINT)MAXCNAMELEN, &Onamel,
                                                                 &Odt[j], &Ors[j], &Odd[j], &Onull)))
                        {
                            ex_err(SQL_HANDLE_STMT, Ost, 120, "Error getting column description", Ost, Ocon, Oenv);
                        }
#ifdef DBLINK_DEBUG
                        srvInterface.log("DEBUG DBLink SQLDescribeCol src column=%u name=%s data_type=%d length=%zu", j, (char *)Ocname, Odt[j], Ors[j]);
#endif

                        Olen[j] = (SQLLEN *)srvInterface.allocator->alloc(sizeof(SQLLEN) * rowset);
                        std::string cname((char *)Ocname);
                        switch (Odt[j])
                        {
                        case SQL_SMALLINT:
                        case SQL_INTEGER:
                        case SQL_TINYINT:
                        case SQL_BIGINT:
                            desz[j] = (dbt == ORACLE) ? (size_t)(Ors[j] + 1) : sizeof(vint);
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, (dbt == ORACLE) ? SQL_C_CHAR : SQL_C_SBIGINT, Ores[j], desz[j], Olen[j])))
                            {
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            }
                            break;
                        case SQL_REAL:
                        case SQL_DOUBLE:
                        case SQL_FLOAT:
                            desz[j] = sizeof(vfloat);
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_DOUBLE, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_NUMERIC:
                        case SQL_DECIMAL:
                            desz[j] = MAX_NUMERIC_CHARLEN;
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_CHAR, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_CHAR:
                        case SQL_VARCHAR:
                        case SQL_WCHAR:
                        case SQL_WVARCHAR:
                            if (!SQL_SUCCEEDED(Oret = SQLColAttribute(Ost, (SQLUSMALLINT)(j + 1), SQL_DESC_OCTET_LENGTH,
                                                                      (SQLPOINTER)NULL, (SQLSMALLINT)0, (SQLSMALLINT *)NULL, &Ool)))
                            {
                                ex_err(SQL_HANDLE_STMT, Ost, 120, "Error getting column description", Ost, Ocon, Oenv);
                            }
#ifdef DBLINK_DEBUG
                            srvInterface.log("DEBUG DBLink SQLColAttribute SQL_DESC_OCTET_LENGTH src column=%u name=%s data_type=%d length=%ld", j, (char *)Ocname, Odt[j], Ool);
#endif
                            if (Ool > 0 && (SQLULEN)Ool > Ors[j])
                            {
                                Ors[j] = Ool;
                            }
                            if (Ors[j] > MAX_CHAR_LEN)
                            {
                                srvInterface.log("DBLink SQL_[W]CHAR/SQL_[W]VARCHAR column %s of length %zu limited to %d bytes", (char *)Ocname, Ors[j], MAX_CHAR_LEN);
                                Ors[j] = MAX_CHAR_LEN;
                            }
                            desz[j] = (size_t)(Ors[j] + 1);
                            if (!Ors[j])
                            {
                                Ors[j] = 1;
                            }
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_CHAR, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_LONGVARCHAR:
                        case SQL_WLONGVARCHAR:
                            if (!SQL_SUCCEEDED(Oret = SQLColAttribute(Ost, (SQLUSMALLINT)(j + 1), SQL_DESC_OCTET_LENGTH,
                                                                      (SQLPOINTER)NULL, (SQLSMALLINT)0, (SQLSMALLINT *)NULL, &Ool)))
                            {
                                ex_err(SQL_HANDLE_STMT, Ost, 120, "Error getting column description", Ost, Ocon, Oenv);
                            }
#ifdef DBLINK_DEBUG
                            srvInterface.log("DEBUG DBLink SQLColAttribute SQL_DESC_OCTET_LENGTH src column=%u name=%s data_type=%d length=%ld", j, (char *)Ocname, Odt[j], Ool);
#endif
                            if (Ool > 0 && (SQLULEN)Ool > Ors[j])
                            {
                                Ors[j] = Ool;
                            }
                            if (Ors[j] > MAX_LONGCHAR_LEN)
                            {
                                Ors[j] = MAX_LONGCHAR_LEN;
                            }
                            desz[j] = (size_t)(Ors[j] + 1);
                            if (!Ors[j])
                            {
                                Ors[j] = 1;
                            }
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_CHAR, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_TYPE_TIME:
                            desz[j] = sizeof(SQL_TIME_STRUCT);
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_TIME, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_TYPE_DATE:
                            desz[j] = sizeof(SQL_DATE_STRUCT);
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_DATE, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_TYPE_TIMESTAMP:
                            desz[j] = sizeof(SQL_TIMESTAMP_STRUCT);
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_TIMESTAMP, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_BIT:
                            desz[j] = 1;
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_BIT, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_BINARY:
                        case SQL_VARBINARY:
                            if (Ors[j] > MAX_BINARY_LEN)
                            {
                                Ors[j] = MAX_BINARY_LEN;
                            }
                            desz[j] = (size_t)(Ors[j] + 1);
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_BINARY, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_LONGVARBINARY:
                            if (Ors[j] > MAX_LONGBINARY_LEN)
                            {
                                Ors[j] = MAX_LONGBINARY_LEN;
                            }
                            desz[j] = (size_t)(Ors[j] + 1);
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_BINARY, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_INTERVAL_YEAR_TO_MONTH:
                            desz[j] = sizeof(SQL_INTERVAL_STRUCT);
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_INTERVAL_YEAR_TO_MONTH, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        case SQL_INTERVAL_DAY_TO_SECOND:
                            desz[j] = sizeof(SQL_INTERVAL_STRUCT);
                            Ores[j] = (SQLPOINTER)srvInterface.allocator->alloc(desz[j] * rowset);
                            if (!SQL_SUCCEEDED(Oret = SQLBindCol(Ost, j + 1, SQL_C_INTERVAL_DAY_TO_SECOND, Ores[j], desz[j], Olen[j])))
                                ex_err(SQL_HANDLE_STMT, Ost, 401, "Error binding column", Ost, Ocon, Oenv);
                            break;
                        default:
                            vt_report_error(121, "DBLink. Unsupported data type for column %u", j);
                        }
                    }
#ifdef DBLINK_DEBUG
                    srvInterface.log("DEBUG DBLink Allocation and Binding were completed");
#endif

                    // Set Statement attributes:
                    if (!SQL_SUCCEEDED(Oret = SQLSetStmtAttr(Ost, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)SQL_BIND_BY_COLUMN, 0)))
                    {
                        ex_err(SQL_HANDLE_STMT, Ost, 402, "Error setting statement attribute SQL_ATTR_ROW_BIND_TYPE", Ost, Ocon, Oenv);
                    }
                    if (!SQL_SUCCEEDED(Oret = SQLSetStmtAttr(Ost, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)rowset, 0)))
                    {
                        ex_err(SQL_HANDLE_STMT, Ost, 402, "Error setting statement attribute SQL_ATTR_ROW_ARRAY_SIZE", Ost, Ocon, Oenv);
                    }
                    if (!SQL_SUCCEEDED(Oret = SQLSetStmtAttr(Ost, SQL_ATTR_ROWS_FETCHED_PTR, &nfr, 0)))
                    {
                        ex_err(SQL_HANDLE_STMT, Ost, 402, "Error setting statement attribute SQL_ATTR_ROWS_FETCHED_PTR", Ost, Ocon, Oenv);
                    }
#ifdef DBLINK_DEBUG
                    srvInterface.log("DEBUG DBLink Setting attributes were completed");
#endif

                    // Execute Stateent:
                    if (!SQL_SUCCEEDED(Oret = SQLExecute(Ost)) && Oret != SQL_NO_DATA)
                    {
                        ex_err(SQL_HANDLE_STMT, Ost, 403, "Error executing the statement", Ost, Ocon, Oenv);
                    }
#ifdef DBLINK_DEBUG
                    srvInterface.log("DEBUG DBLink Executed the statement");
#endif

                    // Fetch loop:
                    while (SQL_SUCCEEDED(Oret = SQLFetchScroll(Ost, SQL_FETCH_NEXT, 0)) && !isCanceled())
                    {
                        if (Oret == SQL_NO_DATA_FOUND)
                        {
#ifdef DBLINK_DEBUG
                            srvInterface.log("DEBUG DBLink End of record set");
#endif
                            break;
                        }

#ifdef DBLINK_DEBUG
                        srvInterface.log("DEBUG DBLink rows fetched=%lu", nfr);
#endif

                        for (unsigned int i = 0; i < nfr; i++, outputWriter.next())
                        {
                            for (unsigned int j = 0; j < Oncol; j++)
                            {
                                Odp = (SQLPOINTER)((uint8_t *)Ores[j] + desz[j] * i);
                                Odl = Olen[j][i];

                                if ((int)Odl == (int)SQL_NULL_DATA)
                                {
                                    outputWriter.setNull(j);
                                    continue;
                                }

                                switch (Odt[j])
                                {
                                case SQL_SMALLINT:
                                case SQL_INTEGER:
                                case SQL_TINYINT:
                                case SQL_BIGINT:
                                {
                                    if (dbt == ORACLE)
                                    {
                                        outputWriter.setInt(j, ((int)Odl == SQL_NTS) ? vint_null : (vint)atoll((char *)Odp));
                                    }
                                    else
                                    {
                                        outputWriter.setInt(j, *(SQLBIGINT *)Odp);
                                    }
                                    break;
                                }
                                case SQL_REAL:
                                case SQL_DOUBLE:
                                case SQL_FLOAT:
                                {
                                    outputWriter.setFloat(j, *(SQLDOUBLE *)Odp);
                                    break;
                                }
                                case SQL_NUMERIC:
                                case SQL_DECIMAL:
                                {
                                    std::string rejectReason = "Unrecognized remote database format";
                                    if (*(char *)Odp == '\0')
                                    { // some DBs might use empty strings for NUMERIC nulls
                                        outputWriter.setNull(j);
                                    }
                                    else
                                    {
                                        if (!parser.parseNumeric((char *)Odp, (size_t)Odl, j,
                                                                 outputWriter.getNumericRef(j), outputWriter.getTypeMetaData().getColumnType(j), rejectReason))
                                        {
                                            ex_err(0, 0, 404, "Error parsing Numeric", Ost, Ocon, Oenv);
                                        }
                                    }
                                    break;
                                }
                                case SQL_CHAR:
                                case SQL_WCHAR:
                                case SQL_VARCHAR:
                                case SQL_WVARCHAR:
                                case SQL_LONGVARCHAR:
                                case SQL_WLONGVARCHAR:
                                case SQL_BINARY:
                                case SQL_VARBINARY:
                                case SQL_LONGVARBINARY:
                                {
                                    if ((int)Odl == SQL_NTS)
                                    {
                                        Odl = (SQLULEN)strnlen((char *)Odp, desz[j]);
                                    }
                                    outputWriter.getStringRef(j).copy((char *)Odp, Odl);
                                    break;
                                }
                                case SQL_TYPE_TIME:
                                {
                                    SQL_TIME_STRUCT &st = *(SQL_TIME_STRUCT *)Odp;
                                    outputWriter.setTime(j, getTimeFromUnixTime(st.second + st.minute * 60 + st.hour * 3600));
                                    break;
                                }
                                case SQL_TYPE_DATE:
                                {
                                    SQL_DATE_STRUCT &sd = *(SQL_DATE_STRUCT *)Odp;
                                    struct tm d = {0, 0, 0, sd.day, sd.month - 1, sd.year - 1900, 0, 0, -1};
                                    time_t utime = mktime(&d);
                                    outputWriter.setDate(j, getDateFromUnixTime(utime + d.tm_gmtoff));
                                    break;
                                }
                                case SQL_TYPE_TIMESTAMP:
                                {
                                    SQL_TIMESTAMP_STRUCT &ss = *(SQL_TIMESTAMP_STRUCT *)Odp;
                                    struct tm ts = {ss.second, ss.minute, ss.hour, ss.day, ss.month - 1, ss.year - 1900, 0, 0, -1};
                                    time_t utime = mktime(&ts);
                                    outputWriter.setTimestamp(j, getTimestampFromUnixTime(utime + ts.tm_gmtoff) + ss.fraction / 1000);
                                    break;
                                }
                                case SQL_BIT:
                                    outputWriter.setBool(j, *(SQLCHAR *)Odp == SQL_TRUE ? VTrue : VFalse);
                                    break;
                                case SQL_INTERVAL_YEAR_TO_MONTH: // Vertica stores these Intervals as durations in months
                                {
                                    SQL_INTERVAL_STRUCT &intv = *(SQL_INTERVAL_STRUCT *)Odp;
                                    if (intv.interval_type != SQL_IS_YEAR_TO_MONTH)
                                    {
                                        ex_err(0, 0, 405, "Unsupported INTERVAL data type. Expecting SQL_IS_YEAR_TO_MONTH", Ost, Ocon, Oenv);
                                    }
                                    Interval ret = ((intv.intval.year_month.year * MONTHS_PER_YEAR) + (intv.intval.year_month.month)) * (intv.interval_sign == SQL_TRUE ? -1 : 1);
                                    outputWriter.setInterval(j, ret);
                                    break;
                                }
                                case SQL_INTERVAL_DAY_TO_SECOND: // Vertica stores these Intervals as durations in microseconds
                                {
                                    SQL_INTERVAL_STRUCT &intv = *(SQL_INTERVAL_STRUCT *)Odp;
                                    if (intv.interval_type != SQL_IS_DAY_TO_SECOND)
                                    {
                                        ex_err(0, 0, 406, "Unsupported INTERVAL data type. Expecting SQL_IS_DAY_TO_SECOND", Ost, Ocon, Oenv);
                                    }
                                    Interval ret = ((intv.intval.day_second.day * usPerDay) + (intv.intval.day_second.hour * usPerHour) + (intv.intval.day_second.minute * usPerMinute) + (intv.intval.day_second.second * usPerSecond) + (intv.intval.day_second.fraction / 1000)) * (intv.interval_sign == SQL_TRUE ? -1 : 1);
                                    outputWriter.setInterval(j, ret);
                                    break;
                                }
                                default:
                                    vt_report_error(407, "DBLINK. Unsupported data type for column %u", j);
                                    break;
                                }
                            }
                        }
                    }
                }
                else
                {
                    if (!SQL_SUCCEEDED(Oret = SQLExecDirect(Ost, (SQLCHAR *)query.c_str(), SQL_NTS)))
                    {
                        ex_err(SQL_HANDLE_STMT, Ost, 408, "Error executing statement", Ost, Ocon, Oenv);
                    }
                    outputWriter.setInt(0, (vint)Oret);
                    outputWriter.next();
                }
            }
            catch (exception &e)
            {
#ifdef DBLINK_DEBUG
                srvInterface.log("DEBUG DBLink clean called in catch in DBLink::processPartition");
#endif
                clean(Ost, Ocon, Oenv);
                vt_report_error(400, "Exception while processing partition: [%s]", e.what());
            }
        }
    };

    class DBLinkFactory : public TransformFunctionFactory
    {
        size_t alloc_size_res = 0;

        void getPrototype(ServerInterface &srvInterface,
                          ColumnTypes &argTypes,
                          ColumnTypes &returnType)
        {
            returnType.addAny();
        }

        void getReturnType(ServerInterface &srvInterface,
                           const SizedColumnTypes &inputTypes,
                           SizedColumnTypes &outputTypes)
        {
            SQLCHAR Obuff[64];
            DBs dbt;

            SQLHENV Oenv = nullptr;
            SQLHDBC Ocon = nullptr;
            SQLHSTMT Ost = nullptr;
            SQLRETURN Oret = 0;
            SQLSMALLINT Onamel = 0;
            SQLSMALLINT Onull = 0;
            SQLCHAR Ocname[MAXCNAMELEN];
            SQLUSMALLINT Oncol = 0;
            bool is_select = false;
            std::string cid_value = "";
            std::string query = "";
            size_t rowset = 0;

            getCidValue(srvInterface, cid_value);
            getQuery(srvInterface, query, is_select);

            // Read/Set rowset Param:
            ParamReader params = srvInterface.getParamReader();
            if (params.containsParameter("rowset"))
            {
                vint rowset_param = params.getIntRef("rowset");
                if (rowset_param < 1 || rowset_param > MAX_ROWSET)
                {
                    ex_err(0, 0, 203, "DBLink. Error rowset out of range", Ost, Ocon, Oenv);
                }
                else
                {
                    rowset = (size_t)rowset_param;
                }
            }
            else
            {
                rowset = DEF_ROWSET;
            }

            // ODBC Connection:
            if (!SQL_SUCCEEDED(Oret = SQLAllocHandle(SQL_HANDLE_ENV, (SQLHANDLE)SQL_NULL_HANDLE, &Oenv)))
            {
                ex_err(0, 0, 107, "Error allocating Environment Handle", Ost, Ocon, Oenv);
            }
            if (!SQL_SUCCEEDED(Oret = SQLSetEnvAttr(Oenv, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0)))
            {
                ex_err(0, 0, 108, "Error setting SQL_OV_ODBC3", Ost, Ocon, Oenv);
            }
            if (!SQL_SUCCEEDED(Oret = SQLAllocHandle(SQL_HANDLE_DBC, Oenv, &Ocon)))
            {
                ex_err(0, 0, 109, "Error allocating Connection Handle", Ost, Ocon, Oenv);
            }
            if (!SQL_SUCCEEDED(Oret = SQLDriverConnect(Ocon, (SQLHWND)NULL, (SQLCHAR *)cid_value.c_str(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT)))
            {
                ex_err(SQL_HANDLE_DBC, Ocon, 110, "Error connecting to target database", Ost, Ocon, Oenv);
            }

            // ODBC Statement preparation:
            if (!SQL_SUCCEEDED(Oret = SQLAllocHandle(SQL_HANDLE_STMT, Ocon, &Ost)))
            {
                ex_err(SQL_HANDLE_DBC, Ocon, 111, "Error allocating Statement Handle", Ost, Ocon, Oenv);
            }

            // Check the DBMS we are connecting to:
            if (!SQL_SUCCEEDED(Oret = SQLGetInfo(Ocon, SQL_DBMS_NAME,
                                                 (SQLPOINTER)Obuff, (SQLSMALLINT)sizeof(Obuff), NULL)))
            {
                ex_err(SQL_HANDLE_DBC, Ocon, 202, "Error getting remote DBMS Name", Ost, Ocon, Oenv);
            }
            if (!strcmp((char *)Obuff, "Oracle"))
            {
                dbt = ORACLE;
            }
            else
            {
                dbt = GENERIC;
            }
            memset(&Obuff[0], 0, sizeof(Obuff));

            if (is_select)
            {
                if (!SQL_SUCCEEDED(Oret = SQLPrepare(Ost, (SQLCHAR *)query.c_str(), SQL_NTS)))
                {
                    ex_err(SQL_HANDLE_STMT, Ost, 112, "Error preparing the statement", Ost, Ocon, Oenv);
                }
                if (!SQL_SUCCEEDED(Oret = SQLNumResultCols(Ost, (SQLSMALLINT *)&Oncol)))
                {
                    ex_err(SQL_HANDLE_STMT, Ost, 115, "Error finding the number of resulting columns", Ost, Ocon, Oenv);
                }

                std::unique_ptr<SQLSMALLINT[], decltype(&free)> Odt(static_cast<SQLSMALLINT *>(calloc((size_t)Oncol, sizeof(SQLSMALLINT))), std::free);
                if (Odt.get() == nullptr)
                {
                    ex_err(0, 0, 116, "Error allocating data types array", Ost, Ocon, Oenv);
                }

                std::unique_ptr<SQLULEN[], decltype(&free)> Ors(static_cast<SQLULEN *>(calloc((size_t)Oncol, sizeof(SQLULEN))), std::free);
                if (Ors.get() == nullptr)
                {
                    ex_err(0, 0, 117, "Error allocating result set columns size array", Ost, Ocon, Oenv);
                }

                std::unique_ptr<size_t[], decltype(&free)> desz(static_cast<size_t *>(calloc((size_t)Oncol, sizeof(size_t))), std::free);
                if (desz.get() == nullptr)
                {
                    ex_err(0, 0, 118, "Error allocating data element size array", Ost, Ocon, Oenv);
                }

                std::unique_ptr<SQLSMALLINT[], decltype(&free)> Odd(static_cast<SQLSMALLINT *>(calloc((size_t)Oncol, sizeof(SQLSMALLINT))), std::free);
                if (Odd.get() == nullptr)
                {
                    ex_err(0, 0, 119, "Error allocating result set decimal size array", Ost, Ocon, Oenv);
                }

                for (unsigned int j = 0; j < Oncol; j++)
                {
                    SQLLEN Ool = 0;
                    if (!SQL_SUCCEEDED(Oret = SQLDescribeCol(Ost, (SQLUSMALLINT)(j + 1),
                                                             Ocname, (SQLSMALLINT)MAXCNAMELEN, &Onamel,
                                                             &Odt[j], &Ors[j], &Odd[j], &Onull)))
                    {
                        ex_err(SQL_HANDLE_STMT, Ost, 120, "Error getting column description", Ost, Ocon, Oenv);
                    }
#ifdef DBLINK_DEBUG
                    srvInterface.log("DEBUG DBLinkFactory SQLDescribeCol src column=%u name=%s data_type=%d length=%zu", j, (char *)Ocname, Odt[j], Ors[j]);
#endif

                    alloc_size_res += sizeof(SQLLEN) * rowset;
                    std::string cname((char *)Ocname);
                    switch (Odt[j])
                    {
                    case SQL_SMALLINT:
                    case SQL_INTEGER:
                    case SQL_TINYINT:
                    case SQL_BIGINT:
                        alloc_size_res += (dbt == ORACLE ? (size_t)(Ors[j] + 1) : sizeof(vint)) * rowset;
                        outputTypes.addInt(cname);
                        break;
                    case SQL_REAL:
                    case SQL_DOUBLE:
                    case SQL_FLOAT:
                        alloc_size_res += sizeof(vfloat) * rowset;
                        outputTypes.addFloat(cname);
                        break;
                    case SQL_NUMERIC:
                    case SQL_DECIMAL:
                        alloc_size_res += MAX_NUMERIC_CHARLEN * rowset;
                        outputTypes.addNumeric((int32)Ors[j], (int32)Odd[j], cname);
                        break;
                    case SQL_CHAR:
                    case SQL_WCHAR:
                        if (!SQL_SUCCEEDED(Oret = SQLColAttribute(Ost, (SQLUSMALLINT)(j + 1), SQL_DESC_OCTET_LENGTH,
                                                                  (SQLPOINTER)NULL, (SQLSMALLINT)0, (SQLSMALLINT *)NULL, &Ool)))
                        {
                            ex_err(SQL_HANDLE_STMT, Ost, 120, "Error getting column description", Ost, Ocon, Oenv);
                        }
#ifdef DBLINK_DEBUG
                        srvInterface.log("DEBUG DBLinkFactory SQLColAttribute SQL_DESC_OCTET_LENGTH src column=%u name=%s data_type=%d length=%ld", j, (char *)Ocname, Odt[j], Ool);
#endif
                        if (Ool > 0 && (SQLULEN)Ool > Ors[j])
                        {
                            Ors[j] = Ool;
                        }
                        if (Ors[j] > MAX_CHAR_LEN)
                        {
                            srvInterface.log("DBLinkFactory SQL_[W]CHAR column %s of length %zu limited to %d bytes", (char *)Ocname, Ors[j], MAX_CHAR_LEN);
                            Ors[j] = MAX_CHAR_LEN;
                        }
                        if (!Ors[j])
                        {
                            Ors[j] = 1;
                        }
                        alloc_size_res += (size_t)(Ors[j] + 1) * rowset;
                        outputTypes.addChar((int32)Ors[j], cname);
                        break;
                    case SQL_VARCHAR:
                    case SQL_WVARCHAR:
                        if (!SQL_SUCCEEDED(Oret = SQLColAttribute(Ost, (SQLUSMALLINT)(j + 1), SQL_DESC_OCTET_LENGTH,
                                                                  (SQLPOINTER)NULL, (SQLSMALLINT)0, (SQLSMALLINT *)NULL, &Ool)))
                        {
                            ex_err(SQL_HANDLE_STMT, Ost, 120, "Error getting column description", Ost, Ocon, Oenv);
                        }
#ifdef DBLINK_DEBUG
                        srvInterface.log("DEBUG DBLinkFactory SQLColAttribute SQL_DESC_OCTET_LENGTH src column=%u name=%s data_type=%d length=%ld", j, (char *)Ocname, Odt[j], Ool);
#endif
                        if (Ool > 0 && (SQLULEN)Ool > Ors[j])
                        {
                            Ors[j] = Ool;
                        }
                        if (Ors[j] > MAX_CHAR_LEN)
                        {
                            srvInterface.log("DBLinkFactory SQL_[W]VARCHAR column %s of length %zu limited to %d bytes", (char *)Ocname, Ors[j], MAX_CHAR_LEN);
                            Ors[j] = MAX_CHAR_LEN;
                        }
                        if (!Ors[j])
                        {
                            Ors[j] = 1;
                        }
                        alloc_size_res += (size_t)(Ors[j] + 1) * rowset;
                        outputTypes.addVarchar((int32)Ors[j], cname);
                        break;
                    case SQL_LONGVARCHAR:
                    case SQL_WLONGVARCHAR:
                        if (!SQL_SUCCEEDED(Oret = SQLColAttribute(Ost, (SQLUSMALLINT)(j + 1), SQL_DESC_OCTET_LENGTH,
                                                                  (SQLPOINTER)NULL, (SQLSMALLINT)0, (SQLSMALLINT *)NULL, &Ool)))
                        {
                            ex_err(SQL_HANDLE_STMT, Ost, 120, "Error getting column description", Ost, Ocon, Oenv);
                        }
#ifdef DBLINK_DEBUG
                        srvInterface.log("DEBUG DBLinkFactory SQLColAttribute SQL_DESC_OCTET_LENGTH src column=%u name=%s data_type=%d length=%ld", j, (char *)Ocname, Odt[j], Ool);
#endif
                        if (Ool > 0 && (SQLULEN)Ool > Ors[j])
                        {
                            Ors[j] = Ool;
                        }
                        if (Ors[j] > MAX_LONGCHAR_LEN)
                        {
                            srvInterface.log("DBLinkFactory SQL_LONG[W]VARCHAR column %s of length %zu limited to %d bytes", (char *)Ocname, Ors[j], MAX_LONGCHAR_LEN);
                            Ors[j] = MAX_LONGCHAR_LEN;
                        }
                        if (!Ors[j])
                        {
                            Ors[j] = 1;
                        }
                        alloc_size_res += (size_t)(Ors[j] + 1) * rowset;
                        outputTypes.addLongVarchar((int32)Ors[j], cname);
                        break;
                    case SQL_TYPE_TIME:
                        alloc_size_res += sizeof(SQL_TIME_STRUCT) * rowset;
                        outputTypes.addTime((int32)Odd[j], cname);
                        break;
                    case SQL_TYPE_DATE:
                        alloc_size_res += sizeof(SQL_DATE_STRUCT) * rowset;
                        outputTypes.addDate(cname);
                        break;
                    case SQL_TYPE_TIMESTAMP:
                        alloc_size_res += sizeof(SQL_TIMESTAMP_STRUCT) * rowset;
                        outputTypes.addTimestamp((int32)Odd[j], cname);
                        break;
                    case SQL_BIT:
                        alloc_size_res += rowset;
                        outputTypes.addBool(cname);
                        break;
                    case SQL_BINARY:
                    case SQL_VARBINARY:
                        if (Ors[j] > MAX_BINARY_LEN)
                        {
                            srvInterface.log("DBLinkFactory SQL_[VAR]BINARY column %s of length %zu limited to %d bytes", (char *)Ocname, Ors[j], MAX_BINARY_LEN);
                            Ors[j] = MAX_BINARY_LEN;
                        }
                        alloc_size_res += (size_t)(Ors[j] + 1) * rowset;
                        outputTypes.addBinary((int32)Ors[j], cname);
                        break;
                    case SQL_LONGVARBINARY:
                        if (Ors[j] > MAX_LONGBINARY_LEN)
                        {
                            srvInterface.log("DBLinkFactory SQL_LONGVARBINARY column %s of length %zu limited to %d bytes", (char *)Ocname, Ors[j], MAX_LONGBINARY_LEN);
                            Ors[j] = MAX_LONGBINARY_LEN;
                        }
                        alloc_size_res += (size_t)(Ors[j] + 1) * rowset;
                        outputTypes.addLongVarbinary((int32)Ors[j], cname);
                        break;
                    case SQL_INTERVAL_YEAR_TO_MONTH:
                        alloc_size_res += sizeof(SQL_INTERVAL_STRUCT) * rowset;
                        outputTypes.addIntervalYM(INTERVAL_YEAR2MONTH, cname);
                        break;
                    case SQL_INTERVAL_DAY_TO_SECOND:
                        alloc_size_res += sizeof(SQL_INTERVAL_STRUCT) * rowset;
                        outputTypes.addInterval((int32)Odd[j], INTERVAL_DAY2SECOND, cname);
                        break;
                    default:
                        vt_report_error(121, "DBLinkFactory. Unsupported data type for column %u", j);
                    }
                }
            }
            else
            {
                outputTypes.addInt("dblink");
            }

#ifdef DBLINK_DEBUG
            srvInterface.log("DEBUG DBLinkFactory clean called in DBLinkFactory::getReturnType");
#endif
            clean(Ost, Ocon, Oenv);
        }

        void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes)
        {
            parameterTypes.addVarchar(1024, "cid", {true, false, false, "Connection Identifier Database. Identifies an entry in the connection identifier database."});
            parameterTypes.addVarchar(1024, "connect", {true, false, false, "The ODBC connection string containing the DSN and credentials."});
            parameterTypes.addVarchar(1024, "connect_secret", {true, false, false, "The ODBC connection string containing the DSN and credentials."});
            parameterTypes.addVarchar(1024, "cidfile", {true, false, false, "Connection Identifier File Path."});
            parameterTypes.addVarchar(65000, "query", {true, false, false, "The query being pushed on the remote database. Or, '@' followed by the name of the file containing the query."});
            parameterTypes.addInt("rowset", {true, false, false, "Number of rows retrieved from the remote database during each SQLFetch() cycle. Default is 100."});
        }

        void getPerInstanceResources(ServerInterface &srvInterface,
                                     VResources &res)
        {
#ifdef DBLINK_DEBUG
            srvInterface.log("DEBUG DBLinkFactory Total allocated size for result set = %lu", alloc_size_res);
#endif
            res.scratchMemory += alloc_size_res;
            alloc_size_res = 0;
        }

        TransformFunction *createTransformFunction(ServerInterface &srvInterface)
        {
            return vt_createFuncObject<DBLink>(srvInterface.allocator);
        }
    };

    RegisterFactory(DBLinkFactory);
}

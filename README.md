# DBLINK()

`DBLINK()` is a Vertica [User Defined Transform Function](https://docs.vertica.com/latest/en/extending/developing-udxs/transform-functions-udtfs/) coded in C++ to run SQL against other databases. The original repository is [vertica / dblink](https://github.com/vertica/dblink).

For 'Usage' information, please refer to the [README](https://github.com/h-serizawa/dblink?tab=readme-ov-file#usage) file in the original repository.

As of dblink 0.3.2, it has a limitation that `DBLINK()` is allowed to call only once in one query. The following error occurs if it is called multiple times.

```sql
=> SELECT l.id, l.description, tab_a.value value_a, tab_b.value value_b
-> FROM tab_local l
-> LEFT JOIN (SELECT DBLINK(USING PARAMETERS cid='orcl', query='SELECT * FROM tab1') OVER()) tab_a
->   ON tab_a.id = l.id
-> LEFT JOIN (SELECT DBLINK(USING PARAMETERS cid='orcl', query='SELECT * FROM tab1') OVER()) tab_b
->   ON tab_b.id = l.id;

ERROR 8092:  Failure in UDx RPC call InvokeProcessPartition() in User Defined Object [dblink]: UDx side process has exited abnormally
```

This repository provides `DBLINK()` without this limitation.

### Installation

Set up your environment to meet C++ Requirements described on the following page.
https://docs.vertica.com/latest/en/extending/developing-udxs/developing-with-sdk/setting-up-development-environment/

To compile the source codes in 24.1 or later version, run the following command:

```
$ make
```

In 23.4 or previous version, run the following command:

```
$ CXXFLAGS=-D_GLIBCXX_USE_CXX11_ABI=0 make
```

To install DBLINK function, run the following command:

```
$ make install
```

To uninstall DBLINK function, run the following command:

```
$ make uninstall
```

### Notes

DBLINK function has been tested in Vertica 24.4.
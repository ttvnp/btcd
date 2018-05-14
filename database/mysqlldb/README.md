myqlldb
=====

Package myqlldb implements a driver for the database package that uses leveldb for
the backing metadata and mysql for block storage.

This driver is the recommended driver for use with btcd.  It makes use leveldb
for the metadata, mysql for block storage, and checksums in key areas to
ensure data integrity.

Package myqlldb is licensed under the copyfree ISC license.

## Prerequisite

This package requires mysqld process.
You have to setup mysql database which holds specific tables in advance.
see [./mysql/setup.sql](./mysql/setup.sql)

## Usage

This package is a driver to the database package and provides the database type
of "myqlldb".  The parameters the Open and Create functions take are the
database path as a string and the block network.

```Go
db, err := database.Open(
    "myqlldb",
    "path/to/database",
    wire.MainNet,
    "dbrwconnection=writable_user_name:writable_user_password@tcp(host:port)/db_name",
    "dbroconnection=readonly_user_name:readonly_user_password@tcp(host:port)/db_name",
)
if err != nil {
	// Handle error
}
```

```Go
db, err := database.Create(
    "myqlldb",
    "path/to/database",
    wire.MainNet,
    "dbrwconnection=writable_user_name:writable_user_password@tcp(host:port)/db_name",
    "dbroconnection=readonly_user_name:readonly_user_password@tcp(host:port)/db_name",
)
if err != nil {
	// Handle error
}
```

## License

Package myqlldb is licensed under the [copyfree](http://copyfree.org) ISC
License.

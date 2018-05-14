package myqlldb

import (
	"fmt"

	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/database/mysqlldb/mysql"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btclog"
)

const (
	dbType = "mysqlldb"
)

func init() {
	// Register the driver.
	parseArgs := func(funcName string, args ...interface{}) (string, *mysql.Config, *mysql.Config, wire.BitcoinNet, error) {
		if len(args) != 3 && len(args) != 4 {
			return "", nil, nil, 0, fmt.Errorf(
				"invalid arguments to %s.%s -- expected database path, block network and database connection settings",
				dbType,
				funcName,
			)
		}

		dbPath, ok := args[0].(string)
		if !ok {
			return "", nil, nil, 0, fmt.Errorf("first argument to %s.%s is invalid -- "+
				"expected database path string", dbType, funcName)
		}

		network, ok := args[1].(wire.BitcoinNet)
		if !ok {
			return "", nil, nil, 0, fmt.Errorf("second argument to %s.%s is invalid -- "+
				"expected block network", dbType, funcName)
		}

		dbrwconnection, ok := args[2].(string)
		if !ok {
			return "", nil, nil, 0, fmt.Errorf("third argument to %s.%s is invalid -- "+
				"expected database connection url string", dbType, funcName)
		}
		readWriteConfig, err := mysql.ParseConfig(dbrwconnection)
		if err != nil {
			return "", nil, nil, 0, fmt.Errorf("third argument to %s.%s is invalid -- "+
				"expected database connection url string", dbType, funcName)
		}

		var readOnlyConfig *mysql.Config
		if len(args) == 4 {
			dbroconnection, ok := args[3].(string)
			if !ok {
				return "", nil, nil, 0, fmt.Errorf("fourth argument to %s.%s is invalid -- "+
					"expected database connection url string", dbType, funcName)
			}
			readOnlyConfig, err = mysql.ParseConfig(dbroconnection)
			if err != nil {
				return "", nil, nil, 0, fmt.Errorf("fourth argument to %s.%s is invalid -- "+
					"expected database connection url string", dbType, funcName)
			}
		}

		return dbPath, readWriteConfig, readOnlyConfig, network, nil
	}

	create := func(args ...interface{}) (database.DB, error) {
		dbPath, network, readWriteConfig, readOnlyConfig, err := parseArgs("Create", args...)
		if err != nil {
			return nil, err
		}
		return openDB(dbPath, network, readWriteConfig, readOnlyConfig, true)
	}

	open := func(args ...interface{}) (database.DB, error) {
		dbPath, network, readWriteConfig, readOnlyConfig, err := parseArgs("Open", args...)
		if err != nil {
			return nil, err
		}
		return openDB(dbPath, network, readWriteConfig, readOnlyConfig, false)
	}

	useLogger := func(logger btclog.Logger) {
		log = logger
	}

	driver := database.Driver{
		DbType:    dbType,
		Create:    create,
		Open:      open,
		UseLogger: useLogger,
	}
	if err := database.RegisterDriver(driver); err != nil {
		panic(fmt.Sprintf("Failed to regiser database driver '%s': %v", dbType, err))
	}
}

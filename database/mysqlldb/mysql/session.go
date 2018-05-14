package mysql

import "github.com/gocraft/dbr"

func NewSession(cfg *Config) *dbr.Session {
	conn, err := dbr.Open("mysql", cfg.FormatDSN(), nil)
	if err != nil {
		panic(err)
	}
	conn.SetMaxIdleConns(0)
	conn.SetMaxOpenConns(100)
	return conn.NewSession(nil)
}

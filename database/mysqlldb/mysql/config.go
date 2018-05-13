package mysql

import (
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Config struct {
	config *mysql.Config
}

func NewConfig(host string, port uint, user, password, dbName string) *Config {
	cfg := &mysql.Config{
		User:         user,
		Passwd:       password,
		Net:          "tcp",
		Addr:         fmt.Sprintf("%s:%d", host, port),
		DBName:       dbName,
		Params:       map[string]string{"charset": "utf8", "loc": "UTC"},
		Collation:    "utf8_general_ci",
		Timeout:      5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		ParseTime:    true,
	}
	return &Config{
		config: cfg,
	}
}

func ParseConfig(dsn string) (*Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &Config{
		config: cfg,
	}, nil
}

func (cfg *Config) FormatDSN() string {
	return cfg.config.FormatDSN()
}

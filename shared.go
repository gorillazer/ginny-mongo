package mongo

import (
	"github.com/google/wire"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Provider
var Provider = wire.NewSet(NewConfig, New)

// NewConfig
func NewConfig(v *viper.Viper) (*Config, error) {
	var err error
	o := new(Config)
	if err = v.UnmarshalKey("mongo", o); err != nil {
		return nil, errors.Wrap(err, "unmarshal app option error")
	}
	return o, err
}

// New
func New(conf *Config, logger *zap.Logger) *Manager {
	mgr, err := NewManager(conf, logger)
	if err != nil {
		logger.Sugar().Fatalf("mongodb manager error: %s", err.Error())
	}
	return mgr
}

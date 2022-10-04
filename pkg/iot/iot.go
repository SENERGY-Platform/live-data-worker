package iot

import (
	"context"
	"errors"
	"github.com/SENERGY-Platform/device-command/pkg/command/dependencies/impl/cloud"
	"github.com/SENERGY-Platform/device-command/pkg/command/dependencies/impl/mgw"
	"github.com/SENERGY-Platform/device-command/pkg/command/dependencies/interfaces"
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
)

var singleton interfaces.Iot

func NewIoT(ctx context.Context, config configuration.Config) (iot interfaces.Iot, err error) {
	if singleton != nil {
		return singleton, nil
	}
	dcConf := config.ToDcConf()
	if !config.MgwMode {
		singleton, err = cloud.IotFactory(ctx, dcConf)
		if err != nil {
			return
		}
	} else {
		singleton, err = mgw.IotFactory(ctx, dcConf)
		if err != nil {
			return
		}
	}
	return singleton, nil
}

func SingletonOrErr() (iot interfaces.Iot, err error) {
	if singleton == nil {
		return nil, errors.New("singleton not initialized")
	}
	return singleton, err
}

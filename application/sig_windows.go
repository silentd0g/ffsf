//go:build windows
// +build windows

package application

import (
	"github.com/silentd0g/ffsf/logger"
	"os"
	"os/signal"
	"syscall"
)

func SignalNotify() {
	signal.Notify(sig, syscall.SIGABRT, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
}

func (a *Application) checkSysSignal() {
	select {
	case s := <-sig:
		switch s {
		default:
			logger.Infof("onexit")
			a.exit()
			logger.Flush()
			os.Exit(0)
		}
	default:
	}
}

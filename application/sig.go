//go:build !windows
// +build !windows

package application

import (
	"ffsf/logger"
	"os"
	"os/signal"
	"syscall"
)

func SignalNotify() {
	signal.Notify(sig, syscall.SIGABRT, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
}

func (a *Application) checkSysSignal() {
	select {
	case s := <-sig:
		switch s {
		case syscall.SIGUSR1:
			logger.Infof("onreload")
			a.reload()
		default:
			logger.Infof("onexit")
			a.exit()
			logger.Flush()
			os.Exit(0)
		}
	default:
	}
}
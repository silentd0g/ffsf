package application

import (
	"github.com/silentd0g/ffsf/datetime"
	"github.com/silentd0g/ffsf/logger"
	"os"
	"time"
)

type AppInterface interface {
	OnInit() error
	OnReload() error
	OnProc() bool // return: isIdle
	OnTick(nowMs int64)
	OnExit()
}

type Application struct {
	appHandler AppInterface

	idleLoopCnt int

	tickInterval int64
	lastTickTime int64
}

var sig = make(chan os.Signal, 1)
var app Application

func Init(handler AppInterface, tickInterval int64) int {
	app.appHandler = handler
	err := app.appHandler.OnInit()
	if err != nil {
		logger.Errorf("Initialized fail. {err:%v}", err)
		logger.Flush()
		os.Exit(-1)
		return -1
	}

	app.tickInterval = tickInterval

	SignalNotify()
	return 0
}

func (a *Application) exit() {
	a.appHandler.OnExit()
}

func (a *Application) reload() error {
	return a.appHandler.OnReload()
}

func (a *Application) loopOnce() bool {
	return a.appHandler.OnProc()
}

func (a *Application) tick(nowMs int64) {
	a.appHandler.OnTick(nowMs)
}

func Run() {
	for {
		app.checkSysSignal()

		nowMs := datetime.NowMs()

		if nowMs >= app.lastTickTime+app.tickInterval {
			app.tick(nowMs)
			app.lastTickTime = nowMs
		}

		isIdle := app.loopOnce()
		if isIdle {
			app.idleLoopCnt += 1
		} else {
			app.idleLoopCnt = 0
		}

		if app.idleLoopCnt > 1000 {
			app.idleLoopCnt = 0
			time.Sleep(5 * time.Millisecond)
		}
	}
}

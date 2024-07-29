package datetime

import (
	"time"
)

const (
	SECONDS_PER_MINUTE = 60
	MINUTES_PER_HOUR   = 60
	HOURS_PER_DAY      = 24
	SECONDS_PER_HOUR   = SECONDS_PER_MINUTE * MINUTES_PER_HOUR
	SECONDS_PER_DAY    = SECONDS_PER_HOUR * HOURS_PER_DAY
	MINUTES_PER_DAY    = MINUTES_PER_HOUR * HOURS_PER_DAY

	MS_PER_MINUTE = SECONDS_PER_MINUTE * 1000
	MS_PER_HOUR   = SECONDS_PER_HOUR * 1000
	MS_PER_DAY    = SECONDS_PER_DAY * 1000
)

// YYYYMMDD
func GetData() string {
	return time.Now().Format("20060102")
}

func Now() int32 {
	return int32(time.Now().Unix())
}

func NowMs() int64 {
	return time.Now().UnixNano() / 1000000
}

func NowNano() int64 {
	return time.Now().UnixNano()
}

func TimeFromUnix(t int32) time.Time {
	return time.Unix(int64(t), 0)
}

func IsSameMinute(t1, t2 int32) bool {
	return t1/SECONDS_PER_MINUTE == t2/SECONDS_PER_MINUTE
}

func IsSameHour(t1, t2 int32) bool {
	return t1/SECONDS_PER_HOUR == t2/SECONDS_PER_HOUR
}

func IsSameDay(t1, t2 int32) bool {
	return t1/SECONDS_PER_DAY == t2/SECONDS_PER_DAY
}

func IsSameWeek(t1, t2 int32) bool {
	tt1 := time.Unix(int64(t1), int64(t1*1000))
	tt2 := time.Unix(int64(t2), int64(t2*1000))
	y1, w1 := tt1.ISOWeek()
	y2, w2 := tt2.ISOWeek()
	return y1 == y2 && w1 == w2
}

func IsSameMonth(t1, t2 int32) bool {
	tt1 := time.Unix(int64(t1), int64(t1*1000))
	tt2 := time.Unix(int64(t2), int64(t2*1000))
	return tt1.Year() == tt2.Year() && tt1.Month() == tt2.Month()
}

func GetDayOfMonth(t1 int32) int {
	tt1 := time.Unix(int64(t1), int64(t1*1000))
	_, _, day := tt1.Date()
	return day
}

func BeginTimeOfToday() int32 {
	now := Now()
	left := now % SECONDS_PER_DAY
	return now - left
}

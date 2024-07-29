package misc

import (
	"log"
	"net/http"
	"runtime"
	"strconv"
)

func ByteSliceClone(src []byte) []byte {
	return append(src[:0:0], src...)
}

func Uint32SliceDeduplicateSorted(s []uint32) []uint32 {
	if s == nil || len(s) <= 1 {
		return s
	}

	out := []uint32{s[0]}
	for i := 1; i < len(s); i++ {
		if s[i] != out[len(out)-1] {
			out = append(out, s[i])
		}
	}

	return out
}

// 需要在main中加：  import _ "net/http/pprof"
func StartProfile(port int) {
	runtime.SetBlockProfileRate(1)
	//远程获取pprof数据
	go func() {
		log.Println(http.ListenAndServe("localhost:"+strconv.Itoa(port), nil))
	}()
}

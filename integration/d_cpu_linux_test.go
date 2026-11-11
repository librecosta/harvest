//go:build integration && linux

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

var clockTicks = sync.OnceValues(func() (float64, error) {
	data, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
})

func processCPU(pid int) (float64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	end := strings.LastIndexByte(string(data), ')')
	fields := strings.Fields(string(data[end+1:]))
	if end < 0 || len(fields) < 13 {
		return 0, fmt.Errorf("cannot read process CPU counters")
	}
	user, userErr := strconv.ParseUint(fields[11], 10, 64)
	kernel, kernelErr := strconv.ParseUint(fields[12], 10, 64)
	ticks, tickErr := clockTicks()
	if userErr != nil || kernelErr != nil || tickErr != nil || ticks <= 0 {
		return 0, fmt.Errorf("invalid process CPU counters")
	}
	return float64(user+kernel) / ticks, nil
}

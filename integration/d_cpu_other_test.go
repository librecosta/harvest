//go:build integration && !linux && !windows

package integration

func processCPU(int) (float64, error) { return 0, nil }

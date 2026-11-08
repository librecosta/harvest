package config

import "errors"

type Retention struct {
	EventDays    int   `mapstructure:"event_days"`
	HistoryDays  int   `mapstructure:"history_days"`
	SampleDays   int   `mapstructure:"sample_days"`
	EventRows    int64 `mapstructure:"event_rows"`
	EventBytes   int64 `mapstructure:"event_bytes"`
	HistoryRows  int64 `mapstructure:"history_rows"`
	HistoryBytes int64 `mapstructure:"history_bytes"`
}

func DefaultRetention() Retention {
	return Retention{14, 30, 30, 100_000, 256 << 20, 200_000, 512 << 20}
}

func (r Retention) validate() error {
	for _, days := range []int{r.EventDays, r.HistoryDays, r.SampleDays} {
		if days < 1 || days > 365 {
			return errors.New("retention horizons must be 1–365 days")
		}
	}
	if r.EventRows < 128 || r.HistoryRows < 128 || r.EventRows > 10_000_000 || r.HistoryRows > 10_000_000 ||
		r.EventBytes < 4<<20 || r.HistoryBytes < 4<<20 || r.EventBytes > 64<<30 || r.HistoryBytes > 64<<30 {
		return errors.New("retention row or byte budget out of range")
	}
	return nil
}

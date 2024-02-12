package syslog

type (
	// SyslogUDPIngester struct
	SyslogUDPIngester struct {
		Port int
		Host string
	}
)

func NewSyslogUDPIngester() *SyslogUDPIngester {
	return &SyslogUDPIngester{}
}

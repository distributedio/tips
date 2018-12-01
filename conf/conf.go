package conf

type Tips struct {
	Server      Server     `cfg:"server"`
	Status      Status     `cfg:"status"`
	TikvLog     TikvLogger `cfg:"tikv-logger"`
	Logger      Logger     `cfg:"logger"`
	PIDFileName string     `cfg:"pid-filename; titan.pid; ; the file name to record connd PID"`
}

type Server struct {
	Tikv   Tikv   `cfg:"tikv"`
	Listen string `cfg:"listen; 0.0.0.0:7369; netaddr; address to listen"`
}

type Tikv struct {
	PdAddrs string `cfg:"pd-addrs;required; ;pd address in tidb"`
}

type Logger struct {
	Name       string `cfg:"name; titan; ; the default logger name"`
	Path       string `cfg:"path; logs/titan; ; the default log path"`
	Level      string `cfg:"level; info; ; log level(debug, info, warn, error, panic, fatal)"`
	Compress   bool   `cfg:"compress; false; boolean; true for enabling log compress"`
	TimeRotate string `cfg:"time-rotate; 0 0 0 * * *; ; log time rotate pattern(s m h D M W)"`
}

type TikvLogger struct {
	Path       string `cfg:"path; logs/tikv;nonempty ; the default log path"`
	Level      string `cfg:"level; info; ; log level(debug, info, warn, error, panic, fatal)"`
	Compress   bool   `cfg:"compress; false; boolean; true for enabling log compress"`
	TimeRotate string `cfg:"time-rotate; 0 0 0 * * *; ; log time rotate pattern(s m h D M W)"`
}

//TODO
type Status struct {
	Listen string `cfg:"listen;0.0.0.0:7345;nonempty; listen address of http server"`
}

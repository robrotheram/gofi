package settings

//Log LEVELS
const (
	LOG_INFO  = "info"
	LOG_DEBUG = "debug"
	LOG_WARN  = "warn"
	LOG_ERROR = "ERROR"
)

//Log Outputs
const (
	LOG_STD_OUT  = "std"
	LOG_FILE_OUT = "file"
)

//Config
const (
	CONFIG_ETCD       = "ETCD"
	CONFIG_WORKERS    = "WORKERS"
	CONFIG_LOG_LEVEL  = "LOG_LEVEL"
	CONFIG_LOG_OUTPUT = "LOG_OUTPUT"
	CONFIG_DATAPATH   = "DATA_PATH"
)

//etcd
const (
	ETCD_ROOT     = "e3w_test"
	ETCD_SERVICE  = "service"
	ETCD_FEED     = "feed"
	ETCD_URL      = "url"
	ETCD_JOB      = "job"
	ETCD_JOB_LIST = "list_job"
)

//Job Types
const (
	JOB_ARTICLE = "ARTICLE"
	JOB_TWITTER = "TWITTER"
)

//minio
const (
	//MINO_BUCKET_NAME = "mymusic"
	MINO_BUCKET_LOCATION = "us-east-1"
)

package settings

import (
	"fmt"
	"github.com/go-yaml/yaml"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type SettingStore struct {
	Ectd            []string `yaml:"etcd"`
	NumberOfworkers int      `yaml:"numberWorker"`
	LogLevel        string   `yaml:"LogLevel"`
	LogOutut        string   `yaml:"LogOutput"`
	Hostname        string

	//should store in etcd
	Endpoint        string `yaml:"s3endpoint"`
	AccessKeyID     string `yaml:"s3access"`
	SecretAccessKey string `yaml:"s3secret"`
	UseSSL          bool   `yaml:"s3SSL"`
	BucketName      string `yaml:"s3BucketName"`

	Elastic string `yaml:"elastic"`
	Debug   bool   `yaml:"debug"`
}

func (s *SettingStore) SetETCD(etcd []string) {
	if etcd != nil {
		s.Ectd = etcd
	}
}

func (s *SettingStore) SetHostname(hostname string) {
	s.Hostname = hostname
}

func (s *SettingStore) SetETCDString(etcd string) {
	if etcd != "" {
		e := strings.Split(etcd, ",")
		s.Ectd = e
	}
}

func (s *SettingStore) SetNumberOfworkers(workers string) {
	if workers != "" {
		if _, err := strconv.Atoi(workers); err == nil {
			s.NumberOfworkers, _ = strconv.Atoi(workers)
		}
	}
}

func (s *SettingStore) SetLogLevel(level string) {
	if level != "" {
		s.LogLevel = level
	}
}

func (s *SettingStore) SetLogOutut(output string) {
	if output != "" {
		s.LogOutut = output
	}
}

func (s SettingStore) Print(Log *logrus.Logger) {
	e := reflect.ValueOf(&s).Elem()
	for i := 0; i < e.NumField(); i++ {
		fieldName := e.Type().Field(i).Name
		fieldValue := e.Field(i)
		Log.WithFields(logrus.Fields{
			fieldName: fieldValue,
		}).Info("Config:")
	}
}

func (s *SettingStore) getFromFile() {

	confContent, err := ioutil.ReadFile("conf.yml")
	if err != nil {
		panic(err)
	}
	// expand environment variables
	confContent = []byte(os.ExpandEnv(string(confContent)))
	if err := yaml.Unmarshal(confContent, s); err != nil {
		panic(err)
	}

	//fmt.Printf("config: %v\n", string(b))
}

func (s *SettingStore) getFromEnviroment() {
	s.SetETCDString(os.Getenv(CONFIG_ETCD))
	s.SetNumberOfworkers(os.Getenv(CONFIG_WORKERS))
	s.SetLogLevel(os.Getenv(CONFIG_LOG_LEVEL))
	s.SetLogOutut(os.Getenv(CONFIG_LOG_OUTPUT))
}

func GetLogger(store *SettingStore) *logrus.Logger {
	Log := logrus.New()
	switch output := store.LogOutut; output {
	case LOG_STD_OUT:
		log.SetOutput(os.Stdout)
	case LOG_FILE_OUT:
		f, err := os.OpenFile("logrus.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to initialize log file %s", err)
			os.Exit(1)
		}
		Log.Out = f
	default:
		log.SetOutput(os.Stdout)
	}
	switch level := store.LogLevel; level {
	case LOG_INFO:
		Log.Level = logrus.InfoLevel
	case LOG_DEBUG:
		Log.Level = logrus.DebugLevel
	case LOG_WARN:
		Log.Level = logrus.WarnLevel
	case LOG_ERROR:
		Log.Level = logrus.ErrorLevel
	default:
		Log.Level = logrus.InfoLevel
	}
	return Log
}

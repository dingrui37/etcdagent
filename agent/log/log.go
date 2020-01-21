package log

import (
    "fmt"
    "io"
    "log"
    "os"
    "time"
)

var Logger *log.Logger

func init() {
    var logFileName string
    var logFile *os.File
    var err error
    now := time.Now()
    logFileName = fmt.Sprintf("etcdagent-%v.%v.%v-%02v:%02v:%02v.log",
        now.Year(),
        int(now.Month()),
        now.Day(),
        now.Hour(),
        now.Minute(),
        now.Second())

    if logFile, err = os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666); err != nil {
        log.Fatalln("Fail to create log file:", logFileName)
    }

    //defer logFile.Close()
    Logger = log.New(io.MultiWriter(logFile), "", log.LstdFlags)
}

func Info(format string, args ...interface{}) {
    Logger.SetPrefix("[Info]")
    Logger.Printf(format, args...)
}

func Warn(format string, args ...interface{}) {
    Logger.SetPrefix("[Warn]")
    Logger.Printf(format, args...)
}

/**
* @file logger_test.go
* @brief
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version
* @date 2016-08-09
 */

package mini_spider

import (
	"testing"
)

func TestLogger(t *testing.T) {
	acts := &DefaultActions{}
	logger1 := getLogFunc(acts, LogInfo, 1, "logger1")
	logger1(LogInfo, "logger1")
	logger2 := getLogFunc(acts, LogInfo, 1, "")
	logger2(LogInfo, "logger2")
	logger3 := getLogFunc(acts, LogInfo, 2, "saver")
	logger3(LogInfo, "logger3")
}

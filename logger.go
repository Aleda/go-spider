/**
* @file logger.go
* @brief
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version 1.0.0
* @date 2016-08-09
 */

package mini_spider

import (
	"fmt"
	"log"
)

func getDefaultLogFunc(verbosity LogFlags) func(LogFlags, string, ...interface{}) {
	return func(minLevel LogFlags, format string, vals ...interface{}) {
		log.Println(verbosity, minLevel, fmt.Sprintf(format, vals...))
	}
}

func getLogFunc(act Actions, verbosity LogFlags, workerIndex int, funcName string) func(LogFlags, string, ...interface{}) {
	return func(minLevel LogFlags, format string, vals ...interface{}) {
		act.Log(verbosity, minLevel,
			fmt.Sprintf(fmt.Sprintf("%s %d - %s", funcName, workerIndex, format), vals...))
	}
}

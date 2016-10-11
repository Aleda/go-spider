/**
* @file worker.go
* @brief every host dose correspond to a worker.
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version
* @date 2016-08-09
 */

package mini_spider

import (
	"testing"
)

func assertEqual(t *testing.T, t1 interface{}, t2 interface{}) {
	if t1 != t2 {
		t.Error("Expected t1 and t2 to be the same.")
		t.Errorf("t1: %d t2: %d", t1, t2)
	}
}

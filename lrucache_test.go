/**
* @file lrucache.go
* @brief
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version
* @date 2016-08-09
 */

package mini_spider

import (
	"testing"
	"time"
)

func TestLRUCache(t *testing.T) {
	lru := NewLRUCache(2*time.Second, 10)
	lru.Set("0", "lru0")
	lru.Set("1", "lru1")
	lru.Set("1", "lru2")
	if _, ok := lru.Get("-1"); ok {
		t.Errorf("Lru hasn't -1")
	}
	if v, ok := lru.Get("0"); ok {
		assertEqual(t, v, "lru0")
	} else {
		t.Errorf("Lru has 0")
	}
	if v, ok := lru.Get("1"); ok {
		assertEqual(t, v, "lru2")
	}
	assertEqual(t, lru.Len(), 2)
	time.Sleep(3 * time.Second)
	assertEqual(t, lru.Delete("-1"), false)
	assertEqual(t, lru.Delete("0"), true)
	if _, ok := lru.Get("1"); ok {
		t.Errorf("Lru timeout not work fun.")
	}
	return
}

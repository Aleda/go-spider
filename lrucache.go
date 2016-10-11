/**
* @file lrucache.go
* @brief
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version 1.0.0
* @date 2016-08-16
 */

package mini_spider

import (
	"container/list"
	"sync"
	"time"
)

type Entry struct {
	key     string
	value   interface{}
	addedAt time.Time
}

// LRUCache
type LRUCache struct {
	list  *list.List               // double linked list
	table map[string]*list.Element // hash table

	mtx        sync.Mutex    // Mutex
	expiration time.Duration // ttl
	maxLen     int           // cache max length
}

// LRUCache's constructor
func NewLRUCache(e time.Duration, m int) *LRUCache {
	return &LRUCache{
		list:  list.New(),
		table: make(map[string]*list.Element, m),

		expiration: e,
		maxLen:     m,
	}
}

func (c *LRUCache) Get(key string) (interface{}, bool) {
	return c.get(key)
}

func (c *LRUCache) get(key string) (interface{}, bool) {
	defer c.mtx.Unlock()

	c.mtx.Lock()

	ele := c.table[key]
	if ele == nil {
		return nil, false
	}
	entry := ele.Value.(*Entry)
	if time.Since(entry.addedAt) > c.expiration { // timeout
		c.deleteElement(ele)
		return nil, false
	}
	c.list.MoveToFront(ele)
	return entry.value, true
}

func (c *LRUCache) Set(key string, value interface{}) {
	defer c.mtx.Unlock()

	c.mtx.Lock()

	if ele := c.table[key]; ele != nil { // in table
		entry := ele.Value.(*Entry)
		entry.value = value
		c.promote(ele, entry)
	} else { // not in table
		c.addNew(key, value)
	}
}

func (c *LRUCache) Delete(key string) bool {
	defer c.mtx.Unlock()
	c.mtx.Lock()

	if ele := c.table[key]; ele != nil {
		c.deleteElement(ele)
		return true
	} else {
		return false
	}
}

func (c *LRUCache) Len() int {
	return c.list.Len()
}

func (c *LRUCache) Flush() {
	defer c.mtx.Unlock()

	c.mtx.Lock()
	c.list = list.New()
	c.table = make(map[string]*list.Element, c.maxLen)
}

func (c *LRUCache) addNew(key string, value interface{}) {
	newEntry := &Entry{
		key:     key,
		value:   value,
		addedAt: time.Now(),
	}
	ele := c.list.PushFront(newEntry)
	c.table[key] = ele
	c.check()
}

func (c *LRUCache) deleteElement(ele *list.Element) {
	c.list.Remove(ele) // O(n) ? TODO
	delete(c.table, ele.Value.(*Entry).key)
}

func (c *LRUCache) check() {
	for c.list.Len() > c.maxLen { // exceeds the capacity of the cache
		ele := c.list.Back()
		c.deleteElement(ele)
	}
}

func (c *LRUCache) promote(ele *list.Element, entry *Entry) {
	entry.addedAt = time.Now()
	c.list.MoveToFront(ele)
}

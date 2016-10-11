/**
* @file cache.go
* @brief
* @author Aleda(aledalee@foxmail.com | aleda.cn)
* @version 1.0.0
* @date 2016-08-16
 */

package mini_spider

import (
	"gopkg.in/redis.v4"
	"time"
)

type CacheType uint

// Cache type
const (
	LinkCacheType CacheType = 1 << iota // Cache link
	PageCacheType                       // Cache Page
)

// the interface for kv storage system.
type KVStorage interface {
	Set(key string, value interface{}, expiration time.Duration) error
	Get(key string) ([]byte, error)
	Del(keys ...string) error
	Scan() ([]string, error)
}

// Cache system
// 1. Cache the links and page.
// 2. sync the both of data, cache and db.
type Cache struct {
	DB        KVStorage
	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error
	LogFunc   func(LogFlags, string, ...interface{})
	Type      CacheType

	LocalCache *LRUCache // local cache
}

func NewCache(db KVStorage, acts Actions, t CacheType) *Cache {
	return &Cache{
		DB:         db,
		Marshal:    acts.Marshal,
		Unmarshal:  acts.Unmarshal,
		LogFunc:    getDefaultLogFunc(LogInfo),
		LocalCache: nil,
		Type:       t,
	}
}

// redis db
type RedisDB struct {
	client *redis.Client
}

func NewRedisDB(opts *redis.Options) *RedisDB {
	return &RedisDB{
		client: redis.NewClient(opts),
	}
}

func (r *RedisDB) Set(key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(key, value, expiration).Err()
}

func (r *RedisDB) Get(key string) ([]byte, error) {
	return r.client.Get(key).Bytes()
}

func (r *RedisDB) Del(keys ...string) error {
	return r.client.Del(keys...).Err()
}

func (r *RedisDB) Scan() (keys []string, err error) {
	keys, err = r.client.Keys("*").Result()
	return keys, err
}

func (c *Cache) Set(key string, value interface{}, e time.Duration) error {
	if e != 0 && e < time.Second {
		panic("Expiration can't be less than 1 second")
	}
	b, err := c.Marshal(value)
	if err != nil {
		c.LogFunc(LogInfo, "Cache: Marshal failed: %s", err)
		return err
	}
	if c.LocalCache != nil {
		c.LocalCache.Set(key, b)
	}

	err = c.DB.Set(key, b, e)
	if err != nil {
		c.LogFunc(LogInfo, "Cache: set key=%s failed: %s", key, err)
	}
	return err
}

func (c *Cache) Get(key string) (interface{}, error) {
	b, err := c.getBytes(key)
	if err != nil {
		return nil, err
	}
	if c.Type == LinkCacheType {
		var i Link
		if err = c.Unmarshal(b, &i); err == nil {
			return i, nil
		}
	} else if c.Type == PageCacheType {
		var i Page
		if err = c.Unmarshal(b, &i); err == nil {
			return i, nil
		}
	}
	return nil, err
}

func (c *Cache) getBytes(key string) ([]byte, error) {
	if c.LocalCache != nil {
		if v, ok := c.LocalCache.Get(key); ok {
			if b, ok := v.([]byte); ok {
				return b, nil
			}
		}
	}
	b, err := c.DB.Get(key)
	if err != nil {
		return nil, err
	}
	if c.LocalCache != nil { // store
		c.LocalCache.Set(key, b)
	}
	return b, err
}

func (c *Cache) Delete(key string) error {
	if c.LocalCache != nil {
		c.LocalCache.Delete(key)
	}
	err := c.DB.Del(key)
	if err != nil {
		c.LogFunc(LogInfo, "Cache: Del key=%s failed: %s", key, err)
		return err
	}
	return nil
}

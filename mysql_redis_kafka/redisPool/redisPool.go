package redisPool

import (
	"flag"
	"github.com/garyburd/redigo/redis"
	"time"
)


var (
	Pool *redis.Pool
	RedisServer   = flag.String("redisServer", ":6379", "")

)
func init() {
	Pool = &redis.Pool{
		MaxIdle:     3, //最大空闲链接数，表示即使没有redis链接事依然可以保持N个空闲链接，而不被清除
		MaxActive:   3, //最大激活连接数，表示同时最多有多少个链接
		IdleTimeout: 240 * time.Second,//最大空闲链接等待时间，超过此时间，空闲将被关闭
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", *RedisServer)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}


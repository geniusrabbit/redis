//
// @project GeniusRabbit 2015
// @author Dmitry Ponomarev <demdxx@gmail.com> 2015
//

package redis

import (
	"errors"
	"net/url"
	"time"

	"github.com/demdxx/gocast"
	"github.com/garyburd/redigo/redis"
)

var (
	pools map[string]*PoolWrapper
)

// Errors set
var (
	ErrInvalidConnectionURL = errors.New("Invalid connection URL")
)

// Register redis connection
func Register(name, server, password string, dbNumber, maxIdle, maxActive int, wait bool, idleTimeout time.Duration) *PoolWrapper {
	// Init pool map
	if nil == pools {
		pools = make(map[string]*PoolWrapper)
	}

	// Create redis pool
	r := &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: idleTimeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if dbNumber > 0 {
				if _, err := c.Do("SELECT", dbNumber); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Wait: wait,
	}

	pw := getPoolWrapper(r)
	pools[name] = pw

	return pw
}

// RegisterURL connection
func RegisterURL(name, sURL string) (*PoolWrapper, error) {
	u, err := url.Parse(sURL)
	if nil != err {
		return nil, err
	}

	if len(u.Path) < 2 {
		return nil, ErrInvalidConnectionURL
	}

	var password string
	if nil != u.User {
		password, _ = u.User.Password()
	}
	return Register(name, u.Host, password,
		gocast.ToInt(u.Path[1:]),
		gocast.ToInt(u.Query().Get("idle")),
		gocast.ToInt(u.Query().Get("maxcon")),
		gocast.ToBool(u.Query().Get("wait")),
		time.Duration(gocast.ToFloat64(u.Query().Get("timeout")))*time.Second), nil
}

// Pool by name
func Pool(name string) *PoolWrapper {
	return pools[name]
}

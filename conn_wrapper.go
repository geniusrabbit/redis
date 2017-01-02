//
// @project GeniusRabbit 2015
// @author Dmitry Ponomarev <demdxx@gmail.com> 2015
//

package redis

import (
	"github.com/garyburd/redigo/redis"
)

type ConnWrapper struct {
	Conn redis.Conn
}

func (c *ConnWrapper) Get(key string) (interface{}, error) {
	c.Conn.Send("GET", key)
	c.Conn.Flush()
	return c.Conn.Receive()
}

func (c *ConnWrapper) GetInt64(key string) (int64, error) {
	return redis.Int64(c.Get(key))
}

func (c *ConnWrapper) GetUint64(key string) (uint64, error) {
	return redis.Uint64(c.Get(key))
}

func (c *ConnWrapper) GetFloat64(key string) (float64, error) {
	return redis.Float64(c.Get(key))
}

func (c *ConnWrapper) GetBool(key string) (bool, error) {
	return redis.Bool(c.Get(key))
}

func (c *ConnWrapper) GetString(key string) (string, error) {
	return redis.String(c.Get(key))
}

func (c *ConnWrapper) MGet(params ...interface{}) error {
	keys := make([]interface{}, len(params)/2)
	vals := make([]interface{}, len(params)/2)

	for i := 0; i < len(params); i += 2 {
		keys[i/2] = params[i]
		vals[i/2] = params[i+1]
	}

	reply, err := redis.Values(c.Conn.Do("MGET", keys...))
	if nil == err {
		_, err = redis.Scan(reply, vals...)
	}
	return err
}

func (c *ConnWrapper) MGetSlice(keys ...interface{}) ([]interface{}, error) {
	reply, err := c.Conn.Do("MGET", keys...)
	if r, ok := reply.([]interface{}); ok {
		return r, err
	}
	return nil, err
}

func (c *ConnWrapper) MGetCleanSlice(keys ...interface{}) ([]interface{}, error) {
	list, err := c.MGetSlice(keys...)
	if nil == err && nil != list {
		result := make([]interface{}, 0, len(list))
		for _, it := range list {
			if nil != it {
				result = append(result, it)
			}
		}
		list = result
	}
	return list, err
}

func (c *ConnWrapper) Set(key string, v interface{}) error {
	_, err := c.Conn.Do("SET", key, v)
	return err
}

func (c *ConnWrapper) SetEx(key string, expire uint64, v interface{}) error {
	_, err := c.Conn.Do("SETEX", key, expire, v)
	return err
}

func (c *ConnWrapper) MSet(params ...interface{}) (interface{}, error) {
	return c.Conn.Do("MSET", params...)
}

func (c *ConnWrapper) Incr(key string) (int64, error) {
	return redis.Int64(c.Conn.Do("INCR", key))
}

func (c *ConnWrapper) IncrBy(key string, v int64) (int64, error) {
	return redis.Int64(c.Conn.Do("INCRBY", key, v))
}

func (c *ConnWrapper) IncrByFloat64(key string, v float64) (float64, error) {
	return redis.Float64(c.Conn.Do("INCRBYFLOAT", key, v))
}

func (c *ConnWrapper) HincrByFloat64(hash, key string, v float64) (float64, error) {
	return redis.Float64(c.Conn.Do("HINCRBYFLOAT", hash, key, v))
}

func (c *ConnWrapper) Exists(params ...interface{}) (bool, error) {
	return redis.Bool(c.Conn.Do("EXISTS", params...))
}

func (c *ConnWrapper) Del(params ...interface{}) (int64, error) {
	return redis.Int64(c.Conn.Do("DEL", params...))
}

func (c *ConnWrapper) Do(cmd string, params ...interface{}) (interface{}, error) {
	return c.Conn.Do(cmd, params...)
}

func (c *ConnWrapper) Send(cmd string, params ...interface{}) error {
	return c.Conn.Send(cmd, params...)
}

func (c *ConnWrapper) Flush() error {
	return c.Conn.Flush()
}

func (c *ConnWrapper) Receive() (interface{}, error) {
	return c.Conn.Receive()
}

func (c *ConnWrapper) Close() error {
	return c.Conn.Close()
}

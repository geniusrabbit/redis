//
// @project GeniusRabbit 2015
// @author Dmitry Ponomarev <demdxx@gmail.com> 2015
//

package redis

import "github.com/garyburd/redigo/redis"

// PoolWrapper object
type PoolWrapper struct {
	Raw *redis.Pool
}

func getPoolWrapper(p *redis.Pool) *PoolWrapper {
	return &PoolWrapper{Raw: p}
}

// Get wrapped connection
func (p *PoolWrapper) Get() ConnWrapper {
	return ConnWrapper{Conn: p.Raw.Get()}
}

// ActiveCount of connections
func (p *PoolWrapper) ActiveCount() int {
	return p.Raw.ActiveCount()
}

// Close pool of connections
func (p *PoolWrapper) Close() error {
	return p.Raw.Close()
}

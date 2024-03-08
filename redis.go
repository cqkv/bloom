package bloom

import (
	"context"

	"github.com/gomodule/redigo/redis"
)

const (
	LuaBloomBatchGetBits = `
  local bloomKey = KEYS[1]
  local bitsCnt = ARGV[1]
  for i=1,bitsCnt,1 do
    local offset = ARGV[1+i]
    local reply = redis.call('getbit',bloomKey,offset)
    if (not reply) then
        error('FAIL')
        return 0
    end
    if (reply == 0) then
        return 0
    end
  end
  return 1
`
	LuaBloomBatchSetBits = `
  local bloomKey = KEYS[1]
  local bitsCnt = ARGV[1]


  for i=1,bitsCnt,1 do
    local offset = ARGV[1+i]
    redis.call('setbit',bloomKey,offset,1)
  end
  return 1
`
)

type RedisClient struct {
	pool *redis.Pool
}

func NewRedisClient(pool *redis.Pool) *RedisClient {
	return &RedisClient{
		pool: pool,
	}
}

// Eval 执行 lua 脚本，保证复合操作的原子性
func (r *RedisClient) Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error) {
	args := make([]interface{}, 2+len(keysAndArgs))
	args[0] = src
	args[1] = keyCount
	copy(args[2:], keysAndArgs)

	// 获取连接
	conn, err := r.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}

	// 放回连接池
	defer conn.Close()

	// 执行 lua 脚本
	return conn.Do("EVAL", args...)
}

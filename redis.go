package bloom

import (
	"context"

	"github.com/gomodule/redigo/redis"
	goredis "github.com/redis/go-redis/v9"
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

type RedisClient interface {
	Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error)
}

type GoRedisClusterClient struct {
	client *goredis.ClusterClient
}

func NewGoRedisClusterClient(client *goredis.ClusterClient) *GoRedisClusterClient {
	return &GoRedisClusterClient{
		client: client,
	}
}

func (r *GoRedisClusterClient) Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error) {
	args := make([]interface{}, 3+len(keysAndArgs))
	args[0] = "EVAL"
	args[1] = src
	args[2] = keyCount
	copy(args[3:], keysAndArgs)

	cmd := r.client.Do(ctx, args...)
	return cmd.Val(), cmd.Err()
}

type RedigoClient struct {
	pool *redis.Pool
}

func NewRedigoClient(pool *redis.Pool) *RedigoClient {
	return &RedigoClient{
		pool: pool,
	}
}

// Eval 执行 lua 脚本，保证复合操作的原子性
func (r *RedigoClient) Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error) {
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

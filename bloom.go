package bloom

import (
	"context"
	"fmt"
	"strconv"

	"github.com/demdxx/gocast"
)

type Filter struct {
	m, k   int32
	hasher Hasher
	client RedisClient
}

func NewFilter(m, k int32, client RedisClient, hasher Hasher) *Filter {
	return &Filter{
		m:      m,
		k:      k,
		hasher: hasher,
		client: client,
	}
}

func (f *Filter) Set(ctx context.Context, key, val string) error {
	// 映射对应的 bit 位
	args := make([]interface{}, 0, f.k+2)
	args = append(args, key, f.k)
	bits := f.hash(val)
	args = append(args, bits)

	rawResp, err := f.client.Eval(ctx, LuaBloomBatchSetBits, 1, args)
	if err != nil {
		return err
	}

	resp := gocast.ToInt(rawResp)
	if resp != 1 {
		return fmt.Errorf("resp: %d", resp)
	}
	return nil
}

func (f *Filter) Exist(ctx context.Context, key, val string) (bool, error) {
	args := make([]interface{}, 0, f.k+2)
	args = append(args, key, f.k)
	bits := f.hash(val)
	args = append(args, bits)

	rawResp, err := f.client.Eval(ctx, LuaBloomBatchGetBits, 1, args)
	if err != nil {
		return false, err
	}

	resp := gocast.ToInt(rawResp)
	if resp == 1 {
		return true, nil
	}

	return false, nil
}

func (f *Filter) hash(origin string) []int32 {
	bits := make([]int32, f.k)
	var i int32
	for ; i < f.k; i++ {
		bit := f.hasher.Hash(origin)
		bits[i] = bit
		origin = strconv.FormatInt(int64(bit), 10)
	}

	return bits
}

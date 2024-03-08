package bloom

import (
	"math"

	"github.com/cqkv/bloom/utils"

	"github.com/spaolacci/murmur3"
)

type Hasher interface {
	Hash(origin string) int32
}

type DefaultHashFunc struct {
}

func (d *DefaultHashFunc) Hash(origin string) int32 {
	hasher := murmur3.New32()
	_, _ = hasher.Write(utils.Str2Byte(origin))
	return int32(hasher.Sum32() % math.MaxInt32)
}

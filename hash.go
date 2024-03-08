package bloom

import (
	"math"

	"github.com/cqkv/bloom/utils"

	"github.com/spaolacci/murmur3"
)

type Hasher interface {
	Hash(origin string) int32
}

type DefaultHasher struct {
}

func NewDefaultHashFunc() *DefaultHasher {
	return &DefaultHasher{}
}

func (d *DefaultHasher) Hash(origin string) int32 {
	hasher := murmur3.New32()
	_, _ = hasher.Write(utils.Str2Byte(origin))
	return int32(hasher.Sum32() % math.MaxInt32)
}

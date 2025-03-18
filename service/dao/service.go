package dao

import (
	"context"
)

type Service[K comparable, T any] interface {
	Save(ctx context.Context, t *T) error

	Load(ctx context.Context, id K) (*T, error)

	Delete(ctx context.Context, id K) error

	List(ctx context.Context, parameters ...*Parameter) ([]*T, error)
}

package correlation

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemoryDAO(t *testing.T) {
	dao := NewMemoryDAO()
	ctx := context.Background()

	g := &Group{ID: "g1", Expected: 2}
	assert.NoError(t, dao.Save(ctx, g))

	loaded, _ := dao.Load(ctx, "g1")
	assert.Equal(t, g, loaded)

	list, _ := dao.List(ctx)
	assert.Len(t, list, 1)

	assert.NoError(t, dao.Delete(ctx, "g1"))
	loaded, _ = dao.Load(ctx, "g1")
	assert.Nil(t, loaded)
}

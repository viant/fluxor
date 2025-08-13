package correlation

import "context"

// DAO abstracts persistence operations for correlation groups so that the
// allocator can recover its in-memory state after a restart.
type DAO interface {
	Save(ctx context.Context, g *Group) error
	Load(ctx context.Context, id string) (*Group, error)
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]*Group, error)
}

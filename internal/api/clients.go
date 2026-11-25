package api

import (
	"context"
	"fmt"
	"strings"

	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// DefaultAPIKeys is the local demo caller. It is a public placeholder,
// not a secret: set DISPATCHD_API_KEYS to replace it anywhere real.
const DefaultAPIKeys = "demo:dk_demo_local_only:admin"

// SeedClients registers "id:key:role" entries separated by commas.
func SeedClients(ctx context.Context, st *store.Store, spec string) error {
	for _, e := range strings.Split(spec, ",") {
		p := strings.Split(strings.TrimSpace(e), ":")
		if len(p) != 3 {
			return fmt.Errorf("bad DISPATCHD_API_KEYS entry %q (want id:key:role)", e)
		}
		if err := st.UpsertClient(ctx, p[0], p[1], p[2]); err != nil {
			return err
		}
	}
	return nil
}

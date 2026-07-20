package modules

import (
	"fmt"

	"github.com/kuetix/engine/boot"
)

// Register explicitly verifies that this package's meta cache entries were
// populated at init() time. See acme-audit/modules/register.go for the
// full rationale.
//
// Returns an error listing the missing service paths, or nil if all
// expected entries are present.
func Register() error {
	required := []string{"collections/ops"}
	missing := []string{}
	for _, key := range required {
		if _, ok := boot.MetaFunctionCache[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("acme-std-collections/modules: boot.MetaFunctionCache missing keys %v — "+
			"meta.go init() did not run; WSL actions for these services will fail at runtime", missing)
	}
	return nil
}

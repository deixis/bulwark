package bulwark

import (
	"fmt"
	"log/slog"

	"github.com/deixis/faults"
)

// AssertValidPriority panics when a priority is out of range.
// A priority is out of range when it is less than 0 or greater than or equal
// to priorities.
func AssertValidPriority(p Priority, priorities int) (Priority, error) {
	if p < 0 || int(p) >= priorities {
		panic(fmt.Sprintf("bulwark: priority must be in the range [0, %d), but got %d", priorities, p))
	}

	return p, nil
}

// ClampInvalidPriority clamps any out-of-range priority to the lowest valid
// priority (priorities-1). This applies to both negative values and values
// that exceed the configured number of priorities, preventing invalid or
// malicious input from being promoted to a higher-importance tier.
func ClampInvalidPriority(p Priority, priorities int) (Priority, error) {
	if p >= 0 && int(p) < priorities {
		return p, nil
	}
	slog.Warn("bulwark: priority is out of range", "max", priorities-1, "priority", p)

	return Priority(priorities - 1), nil
}

// RejectInvalidPriority returns an error when a priority is out of range.
// A priority is out of range when it is less than 0 or greater than or equal
// to priorities.
func RejectInvalidPriority(p Priority, priorities int) (Priority, error) {
	if p < 0 || int(p) >= priorities {
		return p, faults.Bad(&faults.FieldViolation{
			Field:       "priority",
			Description: fmt.Sprintf("priority must be in the range [0, %d), but got %d", priorities, p),
		})
	}

	return p, nil
}

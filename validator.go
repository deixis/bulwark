package bulwark

import (
	"fmt"
	"log/slog"

	"github.com/deixis/faults"
)

var (
	// OnInvalidPriorityPanic panics when a priority is out of range.
	// A priority is out of range when it is less than 0 or greater than or equal
	// to priorities-1.
	OnInvalidPriorityPanic = func(p Priority, priorities int) (Priority, error) {
		if p < 0 || int(p) >= priorities-1 {
			panic(fmt.Sprintf("bulwark: priority must be in the range [0, %d), but got %d", priorities, p))
		}

		return p, nil
	}

	// OnInvalidPriorityAdjust adjusts the priority to the nearest valid value.
	// A negative priority will be set to the lowest priority.
	// A priority is out of range when it is less than 0 or greater than or equal
	// to priorities-1.
	OnInvalidPriorityAdjust = func(p Priority, priorities int) (Priority, error) {
		if p >= 0 && int(p) < priorities {
			return p, nil
		}
		slog.Warn("bulwark: priority is out of range", "max", priorities-1, "priority", p)

		// Receiving an invalid value is likely due to an input that was not properly
		// validated, so this prevents abuse of the system.
		return Priority(priorities - 1), nil
	}

	// OnInvalidPriorityError returns an error when a priority is out of range.
	// A priority is out of range when it is less than 0 or greater than or equal
	// to priorities-1.
	OnInvalidPriorityError = func(p Priority, priorities int) (Priority, error) {
		if p < 0 || int(p) >= priorities-1 {
			return p, faults.Bad(&faults.FieldViolation{
				Field:       "priority",
				Description: fmt.Sprintf("priority must be in the range [0, %d), but got %d", priorities, p),
			})
		}

		return p, nil
	}
)

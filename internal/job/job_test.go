package job

import "testing"

func TestIsValidTransition(t *testing.T) {
	tests := []struct {
		from, to Status
		want     bool
	}{
		{StatusPending, StatusProcessing, true},
		{StatusPending, StatusCancelled, true},
		{StatusPending, StatusCompleted, false},
		{StatusProcessing, StatusCancelled, false},
		{StatusProcessing, StatusCompleted, true},
		{StatusProcessing, StatusFailed, true},
		{StatusProcessing, StatusRetried, true},
		{StatusCompleted, StatusPending, false},
		{StatusCompleted, StatusProcessing, false},
		{StatusFailed, StatusPending, false},
		{StatusCancelled, StatusPending, false},
		{StatusRetried, StatusPending, true},
		{StatusRetried, StatusProcessing, true},
	}

	for _, tt := range tests {
		got := IsValidTransition(tt.from, tt.to)
		if got != tt.want {
			t.Errorf("IsValidTransition(%s, %s) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

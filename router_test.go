package router

import "testing"

func TestVersion(t *testing.T) {
	if Version != "0.1.0" {
		t.Fatalf("expected version 0.1.0, got %s", Version)
	}
}

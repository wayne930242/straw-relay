package server_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wayne930242/straw-relay/internal/linktest"
)

// A store that fails under an open stream ends it and says why in the log, with the team and device; it is not mistaken for a removal.
func TestStreamEndsAndLogsWhenTheStoreFails(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	h.srv.SetAuthHook(func(device string) {
		if device == x.bob.ID {
			if err := h.st.Close(); err != nil {
				t.Errorf("closing the store: %v", err)
			}
		}
	})
	s := h.stream(x.bob, x.Team, "")
	if s.status != 200 {
		t.Fatalf("status %d", s.status)
	}
	if !s.closed() {
		t.Fatal("the stream must end when the store fails")
	}
	logs := h.logs.String()
	for _, want := range []string{"events query failed", "team=" + x.Hex(), "device=" + x.bob.ID, "reason=store-error"} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs lack %q:\n%s", want, logs)
		}
	}
}

// A response writer that cannot extend the connection's deadlines cannot hold a stream: the request fails before any event is sent.
func TestStreamRefusedWhenDeadlinesCannotBeSet(t *testing.T) {
	h := newHarness(t)
	x := h.team3()
	req := httptest.NewRequest("GET", "/v1/teams/"+x.Hex()+"/events", nil)
	linktest.SignRequest(req, x.bob, x.ID, h.now.Load(), nil)
	rec := httptest.NewRecorder()
	h.srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "stream-unsupported") {
		t.Errorf("got %d %q, want 500 stream-unsupported", rec.Code, rec.Body.String())
	}
	if !strings.Contains(h.logs.String(), "reason=stream-unsupported") {
		t.Errorf("logs: %s", h.logs.String())
	}
}

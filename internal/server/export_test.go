package server

import "encoding/hex"

// SetAuthHook makes f run right after a request authenticates, before its handler goes on. Tests use it to land a roster change in that gap.
func (s *Server) SetAuthHook(f func(device string)) { s.hookAuth = f }

// SetLoadHook makes f run each time a roster is loaded from the database.
func (s *Server) SetLoadHook(f func()) { s.hookLoad = f }

// SetLiveDrainHook makes f run in a stream each time it is about to write the live envelopes queued for it, so a test can hold it there.
func (s *Server) SetLiveDrainHook(f func(device string)) { s.hookLive = f }

// LiveStreams is how many open streams the live registry holds a queue for.
func (s *Server) LiveStreams() int {
	s.live.mu.Lock()
	defer s.live.mu.Unlock()
	n := 0
	for _, devices := range s.live.streams {
		for _, qs := range devices {
			n += len(qs)
		}
	}
	return n
}

// LiveQueuedBytes is the bytes of live envelopes now queued for all streams.
func (s *Server) LiveQueuedBytes() int {
	s.live.mu.Lock()
	defer s.live.mu.Unlock()
	n := 0
	for _, devices := range s.live.streams {
		for _, qs := range devices {
			for q := range qs {
				n += q.size
			}
		}
	}
	return n
}

// LiveInject queues a body for a device's open streams as a post would, but without the membership check a post goes through: a test uses it to put an
// envelope in front of a stream whose device has already left.
func (s *Server) LiveInject(team []byte, device string, body []byte) {
	s.live.deliver(hex.EncodeToString(team), device, liveItem{id: make([]byte, 16), from: device, body: body})
}

package server

// SetAuthHook makes f run right after a request authenticates, before its handler goes on. Tests use it to land a roster change in that gap.
func (s *Server) SetAuthHook(f func(device string)) { s.hookAuth = f }

// SetLoadHook makes f run each time a roster is loaded from the database.
func (s *Server) SetLoadHook(f func()) { s.hookLoad = f }

// SetLiveDrainHook makes f run in a stream each time it is about to write the live envelopes queued for it, so a test can hold it there.
func (s *Server) SetLiveDrainHook(f func(device string)) { s.hookLive = f }

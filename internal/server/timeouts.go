package server

import (
	"net/http"
	"time"
)

// Timeouts keep idle or slow connections from holding the server. Zero values take the defaults.
type Timeouts struct {
	ReadHeader time.Duration // 10 s: a client that does not finish its headers
	Read       time.Duration // 30 s: the whole request, body included
	Write      time.Duration // 30 s: a response; streams extend their own deadline on every write
	Idle       time.Duration // 120 s: a kept-alive connection with no request
}

// NewHTTPServer wraps the handler in an http.Server with those timeouts and a 16 KB header limit.
func NewHTTPServer(addr string, h http.Handler, t Timeouts) *http.Server {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&t.ReadHeader, 10*time.Second)
	def(&t.Read, 30*time.Second)
	def(&t.Write, 30*time.Second)
	def(&t.Idle, 120*time.Second)
	return &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: t.ReadHeader, ReadTimeout: t.Read, WriteTimeout: t.Write, IdleTimeout: t.Idle, MaxHeaderBytes: 16 << 10}
}

// Command straw-relay is the relay for Straw Link: it carries end-to-end encrypted envelopes between a team's computers and never decrypts them.
//
//	straw-relay serve [--addr :8080] [--db straw-relay.db]
//	straw-relay token create [--db straw-relay.db]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wayne930242/straw-relay/internal/server"
	"github.com/wayne930242/straw-relay/internal/store"
)

const usage = `usage:
  straw-relay serve [--addr :8080] [--db straw-relay.db]
  straw-relay token create [--db straw-relay.db]

Environment: STRAW_RELAY_ADDR and STRAW_RELAY_DB set the defaults of --addr and --db.`

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "straw-relay:", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return errors.New("a command is required")
	}
	switch {
	case args[0] == "serve":
		return serve(args[1:], stderr)
	case args[0] == "token" && len(args) > 1 && args[1] == "create":
		return tokenCreate(args[2:], stdout, stderr)
	}
	fmt.Fprintln(stderr, usage)
	return fmt.Errorf("unknown command %q", args[0])
}

func tokenCreate(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("token create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	db := fs.String("db", env("STRAW_RELAY_DB", "straw-relay.db"), "database file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*db)
	if err != nil {
		return err
	}
	defer st.Close()
	token, err := st.CreateToken(time.Now().UnixMilli())
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, token)
	return nil
}

func serve(args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", env("STRAW_RELAY_ADDR", ":8080"), "listen address (TLS is the ingress's job)")
	db := fs.String("db", env("STRAW_RELAY_DB", "straw-relay.db"), "database file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	st, err := store.Open(*db)
	if err != nil {
		return err
	}
	defer st.Close()

	srv := server.New(st, server.Options{Logger: log})
	httpSrv := server.NewHTTPServer(*addr, srv.Handler(), server.Timeouts{})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go srv.Run(ctx)

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.ListenAndServe() }()
	log.Info("listening", "addr", *addr, "db", *db)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	srv.Close() // ends the event streams, which would otherwise hold Shutdown
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdown); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Command straw-relay is the relay for Straw Link: it carries end-to-end encrypted envelopes between a team's computers and never decrypts them.
//
//	straw-relay serve [--addr :8080] [--db straw-relay.db]
//	straw-relay token create [--db straw-relay.db]
//	straw-relay backup [--db straw-relay.db] <out.db>
//	straw-relay raise-sequence [--db straw-relay.db] <min-event-id>
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
	"strconv"
	"syscall"
	"time"

	"github.com/wayne930242/straw-relay/internal/server"
	"github.com/wayne930242/straw-relay/internal/store"
)

const usage = `usage:
  straw-relay serve [--addr :8080] [--db straw-relay.db]
  straw-relay token create [--db straw-relay.db]
  straw-relay backup [--db straw-relay.db] <out.db>        consistent snapshot (VACUUM INTO); safe while serving
  straw-relay raise-sequence [--db straw-relay.db] <min>   after a restore: next event id becomes at least <min>

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
	case args[0] == "backup":
		return backup(args[1:], stdout, stderr)
	case args[0] == "raise-sequence":
		return raiseSequence(args[1:], stdout, stderr)
	}
	fmt.Fprintln(stderr, usage)
	return fmt.Errorf("unknown command %q", args[0])
}

func tokenCreate(args []string, stdout, stderr io.Writer) (err error) {
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
	defer func() {
		if cerr := st.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("closing the database: %w", cerr))
		}
	}()
	token, err := st.CreateToken(time.Now().UnixMilli())
	if err != nil {
		return err
	}
	// A token that could not be printed is not one the operator has.
	if _, err := fmt.Fprintln(stdout, token); err != nil {
		return fmt.Errorf("printing the token: %w", err)
	}
	return nil
}

// openWith parses the flags shared by the maintenance commands, opens the database and returns the positional arguments.
func openWith(name string, args []string, stderr io.Writer, nargs int) (*store.Store, []string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	db := fs.String("db", env("STRAW_RELAY_DB", "straw-relay.db"), "database file")
	if err := fs.Parse(args); err != nil {
		return nil, nil, err
	}
	if fs.NArg() != nargs {
		return nil, nil, fmt.Errorf("%s takes %d argument(s), got %d", name, nargs, fs.NArg())
	}
	st, err := store.Open(*db)
	return st, fs.Args(), err
}

func backup(args []string, stdout, stderr io.Writer) (err error) {
	st, rest, err := openWith("backup", args, stderr, 1)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("closing the database: %w", cerr))
		}
	}()
	if err := st.Backup(rest[0]); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "backup written:", rest[0])
	return err
}

func raiseSequence(args []string, stdout, stderr io.Writer) (err error) {
	st, rest, err := openWith("raise-sequence", args, stderr, 1)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("closing the database: %w", cerr))
		}
	}()
	min, perr := strconv.ParseInt(rest[0], 10, 64)
	if perr != nil || min < 0 {
		return fmt.Errorf("invalid event id %q", rest[0])
	}
	if err := st.RaiseEventSequence(min); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "next event id is at least", min+1)
	return err
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
	defer func() {
		if err := st.Close(); err != nil {
			log.Error("closing the database failed", "err", err)
		}
	}()

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

// Command straw-relay is the relay for Straw Link: it carries end-to-end encrypted envelopes between a team's computers and never decrypts them.
//
//	straw-relay serve [--addr :8080] [--db straw-relay.db]
//	straw-relay token create [--db straw-relay.db]
//	straw-relay backup [--db straw-relay.db] <out.db | out-dir>
//	straw-relay raise-sequence [--db straw-relay.db] (--by <n> | <min-event-id>)
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
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/wayne930242/straw-relay/internal/server"
	"github.com/wayne930242/straw-relay/internal/store"
)

const usage = `usage:
  straw-relay serve [--addr :8080] [--db straw-relay.db]
  straw-relay token create [--db straw-relay.db]
  straw-relay backup [--db straw-relay.db] <out.db|dir>  consistent snapshot (VACUUM INTO); safe while serving; a directory gets straw-relay-<UTC time>.db
  straw-relay raise-sequence [--db straw-relay.db] --by <n>   after a restore: add <n> to the event counter (a second restore is raised again)
  straw-relay raise-sequence [--db straw-relay.db] <min>      after a restore: the event counter becomes at least <min>
backup and raise-sequence need the database file to exist; they never create one.

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

// maintenanceFlags registers the flags shared by the maintenance commands.
func maintenanceFlags(name string, stderr io.Writer) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs, fs.String("db", env("STRAW_RELAY_DB", "straw-relay.db"), "database file")
}

// closeInto joins a failed Close into err.
func closeInto(err *error, st *store.Store) {
	if cerr := st.Close(); cerr != nil {
		*err = errors.Join(*err, fmt.Errorf("closing the database: %w", cerr))
	}
}

func backup(args []string, stdout, stderr io.Writer) (err error) {
	fs, db := maintenanceFlags("backup", stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("backup takes 1 argument(s), got %d", fs.NArg())
	}
	st, err := store.OpenExisting(*db)
	if err != nil {
		return err
	}
	defer closeInto(&err, st)
	out := fs.Arg(0)
	if fi, serr := os.Stat(out); serr == nil && fi.IsDir() {
		out = filepath.Join(out, "straw-relay-"+time.Now().UTC().Format("20060102-150405")+".db")
	}
	if err := st.Backup(out); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "backup written:", out)
	return err
}

func raiseSequence(args []string, stdout, stderr io.Writer) (err error) {
	fs, db := maintenanceFlags("raise-sequence", stderr)
	by := fs.Int64("by", 0, "add this many to the event counter instead of taking a target")
	if err := fs.Parse(args); err != nil {
		return err
	}
	bySet := false
	fs.Visit(func(f *flag.Flag) { bySet = bySet || f.Name == "by" })
	if bySet && fs.NArg() != 0 || !bySet && fs.NArg() != 1 {
		return errors.New("raise-sequence takes either --by <n> or one <min> argument")
	}
	var min int64
	if !bySet {
		var perr error
		if min, perr = strconv.ParseInt(fs.Arg(0), 10, 64); perr != nil || min < 0 {
			return fmt.Errorf("invalid event id %q", fs.Arg(0))
		}
	}
	st, err := store.OpenExisting(*db)
	if err != nil {
		return err
	}
	defer closeInto(&err, st)
	var seq int64
	if bySet {
		seq, err = st.RaiseEventSequenceBy(*by)
	} else {
		seq, err = st.RaiseEventSequence(min)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "event counter is now %d; the next event id is %d\n", seq, seq+1)
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

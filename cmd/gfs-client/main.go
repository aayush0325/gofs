// Command gfs-client is a small HTTP CLI for the GFS client API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aayush0325/gofs/internal/api"
	"github.com/aayush0325/gofs/internal/config"
)

// main is the CLI entry point; it delegates to run so errors are cleanly printed.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gfs-client:", err)
		os.Exit(1)
	}
}

// run parses global flags and dispatches to a subcommand.
func run() error {
	server := flag.String("server", config.String("GFS_SERVER", "http://localhost:8080"), "master HTTP API URL")
	token := flag.String("token", config.String("GFS_HTTP_TOKEN", ""), "optional bearer token")
	timeout := flag.Duration("timeout", config.Duration("GFS_CLIENT_TIMEOUT", 5*time.Minute), "request timeout")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: gfs-client [flags] <command> [args]\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Commands:\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  mkdir PATH [--recursive]      create a directory\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  create PATH [--mode OCTAL]   create an empty file\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  stat PATH                    show metadata\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  list PATH [--recursive]      list entries\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  delete PATH [--recursive]    delete a path\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  rename SRC DST               rename a path\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  snapshot SRC DST             create a snapshot\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  read PATH [--offset N] [--length N] [--output FILE]\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  write PATH [FILE|-] [--offset N]\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  append PATH [FILE|-]         append one record (max 4 MiB)\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  status                       show cluster status\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		return errors.New("a command is required")
	}

	client := api.New(*server, *token, *timeout)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	command, rest := args[0], args[1:]
	switch command {
	case "mkdir":
		return cmdMkdir(ctx, client, rest)
	case "create":
		return cmdCreate(ctx, client, rest)
	case "stat":
		return cmdStat(ctx, client, rest)
	case "list":
		return cmdList(ctx, client, rest)
	case "delete":
		return cmdDelete(ctx, client, rest)
	case "rename":
		return cmdRename(ctx, client, rest)
	case "snapshot":
		return cmdSnapshot(ctx, client, rest)
	case "read":
		return cmdRead(ctx, client, rest)
	case "write":
		return cmdWrite(ctx, client, rest)
	case "append":
		return cmdAppend(ctx, client, rest)
	case "status":
		return cmdStatus(ctx, client)
	default:
		flag.Usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

// cmdMkdir implements the mkdir subcommand.
func cmdMkdir(ctx context.Context, client *api.Client, args []string) error {
	fs := flag.NewFlagSet("mkdir", flag.ContinueOnError)
	recursive := fs.Bool("recursive", false, "create missing ancestors")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: gfs-client mkdir PATH [--recursive]")
	}
	info, err := client.Mkdir(ctx, fs.Arg(0), *recursive)
	if err != nil {
		return err
	}
	return printJSON(info)
}

// cmdCreate implements the create subcommand.
func cmdCreate(ctx context.Context, client *api.Client, args []string) error {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	modeText := fs.String("mode", "0644", "file mode in octal")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: gfs-client create PATH [--mode OCTAL]")
	}
	mode, err := strconv.ParseUint(*modeText, 8, 32)
	if err != nil {
		return fmt.Errorf("invalid mode %q: %w", *modeText, err)
	}
	info, err := client.Create(ctx, fs.Arg(0), uint32(mode))
	if err != nil {
		return err
	}
	return printJSON(info)
}

// cmdStat implements the stat subcommand.
func cmdStat(ctx context.Context, client *api.Client, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: gfs-client stat PATH")
	}
	info, err := client.Stat(ctx, args[0])
	if err != nil {
		return err
	}
	return printJSON(info)
}

// cmdList implements the list subcommand.
func cmdList(ctx context.Context, client *api.Client, args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	recursive := fs.Bool("recursive", false, "walk descendants")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: gfs-client list PATH [--recursive]")
	}
	result, err := client.List(ctx, fs.Arg(0), *recursive)
	if err != nil {
		return err
	}
	for _, entry := range result.Entries {
		fmt.Printf("%-9s %10d  %s\n", entry.Type, entry.Size, entry.Path)
	}
	return nil
}

// cmdDelete implements the delete subcommand.
func cmdDelete(ctx context.Context, client *api.Client, args []string) error {
	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	recursive := fs.Bool("recursive", false, "delete a non-empty directory tree")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: gfs-client delete PATH [--recursive]")
	}
	return client.Delete(ctx, fs.Arg(0), *recursive)
}

// cmdRename implements the rename subcommand.
func cmdRename(ctx context.Context, client *api.Client, args []string) error {
	if len(args) != 2 {
		return errors.New("usage: gfs-client rename SRC DST")
	}
	return client.Rename(ctx, args[0], args[1])
}

// cmdSnapshot implements the snapshot subcommand.
func cmdSnapshot(ctx context.Context, client *api.Client, args []string) error {
	if len(args) != 2 {
		return errors.New("usage: gfs-client snapshot SRC DST")
	}
	info, err := client.Snapshot(ctx, args[0], args[1])
	if err != nil {
		return err
	}
	return printJSON(info)
}

// cmdRead implements the read subcommand and supports stdout or a file target.
func cmdRead(ctx context.Context, client *api.Client, args []string) error {
	fs := flag.NewFlagSet("read", flag.ContinueOnError)
	offset := fs.Int64("offset", 0, "byte offset")
	length := fs.Int64("length", -1, "byte length; -1 means to EOF")
	output := fs.String("output", "", "output file (default stdout)")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: gfs-client read PATH [--offset N] [--length N] [--output FILE]")
	}
	reader, err := client.Read(ctx, fs.Arg(0), *offset, *length)
	if err != nil {
		return err
	}
	defer reader.Close()
	target := os.Stdout
	if *output != "" {
		file, err := os.Create(*output)
		if err != nil {
			return err
		}
		defer file.Close()
		target = file
	}
	_, err = io.Copy(target, reader)
	return err
}

// cmdWrite implements the write subcommand with streaming upload.
func cmdWrite(ctx context.Context, client *api.Client, args []string) error {
	fs := flag.NewFlagSet("write", flag.ContinueOnError)
	offset := fs.Int64("offset", 0, "byte offset")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() < 1 || fs.NArg() > 2 {
		return errors.New("usage: gfs-client write PATH [FILE|-] [--offset N]")
	}
	source := "-"
	if fs.NArg() == 2 {
		source = fs.Arg(1)
	}
	reader, closeFn, err := inputReader(source)
	if err != nil {
		return err
	}
	defer closeFn()
	result, err := client.Write(ctx, fs.Arg(0), *offset, reader)
	if err != nil {
		return err
	}
	return printJSON(result)
}

// cmdAppend implements the append subcommand and enforces the 4 MiB record cap
// before sending the request.
func cmdAppend(ctx context.Context, client *api.Client, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return errors.New("usage: gfs-client append PATH [FILE|-]")
	}
	source := "-"
	if len(args) == 2 {
		source = args[1]
	}
	reader, closeFn, err := inputReader(source)
	if err != nil {
		return err
	}
	defer closeFn()
	data, err := io.ReadAll(io.LimitReader(reader, (4<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 4<<20 {
		return errors.New("append records are limited to 4 MiB")
	}
	result, err := client.Append(ctx, args[0], data)
	if err != nil {
		return err
	}
	return printJSON(result)
}

// cmdStatus implements the status subcommand.
func cmdStatus(ctx context.Context, client *api.Client) error {
	status, err := client.Status(ctx)
	if err != nil {
		return err
	}
	return printJSON(status)
}

// parseFlags accepts flags before or after positional arguments, which makes
// the CLI behave like familiar Unix tools. The standard flag package only
// supports flags before the first positional argument.
func parseFlags(fs *flag.FlagSet, args []string) error {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-" || !strings.HasPrefix(arg, "-") {
			positional = append(positional, arg)
			continue
		}
		flags = append(flags, arg)
		if strings.Contains(arg, "=") {
			continue
		}
		name := strings.TrimLeft(arg, "-")
		defined := fs.Lookup(name)
		if defined == nil {
			continue
		}
		if boolFlag, ok := defined.Value.(interface{ IsBoolFlag() bool }); ok && boolFlag.IsBoolFlag() {
			continue
		}
		if i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	return fs.Parse(append(flags, positional...))
}

// inputReader opens a source file, or returns stdin for "-".
func inputReader(source string) (io.Reader, func(), error) {
	if source == "-" {
		return os.Stdin, func() {}, nil
	}
	file, err := os.Open(source)
	if err != nil {
		return nil, func() {}, err
	}
	return file, func() { _ = file.Close() }, nil
}

// printJSON pretty-prints a response value.
func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

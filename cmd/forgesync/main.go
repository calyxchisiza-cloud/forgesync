package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"forgesync"
)

func main() {
	id := flag.String("id", "", "node id")
	dir := flag.String("dir", "data", "node directory")
	listen := flag.String("listen", "", "address to accept sync sessions")
	dial := flag.String("dial", "", "address to sync with once")
	flag.Parse()
	if *id == "" {
		fmt.Fprintln(os.Stderr, "id is required")
		os.Exit(1)
	}
	if (*listen == "") == (*dial == "") {
		fmt.Fprintln(os.Stderr, "set listen or dial")
		os.Exit(1)
	}
	node, err := forgesync.Open(*dir, *id)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer node.Close()
	if *listen != "" {
		srv, err := node.Listen(*listen)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer srv.Close()
		fmt.Printf("ForgeSync listening on %s\n", srv.Addr())
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		<-ctx.Done()
		return
	}
	if err := forgesync.Dial(*dial, node); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	items, err := node.List()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, item := range items {
		fmt.Printf("%s %s\n", item.Key, item.Value)
	}
}

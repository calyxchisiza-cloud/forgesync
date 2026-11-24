# ForgeSync

ForgeSync copies keys between replicas that may be disconnected. Each replica appends its own changes and the changes it learns. A sync sends only the changes the other replica has not applied. The same change can arrive twice. The second time it is ignored.

Two replicas may change one key before they meet. The change with the greater time wins. Equal times break toward the greater node id, then the greater sequence. The losing change stays in the history. A delete is a change, so a later delete removes the key on both sides. This is last-writer-wins. It is not a merge, and it is not a consensus protocol.

A node id is permanent. Two replicas that sync must use different ids. `New` keeps the log in memory. `Open` stores `id` and `changes.log` in a directory and syncs each change before it returns. A restart replays the log. A torn final line is dropped.

```go
a, err := forgesync.New("a")
b, err := forgesync.New("b")
_, err = a.Put("greeting", []byte("hello"))
_, err = b.Put("greeting", []byte("hi"))
err = forgesync.Sync(a, b)
value, ok, err := a.Get("greeting")
```

`Listen` accepts a sync session. `Dial` exchanges changes with that session until both sides are quiet. A session sends at most 128 changes in one frame, then continues.

## Run

```
go run ./cmd/demo
```

The demo prints:

```
a greeting hi
a note left
b greeting hi
b note left
a greeting hello again
a note left
b greeting hello again
b note left
```

`hi` wins the tie because the node ids differ. `hello again` is a later write, so it replaces `hi` on both nodes. `note` was written only on `a` while the nodes were apart.

Two processes:

```
go run ./cmd/forgesync -id a -dir a -listen 127.0.0.1:9070
go run ./cmd/forgesync -id b -dir b -dial 127.0.0.1:9070
```

The dialer prints its keys after the exchange. The listener stays up until interrupt.

## Test

```
go test ./...
go test -race -count=1 ./...
```

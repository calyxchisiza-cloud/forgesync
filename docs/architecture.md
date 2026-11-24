# Architecture

A ForgeSync node is one replica. Its identity does not change. Every local put or delete takes the next sequence for that identity and a Lamport time: one more than any time the node has seen. The change is synced to the log, then it becomes visible.

The log is the source of truth. The current value of a key is the change that wins the comparison. A greater time wins. The same time yields to the greater origin, then the greater sequence. `Get` hides a key whose winning change is a delete. `History` returns every change for that key, oldest first, including the ones that lost.

`Cursor` records the highest contiguous sequence applied from each origin. `Missing` lists local changes with a higher sequence. `Apply` accepts the next sequence for an origin and ignores one it already has. A sequence that skips ahead returns a gap error and leaves that change unstored, so a hole is not treated as delivered.

`Sync` holds both nodes, in id order, and applies each side's missing changes once. Nothing else can write during that pass, so one exchange finishes the job. `Listen` and `Dial` do the same exchange over TCP without holding both logs across the network. Each frame carries at most 128 changes. The peers repeat until a round moves nothing.

`Open` replays `changes.log`. The last complete line for a sequence wins the replay because a duplicate sequence is ignored. A partial line at the end is cut off. The `id` file must match the node that opens the directory.

v1 trusts the peer it dials. It does not elect a leader and it does not refuse a concurrent write. Two offline writes both survive in the log. Readers see one winner.

// Package forgesync synchronises keys between nodes that can be offline.
// Each node keeps an append-only change log. A sync copies the changes the
// other node has not seen. The same key can change on both sides. The later
// change wins, ties break toward the greater node id, and the earlier change
// stays in the history. Applying one change twice does nothing.
package forgesync

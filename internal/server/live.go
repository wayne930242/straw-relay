package server

import "sync"

// The live class (link.KindRule.Live): an envelope the relay keeps in memory only, long enough to write it to the addressee's streams that are open now.
// It is never stored, queued, replayed, or acknowledged, and has no event id. This file holds what that takes: a bounded queue for each stream, the
// registry that finds a device's streams, and a limiter of its own.

// liveItem is one envelope on its way to a stream: the id and sender of its header, and the envelope as posted.
type liveItem struct {
	id   []byte
	from string
	body []byte
}

// liveOut is a queued envelope as a stream takes it; dropped counts what the queue threw away before it (only the first of a batch has any).
type liveOut struct {
	liveItem
	dropped int
}

// liveQueue is what one open stream has yet to write. It holds at most maxItems envelopes or maxBytes of them; a full queue drops its oldest, and
// counts them for the next envelope the stream writes, which tells the receiver to resync.
type liveQueue struct {
	maxItems, maxBytes int
	wake               chan struct{}

	mu      sync.Mutex
	items   []liveItem
	size    int
	dropped int
}

func newLiveQueue(maxItems, maxBytes int) *liveQueue {
	return &liveQueue{maxItems: maxItems, maxBytes: maxBytes, wake: make(chan struct{}, 1)}
}

func (q *liveQueue) push(it liveItem) {
	q.mu.Lock()
	for len(q.items) > 0 && (len(q.items) >= q.maxItems || q.size+len(it.body) > q.maxBytes) {
		q.size -= len(q.items[0].body)
		q.items[0] = liveItem{}
		q.items = q.items[1:]
		q.dropped++
	}
	q.items = append(q.items, it)
	q.size += len(it.body)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// take empties the queue in arrival order.
func (q *liveQueue) take() []liveOut {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]liveOut, len(q.items))
	for i, it := range q.items {
		out[i] = liveOut{liveItem: it}
	}
	if len(out) > 0 {
		out[0].dropped = q.dropped
		q.dropped = 0
	}
	q.items, q.size = nil, 0
	return out
}

func (q *liveQueue) empty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) == 0
}

// clear forgets what is queued and what was dropped: for a stream whose device left the team.
func (q *liveQueue) clear() {
	q.mu.Lock()
	q.items, q.size, q.dropped = nil, 0, 0
	q.mu.Unlock()
}

// liveHub finds the open streams of a device.
type liveHub struct {
	mu      sync.Mutex
	streams map[string]map[string]map[*liveQueue]struct{} // team (hex) → device → queues
}

func (h *liveHub) add(team, device string, q *liveQueue) func() {
	h.mu.Lock()
	if h.streams[team] == nil {
		h.streams[team] = map[string]map[*liveQueue]struct{}{}
	}
	if h.streams[team][device] == nil {
		h.streams[team][device] = map[*liveQueue]struct{}{}
	}
	h.streams[team][device][q] = struct{}{}
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.streams[team][device], q)
		if len(h.streams[team][device]) == 0 {
			delete(h.streams[team], device)
		}
		if len(h.streams[team]) == 0 {
			delete(h.streams, team)
		}
	}
}

// deliver queues an envelope on each open stream of the device and returns how many streams that was.
func (h *liveHub) deliver(team, device string, it liveItem) int {
	h.mu.Lock()
	qs := make([]*liveQueue, 0, len(h.streams[team][device]))
	for q := range h.streams[team][device] {
		qs = append(qs, q)
	}
	h.mu.Unlock()
	for _, q := range qs {
		q.push(it)
	}
	return len(qs)
}

// drop empties the queues of a device's streams: it is no longer a member.
func (h *liveHub) drop(team, device string) {
	h.mu.Lock()
	qs := make([]*liveQueue, 0, len(h.streams[team][device]))
	for q := range h.streams[team][device] {
		qs = append(qs, q)
	}
	h.mu.Unlock()
	for _, q := range qs {
		q.clear()
	}
}

// liveLimiter keeps a sliding minute of live posts, for each (team, sender) by envelopes and bytes and for each team by envelopes. A refused post is
// not counted.
type liveLimiter struct {
	mu      sync.Mutex
	senders map[string][]liveHit
	teams   map[string][]int64
}

type liveHit struct {
	at    int64
	bytes int64
}

func (l *liveLimiter) allow(team, sender string, size, now int64, perSender int, bytesPerSender int64, perTeam int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := team + "/" + sender
	hits := l.senders[key]
	i := 0
	for i < len(hits) && hits[i].at <= now-60_000 {
		i++
	}
	hits = hits[i:]
	var used int64
	for _, x := range hits {
		used += x.bytes
	}
	t := l.teams[team]
	j := 0
	for j < len(t) && t[j] <= now-60_000 {
		j++
	}
	t = t[j:]
	l.senders[key], l.teams[team] = hits, t
	if len(hits) >= perSender || used+size > bytesPerSender || len(t) >= perTeam {
		return false
	}
	l.senders[key] = append(hits, liveHit{now, size})
	l.teams[team] = append(t, now)
	return true
}

func (l *liveLimiter) prune(now int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, hits := range l.senders {
		if len(hits) == 0 || hits[len(hits)-1].at <= now-60_000 {
			delete(l.senders, k)
		}
	}
	for k, t := range l.teams {
		if len(t) == 0 || t[len(t)-1] <= now-60_000 {
			delete(l.teams, k)
		}
	}
}

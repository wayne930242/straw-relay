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
// counts them for the next envelope the stream writes, which tells the receiver to resync. The hub that registered it guards its fields with its own
// lock, so that the byte budgets over all queues stay exact.
type liveQueue struct {
	hub                *liveHub
	team               string
	maxItems, maxBytes int
	wake               chan struct{}

	items   []liveItem
	size    int
	dropped int
}

// pop drops the oldest envelope. The hub's lock is held.
func (q *liveQueue) pop() {
	n := len(q.items[0].body)
	q.size -= n
	q.hub.relayBytes -= n
	q.hub.addTeamBytes(q.team, -n)
	q.items[0] = liveItem{}
	q.items = q.items[1:]
	q.dropped++
}

// push queues an envelope, first dropping the oldest of this queue while it is over its own limits, then the oldest of the largest queue of the team
// while the team is over its budget, then of the largest queue of the relay while the relay is over its. An envelope bigger than a budget by itself
// is dropped instead of queued. The hub's lock is held.
func (q *liveQueue) push(it liveItem) {
	h := q.hub
	n := len(it.body)
	for len(q.items) > 0 && (len(q.items) >= q.maxItems || q.size+n > q.maxBytes) {
		q.pop()
	}
	for h.teamBytes[q.team]+n > h.teamBudget && h.evictLargest(q.team) {
	}
	for h.relayBytes+n > h.relayBudget && h.evictLargest("") {
	}
	if h.teamBytes[q.team]+n > h.teamBudget || h.relayBytes+n > h.relayBudget {
		q.dropped++
		return
	}
	q.items = append(q.items, it)
	q.size += n
	h.relayBytes += n
	h.teamBytes[q.team] += n
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// take empties the queue in arrival order.
func (q *liveQueue) take() []liveOut {
	q.hub.mu.Lock()
	defer q.hub.mu.Unlock()
	out := make([]liveOut, len(q.items))
	for i, it := range q.items {
		out[i] = liveOut{liveItem: it}
	}
	if len(out) > 0 {
		out[0].dropped = q.dropped
		q.dropped = 0
	}
	q.release()
	return out
}

func (q *liveQueue) empty() bool {
	q.hub.mu.Lock()
	defer q.hub.mu.Unlock()
	return len(q.items) == 0
}

// clear forgets what is queued and what was dropped: for a stream whose device left the team.
func (q *liveQueue) clear() {
	q.hub.mu.Lock()
	defer q.hub.mu.Unlock()
	q.release()
	q.dropped = 0
}

// release gives the queued bytes back to the budgets and empties the queue. The hub's lock is held.
func (q *liveQueue) release() {
	q.hub.relayBytes -= q.size
	q.hub.addTeamBytes(q.team, -q.size)
	q.items, q.size = nil, 0
}

// liveHub finds the open streams of a device and holds the byte budgets over every queue of the relay and of each team. One lock guards the registry,
// the budgets, and the queues' fields.
type liveHub struct {
	mu                      sync.Mutex
	streams                 map[string]map[string]map[*liveQueue]struct{} // team (hex) → device → queues
	relayBudget, teamBudget int
	relayBytes              int
	teamBytes               map[string]int
	queueItems, queueBytes  int
}

// evictLargest drops the oldest envelope of the queue holding the most bytes, among the queues of the team (every queue for ""), and reports whether
// there was one to drop. The lock is held.
func (h *liveHub) evictLargest(team string) bool {
	var big *liveQueue
	for t, devices := range h.streams {
		if team != "" && t != team {
			continue
		}
		for _, qs := range devices {
			for q := range qs {
				if len(q.items) > 0 && (big == nil || q.size > big.size) {
					big = q
				}
			}
		}
	}
	if big == nil {
		return false
	}
	big.pop()
	return true
}

func (h *liveHub) addTeamBytes(team string, d int) {
	if h.teamBytes[team] += d; h.teamBytes[team] == 0 {
		delete(h.teamBytes, team)
	}
}

// newQueue registers a queue for a stream of a device and returns it with the function that deregisters it and gives back what it still holds.
func (h *liveHub) newQueue(team, device string) (*liveQueue, func()) {
	q := &liveQueue{hub: h, team: team, maxItems: h.queueItems, maxBytes: h.queueBytes, wake: make(chan struct{}, 1)}
	h.mu.Lock()
	if h.streams[team] == nil {
		h.streams[team] = map[string]map[*liveQueue]struct{}{}
	}
	if h.streams[team][device] == nil {
		h.streams[team][device] = map[*liveQueue]struct{}{}
	}
	h.streams[team][device][q] = struct{}{}
	h.mu.Unlock()
	return q, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		q.release()
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
	defer h.mu.Unlock()
	qs := make([]*liveQueue, 0, len(h.streams[team][device]))
	for q := range h.streams[team][device] {
		qs = append(qs, q)
	}
	for _, q := range qs {
		q.push(it)
	}
	return len(qs)
}

// drop empties the queues of a device's streams: it is no longer a member.
func (h *liveHub) drop(team, device string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for q := range h.streams[team][device] {
		q.release()
		q.dropped = 0
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

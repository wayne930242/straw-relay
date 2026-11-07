package link

// Sender says which devices may send a kind: any member, only an attended one, or only an unattended one.
const (
	SenderAny        = "any"
	SenderAttended   = "attended"
	SenderUnattended = "unattended"
)

// KindRule is what the relay needs to know of a message kind. The ephemeral flag is fixed by the kind, never chosen by the sender.
type KindRule struct {
	Ephemeral bool
	WholeTeam bool
	Sender    string
	// Live marks a kind the relay holds in memory only, for the addressee's open streams now: it is never stored, queued, or replayed.
	Live bool
}

// Kinds are the message kinds of the spec (Message kinds), as the reference implementation fixes them.
var Kinds = map[string]KindRule{
	"tower":           {false, false, SenderAny, false},
	"presence":        {true, true, SenderAny, false},
	"board":           {true, true, SenderAny, false},
	"confirm-request": {false, false, SenderAny, false},
	"confirm-answer":  {false, false, SenderAny, false},
	"dialog-open":     {false, false, SenderUnattended, false},
	"dialog-answer":   {false, false, SenderAttended, false},
	"dialog-close":    {false, false, SenderUnattended, false},
	"unit-status":     {true, false, SenderUnattended, false},
	"notice":          {false, false, SenderUnattended, false},
	"caps":            {true, true, SenderAny, false},
	"tower-reply":     {false, false, SenderAny, false},
	"unit-stop":       {false, false, SenderAttended, false},
	"operate":         {true, false, SenderAny, true},
}

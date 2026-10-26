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
}

// Kinds are the message kinds of the spec (Message kinds), as the reference implementation fixes them.
var Kinds = map[string]KindRule{
	"tower":           {false, false, SenderAny},
	"presence":        {true, true, SenderAny},
	"board":           {true, true, SenderAny},
	"confirm-request": {false, false, SenderAny},
	"confirm-answer":  {false, false, SenderAny},
	"dialog-open":     {false, false, SenderUnattended},
	"dialog-answer":   {false, false, SenderAttended},
	"dialog-close":    {false, false, SenderUnattended},
	"unit-status":     {true, false, SenderUnattended},
	"notice":          {false, false, SenderUnattended},
}

package rulestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"
)

// Reactor follows llm-bridge-server's sessions and runs the active session
// rules against them.
//
// Two paths decide, and the run table's uniqueness makes them safe together:
//
//   - The feed. Every upsert frame updates rule-store's copy of the session and
//     marks it changed; the worker checks every active rule against it. This
//     is what makes a rule fire seconds after a session changes instead of on
//     the next 5-minute tick.
//   - The sweep. Every sweep_interval, rules with an updated_before condition
//     are checked against every session in the states they name, because no
//     frame arrives when a session merely gets older. Rules without a time
//     condition are not swept: nothing about them changes between frames,
//     except kanban-store's side, which sends no feed yet (see CONTRACT.md).
//
// When the feed cannot replay what was missed — first connection, a bridge
// restart, a gap — rule-store reads the whole list again and checks every
// rule against every session, so a change made while it was away is still
// decided.
type Reactor struct {
	store         *Store
	bridge        *LLMBridge
	facts         SessionFacts
	sweepInterval time.Duration
	clock         func() time.Time

	// sweeping is held by whichever sweep is running, so two never overlap:
	// the ticker skips its turn, and the full check after a seed waits.
	sweeping sync.Mutex

	mu      sync.Mutex
	changed map[string]bool
	wake    chan struct{}
	status  ReactorStatus
}

// ReactorStatus is what GET /health reports about the reactor.
type ReactorStatus struct {
	FeedConnected   bool   `json:"feed_connected"`
	FeedStreamID    string `json:"feed_stream_id"`
	LastEventID     string `json:"last_event_id"`
	LastSeedAt      int64  `json:"last_seed_at"`
	LastSeedCount   int    `json:"last_seed_count"`
	LastSweepAt     int64  `json:"last_sweep_at"`
	RunsRecorded    int    `json:"runs_recorded_since_start"`
	ErrorsSinceBoot int    `json:"errors_since_start"`
	LastError       string `json:"last_error"`
	LastErrorAt     int64  `json:"last_error_at"`
}

// NewReactor builds a reactor. Run starts it.
func NewReactor(store *Store, bridge *LLMBridge, facts SessionFacts, sweepInterval time.Duration) *Reactor {
	return &Reactor{store: store, bridge: bridge, facts: facts, sweepInterval: sweepInterval, clock: time.Now,
		changed: map[string]bool{}, wake: make(chan struct{}, 1)}
}

// Status returns a copy of the reactor's status.
func (r *Reactor) Status() ReactorStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

// fail records an error where /health shows it, and logs it. Nothing the
// reactor does is retried by hiding the error: the next frame or sweep tries
// again, and the count says how often that happened.
func (r *Reactor) fail(err error) {
	log.Printf("ERROR: %v", err)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.ErrorsSinceBoot++
	r.status.LastError = err.Error()
	r.status.LastErrorAt = r.clock().Unix()
}

// Run follows the feed, works the changed sessions and sweeps until ctx ends.
func (r *Reactor) Run(ctx context.Context) {
	go r.follow(ctx)
	go r.work(ctx)
	ticker := time.NewTicker(r.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.sweeping.TryLock() {
				continue
			}
			if err := r.Sweep(ctx, true); err != nil {
				r.fail(fmt.Errorf("sweep: %w", err))
			}
			r.sweeping.Unlock()
		}
	}
}

// follow keeps a connection to /session-events open, reconnecting with the
// last event id so the bridge can replay what was missed.
func (r *Reactor) follow(ctx context.Context) {
	lastEventID := ""
	for ctx.Err() == nil {
		var err error
		lastEventID, err = r.bridge.FollowSessions(ctx, lastEventID, FeedHandler{
			Hello:  func(hello FeedHello) error { return r.onHello(ctx, hello) },
			Upsert: r.onUpsert,
			Delete: func(sessionID string) error { return r.store.ForgetSubject(SubjectTypeSession, sessionID) },
		})
		r.mu.Lock()
		r.status.FeedConnected = false
		r.status.LastEventID = lastEventID
		r.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		r.fail(fmt.Errorf("session feed: %w", err))
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
}

func (r *Reactor) onHello(ctx context.Context, hello FeedHello) error {
	r.mu.Lock()
	r.status.FeedConnected = true
	r.status.FeedStreamID = hello.StreamID
	r.mu.Unlock()
	if hello.Resume == "replayed" {
		return nil
	}
	// Frames were missed or this is the first connection: read everything.
	// Frames queue in the bridge's buffer meanwhile; if it overflows, the
	// bridge closes the stream and the next hello says gap again.
	log.Printf("session feed %s says resume=%s: reading every session", hello.StreamID, hello.Resume)
	return r.Seed(ctx)
}

func (r *Reactor) onUpsert(raw json.RawMessage) error {
	session, err := ParseSessionSnapshot(raw)
	if err != nil {
		return err
	}
	if _, err := r.store.ObserveSubjects([]SubjectObservation{{Type: SubjectTypeSession, ID: session.SessionID, State: session.State, Snapshot: raw}}); err != nil {
		return err
	}
	r.mu.Lock()
	r.changed[session.SessionID] = true
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
	return nil
}

// work evaluates the sessions the feed marked changed.
func (r *Reactor) work(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		}
		r.mu.Lock()
		changed := r.changed
		r.changed = map[string]bool{}
		r.mu.Unlock()
		rules, err := r.store.ActiveRules(SourceSession)
		if err != nil {
			r.fail(fmt.Errorf("read active rules: %w", err))
			continue
		}
		for sessionID := range changed {
			subject, err := r.store.GetSubject(SubjectTypeSession, sessionID)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				r.fail(err)
				continue
			}
			for _, rule := range rules {
				r.decide(ctx, rule, *subject, CauseFeed)
			}
		}
	}
}

// Seed reads every session, replaces rule-store's copies, forgets sessions
// the bridge no longer has, and checks every active rule against everything.
func (r *Reactor) Seed(ctx context.Context) error {
	sessions, err := r.bridge.ListSessions(ctx)
	if err != nil {
		return err
	}
	observations := make([]SubjectObservation, 0, len(sessions))
	present := make(map[string]bool, len(sessions))
	for _, raw := range sessions {
		session, err := ParseSessionSnapshot(raw)
		if err != nil {
			return err
		}
		present[session.SessionID] = true
		observations = append(observations, SubjectObservation{Type: SubjectTypeSession, ID: session.SessionID, State: session.State, Snapshot: raw})
	}
	if _, err := r.store.ObserveSubjects(observations); err != nil {
		return err
	}
	stored, err := r.store.SubjectsInStates(SubjectTypeSession, nil)
	if err != nil {
		return err
	}
	for _, subject := range stored {
		if !present[subject.ID] {
			if err := r.store.ForgetSubject(SubjectTypeSession, subject.ID); err != nil {
				return err
			}
		}
	}
	r.mu.Lock()
	r.status.LastSeedAt, r.status.LastSeedCount = r.clock().Unix(), len(sessions)
	r.mu.Unlock()
	log.Printf("read %d sessions", len(sessions))
	// The full check runs beside the feed, not in it: checking every rule
	// against every session can take minutes, and a feed reader that stops
	// reading overflows the bridge's buffer and forces another seed.
	go func() {
		r.sweeping.Lock()
		defer r.sweeping.Unlock()
		if err := r.Sweep(ctx, false); err != nil && ctx.Err() == nil {
			r.fail(fmt.Errorf("full check after reading every session: %w", err))
		}
	}()
	return nil
}

// Sweep checks active rules against every session in the states they name.
// The caller holds r.sweeping. With onlyTimeRules it checks only rules that have an updated_before
// condition, which are the only ones time alone can change.
func (r *Reactor) Sweep(ctx context.Context, onlyTimeRules bool) error {
	rules, err := r.store.ActiveRules(SourceSession)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		if onlyTimeRules && !hasConditionKind(rule.Revision.Definition, ConditionUpdatedBefore) {
			continue
		}
		subjects, err := r.store.SubjectsInStates(SubjectTypeSession, statesNamedBy(rule.Revision.Definition))
		if err != nil {
			return err
		}
		for _, subject := range subjects {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.decide(ctx, rule, subject, CauseSweep)
		}
	}
	r.mu.Lock()
	r.status.LastSweepAt = r.clock().Unix()
	r.mu.Unlock()
	return nil
}

func hasConditionKind(d Definition, kind string) bool {
	return slices.ContainsFunc(d.Conditions, func(c Condition) bool { return c.Kind == kind })
}

// statesNamedBy returns the states a field_in state condition allows, or nil
// when the rule does not limit the state, meaning every session is a
// candidate.
func statesNamedBy(d Definition) []string {
	for _, c := range d.Conditions {
		if c.Kind == ConditionFieldIn && c.Field == "state" {
			return c.Values
		}
	}
	return nil
}

// decide runs one rule against one subject and records a match.
func (r *Reactor) decide(ctx context.Context, rule ActiveRule, subject Subject, cause string) {
	triggerKey := subject.TriggerKey()
	done, err := r.store.HasRun(rule.Revision.ID, subject.Type, subject.ID, triggerKey)
	if err != nil {
		r.fail(err)
		return
	}
	if done {
		return
	}
	decision, err := EvaluateSession(ctx, rule.Revision.Definition, subject, r.clock(), r.facts)
	if err != nil {
		r.fail(fmt.Errorf("rule %s on session %s: %w", rule.Rule.ID, subject.ID, err))
		return
	}
	if !decision.Matched {
		return
	}
	idempotencyKey := fmt.Sprintf("rule-store:%s:%s:%s", rule.Revision.ID, subject.ID, triggerKey)
	for i, effect := range decision.PlannedEffects {
		if effect.Kind == EffectSendSessionMessage {
			body := effect.Body.(map[string]any)
			body["client_request_id"] = idempotencyKey
			decision.PlannedEffects[i].Body = body
		}
	}
	facts, err := factsJSON(decision.Facts)
	if err != nil {
		r.fail(err)
		return
	}
	run := &Run{RuleID: rule.Rule.ID, RuleRevisionID: rule.Revision.ID, SubjectType: subject.Type, SubjectID: subject.ID,
		TriggerKey: triggerKey, Mode: rule.Rule.Mode, Cause: cause, Facts: facts, PlannedEffects: decision.PlannedEffects,
		Outcome: OutcomeShadowed, DecidedAt: r.clock().Unix()}
	if err := r.store.RecordRun(run); err != nil {
		if errors.Is(err, ErrConflict) {
			return
		}
		r.fail(err)
		return
	}
	r.mu.Lock()
	r.status.RunsRecorded++
	r.mu.Unlock()
}

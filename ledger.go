package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type StageState string

const (
	StateInProgress StageState = "IN_PROGRESS"
	StateCompleted  StageState = "COMPLETED"
	StateFailed     StageState = "FAILED"
)

type ClaimDisposition int

const (
	ClaimAcquired ClaimDisposition = iota
	ClaimCompleted
	ClaimBusy
	ClaimFailed
)

type StageRecord struct {
	DeploymentID   string
	Stage          string
	State          StageState
	Attempt        int
	LeaseExpiresAt sql.NullTime
	ResultJSON     json.RawMessage
}
type StageLedger interface {
	Claim(context.Context, string, string, time.Duration) (ClaimDisposition, StageRecord, error)
	Renew(context.Context, string, string, time.Duration) error
	Complete(context.Context, string, string, any) error
	Fail(context.Context, string, string, any) error
	Release(context.Context, string, string) error
	Close() error
}
type PostgresStageLedger struct {
	db    *sql.DB
	table string
}

var schemaNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func NewPostgresStageLedger(ctx context.Context, databaseURL, schema string) (*PostgresStageLedger, error) {
	if databaseURL == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	if !schemaNamePattern.MatchString(schema) {
		return nil, fmt.Errorf("invalid ledger schema %q", schema)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	l := &PostgresStageLedger{db: db, table: schema + ".stage_ledger"}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect stage ledger: %w", err)
	}
	return l, nil
}
func (l *PostgresStageLedger) Close() error { return l.db.Close() }
func (l *PostgresStageLedger) Claim(ctx context.Context, id, stage string, lease time.Duration) (ClaimDisposition, StageRecord, error) {
	tx, err := l.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, StageRecord{}, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	leaseExpiresAt := now.Add(lease)
	result, err := tx.ExecContext(ctx, `INSERT INTO `+l.table+`(deployment_id,stage,state,attempt,lease_expires_at) VALUES($1,$2,$3,1,$4) ON CONFLICT DO NOTHING`, id, stage, StateInProgress, leaseExpiresAt)
	if err != nil {
		return 0, StageRecord{}, err
	}
	if inserted, err := result.RowsAffected(); err != nil {
		return 0, StageRecord{}, err
	} else if inserted == 1 {
		r := StageRecord{DeploymentID: id, Stage: stage, State: StateInProgress, Attempt: 1, LeaseExpiresAt: sql.NullTime{Time: leaseExpiresAt, Valid: true}}
		return ClaimAcquired, r, tx.Commit()
	}
	var r StageRecord
	q := `SELECT deployment_id,stage,state,attempt,lease_expires_at,result_json FROM ` + l.table + ` WHERE deployment_id=$1 AND stage=$2 FOR UPDATE`
	err = tx.QueryRowContext(ctx, q, id, stage).Scan(&r.DeploymentID, &r.Stage, &r.State, &r.Attempt, &r.LeaseExpiresAt, &r.ResultJSON)
	if err != nil {
		return 0, r, err
	}
	if r.State == StateCompleted {
		return ClaimCompleted, r, tx.Commit()
	}
	if r.State == StateFailed {
		return ClaimFailed, r, tx.Commit()
	}
	if r.LeaseExpiresAt.Valid && r.LeaseExpiresAt.Time.After(now) {
		return ClaimBusy, r, tx.Commit()
	}
	r.Attempt++
	r.LeaseExpiresAt = sql.NullTime{Time: now.Add(lease), Valid: true}
	_, err = tx.ExecContext(ctx, `UPDATE `+l.table+` SET state=$3,attempt=$4,lease_expires_at=$5,updated_at=now() WHERE deployment_id=$1 AND stage=$2`, id, stage, StateInProgress, r.Attempt, r.LeaseExpiresAt.Time)
	if err != nil {
		return 0, r, err
	}
	return ClaimAcquired, r, tx.Commit()
}
func (l *PostgresStageLedger) Renew(ctx context.Context, id, stage string, d time.Duration) error {
	_, err := l.db.ExecContext(ctx, `UPDATE `+l.table+` SET lease_expires_at=$3,updated_at=now() WHERE deployment_id=$1 AND stage=$2 AND state=$4`, id, stage, time.Now().UTC().Add(d), StateInProgress)
	return err
}
func (l *PostgresStageLedger) set(ctx context.Context, id, stage string, state StageState, result any) error {
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = l.db.ExecContext(ctx, `UPDATE `+l.table+` SET state=$3,result_json=$4,lease_expires_at=NULL,updated_at=now() WHERE deployment_id=$1 AND stage=$2`, id, stage, state, b)
	return err
}
func (l *PostgresStageLedger) Complete(ctx context.Context, id, stage string, result any) error {
	return l.set(ctx, id, stage, StateCompleted, result)
}
func (l *PostgresStageLedger) Fail(ctx context.Context, id, stage string, result any) error {
	return l.set(ctx, id, stage, StateFailed, result)
}
func (l *PostgresStageLedger) Release(ctx context.Context, id, stage string) error {
	_, err := l.db.ExecContext(ctx, `UPDATE `+l.table+` SET state=$3,lease_expires_at=NULL,updated_at=now() WHERE deployment_id=$1 AND stage=$2`, id, stage, StateInProgress)
	return err
}

type memoryStageLedger struct {
	records map[string]StageRecord
	now     func() time.Time
}

func newMemoryStageLedger() *memoryStageLedger {
	return &memoryStageLedger{records: map[string]StageRecord{}, now: time.Now}
}
func (m *memoryStageLedger) key(id, stage string) string { return id + "\x00" + stage }
func (m *memoryStageLedger) Claim(_ context.Context, id, stage string, d time.Duration) (ClaimDisposition, StageRecord, error) {
	k := m.key(id, stage)
	r, ok := m.records[k]
	now := m.now()
	if !ok {
		r = StageRecord{DeploymentID: id, Stage: stage, State: StateInProgress, Attempt: 1, LeaseExpiresAt: sql.NullTime{Time: now.Add(d), Valid: true}}
		m.records[k] = r
		return ClaimAcquired, r, nil
	}
	if r.State == StateCompleted {
		return ClaimCompleted, r, nil
	}
	if r.State == StateFailed {
		return ClaimFailed, r, nil
	}
	if r.LeaseExpiresAt.Valid && r.LeaseExpiresAt.Time.After(now) {
		return ClaimBusy, r, nil
	}
	r.Attempt++
	r.LeaseExpiresAt = sql.NullTime{Time: now.Add(d), Valid: true}
	m.records[k] = r
	return ClaimAcquired, r, nil
}
func (m *memoryStageLedger) Renew(_ context.Context, id, stage string, d time.Duration) error {
	r := m.records[m.key(id, stage)]
	r.LeaseExpiresAt = sql.NullTime{Time: m.now().Add(d), Valid: true}
	m.records[m.key(id, stage)] = r
	return nil
}
func (m *memoryStageLedger) set(id, stage string, s StageState, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	r := m.records[m.key(id, stage)]
	r.State = s
	r.ResultJSON = b
	m.records[m.key(id, stage)] = r
	return nil
}
func (m *memoryStageLedger) Complete(_ context.Context, id, stage string, v any) error {
	return m.set(id, stage, StateCompleted, v)
}
func (m *memoryStageLedger) Fail(_ context.Context, id, stage string, v any) error {
	return m.set(id, stage, StateFailed, v)
}
func (m *memoryStageLedger) Release(_ context.Context, id, stage string) error {
	r := m.records[m.key(id, stage)]
	r.State = StateInProgress
	r.LeaseExpiresAt = sql.NullTime{}
	m.records[m.key(id, stage)] = r
	return nil
}
func (m *memoryStageLedger) Close() error { return nil }

package execution

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/librecosta/harvest/internal/config"
	q "github.com/librecosta/harvest/internal/database/queries"
	"github.com/librecosta/harvest/internal/egress"
	"github.com/librecosta/harvest/internal/provider"
)

func registerWorkerNode(ctx context.Context, db *q.Queries, p config.Principal, in Registration) error {
	if err := db.InsertWorkerNode(ctx, q.InsertWorkerNodeParams{
		NodeID: in.NodeID, Principal: p.Name, CurrentWorkerID: in.ID, Namespaces: p.Namespaces, Capabilities: p.Capabilities,
	}); err != nil {
		return err
	}
	node, err := db.LockWorkerNode(ctx, in.NodeID)
	if err != nil {
		return err
	}
	if node.Principal != p.Name {
		return ErrForbidden
	}
	if node.CurrentWorkerID != in.ID {
		// A superseded runtime may finish fenced operations but cannot rejoin.
		if _, err := db.Worker(ctx, q.WorkerParams{ID: in.ID, Principal: p.Name}); !errors.Is(err, pgx.ErrNoRows) {
			if err != nil {
				return err
			}
			return ErrConflict
		}
		previous, err := db.LockWorker(ctx, q.LockWorkerParams{ID: node.CurrentWorkerID, Principal: p.Name})
		if err != nil {
			return err
		}
		now, err := db.DBNow(ctx)
		if err != nil {
			return err
		}
		if !previous.Draining && previous.LastSeen.Add(WorkerTTL).After(now) {
			return ErrWorkerNodeBusy
		}
		active, err := db.WorkerLeaseCount(ctx, previous.ID)
		if err != nil {
			return err
		}
		if active > 0 {
			return ErrWorkerNodeBusy
		}
	}
	return db.SetWorkerNodeRuntime(ctx, q.SetWorkerNodeRuntimeParams{
		NodeID: in.NodeID, CurrentWorkerID: in.ID, Namespaces: p.Namespaces, Capabilities: p.Capabilities,
	})
}

func lockRuntime(ctx context.Context, db *q.Queries, principal string, id uuid.UUID) (q.HarvestWorker, string, error) {
	w, err := found(db.Worker(ctx, q.WorkerParams{ID: id, Principal: principal}))
	if err != nil {
		return w, "", err
	}
	var state string
	if w.NodeID != nil {
		node, err := db.LockWorkerNode(ctx, *w.NodeID)
		if err != nil {
			return w, "", err
		}
		if node.CurrentWorkerID != id {
			return w, "", ErrFence
		}
		state = node.DesiredState
	}
	w, err = found(db.LockWorker(ctx, q.LockWorkerParams{ID: id, Principal: principal}))
	return w, state, err
}

type WorkerNode struct {
	NodeID          string                 `json:"node_id"`
	RuntimeID       uuid.UUID              `json:"runtime_id"`
	DesiredState    string                 `json:"desired_state"`
	Revision        int64                  `json:"revision"`
	Online          bool                   `json:"online"`
	RuntimeDraining bool                   `json:"runtime_draining"`
	LastSeen        time.Time              `json:"last_seen"`
	Capacity        int32                  `json:"capacity"`
	ActiveTasks     int64                  `json:"active_tasks"`
	Capabilities    []string               `json:"capabilities"`
	EgressProfiles  []string               `json:"egress_profiles"`
	Paths           []egress.Advertisement `json:"paths"`
	CanManage       bool                   `json:"can_manage"`
	Tasks           []WorkerNodeTask       `json:"tasks,omitempty"`
}

type WorkerNodeTask struct {
	JobID      uuid.UUID `json:"job_id"`
	Capability string    `json:"capability"`
	ClaimedAt  time.Time `json:"claimed_at"`
}

type WorkerNodeList struct {
	Items []WorkerNode `json:"items"`
	Next  string       `json:"next,omitempty"`
}

type WorkerNodeEdit struct {
	Revision  int64      `json:"revision"`
	State     string     `json:"state"`
	RequestID uuid.UUID  `json:"request_id"`
	ActorID   *uuid.UUID `json:"actor_id,omitempty"`
	ActorName string     `json:"actor_name,omitempty"`
	ActorRole string     `json:"actor_role,omitempty"`
}

type WorkerNodeResult struct {
	Revision int64 `json:"revision"`
}

func workerNodeScope(p config.Principal, namespace string, namespaces, capabilities []string) bool {
	// A node-wide action affects every namespace and capability allowed to run on it.
	for _, name := range namespaces {
		if !canNamespace(p, name) || (namespace != "" && name != namespace) {
			return false
		}
	}
	for _, capability := range capabilities {
		if !canCapability(p, capability) {
			return false
		}
	}
	return true
}

func workerNodeNamespace(p config.Principal, namespace string) error {
	if namespace == "" {
		return nil
	}
	if !provider.Identifier(namespace) {
		return ErrInvalid
	}
	if !canNamespace(p, namespace) {
		return ErrForbidden
	}
	return nil
}

func (e *Engine) workerNodeRows(ctx context.Context, p config.Principal, namespace, id, after string, size int32) (WorkerNodeList, error) {
	if err := workerNodeNamespace(p, namespace); err != nil {
		return WorkerNodeList{}, err
	}
	excluded := make([]string, 0)
	for _, policy := range e.workerPolicies {
		if !workerNodeScope(p, namespace, policy.Namespaces, policy.Capabilities) {
			excluded = append(excluded, policy.Name)
		}
	}
	rows, err := e.queries.WorkerNodes(ctx, q.WorkerNodesParams{
		ExcludedPrincipals: excluded, Namespaces: p.Namespaces, Capabilities: append([]string{}, p.Capabilities...),
		Namespace: namespace, ExactID: id, AfterID: after, PageSize: size + 1,
	})
	if err != nil {
		return WorkerNodeList{}, err
	}
	out := WorkerNodeList{Items: make([]WorkerNode, 0, min(len(rows), int(size)))}
	if len(rows) > int(size) {
		rows = rows[:size]
		out.Next = rows[len(rows)-1].NodeID
	}
	for _, row := range rows {
		paths, err := decodePaths(row.NetworkPaths)
		if err != nil {
			return WorkerNodeList{}, err
		}
		caps := make([]string, 0, len(row.Offers))
		profiles := append([]string{}, row.EgressProfiles...)
		for _, path := range paths {
			if !slices.Contains(profiles, path.Profile) {
				profiles = append(profiles, path.Profile)
			}
		}
		for _, offer := range row.Offers {
			capability, _, _ := strings.Cut(offer, ":")
			caps = append(caps, capability)
		}
		out.Items = append(out.Items, WorkerNode{
			NodeID: row.NodeID, RuntimeID: row.CurrentWorkerID, DesiredState: row.DesiredState, Revision: row.Revision,
			Online: row.Online, RuntimeDraining: row.Draining, LastSeen: row.LastSeen.UTC(), Capacity: row.Capacity,
			ActiveTasks: row.ActiveTasks, Capabilities: caps, EgressProfiles: profiles,
			Paths: append([]egress.Advertisement{}, paths...), CanManage: p.Role == "operator",
		})
	}
	return out, nil
}

func (e *Engine) ListWorkerNodes(ctx context.Context, p config.Principal, namespace, after string) (WorkerNodeList, error) {
	if after != "" && !config.ValidWorkerNodeID(after) {
		return WorkerNodeList{}, ErrInvalid
	}
	return e.workerNodeRows(ctx, p, namespace, "", after, 30)
}

func (e *Engine) GetWorkerNode(ctx context.Context, p config.Principal, namespace, id string) (WorkerNode, error) {
	if !config.ValidWorkerNodeID(id) {
		return WorkerNode{}, ErrInvalid
	}
	out, err := e.workerNodeRows(ctx, p, namespace, id, "", 1)
	if err != nil {
		return WorkerNode{}, err
	}
	if len(out.Items) == 0 {
		return WorkerNode{}, ErrNotFound
	}
	node := out.Items[0]
	tasks, err := e.queries.WorkerNodeTasks(ctx, q.WorkerNodeTasksParams{
		WorkerID: node.RuntimeID, Namespaces: p.Namespaces, Capabilities: append([]string{}, p.Capabilities...),
	})
	if err != nil {
		return WorkerNode{}, err
	}
	for _, task := range tasks {
		node.Tasks = append(node.Tasks, WorkerNodeTask(task))
	}
	return node, nil
}

func (e *Engine) EditWorkerNode(ctx context.Context, p config.Principal, namespace, id string, edit WorkerNodeEdit) (WorkerNodeResult, error) {
	if p.Role != "operator" {
		return WorkerNodeResult{}, ErrForbidden
	}
	if err := workerNodeNamespace(p, namespace); err != nil {
		return WorkerNodeResult{}, err
	}
	if !config.ValidWorkerNodeID(id) || edit.Revision < 1 || edit.RequestID == uuid.Nil() ||
		(edit.State != "active" && edit.State != "paused" && edit.State != "draining") {
		return WorkerNodeResult{}, ErrInvalid
	}
	if edit.ActorID == nil && edit.ActorName == "" && edit.ActorRole == "" {
		edit.ActorName, edit.ActorRole = p.Name, p.Role
	} else if edit.ActorID == nil || *edit.ActorID == uuid.Nil() || strings.TrimSpace(edit.ActorName) == "" ||
		utf8.RuneCountInString(edit.ActorName) > 80 ||
		strings.ContainsFunc(edit.ActorName, func(r rune) bool { return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) }) ||
		(edit.ActorRole != "owner" && edit.ActorRole != "operator") {
		return WorkerNodeResult{}, ErrInvalid
	}
	return tx(ctx, e, func(db *q.Queries) (WorkerNodeResult, error) {
		node, err := found(db.LockWorkerNode(ctx, id))
		if err != nil {
			return WorkerNodeResult{}, err
		}
		if !workerNodeScope(p, namespace, node.Namespaces, node.Capabilities) {
			return WorkerNodeResult{}, ErrNotFound
		}
		for _, policy := range e.workerPolicies {
			if policy.Name == node.Principal && !workerNodeScope(p, namespace, policy.Namespaces, policy.Capabilities) {
				return WorkerNodeResult{}, ErrNotFound
			}
		}
		previous, err := db.WorkerNodeAction(ctx, edit.RequestID)
		if err == nil {
			sameActor := previous.ActorID == nil && edit.ActorID == nil ||
				previous.ActorID != nil && edit.ActorID != nil && *previous.ActorID == *edit.ActorID
			if previous.NodeID != id || previous.Principal != p.Name || !sameActor || previous.ActorName != edit.ActorName ||
				previous.ActorRole != edit.ActorRole || previous.Revision-1 != edit.Revision || previous.DesiredState != edit.State {
				return WorkerNodeResult{}, ErrConflict
			}
			return WorkerNodeResult{Revision: previous.Revision}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return WorkerNodeResult{}, err
		}
		if node.Revision != edit.Revision || node.DesiredState == edit.State {
			return WorkerNodeResult{}, ErrConflict
		}
		revision, err := db.SetWorkerNodeState(ctx, q.SetWorkerNodeStateParams{
			NodeID: id, DesiredState: edit.State, Revision: edit.Revision,
		})
		if err != nil {
			return WorkerNodeResult{}, err
		}
		err = db.InsertWorkerNodeAction(ctx, q.InsertWorkerNodeActionParams{
			RequestID: edit.RequestID, NodeID: id, Principal: p.Name, ActorID: edit.ActorID, ActorName: edit.ActorName,
			ActorRole: edit.ActorRole, PreviousState: node.DesiredState, DesiredState: edit.State, Revision: revision,
		})
		return WorkerNodeResult{Revision: revision}, err
	})
}

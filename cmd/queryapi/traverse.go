package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"graph-sub-pred-obj/model"
)

// Roles a value can play in a triple, named by the bin that holds it.
const (
	RoleSubject   = "subject"
	RolePredicate = "predicate"
	RoleObject    = "object"
)

var allRoles = []string{RoleSubject, RolePredicate, RoleObject}

// Fetcher returns up to limit triples whose bin (a role) equals value,
// keeping only those with one of predicates when any are given.
type Fetcher interface {
	Fetch(role, value string, predicates []string, limit int64) ([]*model.Triple, error)
}

// TraverseRequest is a multi-hop traversal from one or more start values.
type TraverseRequest struct {
	Start []string `json:"start"`
	// Hops is how many triples away from a start value to go, at least 1.
	Hops int `json:"hops"`
	// Direction is the way each hop past the first follows triples: "out"
	// (node as subject, on to its objects), "in" (node as object, back to
	// its subjects), or "both".
	Direction string `json:"direction"`
	// As is the roles the start values play in first-hop triples: any of
	// subject, predicate and object. Defaults to the roles Direction follows.
	As []string `json:"as"`
	// Predicates, when given, are the only predicates followed.
	Predicates []string `json:"predicates"`
	// MaxDegree caps the triples fetched per node and direction. A node past
	// the first hop with more is a hub: reported, but its triples are not
	// returned and it is not expanded. Start values return their first
	// MaxDegree triples per role.
	MaxDegree int64 `json:"max_degree"`
	// MaxTriples caps the triples returned.
	MaxTriples int `json:"max_triples"`
	// Props includes each triple's props.
	Props *bool `json:"props"`
}

// Node is a value reached by a traversal.
type Node struct {
	ID    string `json:"id"`
	Depth int    `json:"depth"`
	// Hub: more than max_degree triples, so not expanded.
	Hub bool `json:"hub,omitempty"`
	// Truncated: a start value with more than max_degree triples in a role;
	// only the first max_degree were followed.
	Truncated bool `json:"truncated,omitempty"`
}

// HopTriple is a triple reached by a traversal.
type HopTriple struct {
	TripleJSON
	// Hop is how many triples from a start value this one is.
	Hop int `json:"hop"`
	// From is the node this triple was reached from, and Via the role that
	// node plays in it.
	From string `json:"from"`
	Via  string `json:"via"`
}

// TraverseResult is the outcome of a traversal.
type TraverseResult struct {
	Nodes   []*Node     `json:"nodes"`
	Triples []HopTriple `json:"triples"`
	// Truncated: max_triples was reached or the request timed out before
	// every hop completed.
	Truncated bool   `json:"truncated"`
	TimedOut  bool   `json:"timed_out,omitempty"`
	Queries   int64  `json:"queries"`
	Elapsed   string `json:"elapsed,omitempty"`
}

// Traverser runs traversals, querying the nodes of each hop concurrently.
type Traverser struct {
	Fetch Fetcher
	// Threads is the concurrent queries per traversal.
	Threads int
}

// fetchJob is one index query of a hop: the triples where node plays role.
type fetchJob struct {
	node  string
	role  string
	limit int64
	start bool

	triples []*model.Triple
	err     error
}

// followRoles returns the roles a node plays in the triples a direction
// follows.
func followRoles(direction string) ([]string, error) {
	switch direction {
	case "out":
		return []string{RoleSubject}, nil
	case "in":
		return []string{RoleObject}, nil
	case "both":
		return []string{RoleSubject, RoleObject}, nil
	}
	return nil, fmt.Errorf("direction must be out, in or both, not %q", direction)
}

// Run traverses breadth first: each hop queries every node of the frontier
// in parallel, then the unvisited ends of the triples found become the next
// frontier. ctx's deadline stops the traversal between hops.
func (tr *Traverser) Run(ctx context.Context, req *TraverseRequest) (*TraverseResult, error) {
	follow, err := followRoles(req.Direction)
	if err != nil {
		return nil, err
	}
	first := req.As
	if len(first) == 0 {
		first = follow
	}
	props := req.Props == nil || *req.Props

	res := &TraverseResult{Nodes: []*Node{}, Triples: []HopTriple{}}
	nodes := map[string]*Node{}
	seen := map[string]bool{} // triple keys returned
	var frontier []string
	for _, s := range req.Start {
		if nodes[s] == nil {
			n := &Node{ID: s, Depth: 0}
			nodes[s] = n
			res.Nodes = append(res.Nodes, n)
			frontier = append(frontier, s)
		}
	}

	for hop := 1; hop <= req.Hops && len(frontier) > 0; hop++ {
		if ctx.Err() != nil {
			res.Truncated, res.TimedOut = true, true
			break
		}
		var jobs []*fetchJob
		for _, node := range frontier {
			roles := follow
			if hop == 1 {
				roles = first
			}
			for _, role := range roles {
				// One more than max_degree tells a hub apart from a node
				// with exactly max_degree triples
				jobs = append(jobs, &fetchJob{node: node, role: role, limit: req.MaxDegree + 1, start: hop == 1})
			}
		}
		tr.runJobs(jobs, req.Predicates)
		res.Queries += int64(len(jobs))

		var next []string
		for _, j := range jobs {
			if j.err != nil {
				return nil, fmt.Errorf("query %s=%q: %w", j.role, j.node, j.err)
			}
			triples := j.triples
			if int64(len(triples)) > req.MaxDegree {
				if !j.start {
					nodes[j.node].Hub = true
					continue
				}
				nodes[j.node].Truncated = true
				triples = triples[:req.MaxDegree]
			}
			for _, t := range triples {
				key := t.Key()
				if seen[key] {
					continue
				}
				if len(res.Triples) >= req.MaxTriples {
					res.Truncated = true
					break
				}
				seen[key] = true
				res.Triples = append(res.Triples, HopTriple{
					TripleJSON: tripleJSON(t, key, props),
					Hop:        hop, From: j.node, Via: j.role,
				})
				for _, end := range []string{t.Subject, t.Object} {
					if nodes[end] == nil {
						n := &Node{ID: end, Depth: hop}
						nodes[end] = n
						res.Nodes = append(res.Nodes, n)
						next = append(next, end)
					}
				}
			}
			if res.Truncated {
				break
			}
		}
		if res.Truncated {
			break
		}
		frontier = next
	}
	return res, nil
}

// runJobs runs a hop's queries on up to Threads goroutines.
func (tr *Traverser) runJobs(jobs []*fetchJob, predicates []string) {
	threads := min(max(tr.Threads, 1), len(jobs))
	var next atomic.Int64
	var wg sync.WaitGroup
	for range threads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= int64(len(jobs)) {
					return
				}
				j := jobs[i]
				j.triples, j.err = tr.Fetch.Fetch(j.role, j.node, predicates, j.limit)
			}
		}()
	}
	wg.Wait()
}

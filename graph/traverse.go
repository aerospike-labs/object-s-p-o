package graph

import (
	"graph-sub-pred-obj/model"
	"graph-sub-pred-obj/store"
	"sync"
)

// TraversalResult holds a path of triples discovered during traversal.
type TraversalResult struct {
	Path  []model.Triple
	Depth int
}

// TraverseOutbound performs BFS from a starting subject, following object links.
// maxDepth limits how many hops to follow. predicate optionally filters by relationship type.
func TraverseOutbound(gs *store.GraphStore, startSubject string, maxDepth int, predicate string) ([]TraversalResult, error) {
	type entry struct {
		node  string
		depth int
		path  []model.Triple
	}

	visited := map[string]bool{startSubject: true}
	queue := []entry{{node: startSubject, depth: 0}}
	var results []TraversalResult

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.depth >= maxDepth {
			continue
		}

		var triples []*model.Triple
		var err error
		if predicate != "" {
			triples, err = gs.QuerySP(current.node, predicate)
		} else {
			triples, err = gs.QueryBySubject(current.node)
		}
		if err != nil {
			return nil, err
		}

		for _, t := range triples {
			newPath := make([]model.Triple, len(current.path)+1)
			copy(newPath, current.path)
			newPath[len(current.path)] = *t

			results = append(results, TraversalResult{
				Path:  newPath,
				Depth: current.depth + 1,
			})

			if !visited[t.Object] {
				visited[t.Object] = true
				queue = append(queue, entry{
					node:  t.Object,
					depth: current.depth + 1,
					path:  newPath,
				})
			}
		}
	}
	return results, nil
}

// TraverseInbound performs BFS from a starting object, following subject links backwards.
func TraverseInbound(gs *store.GraphStore, startObject string, maxDepth int, predicate string) ([]TraversalResult, error) {
	type entry struct {
		node  string
		depth int
		path  []model.Triple
	}

	visited := map[string]bool{startObject: true}
	queue := []entry{{node: startObject, depth: 0}}
	var results []TraversalResult

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.depth >= maxDepth {
			continue
		}

		var triples []*model.Triple
		var err error
		if predicate != "" {
			triples, err = gs.QueryPO(predicate, current.node)
		} else {
			triples, err = gs.QueryByObject(current.node)
		}
		if err != nil {
			return nil, err
		}

		for _, t := range triples {
			newPath := make([]model.Triple, len(current.path)+1)
			copy(newPath, current.path)
			newPath[len(current.path)] = *t

			results = append(results, TraversalResult{
				Path:  newPath,
				Depth: current.depth + 1,
			})

			if !visited[t.Subject] {
				visited[t.Subject] = true
				queue = append(queue, entry{
					node:  t.Subject,
					depth: current.depth + 1,
					path:  newPath,
				})
			}
		}
	}
	return results, nil
}

// TraverseOutboundConcurrent performs BFS with concurrent queries at each level.
// concurrency controls the maximum number of parallel Aerospike queries.
func TraverseOutboundConcurrent(gs *store.GraphStore, startSubject string, maxDepth int, concurrency int) ([]TraversalResult, error) {
	if concurrency <= 0 {
		concurrency = 8
	}

	type entry struct {
		node string
		path []model.Triple
	}

	visited := map[string]bool{startSubject: true}
	currentLevel := []entry{{node: startSubject}}
	var results []TraversalResult

	for depth := 0; depth < maxDepth && len(currentLevel) > 0; depth++ {
		type queryResult struct {
			idx     int
			triples []*model.Triple
			err     error
		}

		sem := make(chan struct{}, concurrency)
		resultsCh := make(chan queryResult, len(currentLevel))
		var wg sync.WaitGroup

		for i, e := range currentLevel {
			wg.Add(1)
			go func(idx int, node string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				triples, err := gs.QueryBySubject(node)
				resultsCh <- queryResult{idx: idx, triples: triples, err: err}
			}(i, e.node)
		}

		go func() {
			wg.Wait()
			close(resultsCh)
		}()

		var nextLevel []entry
		levelResults := make(map[int]queryResult)
		for qr := range resultsCh {
			if qr.err != nil {
				return nil, qr.err
			}
			levelResults[qr.idx] = qr
		}

		for i, e := range currentLevel {
			qr := levelResults[i]
			for _, t := range qr.triples {
				newPath := make([]model.Triple, len(e.path)+1)
				copy(newPath, e.path)
				newPath[len(e.path)] = *t

				results = append(results, TraversalResult{
					Path:  newPath,
					Depth: depth + 1,
				})

				if !visited[t.Object] {
					visited[t.Object] = true
					nextLevel = append(nextLevel, entry{node: t.Object, path: newPath})
				}
			}
		}
		currentLevel = nextLevel
	}
	return results, nil
}

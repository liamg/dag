// Package dag implements directed acyclic graphs (DAGs).
package dag

import (
	"fmt"
	"sync"
)

// IDInterface describes the interface a type must implement in order to
// explicitly specify vertex id.
//
// Objects of types not implementing this interface will receive automatically
// generated ids (as of adding them to the graph).
type IDInterface interface {
	ID() string
}

// DAG implements the data structure of the DAG.
type DAG[T IDInterface] struct {
	muDAG            sync.RWMutex
	vertexIds        map[string]T
	inboundEdge      map[string]map[string]struct{}
	outboundEdge     map[string]map[string]struct{}
	muCache          sync.RWMutex
	verticesLocked   *dMutex
	ancestorsCache   map[string]map[string]struct{}
	descendantsCache map[string]map[string]struct{}
}

// NewDAG creates / initializes a new DAG.
func NewDAG[T IDInterface]() *DAG[T] {
	return &DAG[T]{
		vertexIds:        make(map[string]T),
		inboundEdge:      make(map[string]map[string]struct{}),
		outboundEdge:     make(map[string]map[string]struct{}),
		verticesLocked:   newDMutex(),
		ancestorsCache:   make(map[string]map[string]struct{}),
		descendantsCache: make(map[string]map[string]struct{}),
	}
}

// AddVertex adds the vertex v to the DAG. AddVertex returns an error, if v is
// nil, v is already part of the graph, or the id of v is already part of the
// graph.
func (d *DAG[T]) AddVertex(v T) (string, error) {

	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	return d.addVertex(v)
}

func (d *DAG[T]) addVertex(v T) (string, error) {
	id := v.ID()
	err := d.addVertexByID(id, v)
	return id, err
}

// AddVertexByID adds the vertex v and the specified id to the DAG.
// AddVertexByID returns an error, if v is nil, v is already part of the graph,
// or the specified id is already part of the graph.
func (d *DAG[T]) AddVertexByID(id string, v T) error {

	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	return d.addVertexByID(id, v)
}

func (d *DAG[T]) addVertexByID(id string, v T) error {

	if _, exists := d.vertexIds[id]; exists {
		return IDDuplicateError{id}
	}

	d.vertexIds[id] = v

	return nil
}

// GetVertex returns a vertex by its id. GetVertex returns an error, if id is
// the empty string or unknown.
func (d *DAG[T]) GetVertex(id string) (T, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	var zeroValue T

	if id == "" {
		return zeroValue, IDEmptyError{}
	}

	v, exists := d.vertexIds[id]
	if !exists {
		return zeroValue, IDUnknownError{id}
	}
	return v, nil
}

// DeleteVertex deletes the vertex with the given id. DeleteVertex also
// deletes all attached edges (inbound and outbound). DeleteVertex returns
// an error, if id is empty or unknown.
func (d *DAG[T]) DeleteVertex(id string) error {

	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	if err := d.saneID(id); err != nil {
		return err
	}

	// get descendents and ancestors as they are now
	descendants := copyMap(d.getDescendants(id))
	ancestors := copyMap(d.getAncestors(id))

	// delete v in outbound edges of parents
	if _, exists := d.inboundEdge[id]; exists {
		for parent := range d.inboundEdge[id] {
			delete(d.outboundEdge[parent], id)
		}
	}

	// delete v in inbound edges of children
	if _, exists := d.outboundEdge[id]; exists {
		for child := range d.outboundEdge[id] {
			delete(d.inboundEdge[child], id)
		}
	}

	// delete in- and outbound of v itself
	delete(d.inboundEdge, id)
	delete(d.outboundEdge, id)

	// for v and all its descendants delete cached ancestors
	for descendant := range descendants {
		delete(d.ancestorsCache, descendant)
	}
	delete(d.ancestorsCache, id)

	// for v and all its ancestors delete cached descendants
	for ancestor := range ancestors {
		delete(d.descendantsCache, ancestor)
	}
	delete(d.descendantsCache, id)

	// delete v itself
	delete(d.vertexIds, id)

	return nil
}

// AddEdge adds an edge between srcID and dstID. AddEdge returns an
// error, if srcID or dstID are empty strings or unknown, if the edge
// already exists, or if the new edge would create a loop.
func (d *DAG[T]) AddEdge(srcID, dstID string) error {

	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	if err := d.saneID(srcID); err != nil {
		return err
	}

	if err := d.saneID(dstID); err != nil {
		return err
	}

	if srcID == dstID {
		return SrcDstEqualError{srcID, dstID}
	}

	// if the edge is already known, there is nothing else to do
	if d.isEdge(srcID, dstID) {
		return EdgeDuplicateError{srcID, dstID}
	}

	// get descendents and ancestors as they are now
	descendants := copyMap(d.getDescendants(dstID))
	ancestors := copyMap(d.getAncestors(srcID))

	if _, exists := descendants[srcID]; exists {
		return EdgeLoopError{srcID, dstID}
	}

	// prepare d.outbound[src], iff needed
	if _, exists := d.outboundEdge[srcID]; !exists {
		d.outboundEdge[srcID] = make(map[string]struct{})
	}

	// dst is a child of src
	d.outboundEdge[srcID][dstID] = struct{}{}

	// prepare d.inboundEdge[dst], iff needed
	if _, exists := d.inboundEdge[dstID]; !exists {
		d.inboundEdge[dstID] = make(map[string]struct{})
	}

	// src is a parent of dst
	d.inboundEdge[dstID][srcID] = struct{}{}

	// for dst and all its descendants delete cached ancestors
	for descendant := range descendants {
		delete(d.ancestorsCache, descendant)
	}
	delete(d.ancestorsCache, dstID)

	// for src and all its ancestors delete cached descendants
	for ancestor := range ancestors {
		delete(d.descendantsCache, ancestor)
	}
	delete(d.descendantsCache, srcID)

	return nil
}

// IsEdge returns true, if there exists an edge between srcID and dstID.
// IsEdge returns false, if there is no such edge. IsEdge returns an error,
// if srcID or dstID are empty, unknown, or the same.
func (d *DAG[T]) IsEdge(srcID, dstID string) (bool, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	if err := d.saneID(srcID); err != nil {
		return false, err
	}
	if err := d.saneID(dstID); err != nil {
		return false, err
	}
	if srcID == dstID {
		return false, SrcDstEqualError{srcID, dstID}
	}

	return d.isEdge(srcID, dstID), nil
}

func (d *DAG[T]) isEdge(srcID, dstID string) bool {

	if _, exists := d.outboundEdge[srcID]; !exists {
		return false
	}
	if _, exists := d.outboundEdge[srcID][dstID]; !exists {
		return false
	}
	if _, exists := d.inboundEdge[dstID]; !exists {
		return false
	}
	if _, exists := d.inboundEdge[dstID][srcID]; !exists {
		return false
	}
	return true
}

// DeleteEdge deletes the edge between srcID and dstID. DeleteEdge
// returns an error, if srcID or dstID are empty or unknown, or if,
// there is no edge between srcID and dstID.
func (d *DAG[T]) DeleteEdge(srcID, dstID string) error {

	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	if err := d.saneID(srcID); err != nil {
		return err
	}
	if err := d.saneID(dstID); err != nil {
		return err
	}
	if srcID == dstID {
		return SrcDstEqualError{srcID, dstID}
	}

	src := d.vertexIds[srcID]
	srcHash := d.hashVertex(src)
	dst := d.vertexIds[dstID]
	dstHash := d.hashVertex(dst)

	if !d.isEdge(srcHash, dstHash) {
		return EdgeUnknownError{srcID, dstID}
	}

	// get descendents and ancestors as they are now
	descendants := copyMap(d.getDescendants(srcHash))
	ancestors := copyMap(d.getAncestors(dstHash))

	// delete outbound and inbound
	delete(d.outboundEdge[srcHash], dstHash)
	delete(d.inboundEdge[dstHash], srcHash)

	// for src and all its descendants delete cached ancestors
	for descendant := range descendants {
		delete(d.ancestorsCache, descendant)
	}
	delete(d.ancestorsCache, srcHash)

	// for dst and all its ancestors delete cached descendants
	for ancestor := range ancestors {
		delete(d.descendantsCache, ancestor)
	}
	delete(d.descendantsCache, dstHash)

	return nil
}

// GetOrder returns the number of vertices in the graph.
func (d *DAG[T]) GetOrder() int {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	return d.getOrder()
}

func (d *DAG[T]) getOrder() int {
	return len(d.vertexIds)
}

// GetSize returns the number of edges in the graph.
func (d *DAG[T]) GetSize() int {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	return d.getSize()
}

func (d *DAG[T]) getSize() int {
	count := 0
	for _, value := range d.outboundEdge {
		count += len(value)
	}
	return count
}

// GetLeaves returns all vertices without children.
func (d *DAG[T]) GetLeaves() map[string]T {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	return d.getLeaves()
}

func (d *DAG[T]) getLeaves() map[string]T {
	leaves := make(map[string]T)
	for id := range d.vertexIds {
		dstIDs, ok := d.outboundEdge[id]
		if !ok || len(dstIDs) == 0 {
			leaves[id] = d.vertexIds[id]
		}
	}
	return leaves
}

// IsLeaf returns true, if the vertex with the given id has no children. IsLeaf
// returns an error, if id is empty or unknown.
func (d *DAG[T]) IsLeaf(id string) (bool, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return false, err
	}
	return d.isLeaf(id), nil
}

func (d *DAG[T]) isLeaf(id string) bool {
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)
	dstIDs, ok := d.outboundEdge[vHash]
	if !ok || len(dstIDs) == 0 {
		return true
	}
	return false
}

// GetRoots returns all vertices without parents.
func (d *DAG[T]) GetRoots() map[string]T {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	return d.getRoots()
}

func (d *DAG[T]) getRoots() map[string]T {
	roots := make(map[string]T)
	for id := range d.vertexIds {
		srcIDs, ok := d.inboundEdge[id]
		if !ok || len(srcIDs) == 0 {
			roots[id] = d.vertexIds[id]
		}
	}
	return roots
}

// IsRoot returns true, if the vertex with the given id has no parents. IsRoot
// returns an error, if id is empty or unknown.
func (d *DAG[T]) IsRoot(id string) (bool, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return false, err
	}
	return d.isRoot(id), nil
}

func (d *DAG[T]) isRoot(id string) bool {
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)
	srcIDs, ok := d.inboundEdge[vHash]
	if !ok || len(srcIDs) == 0 {
		return true
	}
	return false
}

// GetVertices returns all vertices.
func (d *DAG[T]) GetVertices() map[string]T {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	out := make(map[string]T)
	for id, value := range d.vertexIds {
		out[id] = value
	}
	return out
}

// GetParents returns the all parents of the vertex with the id
// id. GetParents returns an error, if id is empty or unknown.
func (d *DAG[T]) GetParents(id string) (map[string]T, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return nil, err
	}
	parents := make(map[string]T)
	for eid := range d.inboundEdge[id] {
		parents[eid] = d.vertexIds[eid]
	}
	return parents, nil
}

// GetChildren returns all children of the vertex with the id
// id. GetChildren returns an error, if id is empty or unknown.
func (d *DAG[T]) GetChildren(id string) (map[string]T, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	return d.getChildren(id)
}

func (d *DAG[T]) getChildren(id string) (map[string]T, error) {
	if err := d.saneID(id); err != nil {
		return nil, err
	}
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)
	children := make(map[string]T)
	for cid := range d.outboundEdge[vHash] {
		children[cid] = d.vertexIds[cid]
	}
	return children, nil
}

// GetAncestors return all ancestors of the vertex with the id id. GetAncestors
// returns an error, if id is empty or unknown.
//
// Note, in order to get the ancestors, GetAncestors populates the ancestor-
// cache as needed. Depending on order and size of the sub-graph of the vertex
// with id id this may take a long time and consume a lot of memory.
func (d *DAG[T]) GetAncestors(id string) (map[string]T, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return nil, err
	}
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)
	ancestors := make(map[string]T)
	for aid := range d.getAncestors(vHash) {
		ancestors[aid] = d.vertexIds[aid]
	}
	return ancestors, nil
}

func (d *DAG[T]) getAncestors(id string) map[string]struct{} {

	// in the best case we have already a populated cache
	d.muCache.RLock()
	cache, exists := d.ancestorsCache[id]
	d.muCache.RUnlock()
	if exists {
		return cache
	}

	// lock this vertex to work on it exclusively
	d.verticesLocked.lock(id)
	defer d.verticesLocked.unlock(id)

	// now as we have locked this vertex, check (again) that no one has
	// meanwhile populated the cache
	d.muCache.RLock()
	cache, exists = d.ancestorsCache[id]
	d.muCache.RUnlock()
	if exists {
		return cache
	}

	// as there is no cache, we start from scratch and collect all ancestors locally
	cache = make(map[string]struct{})
	var mu sync.Mutex
	if parents, ok := d.inboundEdge[id]; ok {

		// for each parent collect its ancestors
		for parent := range parents {
			parentAncestors := d.getAncestors(parent)
			mu.Lock()
			for ancestor := range parentAncestors {
				cache[ancestor] = struct{}{}
			}
			cache[parent] = struct{}{}
			mu.Unlock()
		}
	}

	// remember the collected descendents
	d.muCache.Lock()
	d.ancestorsCache[id] = cache
	d.muCache.Unlock()
	return cache
}

// GetOrderedAncestors returns all ancestors of the vertex with id id
// in a breath-first order. Only the first occurrence of each vertex is
// returned. GetOrderedAncestors returns an error, if id is empty or
// unknown.
//
// Note, there is no order between sibling vertices. Two consecutive runs of
// GetOrderedAncestors may return different results.
func (d *DAG[T]) GetOrderedAncestors(id string) ([]string, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	ids, _, err := d.AncestorsWalker(id)
	if err != nil {
		return nil, err
	}
	var ancestors []string
	for aid := range ids {
		ancestors = append(ancestors, aid)
	}
	return ancestors, nil
}

// AncestorsWalker returns a channel and subsequently returns / walks all
// ancestors of the vertex with id id in a breath first order. The second
// channel returned may be used to stop further walking. AncestorsWalker
// returns an error, if id is empty or unknown.
//
// Note, there is no order between sibling vertices. Two consecutive runs of
// AncestorsWalker may return different results.
func (d *DAG[T]) AncestorsWalker(id string) (chan string, chan bool, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return nil, nil, err
	}
	ids := make(chan string)
	signal := make(chan bool, 1)
	go func() {
		d.muDAG.RLock()
		d.walkAncestors(id, ids, signal)
		d.muDAG.RUnlock()
		close(ids)
		close(signal)
	}()
	return ids, signal, nil
}

func (d *DAG[T]) walkAncestors(id string, ids chan string, signal chan bool) {

	var fifo []string
	visited := make(map[string]struct{})
	for parent := range d.inboundEdge[id] {
		visited[parent] = struct{}{}
		fifo = append(fifo, parent)
	}
	for {
		if len(fifo) == 0 {
			return
		}
		top := fifo[0]
		fifo = fifo[1:]
		for parent := range d.inboundEdge[top] {
			if _, exists := visited[parent]; !exists {
				visited[parent] = struct{}{}
				fifo = append(fifo, parent)
			}
		}
		select {
		case <-signal:
			return
		default:
			ids <- top
		}
	}
}

// GetDescendants return all descendants of the vertex with id id.
// GetDescendants returns an error, if id is empty or unknown.
//
// Note, in order to get the descendants, GetDescendants populates the
// descendants-cache as needed. Depending on order and size of the sub-graph
// of the vertex with id id this may take a long time and consume a lot
// of memory.
func (d *DAG[T]) GetDescendants(id string) (map[string]T, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	if err := d.saneID(id); err != nil {
		return nil, err
	}
	v := d.vertexIds[id]
	vHash := d.hashVertex(v)

	descendants := make(map[string]T)
	for did := range d.getDescendants(vHash) {
		descendants[did] = d.vertexIds[did]
	}
	return descendants, nil
}

func (d *DAG[T]) getDescendants(id string) map[string]struct{} {

	// in the best case we have already a populated cache
	d.muCache.RLock()
	cache, exists := d.descendantsCache[id]
	d.muCache.RUnlock()
	if exists {
		return cache
	}

	// lock this vertex to work on it exclusively
	d.verticesLocked.lock(id)
	defer d.verticesLocked.unlock(id)

	// now as we have locked this vertex, check (again) that no one has
	// meanwhile populated the cache
	d.muCache.RLock()
	cache, exists = d.descendantsCache[id]
	d.muCache.RUnlock()
	if exists {
		return cache
	}

	// as there is no cache, we start from scratch and collect all descendants
	// locally
	cache = make(map[string]struct{})
	var mu sync.Mutex
	if children, ok := d.outboundEdge[id]; ok {

		// for each child use a goroutine to collect its descendants
		// var waitGroup sync.WaitGroup
		// waitGroup.Add(len(children))
		for child := range children {
			// go func(child T, mu *sync.Mutex, cache map[T]bool) {
			childDescendants := d.getDescendants(child)
			mu.Lock()
			for descendant := range childDescendants {
				cache[descendant] = struct{}{}
			}
			cache[child] = struct{}{}
			mu.Unlock()
			// waitGroup.Done()
			// }(child, &mu, cache)
		}
		// waitGroup.Wait()
	}

	// remember the collected descendents
	d.muCache.Lock()
	d.descendantsCache[id] = cache
	d.muCache.Unlock()
	return cache
}

// GetOrderedDescendants returns all descendants of the vertex with id id
// in a breath-first order. Only the first occurrence of each vertex is
// returned. GetOrderedDescendants returns an error, if id is empty or
// unknown.
//
// Note, there is no order between sibling vertices. Two consecutive runs of
// GetOrderedDescendants may return different results.
func (d *DAG[T]) GetOrderedDescendants(id string) ([]string, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	ids, _, err := d.DescendantsWalker(id)
	if err != nil {
		return nil, err
	}
	var descendants []string
	for did := range ids {
		descendants = append(descendants, did)
	}
	return descendants, nil
}

// GetDescendantsGraph returns a new DAG consisting of the vertex with id id and
// all its descendants (i.e. the subgraph). GetDescendantsGraph also returns the
// id of the (copy of the) given vertex within the new graph (i.e. the id of the
// single root of the new graph). GetDescendantsGraph returns an error, if id is
// empty or unknown.
//
// Note, the new graph is a copy of the relevant part of the original graph.
func (d *DAG[T]) GetDescendantsGraph(id string) (*DAG[T], error) {

	// recursively add the current vertex and all its descendants
	return d.getRelativesGraph(id, false)
}

// GetAncestorsGraph returns a new DAG consisting of the vertex with id id and
// all its ancestors (i.e. the subgraph). GetAncestorsGraph also returns the id
// of the (copy of the) given vertex within the new graph (i.e. the id of the
// single leaf of the new graph). GetAncestorsGraph returns an error, if id is
// empty or unknown.
//
// Note, the new graph is a copy of the relevant part of the original graph.
func (d *DAG[T]) GetAncestorsGraph(id string) (*DAG[T], error) {

	// recursively add the current vertex and all its ancestors
	return d.getRelativesGraph(id, true)
}

func (d *DAG[T]) getRelativesGraph(id string, asc bool) (*DAG[T], error) {
	// sanity checking
	if id == "" {
		return nil, IDEmptyError{}
	}
	v, exists := d.vertexIds[id]
	if !exists {
		return nil, IDUnknownError{id}
	}

	// create a new dag
	newDAG := NewDAG[T]()

	// protect the graph from modification
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	// recursively add the current vertex and all its relatives
	err := d.getRelativesGraphRec(v, newDAG, make(map[string]struct{}), asc)
	return newDAG, err
}

func (d *DAG[T]) getRelativesGraphRec(vertex T, newDAG *DAG[T], visited map[string]struct{}, asc bool) (err error) {

	// copy this vertex to the new graph
	if _, err = newDAG.AddVertex(vertex); err != nil {
		return
	}

	id := vertex.ID()

	// mark this vertex as visited
	visited[id] = struct{}{}

	// get the direct relatives (depending on the direction either parents or children)
	var relatives map[string]struct{}
	var ok bool
	if asc {
		relatives, ok = d.inboundEdge[id]
	} else {
		relatives, ok = d.outboundEdge[id]
	}

	// for all direct relatives in the original graph
	if ok {
		for relative := range relatives {

			// if we haven't seen this relative
			_, exists := visited[relative]
			if !exists {

				rel := d.vertexIds[relative]

				// recursively add this relative
				if err = d.getRelativesGraphRec(rel, newDAG, visited, asc); err != nil {
					return
				}
			}

			// add edge to this relative (depending on the direction)
			var srcID, dstID string
			if asc {
				srcID, dstID = relative, id

			} else {
				srcID, dstID = id, relative
			}
			if err = newDAG.AddEdge(srcID, dstID); err != nil {
				return
			}
		}
	}
	return
}

// DescendantsWalker returns a channel and subsequently returns / walks all
// descendants of the vertex with id in a breath first order. The second
// channel returned may be used to stop further walking. DescendantsWalker
// returns an error, if id is empty or unknown.
//
// Note, there is no order between sibling vertices. Two consecutive runs of
// DescendantsWalker may return different results.
func (d *DAG[T]) DescendantsWalker(id string) (chan string, chan bool, error) {
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()
	if err := d.saneID(id); err != nil {
		return nil, nil, err
	}
	ids := make(chan string)
	signal := make(chan bool, 1)
	go func() {
		d.muDAG.RLock()
		d.walkDescendants(id, ids, signal)
		d.muDAG.RUnlock()
		close(ids)
		close(signal)
	}()
	return ids, signal, nil
}

func (d *DAG[T]) walkDescendants(id string, ids chan string, signal chan bool) {
	var fifo []string
	visited := make(map[string]struct{})
	for child := range d.outboundEdge[id] {
		visited[child] = struct{}{}
		fifo = append(fifo, child)
	}
	for {
		if len(fifo) == 0 {
			return
		}
		top := fifo[0]
		fifo = fifo[1:]
		for child := range d.outboundEdge[top] {
			if _, exists := visited[child]; !exists {
				visited[child] = struct{}{}
				fifo = append(fifo, child)
			}
		}
		select {
		case <-signal:
			return
		default:
			ids <- top
		}
	}
}

// FlowResult describes the data to be passed between vertices in a DescendantsFlow.
type FlowResult struct {

	// The id of the vertex that produced this result.
	ID string

	// The actual result.
	Result interface{}

	// Any error. Note, DescendantsFlow does not care about this error. It is up to
	// the FlowCallback of downstream vertices to handle the error as needed - if
	// needed.
	Error error
}

// ReduceTransitively transitively reduce the graph.
//
// Note, in order to do the reduction the descendant-cache of all vertices is
// populated (i.e. the transitive closure). Depending on order and size of DAG
// this may take a long time and consume a lot of memory.
func (d *DAG[T]) ReduceTransitively() {

	d.muDAG.Lock()
	defer d.muDAG.Unlock()

	graphChanged := false

	// populate the descendents cache for all roots (i.e. the whole graph)
	for root := range d.getRoots() {
		_ = d.getDescendants(root)
	}

	// for each vertex
	for id := range d.vertexIds {

		// map of descendants of the children of v
		descendentsOfChildrenOfV := make(map[string]struct{})

		// for each child of v
		for childOfV := range d.outboundEdge[id] {

			// collect child descendants
			for descendent := range d.descendantsCache[childOfV] {
				descendentsOfChildrenOfV[descendent] = struct{}{}
			}
		}

		// for each child of v
		for childOfV := range d.outboundEdge[id] {

			// remove the edge between v and child, iff child is a
			// descendant of any of the children of v
			if _, exists := descendentsOfChildrenOfV[childOfV]; exists {
				delete(d.outboundEdge[id], childOfV)
				delete(d.inboundEdge[childOfV], id)
				graphChanged = true
			}
		}
	}

	// flush the descendants- and ancestor cache if the graph has changed
	if graphChanged {
		d.flushCaches()
	}
}

// FlushCaches completely flushes the descendants- and ancestor cache.
//
// Note, the only reason to call this method is to free up memory.
// Normally the caches are automatically maintained.
func (d *DAG[T]) FlushCaches() {
	d.muDAG.Lock()
	defer d.muDAG.Unlock()
	d.flushCaches()
}

func (d *DAG[T]) flushCaches() {
	d.ancestorsCache = make(map[string]map[string]struct{})
	d.descendantsCache = make(map[string]map[string]struct{})
}

// Copy returns a copy of the DAG.
func (d *DAG[T]) Copy() (newDAG *DAG[T], err error) {

	// create a new dag
	newDAG = NewDAG[T]()

	// create a map of visited vertices
	visited := make(map[string]struct{})

	// protect the graph from modification
	d.muDAG.RLock()
	defer d.muDAG.RUnlock()

	// add all roots and their descendants to the new DAG
	for _, root := range d.GetRoots() {
		if err = d.getRelativesGraphRec(root, newDAG, visited, false); err != nil {
			return
		}
	}
	return
}

// String returns a textual representation of the graph.
func (d *DAG[T]) String() string {
	result := fmt.Sprintf("DAG Vertices: %d - Edges: %d\n", d.GetOrder(), d.GetSize())
	result += "Vertices:\n"
	d.muDAG.RLock()
	for k := range d.vertexIds {
		result += fmt.Sprintf("  %v\n", k)
	}
	result += "Edges:\n"
	for v, children := range d.outboundEdge {
		for child := range children {
			result += fmt.Sprintf("  %v -> %v\n", v, child)
		}
	}
	d.muDAG.RUnlock()
	return result
}

func (d *DAG[T]) saneID(id string) error {
	// sanity checking
	if id == "" {
		return IDEmptyError{}
	}
	_, exists := d.vertexIds[id]
	if !exists {
		return IDUnknownError{id}
	}
	return nil
}

func (d *DAG[T]) hashVertex(v IDInterface) string {
	return v.ID()
}

func copyMap(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for id, value := range in {
		out[id] = value
	}
	return out
}

/***************************
********** Errors **********
****************************/

// VertexNilError is the error type to describe the situation, that a nil is
// given instead of a vertex.
type VertexNilError struct{}

// Implements the error interface.
func (e VertexNilError) Error() string {
	return "don't know what to do with 'nil'"
}

// IDDuplicateError is the error type to describe the situation, that a given
// vertex id already exists in the graph.
type IDDuplicateError struct {
	id string
}

// Implements the error interface.
func (e IDDuplicateError) Error() string {
	return fmt.Sprintf("the id '%s' is already known", e.id)
}

// IDEmptyError is the error type to describe the situation, that an empty
// string is given instead of a valid id.
type IDEmptyError struct{}

// Implements the error interface.
func (e IDEmptyError) Error() string {
	return "don't know what to do with \"\""
}

// IDUnknownError is the error type to describe the situation, that a given
// vertex does not exit in the graph.
type IDUnknownError struct {
	id string
}

// Implements the error interface.
func (e IDUnknownError) Error() string {
	return fmt.Sprintf("'%s' is unknown", e.id)
}

// EdgeDuplicateError is the error type to describe the situation, that an edge
// already exists in the graph.
type EdgeDuplicateError struct {
	src string
	dst string
}

// Implements the error interface.
func (e EdgeDuplicateError) Error() string {
	return fmt.Sprintf("edge between '%s' and '%s' is already known", e.src, e.dst)
}

// EdgeUnknownError is the error type to describe the situation, that a given
// edge does not exit in the graph.
type EdgeUnknownError struct {
	src string
	dst string
}

// Implements the error interface.
func (e EdgeUnknownError) Error() string {
	return fmt.Sprintf("edge between '%s' and '%s' is unknown", e.src, e.dst)
}

// EdgeLoopError is the error type to describe loop errors (i.e. errors that
// where raised to prevent establishing loops in the graph).
type EdgeLoopError struct {
	src string
	dst string
}

// Implements the error interface.
func (e EdgeLoopError) Error() string {
	return fmt.Sprintf("edge between '%s' and '%s' would create a loop", e.src, e.dst)
}

// SrcDstEqualError is the error type to describe the situation, that src and
// dst are equal.
type SrcDstEqualError struct {
	src string
	dst string
}

// Implements the error interface.
func (e SrcDstEqualError) Error() string {
	return fmt.Sprintf("src ('%s') and dst ('%s') equal", e.src, e.dst)
}

/***************************
********** dMutex **********
****************************/

type cMutex struct {
	mutex sync.Mutex
	count int
}

// Structure for dynamic mutexes.
type dMutex struct {
	mutexes     map[string]*cMutex
	globalMutex sync.Mutex
}

// Initialize a new dynamic mutex structure.
func newDMutex() *dMutex {
	return &dMutex{
		mutexes: make(map[string]*cMutex),
	}
}

// Get a lock for instance i
func (d *dMutex) lock(i string) {

	// acquire global lock
	d.globalMutex.Lock()

	// if there is no cMutex for i, create it
	if _, ok := d.mutexes[i]; !ok {
		d.mutexes[i] = new(cMutex)
	}

	// increase the count in order to show, that we are interested in this
	// instance mutex (thus now one deletes it)
	d.mutexes[i].count++

	// remember the mutex for later
	mutex := &d.mutexes[i].mutex

	// as the cMutex is there, we have increased the count, and we know the
	// instance mutex, we can release the global lock
	d.globalMutex.Unlock()

	// and wait on the instance mutex
	(*mutex).Lock()
}

// Release the lock for instance i.
func (d *dMutex) unlock(i string) {

	// acquire global lock
	d.globalMutex.Lock()

	// unlock instance mutex
	d.mutexes[i].mutex.Unlock()

	// decrease the count, as we are no longer interested in this instance
	// mutex
	d.mutexes[i].count--

	// if we are the last one interested in this instance mutex delete the
	// cMutex
	if d.mutexes[i].count == 0 {
		delete(d.mutexes, i)
	}

	// release the global lock
	d.globalMutex.Unlock()
}

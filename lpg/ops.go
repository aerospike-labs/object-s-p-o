package lpg

import (
	"sort"

	aero "github.com/aerospike/aerospike-client-go/v8"
)

// Record operations that merge into what is stored. Every write here can be
// repeated with the same result: map entries are inserted if new and merged
// if not, and counters are recomputed from the entries actually stored, so a
// retried batch, a reloaded file or a refreshed edge changes no count.

var (
	// insertPolicy adds map entries that are not there yet and leaves the
	// others for mergeOp.
	insertPolicy = aero.NewMapPolicyWithFlags(aero.MapOrder.KEY_ORDERED,
		aero.MapWriteFlagsCreateOnly|aero.MapWriteFlagsNoFail|aero.MapWriteFlagsPartial)
	updatePolicy = aero.NewMapPolicy(aero.MapOrder.KEY_ORDERED, aero.MapWriteMode.UPDATE)
	listPolicy   = aero.DefaultListPolicy()
)

func strCtx(key string) *aero.CDTContext { return aero.CtxMapKey(aero.NewStringValue(key)) }

// hasKey is true when map bin holds key at its top level.
func hasKey(bin, key string) *aero.Expression {
	return aero.ExpAnd(aero.ExpBinExists(bin),
		aero.ExpMapGetByKey(aero.MapReturnType.EXISTS, aero.ExpTypeBOOL, aero.ExpStringVal(key), aero.ExpMapBin(bin)))
}

// intOr0 is an integer bin's value, or 0 when it is absent.
func intOr0(bin string) *aero.Expression {
	return aero.ExpCond(aero.ExpBinExists(bin), aero.ExpIntBin(bin), aero.ExpIntVal(0))
}

// entries is the number of entries under outer keys in a {outer: {key: value}}
// map bin.
func entries(bin string, outer []string) *aero.Expression {
	terms := []*aero.Expression{aero.ExpIntVal(0)}
	for _, o := range outer {
		terms = append(terms, aero.ExpCond(hasKey(bin, o), aero.ExpMapSize(aero.ExpMapBin(bin), strCtx(o)), aero.ExpIntVal(0)))
	}
	if len(terms) == 1 {
		return terms[0]
	}
	return aero.ExpNumAdd(terms...)
}

// counter is a count bin kept equal, by withCounters, to the number of
// entries counted by n.
type counter struct {
	bin string
	n   *aero.Expression
}

// withCounters wraps ops so that each counter's bin grows by exactly the
// entries the ops add: it subtracts the entries before them and adds the
// entries after.
func withCounters(ops []*aero.Operation, cs ...counter) []*aero.Operation {
	out := make([]*aero.Operation, 0, len(ops)+2*len(cs))
	for _, c := range cs {
		out = append(out, aero.ExpWriteOp(c.bin, aero.ExpNumSub(intOr0(c.bin), c.n), aero.ExpWriteFlagDefault))
	}
	out = append(out, ops...)
	for _, c := range cs {
		out = append(out, aero.ExpWriteOp(c.bin, aero.ExpNumAdd(aero.ExpIntBin(c.bin), c.n), aero.ExpWriteFlagDefault))
	}
	return out
}

// mergeOp merges a [created, last_seen, ...] tuple into the stored tuple at
// bin[outer][key], which must exist: created becomes the earlier of the two
// and last_seen the later, a zero meaning unknown, and rest, when given,
// updates what follows them in the stored tuple t.
func mergeOp(bin, outer, key string, created, lastSeen int64, rest func(merged, t *aero.Expression) *aero.Expression) *aero.Operation {
	t := aero.ExpVar("t")
	at := func(i int64) *aero.Expression {
		return aero.ExpListGetByIndex(aero.ListReturnTypeValue, aero.ExpTypeINT, aero.ExpIntVal(i), t)
	}
	merged := t
	if created != 0 {
		c := aero.ExpIntVal(created)
		merged = aero.ExpListSet(listPolicy, aero.ExpIntVal(0),
			aero.ExpCond(aero.ExpEq(at(0), aero.ExpIntVal(0)), c, aero.ExpMin(at(0), c)), merged)
	}
	if lastSeen != 0 {
		merged = aero.ExpListSet(listPolicy, aero.ExpIntVal(1), aero.ExpMax(at(1), aero.ExpIntVal(lastSeen)), merged)
	}
	if rest != nil {
		merged = rest(merged, t)
	}
	ctx := strCtx(outer)
	return aero.ExpWriteOp(bin, aero.ExpLet(
		aero.ExpDef("t", aero.ExpMapGetByKey(aero.MapReturnType.VALUE, aero.ExpTypeLIST, aero.ExpStringVal(key), aero.ExpMapBin(bin), ctx)),
		aero.ExpMapPut(updatePolicy, aero.ExpStringVal(key), merged, aero.ExpMapBin(bin), ctx)), aero.ExpWriteFlagDefault)
}

// edgeOps returns the operations adding edges to a {predicate: {neighbor:
// tuple}} map bin. A new edge is inserted; an edge already there is merged:
// the earlier created, the later last_seen, and its props with these added.
func edgeOps(bin string, edges map[string]map[string]Edge) []*aero.Operation {
	var ops []*aero.Operation
	for _, pred := range sortedKeys(edges) {
		nbrs := edges[pred]
		items := make(map[interface{}]interface{}, len(nbrs))
		for n, e := range nbrs {
			items[n] = e.tuple()
		}
		ops = append(ops, aero.MapPutItemsOp(insertPolicy, bin, items,
			aero.CtxMapKeyCreate(aero.NewStringValue(pred), aero.MapOrder.KEY_ORDERED)))
		for _, n := range sortedKeys(nbrs) {
			e := nbrs[n]
			var rest func(merged, t *aero.Expression) *aero.Expression
			if len(e.Props) > 0 {
				props := make(aero.MapValue, len(e.Props))
				for k, v := range e.Props {
					props[k] = v
				}
				rest = func(merged, t *aero.Expression) *aero.Expression {
					old := aero.ExpCond(aero.ExpGreater(aero.ExpListSize(t), aero.ExpIntVal(2)),
						aero.ExpListGetByIndex(aero.ListReturnTypeValue, aero.ExpTypeMAP, aero.ExpIntVal(2), t),
						aero.ExpMapVal(aero.MapValue{}))
					return aero.ExpListSet(listPolicy, aero.ExpIntVal(2), aero.ExpMapPutItems(updatePolicy, aero.ExpMapVal(props), old), merged)
				}
			}
			ops = append(ops, mergeOp(bin, pred, n, e.Created, e.LastSeen, rest))
		}
	}
	return ops
}

// identifierOps returns the operations adding identifiers to the
// {type: {value: [created, last_seen, decay]}} bin, merging those already
// there, with the decay of these.
func identifierOps(ids map[string]map[string]Identifier) []*aero.Operation {
	var ops []*aero.Operation
	for _, idType := range sortedKeys(ids) {
		vals := ids[idType]
		items := make(map[interface{}]interface{}, len(vals))
		for v, id := range vals {
			items[v] = []interface{}{id.Created, id.LastSeen, id.Decay}
		}
		ops = append(ops, aero.MapPutItemsOp(insertPolicy, binIDs, items,
			aero.CtxMapKeyCreate(aero.NewStringValue(idType), aero.MapOrder.KEY_ORDERED)))
		for _, v := range sortedKeys(vals) {
			decay := vals[v].Decay
			ops = append(ops, mergeOp(binIDs, idType, v, vals[v].Created, vals[v].LastSeen,
				func(merged, _ *aero.Expression) *aero.Expression {
					return aero.ExpListSet(listPolicy, aero.ExpIntVal(2), aero.ExpFloatVal(decay), merged)
				}))
		}
	}
	return ops
}

// typeOps returns the operations setting a vertex's type, if it has none
// yet, and its creation time, if earlier than the stored one.
func typeOps(m *Mutation) []*aero.Operation {
	var ops []*aero.Operation
	if m.Type != "" {
		ops = append(ops, aero.ExpWriteOp(binType,
			aero.ExpCond(aero.ExpBinExists(binType), aero.ExpStringBin(binType), aero.ExpStringVal(m.Type)), aero.ExpWriteFlagDefault))
	}
	if m.Created != 0 {
		c := aero.ExpIntVal(m.Created)
		ops = append(ops, aero.ExpWriteOp(binCreated, aero.ExpCond(
			aero.ExpAnd(aero.ExpBinExists(binCreated), aero.ExpNotEq(aero.ExpIntBin(binCreated), aero.ExpIntVal(0))),
			aero.ExpMin(aero.ExpIntBin(binCreated), c), c), aero.ExpWriteFlagDefault))
	}
	return ops
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// keysIn reads which of keys bin[outer] holds, as a list, empty when bin or
// outer is absent.
func keysIn(bin, outer string, keys []string) *aero.Expression {
	vals := make([]aero.Value, len(keys))
	for i, k := range keys {
		vals[i] = aero.NewStringValue(k)
	}
	return aero.ExpCond(hasKey(bin, outer),
		aero.ExpMapGetByKeyList(aero.MapReturnType.KEY, aero.ExpListVal(vals...), aero.ExpMapBin(bin), strCtx(outer)),
		aero.ExpListVal())
}

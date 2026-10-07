package store

import (
	aero "github.com/aerospike/aerospike-client-go/v8"
)

// EntitySetName holds one record per shareable entity: its type and how many
// INDIVIDUALs link to it. Generators use it to find entities to share without
// scanning the triples set.
const EntitySetName = "entities"

// PartitionCount is the number of Aerospike partitions in a namespace.
const PartitionCount = 4096

// EntityLinks is a shareable entity and its INDIVIDUAL link count.
type EntityLinks struct {
	ID    string
	Type  string
	Links int
}

func (gs *GraphStore) entityKey(id string) (*aero.Key, error) {
	return aero.NewKey(gs.Namespace, EntitySetName, id)
}

// BatchAddEntityLinks atomically adds each entry's Links to the stored count,
// creating the record if needed. Additive, so concurrent loads never lose
// updates.
func (gs *GraphStore) BatchAddEntityLinks(entities []EntityLinks) error {
	return gs.batchWriteEntities(entities, func(e EntityLinks) *aero.Operation {
		return aero.AddOp(aero.NewBin("links", e.Links))
	})
}

// BatchPutEntityLinks overwrites each entity's stored count with Links.
func (gs *GraphStore) BatchPutEntityLinks(entities []EntityLinks) error {
	return gs.batchWriteEntities(entities, func(e EntityLinks) *aero.Operation {
		return aero.PutOp(aero.NewBin("links", e.Links))
	})
}

func (gs *GraphStore) batchWriteEntities(entities []EntityLinks, linksOp func(EntityLinks) *aero.Operation) error {
	if len(entities) == 0 {
		return nil
	}
	bwp := aero.NewBatchWritePolicy()
	bwp.RecordExistsAction = aero.UPDATE
	records := make([]aero.BatchRecordIfc, 0, len(entities))
	for _, e := range entities {
		key, err := gs.entityKey(e.ID)
		if err != nil {
			return err
		}
		records = append(records, aero.NewBatchWrite(bwp, key,
			aero.PutOp(aero.NewBin("id", e.ID)),
			aero.PutOp(aero.NewBin("type", e.Type)),
			linksOp(e),
		))
	}
	if err := gs.Client.BatchOperate(gs.BatchPolicy, records); err != nil {
		return err
	}
	return batchRecordErrors(records)
}

// ScanEntities streams the entity records in partitions
// [begin, begin+count) to fn. Splitting the 4096 partitions between hosts
// gives each host a disjoint set of entities.
func (gs *GraphStore) ScanEntities(begin, count int, fn func(EntityLinks) error) error {
	sp := aero.NewScanPolicy()
	rs, err := gs.Client.ScanPartitions(sp, aero.NewPartitionFilterByRange(begin, count),
		gs.Namespace, EntitySetName, "id", "type", "links")
	if err != nil {
		return err
	}
	defer rs.Close()

	for r := range rs.Results() {
		if r.Err != nil {
			return r.Err
		}
		e := EntityLinks{}
		e.ID, _ = r.Record.Bins["id"].(string)
		e.Type, _ = r.Record.Bins["type"].(string)
		e.Links, _ = r.Record.Bins["links"].(int)
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

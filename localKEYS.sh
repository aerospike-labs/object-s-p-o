#!/bin/bash


asadm -e 'enable; manage sindex delete idx_identity_match ns test set identity_identifiers bin match_key'
asadm -e 'enable; manage sindex delete idx_identity_orphan ns test set identity_identifiers bin orphan'
asadm -e 'enable; manage sindex delete idx_entity_type ns test set identity_identifiers bin entity_type'
asadm -e 'enable; manage sindex delete idx_edge_from_entity ns test set identity_identifiers bin from_entity_id'
asadm -e 'enable; manage sindex delete idx_edge_to_entity ns test set identity_identifiers bin to_entity_id'

asadm -e 'enable; manage sindex create string idx_identity_match ns test set identity_identifiers bin match_key'
asadm -e 'enable; manage sindex create numeric idx_identity_orphan ns test set identity_identifiers bin orphan'
asadm -e 'enable; manage sindex create string idx_entity_type ns test set identity_entities bin entity_type'
asadm -e 'enable; manage sindex create string idx_edge_from_entity ns test set identity_edges bin from_entity_id'
asadm -e 'enable; manage sindex create string idx_edge_to_entity ns test set identity_edges bin to_entity_id'

#asinfo -v 'sindex-create:ns=test;set=identity_identifiers;indexname=idx_identity_match_key;bin=match_key;bin-type=string'
#asinfo -v 'sindex-create:ns=test;set=identity_identifiers;indexname=idx_identity_orphan;bin=orphan;bin-type=numeric'
#asinfo -v 'sindex-create:ns=test;set=identity_entities;indexname=idx_entity_type;bin=entity_type;bin-type=string'
#asinfo -v 'sindex-create:ns=test;set=identity_edges;indexname=idx_edge_from_entity;bin=from_entity_id;bin-type=string'
#asinfo -v 'sindex-create:ns=test;set=identity_edges;indexname=idx_edge_to_entity;bin=to_entity_id;bin-type=string'

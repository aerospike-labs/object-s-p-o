#!/bin/bash

clusterIP=$(clusterPublicIp.sh)

asadm -h ${clusterIP} -e 'enable; manage sindex delete idx_identity_match ns test set identity_identifiers bin match_key'
asadm -h ${clusterIP} -e 'enable; manage sindex delete idx_identity_orphan ns test set identity_identifiers bin orphan'
asadm -h ${clusterIP} -e 'enable; manage sindex delete idx_entity_type ns test set identity_identifiers bin entity_type'
asadm -h ${clusterIP} -e 'enable; manage sindex delete idx_edge_from_entity ns test set identity_identifiers bin from_entity_id'
asadm -h ${clusterIP} -e 'enable; manage sindex delete idx_edge_to_entity ns test set identity_identifiers bin to_entity_id'

asadm -h ${clusterIP} -e 'enable; manage sindex create string idx_identity_match ns test set identity_identifiers bin match_key'
asadm -h ${clusterIP} -e 'enable; manage sindex create numeric idx_identity_orphan ns test set identity_identifiers bin orphan'
asadm -h ${clusterIP} -e 'enable; manage sindex create string idx_entity_type ns test set identity_identifiers bin entity_type'
asadm -h ${clusterIP} -e 'enable; manage sindex create string idx_edge_from_entity ns test set identity_identifiers bin from_entity_id'
asadm -h ${clusterIP} -e 'enable; manage sindex create string idx_edge_to_entity ns test set identity_identifiers bin to_entity_id'

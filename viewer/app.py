#!/usr/bin/env python3
"""
Interactive RDF Property Graph Viewer.

Connects to Aerospike and provides a web-based interactive graph visualization
with double-click expansion, predicate filtering, hop limits, and property inspection.

Usage:
    python app.py --host 127.0.0.1 --port 3000 --namespace test
"""

import argparse
import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor

import aerospike
from aerospike_helpers import expressions as exp
import dash
from dash import html, dcc, callback_context
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_cytoscape as cyto

# ---------------------------------------------------------------------------
# Aerospike helpers
# ---------------------------------------------------------------------------

SETNAME = "triples"

# Max node suggestions sent to the browser per search. The full node list can be
# hundreds of thousands of entries, which freezes the page if sent as options.
MAX_NODE_OPTIONS = 50
MIN_SEARCH_CHARS = 2

# Nodes with more edges than this (after predicate/direction filtering) are
# hubs: shown with their degree but not expanded, so a shared node such as a
# type doesn't pull thousands of neighbours into the graph. The start node is
# always expanded.
MAX_EXPAND_DEGREE = 10
# Max edges listed per direction in the info panel.
MAX_INFO_EDGES = 50
# Max edges fetched per direction for one node. Hub degrees above this are
# shown as "1000+" instead of fetching every edge just to count them.
HUB_COUNT_CAP = 1000
# Concurrent queries per BFS level; hides round-trip latency to remote clusters.
QUERY_THREADS = 16
# Triples sampled for node search and the predicate list, instead of scanning
# the whole set. Any full node ID can still be found by typing it exactly.
DEFAULT_SAMPLE_SIZE = 100000

# Hashed content (sha-256 or sha-512 hex) is displayed shortened to this many
# characters plus an ellipsis; the full value is still used for queries.
HASH_DISPLAY_CHARS = 20
HASH_RE = re.compile(r"[0-9a-f]{64}|[0-9a-f]{128}")


DEFAULT_ONTOLOGY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "descriptors", "ontology.json")


def load_ontology(path):
    """Relationship predicates and entity types from the ontology descriptor.

    These are never hashed, so navigating by them works with any hash type.
    """
    with open(path) as f:
        rules = json.load(f)["ontology"]
    predicates = sorted({r["PRED"] for r in rules})
    types = sorted({r["SUB"] for r in rules} | {r["OBJ"] for r in rules})
    return predicates, types


def short(value):
    """Shorten hashed content for display; other values are unchanged."""
    if isinstance(value, str) and HASH_RE.fullmatch(value):
        return value[:HASH_DISPLAY_CHARS] + "\u2026"
    return value


def short_props(props):
    """Props with hashed string values shortened, as JSON for display."""
    return json.dumps({k: short(v) for k, v in props.items()}, default=str, ensure_ascii=False)


def parse_hosts(hosts, default_port, tls_name=None):
    """Parse "host[:port],host[:port]" into Aerospike client host tuples."""
    seeds = []
    for h in hosts.split(","):
        h = h.strip()
        if not h:
            continue
        name, _, port = h.rpartition(":") if ":" in h else (h, "", "")
        seed = (name, int(port) if port else default_port)
        if tls_name:
            seed += (tls_name,)
        seeds.append(seed)
    return seeds


def connect_aerospike(host, port, namespace, hosts=None, user=None, password=None,
                      tls_name=None, tls_cafile=None, alternate_access=False):
    """Connect to a local or remote cluster; hosts overrides host/port."""
    config = {
        "hosts": parse_hosts(hosts or f"{host}:{port}", port, tls_name),
        "use_services_alternate": alternate_access,
    }
    if user:
        config["user"] = user
        config["password"] = password or os.environ.get("AEROSPIKE_PASSWORD", "")
    if tls_name or tls_cafile:
        config["tls"] = {"enable": True}
        if tls_cafile:
            config["tls"]["cafile"] = tls_cafile
    client = aerospike.client(config).connect()
    return client, namespace


def triple_key(s, p, o):
    h = hashlib.sha256()
    h.update(s.encode())
    h.update(b"\x00")
    h.update(p.encode())
    h.update(b"\x00")
    h.update(o.encode())
    return h.hexdigest()


def predicate_filter(predicates):
    """Server-side filter keeping only triples with one of the predicates."""
    if not predicates:
        return None
    terms = [exp.Eq(exp.StrBin("predicate"), p) for p in predicates]
    return (terms[0] if len(terms) == 1 else exp.Or(*terms)).compile()


def query_by_bin(client, namespace, bin_name, value, predicates=None, limit=None):
    """Secondary index query on a single bin, optionally filtered by predicate
    on the server and capped at limit records."""
    query = client.query(namespace, SETNAME)
    query.where(aerospike.predicates.equals(bin_name, value))
    if limit:
        query.max_records = limit
    policy = {}
    flt = predicate_filter(predicates)
    if flt is not None:
        policy["expressions"] = flt
    return [bins for _, _, bins in query.results(policy=policy)]


def sample_graph(client, namespace, sample_size):
    """Scan up to sample_size triples for node IDs and predicates."""
    scan = client.scan(namespace, SETNAME)
    scan.select("subject", "predicate", "object")
    nodes, predicates = set(), set()
    for _, _, bins in scan.results(policy={"max_records": sample_size}):
        nodes.add(bins.get("subject", ""))
        nodes.add(bins.get("object", ""))
        predicates.add(bins.get("predicate", ""))
    nodes.discard("")
    predicates.discard("")
    return sorted(nodes), sorted(predicates)


def node_exists(client, namespace, node):
    """Whether any triple has node as its subject or object."""
    return any(query_by_bin(client, namespace, b, node, limit=1) for b in ("subject", "object"))


def expand_node(client, namespace, node, predicates=None, direction="both", limit=HUB_COUNT_CAP):
    """Get triples connected to a node, filtered by predicates and direction.

    Returns (triples, truncated): at most limit triples per direction, and
    whether either direction hit the limit.
    """
    triples, truncated = [], False
    bins = {"both": ("subject", "object"), "outbound": ("subject",), "inbound": ("object",)}[direction]
    for b in bins:
        found = query_by_bin(client, namespace, b, node, predicates, limit)
        truncated |= len(found) >= limit
        triples.extend(found)
    return triples, truncated


def degree_label(triples, truncated):
    return f"{len(triples)}+" if truncated else str(len(triples))


def bfs_expand(client, namespace, start_node, max_hops, predicates=None, direction="both"):
    """BFS traversal from start_node up to max_hops.

    Returns (triples, hubs) where hubs maps each node that was not expanded
    because its degree exceeds MAX_EXPAND_DEGREE to its degree label. Each
    level's nodes are queried concurrently.
    """
    hubs = {}
    visited_nodes = {start_node}
    visited_edges = set()
    all_triples = []
    current_level = [start_node]

    with ThreadPoolExecutor(QUERY_THREADS) as pool:
        for _ in range(max_hops):
            if not current_level:
                break
            results = pool.map(
                lambda n: expand_node(client, namespace, n, predicates, direction), current_level)
            next_level = []
            for node, (triples, truncated) in zip(current_level, results):
                if node != start_node and len(triples) > MAX_EXPAND_DEGREE:
                    hubs[node] = degree_label(triples, truncated)
                    continue
                for t in triples:
                    s, p, o = t["subject"], t["predicate"], t["object"]
                    edge_key = (s, p, o)
                    if edge_key not in visited_edges:
                        visited_edges.add(edge_key)
                        all_triples.append(t)
                        for n in (s, o):
                            if n not in visited_nodes:
                                visited_nodes.add(n)
                                next_level.append(n)
            current_level = next_level

    return all_triples, hubs


# ---------------------------------------------------------------------------
# Graph element builders
# ---------------------------------------------------------------------------

NODE_COLORS = {
    "INDIVIDUAL": "#4FC3F7",
    "ACCOUNT": "#81C784",
    "HOUSEHOLD": "#FFB74D",
    "ADDRESS": "#CE93D8",
    "CREDIT_DEVICE": "#EF5350",
    "DISPLAY_DEVICE": "#FFD54F",
    "user": "#4FC3F7",
    "post": "#81C784",
    "topic": "#FFB74D",
    "default": "#B0BEC5",
}


def node_type(node_id):
    if ":" in node_id:
        return node_id.split(":")[0]
    return "default"


def build_elements(triples):
    """Convert triples to Cytoscape elements (nodes + edges)."""
    nodes = {}
    edges = []

    for t in triples:
        s, p, o = t["subject"], t["predicate"], t["object"]
        props = t.get("props", {})
        if isinstance(props, bytes):
            props = {}

        for nid in (s, o):
            if nid not in nodes:
                nt = node_type(nid)
                nodes[nid] = {
                    "data": {
                        "id": nid,
                        "label": short(nid),
                        "type": nt,
                        "color": NODE_COLORS.get(nt, NODE_COLORS["default"]),
                    }
                }

        edge_id = f"{s}|{p}|{o}"
        props_str = json.dumps(props, default=str) if props else "{}"
        edges.append({
            "data": {
                "id": edge_id,
                "source": s,
                "target": o,
                "label": short(p),
                "props": props_str,
            }
        })

    return list(nodes.values()) + edges


# ---------------------------------------------------------------------------
# Dash app
# ---------------------------------------------------------------------------

def create_app(client, namespace, sample_size=DEFAULT_SAMPLE_SIZE, ontology_path=DEFAULT_ONTOLOGY):
    ontology_preds, ontology_types = load_ontology(ontology_path)

    def effective_predicates(pred_filter, ontology_on):
        """Predicates to traverse: the selected ones, limited to the ontology's
        relationships (all of them if none selected) when ontology_on."""
        if ontology_on:
            chosen = [p for p in (pred_filter or []) if p in ontology_preds]
            return chosen or ontology_preds
        return pred_filter or None

    app = dash.Dash(__name__)

    cyto.load_extra_layouts()

    app.layout = html.Div([
        # Header
        html.Div([
            html.H2("RDF Property Graph Viewer",
                     style={"margin": "0", "color": "#fff"}),
            html.Span("Double-click a node to expand its relationships",
                       style={"color": "#B0BEC5", "fontSize": "14px"}),
        ], style={
            "background": "#263238", "padding": "16px 24px",
            "display": "flex", "justifyContent": "space-between", "alignItems": "center",
        }),

        html.Div([
            # Left panel - controls
            html.Div([
                html.H4("Controls", style={"marginTop": "0"}),

                html.Button("Refresh Data", id="refresh-btn", n_clicks=0,
                             style={
                                 "width": "100%", "padding": "8px",
                                 "background": "#78909C", "color": "#fff",
                                 "border": "none", "borderRadius": "4px",
                                 "cursor": "pointer", "fontSize": "13px",
                                 "marginBottom": "12px",
                             }),
                html.Div(id="refresh-status", style={"fontSize": "12px", "color": "#78909C", "marginBottom": "8px"}),

                html.Label("Start Node"),
                dcc.Dropdown(
                    id="start-node",
                    options=[],
                    placeholder="Type to search nodes...",
                    searchable=True,
                    style={"marginBottom": "12px"},
                ),

                html.Label("Max Hops"),
                dcc.Slider(
                    id="max-hops",
                    min=1, max=6, step=1, value=2,
                    marks={i: str(i) for i in range(1, 7)},
                ),

                html.Label("Predicate Filter", style={"marginTop": "12px"}),
                dcc.Checklist(
                    id="ontology-only",
                    options=[{"label": " Ontology relationships only", "value": "on"}],
                    value=["on"],
                    style={"fontSize": "13px", "margin": "4px 0"},
                ),
                dcc.Dropdown(
                    id="predicate-filter",
                    options=[],
                    multi=True,
                    placeholder="All predicates",
                    style={"marginBottom": "12px"},
                ),

                html.Label("Direction"),
                dcc.RadioItems(
                    id="direction",
                    options=[
                        {"label": "Both", "value": "both"},
                        {"label": "Outbound", "value": "outbound"},
                        {"label": "Inbound", "value": "inbound"},
                    ],
                    value="both",
                    style={"marginBottom": "12px"},
                ),

                html.Label("Layout"),
                dcc.Dropdown(
                    id="layout-select",
                    options=[
                        {"label": "Cola (force-directed)", "value": "cola"},
                        {"label": "Dagre (hierarchical)", "value": "dagre"},
                        {"label": "Breadthfirst", "value": "breadthfirst"},
                        {"label": "Circle", "value": "circle"},
                        {"label": "Concentric", "value": "concentric"},
                        {"label": "Grid", "value": "grid"},
                    ],
                    value="cola",
                    clearable=False,
                    style={"marginBottom": "16px"},
                ),

                html.Button("Explore", id="explore-btn", n_clicks=0,
                             style={
                                 "width": "100%", "padding": "10px",
                                 "background": "#4FC3F7", "color": "#fff",
                                 "border": "none", "borderRadius": "4px",
                                 "cursor": "pointer", "fontSize": "16px",
                                 "marginBottom": "16px",
                             }),

                html.Button("Clear Graph", id="clear-btn", n_clicks=0,
                             style={
                                 "width": "100%", "padding": "10px",
                                 "background": "#EF5350", "color": "#fff",
                                 "border": "none", "borderRadius": "4px",
                                 "cursor": "pointer", "fontSize": "14px",
                                 "marginBottom": "16px",
                             }),

                html.Hr(),

                # Node/Edge info panel
                html.H4("Selected Element"),
                html.Div(id="info-panel",
                         children="Click a node or edge to see details.",
                         style={
                             "background": "#f5f5f5", "padding": "12px",
                             "borderRadius": "4px", "fontSize": "13px",
                             "whiteSpace": "pre-wrap", "maxHeight": "300px",
                             "overflowY": "auto",
                         }),

                html.Hr(),

                # Stats
                html.H4("Graph Stats"),
                html.Div(id="stats-panel", style={"fontSize": "13px"}),

            ], style={
                "width": "300px", "padding": "16px",
                "borderRight": "1px solid #e0e0e0",
                "overflowY": "auto", "height": "calc(100vh - 60px)",
            }),

            # Right panel - graph
            html.Div([
                cyto.Cytoscape(
                    id="graph",
                    elements=[],
                    layout={"name": "cola", "animate": True, "maxSimulationTime": 2000},
                    style={"width": "100%", "height": "calc(100vh - 60px)"},
                    stylesheet=[
                        # Node styles
                        {
                            "selector": "node",
                            "style": {
                                "label": "data(label)",
                                "background-color": "data(color)",
                                "color": "#263238",
                                "font-size": "12px",
                                "text-valign": "bottom",
                                "text-margin-y": "8px",
                                "width": "40px",
                                "height": "40px",
                                "border-width": "2px",
                                "border-color": "#455A64",
                            },
                        },
                        # Hub nodes (degree too high to expand)
                        {
                            "selector": ".hub",
                            "style": {
                                "border-style": "dashed",
                                "border-width": "4px",
                                "border-color": "#D84315",
                                "width": "56px",
                                "height": "56px",
                            },
                        },
                        # Expanded node highlight
                        {
                            "selector": "node:selected",
                            "style": {
                                "border-width": "4px",
                                "border-color": "#FF7043",
                                "width": "50px",
                                "height": "50px",
                            },
                        },
                        # Edge styles
                        {
                            "selector": "edge",
                            "style": {
                                "label": "data(label)",
                                "curve-style": "bezier",
                                "target-arrow-shape": "triangle",
                                "target-arrow-color": "#78909C",
                                "line-color": "#B0BEC5",
                                "font-size": "10px",
                                "color": "#546E7A",
                                "text-rotation": "autorotate",
                                "text-margin-y": "-10px",
                                "width": 2,
                                "arrow-scale": 1.2,
                            },
                        },
                        {
                            "selector": "edge:selected",
                            "style": {
                                "line-color": "#FF7043",
                                "target-arrow-color": "#FF7043",
                                "width": 3,
                            },
                        },
                    ],
                ),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "height": "calc(100vh - 60px)"}),

        # Hidden stores
        dcc.Store(id="graph-data", data={"elements": [], "expanded": [], "hubs": {}}),
        dcc.Store(id="sampled-predicates", data=[]),

    ], style={"fontFamily": "system-ui, sans-serif", "margin": "0"})

    # --- Callbacks ---

    # Sampled node IDs are cached server-side; the dropdown is fed via search below.
    node_cache = []

    @app.callback(
        [Output("sampled-predicates", "data"),
         Output("refresh-status", "children")],
        Input("refresh-btn", "n_clicks"),
        running=[(Output("refresh-btn", "disabled"), True, False),
                 (Output("refresh-status", "children"), f"Sampling up to {sample_size:,} triples...", "")],
    )
    def refresh_dropdowns(n_clicks):
        nodes, predicates = sample_graph(client, namespace, sample_size)
        node_cache[:] = nodes
        status = (f"Sampled {len(nodes):,} nodes, {len(predicates)} predicates. "
                  "Type a full node ID to find any node.")
        return predicates, status

    @app.callback(
        [Output("predicate-filter", "options"),
         Output("predicate-filter", "placeholder")],
        [Input("sampled-predicates", "data"),
         Input("ontology-only", "value")],
    )
    def predicate_options(sampled, ontology_on):
        if ontology_on:
            return ([{"label": p, "value": p} for p in ontology_preds],
                    "All ontology relationships")
        return [{"label": short(p), "value": p} for p in sampled or []], "All predicates"

    @app.callback(
        Output("start-node", "options"),
        Input("start-node", "search_value"),
        [State("start-node", "value"),
         State("ontology-only", "value")],
    )
    def search_nodes(search, selected, ontology_on):
        if not search or len(search) < MIN_SEARCH_CHARS:
            if selected:
                return [{"label": short(selected), "value": selected}]
            raise PreventUpdate
        # In ontology mode, offer only entities of the ontology's types
        wanted = (lambda n: node_type(n) in ontology_types) if ontology_on else (lambda n: True)
        needle = search.lower()
        matches = []
        for n in node_cache:
            if needle in n.lower() and wanted(n):
                matches.append({"label": short(n), "value": n})
                if len(matches) >= MAX_NODE_OPTIONS:
                    break
        # Nodes outside the sample are found by exact ID
        exact = search.strip()
        if (all(m["value"] != exact for m in matches) and wanted(exact)
                and node_exists(client, namespace, exact)):
            matches.insert(0, {"label": short(exact), "value": exact})
        return matches

    @app.callback(
        Output("graph-data", "data"),
        [Input("explore-btn", "n_clicks"),
         Input("clear-btn", "n_clicks"),
         Input("graph", "tapNodeData"),
         Input("max-hops", "value"),
         Input("predicate-filter", "value"),
         Input("direction", "value"),
         Input("ontology-only", "value")],
        [State("start-node", "value"),
         State("graph-data", "data"),
         State("graph", "tapNode")],
        prevent_initial_call=True,
    )
    def update_graph_data(explore_clicks, clear_clicks, tap_data,
                          max_hops, pred_filter, direction, ontology_on,
                          start_node, current_data, tap_node):
        ctx = callback_context
        if not ctx.triggered:
            return current_data

        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if trigger_id == "clear-btn":
            return {"elements": [], "expanded": [], "hubs": {}}

        # Changing a query control rebuilds the current graph with the new
        # settings; expanding with tap can only add edges, never remove them.
        if trigger_id in ("max-hops", "predicate-filter", "direction", "ontology-only"):
            if not start_node or not current_data.get("elements"):
                return current_data
            trigger_id = "explore-btn"

        if trigger_id == "explore-btn":
            if not start_node:
                return current_data
            preds = effective_predicates(pred_filter, ontology_on)
            triples, hubs = bfs_expand(client, namespace, start_node, max_hops, preds, direction)
            elements = build_elements(triples)
            return {"elements": elements, "expanded": [start_node], "hubs": hubs}

        if trigger_id == "graph" and tap_data:
            # Double-click detection via tap - expand the node
            node_id = tap_data["id"]
            expanded = current_data.get("expanded", [])

            if node_id in expanded:
                return current_data

            expanded.append(node_id)
            preds = effective_predicates(pred_filter, ontology_on)
            new_triples, truncated = expand_node(client, namespace, node_id, preds, direction)
            hubs = current_data.get("hubs", {})
            if node_id != start_node and len(new_triples) > MAX_EXPAND_DEGREE:
                hubs[node_id] = degree_label(new_triples, truncated)
                return {**current_data, "expanded": expanded, "hubs": hubs}
            new_elements = build_elements(new_triples)

            existing = current_data.get("elements", [])
            existing_ids = {e["data"]["id"] for e in existing}
            for el in new_elements:
                if el["data"]["id"] not in existing_ids:
                    existing.append(el)

            return {"elements": existing, "expanded": expanded, "hubs": hubs}

        return current_data

    @app.callback(
        [Output("graph", "elements"),
         Output("graph", "layout")],
        [Input("graph-data", "data"),
         Input("layout-select", "value")],
    )
    def render_graph(data, layout_name):
        elements = data.get("elements", []) if data else []
        hubs = data.get("hubs", {}) if data else {}
        if hubs:
            decorated = []
            for el in elements:
                nid = el["data"]["id"]
                if nid in hubs:
                    el = {
                        "data": {**el["data"], "label": f"{short(nid)} ({hubs[nid]} edges)"},
                        "classes": "hub",
                    }
                decorated.append(el)
            elements = decorated
        layout = {"name": layout_name, "animate": True}
        if layout_name == "cola":
            layout["maxSimulationTime"] = 2000
        if layout_name == "dagre":
            layout["rankDir"] = "LR"
        return elements, layout

    @app.callback(
        Output("info-panel", "children"),
        [Input("graph", "tapNodeData"),
         Input("graph", "tapEdgeData")],
        prevent_initial_call=True,
    )
    def show_info(node_data, edge_data):
        ctx = callback_context
        if not ctx.triggered:
            return "Click a node or edge to see details."

        trigger = ctx.triggered[0]["prop_id"]

        if "tapNodeData" in trigger and node_data:
            node_id = node_data["id"]
            # Fetch this node's triples, capped so hubs stay cheap to inspect
            outbound = query_by_bin(client, namespace, "subject", node_id, limit=HUB_COUNT_CAP)
            inbound = query_by_bin(client, namespace, "object", node_id, limit=HUB_COUNT_CAP)
            def count(ts, skip=0):
                return f"{len(ts) - skip}" + ("+" if len(ts) >= HUB_COUNT_CAP else "")

            lines = [f"Node: {short(node_id)}", f"Type: {node_type(node_id)}", ""]
            if short(node_id) != node_id:
                lines.insert(1, f"Full ID: {node_id}")
            # Identifiers are shown as properties of the entity, by type
            identifiers = [t for t in outbound if t["predicate"] == "HAS_IDENTIFIER"]
            relations = [t for t in outbound if t["predicate"] not in ("HAS_IDENTIFIER", "IS_TYPE")]
            if identifiers:
                lines.append(f"Identifiers: {len(identifiers)}")
                for t in sorted(identifiers, key=lambda t: str((t.get("props") or {}).get("identifier_type", ""))):
                    props = t.get("props") or {}
                    lines.append(f"  {short(props.get('identifier_type', '?'))} = {short(props.get('value', t['object']))}")
                lines.append("")

            lines.append(f"Outbound relationships: {count(relations)}")
            for t in relations[:MAX_INFO_EDGES]:
                props = t.get("props", {})
                prop_str = f"  {short_props(props)}" if props else ""
                lines.append(f"  -[{short(t['predicate'])}]-> {short(t['object'])}{prop_str}")
            if len(relations) > MAX_INFO_EDGES:
                lines.append(f"  ... and {count(relations, MAX_INFO_EDGES)} more")

            lines.append(f"\nInbound relationships: {count(inbound)}")
            for t in inbound[:MAX_INFO_EDGES]:
                props = t.get("props", {})
                prop_str = f"  {short_props(props)}" if props else ""
                lines.append(f"  {short(t['subject'])} -[{short(t['predicate'])}]->{prop_str}")
            if len(inbound) > MAX_INFO_EDGES:
                lines.append(f"  ... and {count(inbound, MAX_INFO_EDGES)} more")

            return "\n".join(lines)

        if "tapEdgeData" in trigger and edge_data:
            lines = [
                f"Edge: {edge_data.get('label', '')}",
                f"Source: {short(edge_data.get('source', ''))}",
                f"Target: {short(edge_data.get('target', ''))}",
                "",
                "Properties:",
                short_props(json.loads(edge_data.get("props", "{}"))),
            ]
            return "\n".join(lines)

        return "Click a node or edge to see details."

    @app.callback(
        Output("stats-panel", "children"),
        Input("graph-data", "data"),
    )
    def update_stats(data):
        if not data or not data.get("elements"):
            return "No data loaded."
        elements = data["elements"]
        nodes = [e for e in elements if "source" not in e["data"]]
        edges = [e for e in elements if "source" in e["data"]]
        predicates = set(e["data"]["label"] for e in edges)
        node_types = {}
        for n in nodes:
            nt = n["data"].get("type", "default")
            node_types[nt] = node_types.get(nt, 0) + 1

        lines = [
            html.Div(f"Nodes: {len(nodes)}"),
            html.Div(f"Edges: {len(edges)}"),
            html.Div(f"Predicates: {', '.join(sorted(predicates))}"),
            html.Div(f"Expanded: {len(data.get('expanded', []))} nodes"),
        ]
        hubs = data.get("hubs", {})
        if hubs:
            lines.append(html.Div(f"Hubs not expanded (> {MAX_EXPAND_DEGREE} edges):",
                                  style={"marginTop": "8px"}))
            for nid, degree in sorted(hubs.items(), key=lambda h: -int(h[1].rstrip("+"))):
                lines.append(html.Div(f"  {short(nid)}: {degree} edges",
                                      style={"color": "#D84315", "paddingLeft": "8px"}))
        lines.append(html.Hr())
        for nt, count in sorted(node_types.items()):
            color = NODE_COLORS.get(nt, NODE_COLORS["default"])
            lines.append(html.Div([
                html.Span("\u25cf ", style={"color": color, "fontSize": "16px"}),
                html.Span(f"{nt}: {count}"),
            ]))
        return lines

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="RDF Property Graph Viewer")
    parser.add_argument("--host", default="127.0.0.1", help="Aerospike host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=3000, help="Aerospike port (default: 3000)")
    parser.add_argument("--hosts", help="Comma-separated seed hosts host[:port] (overrides --host/--port)")
    parser.add_argument("--namespace", default="test", help="Aerospike namespace (default: test)")
    parser.add_argument("--user", help="Aerospike user (security-enabled clusters)")
    parser.add_argument("--password", help="Aerospike password (default: $AEROSPIKE_PASSWORD)")
    parser.add_argument("--tls-name", help="TLS name of the cluster nodes; enables TLS")
    parser.add_argument("--tls-cafile", help="CA certificate file for TLS")
    parser.add_argument("--alternate-access", action="store_true",
                        help="Connect via the nodes' alternate-access-address (cloud/NAT/Docker)")
    parser.add_argument("--listen-port", type=int, default=8050, help="Viewer web port (default: 8050)")
    parser.add_argument("--ontology", default=DEFAULT_ONTOLOGY,
                        help="Ontology descriptor whose relationships drive navigation (default: descriptors/ontology.json)")
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE,
                        help=f"Triples sampled for node search and predicates (default: {DEFAULT_SAMPLE_SIZE})")
    parser.add_argument("--debug", action="store_true", help="Enable Dash debug mode")
    args = parser.parse_args()

    global client, namespace
    client, namespace = connect_aerospike(
        args.host, args.port, args.namespace, hosts=args.hosts, user=args.user,
        password=args.password, tls_name=args.tls_name, tls_cafile=args.tls_cafile,
        alternate_access=args.alternate_access,
    )
    print(f"Connected to Aerospike at {args.hosts or f'{args.host}:{args.port}'}, namespace={args.namespace}")

    app = create_app(client, namespace, args.sample_size, args.ontology)
    print(f"Starting viewer at http://127.0.0.1:{args.listen_port}")
    app.run(debug=args.debug, host="0.0.0.0", port=args.listen_port)


if __name__ == "__main__":
    main()

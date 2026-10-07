package store

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	aero "github.com/aerospike/aerospike-client-go/v8"
)

const SetName = "triples"

// GraphStore wraps an Aerospike client with preconfigured policies for graph operations.
type GraphStore struct {
	Client      *aero.Client
	Namespace   string
	WritePolicy *aero.WritePolicy
	QueryPolicy *aero.QueryPolicy
	BatchPolicy *aero.BatchPolicy
}

// ConnConfig describes how to reach a local or remote Aerospike cluster.
type ConnConfig struct {
	Host            string
	Port            int
	Hosts           string // comma-separated seeds, host[:port]; overrides Host/Port
	User            string
	Password        string // falls back to $AEROSPIKE_PASSWORD
	TLSName         string
	TLSCAFile       string
	AlternateAccess bool
	ConnQueueSize   int
}

// RegisterFlags adds the connection flags shared by all commands.
func (c *ConnConfig) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Host, "host", "127.0.0.1", "Aerospike host")
	fs.IntVar(&c.Port, "port", 3000, "Aerospike port")
	fs.StringVar(&c.Hosts, "hosts", "", "Comma-separated seed hosts host[:port] (overrides -host/-port)")
	fs.StringVar(&c.User, "user", "", "Aerospike user (security-enabled clusters)")
	fs.StringVar(&c.Password, "password", "", "Aerospike password (default $AEROSPIKE_PASSWORD)")
	fs.StringVar(&c.TLSName, "tls-name", "", "TLS name of the cluster nodes; enables TLS")
	fs.StringVar(&c.TLSCAFile, "tls-cafile", "", "CA certificate file for TLS")
	fs.BoolVar(&c.AlternateAccess, "alternate-access", false, "Connect via the nodes' alternate-access-address (cloud/NAT/Docker)")
	fs.IntVar(&c.ConnQueueSize, "conn-queue", 0, "Max connections per node (default client setting)")
}

func (c *ConnConfig) hosts() ([]*aero.Host, error) {
	seeds := c.Hosts
	if seeds == "" {
		seeds = fmt.Sprintf("%s:%d", c.Host, c.Port)
	}
	var hosts []*aero.Host
	for _, s := range strings.Split(seeds, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		name, port := s, c.Port
		if i := strings.LastIndex(s, ":"); i >= 0 {
			p, err := strconv.Atoi(s[i+1:])
			if err != nil {
				return nil, fmt.Errorf("invalid seed host %q", s)
			}
			name, port = s[:i], p
		}
		h := aero.NewHost(name, port)
		h.TLSName = c.TLSName
		hosts = append(hosts, h)
	}
	if len(hosts) == 0 {
		return nil, fmt.Errorf("no seed hosts given")
	}
	return hosts, nil
}

func (c *ConnConfig) policy() (*aero.ClientPolicy, error) {
	cp := aero.NewClientPolicy()
	cp.User = c.User
	cp.Password = c.Password
	if cp.User != "" && cp.Password == "" {
		cp.Password = os.Getenv("AEROSPIKE_PASSWORD")
	}
	cp.UseServicesAlternate = c.AlternateAccess
	if c.ConnQueueSize > 0 {
		cp.ConnectionQueueSize = c.ConnQueueSize
	}
	if c.TLSName != "" || c.TLSCAFile != "" {
		tc := &tls.Config{}
		if c.TLSCAFile != "" {
			pem, err := os.ReadFile(c.TLSCAFile)
			if err != nil {
				return nil, fmt.Errorf("read TLS CA file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("no certificates in %s", c.TLSCAFile)
			}
			tc.RootCAs = pool
		}
		cp.TlsConfig = tc
	}
	return cp, nil
}

// Connect connects to the cluster described by the config.
func (c *ConnConfig) Connect(namespace string) (*GraphStore, error) {
	hosts, err := c.hosts()
	if err != nil {
		return nil, err
	}
	cp, err := c.policy()
	if err != nil {
		return nil, err
	}
	client, err := aero.NewClientWithPolicyAndHost(cp, hosts...)
	if err != nil {
		return nil, err
	}
	return newGraphStore(client, namespace), nil
}

// NewGraphStore connects to Aerospike and returns a configured GraphStore.
func NewGraphStore(host string, port int, namespace string) (*GraphStore, error) {
	c := &ConnConfig{Host: host, Port: port}
	return c.Connect(namespace)
}

func newGraphStore(client *aero.Client, namespace string) *GraphStore {
	wp := aero.NewWritePolicy(0, 0)
	wp.RecordExistsAction = aero.UPDATE // upsert semantics

	qp := aero.NewQueryPolicy()

	bp := aero.NewBatchPolicy()

	return &GraphStore{
		Client:      client,
		Namespace:   namespace,
		WritePolicy: wp,
		QueryPolicy: qp,
		BatchPolicy: bp,
	}
}

// UseShortQueries runs secondary index queries as short queries, which the
// server optimizes for small results (under ~100 records) at high rates. They
// run on service threads, so they aren't rejected with FAIL_FORBIDDEN when
// concurrent queries exceed the server's query-threads-limit.
func (gs *GraphStore) UseShortQueries() {
	gs.QueryPolicy.ExpectedDuration = aero.SHORT
}

// Close shuts down the Aerospike client connection.
func (gs *GraphStore) Close() {
	gs.Client.Close()
}

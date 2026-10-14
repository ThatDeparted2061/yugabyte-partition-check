// Package nemesis injects faults into a Docker Compose YugabyteDB cluster:
// network partitions (iptables), latency (tc netem), process freezes
// (docker pause) and crashes (docker kill -s KILL, then restart).
//
// Network faults run iptables/tc from a throwaway container that joins the
// target node's network namespace (--net container:<node>), so the database
// image needs no extra tools or privileges.
package nemesis

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os/exec"
	"strings"
	"time"
)

// Fault is one injected fault and when it was active.
type Fault struct {
	Kind   string        `json:"kind"` // partition | bridge | kill | pause | latency
	Detail string        `json:"detail"`
	Start  time.Duration `json:"start"`
	End    time.Duration `json:"end"`
}

// Nemesis drives faults against the named containers.
type Nemesis struct {
	Nodes      []string
	FaultImage string // alpine with iptables + iproute2
	Start      time.Time
	Log        func(format string, args ...any)
}

func docker(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

// in runs a shell command inside node's network namespace.
func (n *Nemesis) in(ctx context.Context, node, script string) error {
	_, err := docker(ctx, "run", "--rm", "--net", "container:"+node, "--cap-add", "NET_ADMIN", n.FaultImage, "sh", "-c", script)
	return err
}

func (n *Nemesis) ip(ctx context.Context, node string) (string, error) {
	return docker(ctx, "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", node)
}

// cut drops all traffic between a and b, in both directions.
func (n *Nemesis) cut(ctx context.Context, a, b string) error {
	ipA, err := n.ip(ctx, a)
	if err != nil {
		return err
	}
	ipB, err := n.ip(ctx, b)
	if err != nil {
		return err
	}
	if err := n.in(ctx, a, fmt.Sprintf("iptables -A INPUT -s %s -j DROP && iptables -A OUTPUT -d %s -j DROP", ipB, ipB)); err != nil {
		return err
	}
	return n.in(ctx, b, fmt.Sprintf("iptables -A INPUT -s %s -j DROP && iptables -A OUTPUT -d %s -j DROP", ipA, ipA))
}

// HealAll removes every network fault and unpauses and starts every node.
func (n *Nemesis) HealAll(ctx context.Context) {
	for _, node := range n.Nodes {
		docker(ctx, "unpause", node)
		docker(ctx, "start", node)
		n.in(ctx, node, "iptables -F; tc qdisc del dev eth0 root 2>/dev/null; true")
	}
}

// Inject applies one random fault for d and then heals it.
func (n *Nemesis) Inject(ctx context.Context, rng *rand.Rand, d time.Duration) (Fault, error) {
	node := n.Nodes[rng.IntN(len(n.Nodes))]
	others := []string{}
	for _, o := range n.Nodes {
		if o != node {
			others = append(others, o)
		}
	}
	f := Fault{Start: time.Since(n.Start)}
	var heal func() error
	var err error
	switch x := rng.Float64(); {
	case x < 0.35: // majority/minority: one node cut off from the other two
		f.Kind, f.Detail = "partition", fmt.Sprintf("%s | %s", node, strings.Join(others, ","))
		for _, o := range others {
			if err = n.cut(ctx, node, o); err != nil {
				break
			}
		}
		heal = func() error { return n.healNet(ctx) }
	case x < 0.50: // non-transitive: node <-/-> others[0], both still see others[1]
		f.Kind, f.Detail = "bridge", fmt.Sprintf("%s x %s, both reach %s", node, others[0], others[1])
		err = n.cut(ctx, node, others[0])
		heal = func() error { return n.healNet(ctx) }
	case x < 0.75:
		f.Kind, f.Detail = "kill", node+" (SIGKILL, then restart)"
		_, err = docker(ctx, "kill", "-s", "KILL", node)
		heal = func() error { _, err := docker(ctx, "start", node); return err }
	case x < 0.85:
		f.Kind, f.Detail = "pause", node+" (frozen)"
		_, err = docker(ctx, "pause", node)
		heal = func() error { _, err := docker(ctx, "unpause", node); return err }
	default:
		f.Kind, f.Detail = "latency", node+" +150ms ±50ms"
		err = n.in(ctx, node, "tc qdisc add dev eth0 root netem delay 150ms 50ms")
		heal = func() error { return n.in(ctx, node, "tc qdisc del dev eth0 root") }
	}
	if err != nil {
		n.healNet(ctx)
		return f, err
	}
	n.Log("fault: %s %s for %s", f.Kind, f.Detail, d.Round(time.Second))
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
	herr := heal()
	f.End = time.Since(n.Start)
	n.Log("healed: %s", f.Kind)
	return f, herr
}

func (n *Nemesis) healNet(ctx context.Context) error {
	for _, node := range n.Nodes {
		if err := n.in(ctx, node, "iptables -F"); err != nil {
			return err
		}
	}
	return nil
}

// Healthy reports whether every node answers pg_isready.
func (n *Nemesis) Healthy(ctx context.Context) bool {
	for _, node := range n.Nodes {
		if _, err := docker(ctx, "exec", node, "/home/yugabyte/postgres/bin/pg_isready", "-h", node, "-p", "5433"); err != nil {
			return false
		}
	}
	return true
}

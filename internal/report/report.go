// Package report renders a run as a self-contained HTML page.
package report

import (
	"fmt"
	"html/template"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/ThatDeparted2061/yugabyte-partition-check/internal/checker"
	"github.com/ThatDeparted2061/yugabyte-partition-check/internal/nemesis"
	"github.com/ThatDeparted2061/yugabyte-partition-check/internal/workload"
)

// Run is everything a report shows; it is also written out as JSON.
type Run struct {
	Started   time.Time       `json:"started"`
	Duration  time.Duration   `json:"duration"`
	YBVersion string          `json:"yb_version"`
	Config    workload.Config `json:"config"`
	Faults    []nemesis.Fault `json:"faults"`
	Check     checker.Result  `json:"check"`
	Errors    map[string]int  `json:"errors"`    // failed/indeterminate ops by error class
	PerSecond []Second        `json:"per_second"`
}

// Second is the workload's throughput in one second of the run.
type Second struct {
	OK, Failed, Info, Reads int
}

// FaultCounts returns how many faults of each kind ran.
func (r Run) FaultCounts() map[string]int {
	m := map[string]int{}
	for _, f := range r.Faults {
		m[f.Kind]++
	}
	return m
}

// Summarise buckets ops per second and counts errors.
func Summarise(ops []workload.Op, d time.Duration) ([]Second, map[string]int) {
	secs := make([]Second, int(d.Seconds())+1)
	errs := map[string]int{}
	for _, op := range ops {
		i := min(int(op.End.Seconds()), len(secs)-1)
		switch {
		case op.Kind == "read" && op.Outcome == workload.OK:
			secs[i].Reads++
		case op.Outcome == workload.OK:
			secs[i].OK++
		case op.Outcome == workload.Info:
			secs[i].Info++
			errs["indeterminate: "+op.Error]++
		default:
			secs[i].Failed++
			errs[op.Kind+": "+op.Error]++
		}
	}
	return secs, errs
}

var colors = map[string]string{"partition": "#e5484d", "bridge": "#f76b15", "kill": "#8e4ec6", "pause": "#3e63dd", "latency": "#12a594"}

type bar struct {
	X, W  float64
	Color string
	Label string
}

type view struct {
	Run
	Width, Height float64
	OKLine        string
	FailLine      string
	MaxY          int
	Bars          []bar
	Ticks         []tick
	Kinds         []string
	Counts        map[string]int
	ErrorKeys     []string
	Colors        map[string]string
}

type tick struct {
	X     float64
	Label string
}

// Write renders r as HTML.
func Write(w io.Writer, r Run) error {
	v := view{Run: r, Width: 1000, Height: 260, Counts: r.FaultCounts(), Colors: colors}
	maxY := 1
	for _, s := range r.PerSecond {
		maxY = max(maxY, s.OK, s.Failed+s.Info)
	}
	v.MaxY = maxY
	n := float64(max(1, len(r.PerSecond)-1))
	x := func(sec float64) float64 { return sec / n * v.Width }
	y := func(c int) float64 { return v.Height - float64(c)/float64(maxY)*(v.Height-20) }
	for i, s := range r.PerSecond {
		v.OKLine += fmt.Sprintf("%.1f,%.1f ", x(float64(i)), y(s.OK))
		v.FailLine += fmt.Sprintf("%.1f,%.1f ", x(float64(i)), y(s.Failed+s.Info))
	}
	for _, f := range r.Faults {
		v.Bars = append(v.Bars, bar{X: x(f.Start.Seconds()), W: max(1, x(f.End.Seconds())-x(f.Start.Seconds())), Color: colors[f.Kind], Label: f.Kind})
	}
	step := 60.0
	if n < 180 {
		step = 30
	}
	for s := 0.0; s <= n; s += step {
		v.Ticks = append(v.Ticks, tick{x(s), (time.Duration(s) * time.Second).String()})
	}
	v.Kinds = slices.Sorted(maps.Keys(v.Counts))
	v.ErrorKeys = slices.Sorted(maps.Keys(r.Errors))
	return page.Execute(w, v)
}

var page = template.Must(template.New("report").Funcs(template.FuncMap{
	"dur": func(d time.Duration) string { return d.Round(time.Second).String() },
	"pct": func(a, b int) string {
		if b == 0 {
			return "0%"
		}
		return fmt.Sprintf("%.1f%%", float64(a)/float64(b)*100)
	},
}).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>yb-partition-check report</title>
<style>
:root{--bg:#fff;--fg:#1c2024;--muted:#60646c;--card:#f9f9fb;--line:#e0e1e6;--ok:#30a46c;--bad:#e5484d}
@media (prefers-color-scheme:dark){:root{--bg:#111113;--fg:#edeef0;--muted:#b0b4ba;--card:#18191b;--line:#2e3135}}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 -apple-system,system-ui,Segoe UI,sans-serif}
main{max-width:1060px;margin:0 auto;padding:28px 16px}
h1{font-size:22px;margin:0 0 4px} h2{font-size:16px;margin:28px 0 10px}
.muted{color:var(--muted)} .verdict{padding:14px 18px;border-radius:10px;font-size:18px;font-weight:600;margin:18px 0}
.ok{background:color-mix(in srgb,var(--ok) 15%,transparent);color:var(--ok)} .bad{background:color-mix(in srgb,var(--bad) 15%,transparent);color:var(--bad)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px}
.card b{display:block;font-size:22px} table{border-collapse:collapse;width:100%;font-size:14px}
td,th{border-bottom:1px solid var(--line);padding:6px 8px;text-align:left} th{color:var(--muted);font-weight:500}
svg{width:100%;height:auto;background:var(--card);border:1px solid var(--line);border-radius:10px}
.legend span{display:inline-block;margin-right:14px} .sw{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px}
code{font-size:13px}
</style></head><body><main>
<h1>yb-partition-check</h1>
<div class="muted">YugabyteDB {{.YBVersion}} · 3 nodes, RF 3 · started {{.Started.Format "2006-01-02 15:04 MST"}} · {{dur .Duration}}</div>
{{if .Check.Valid}}<div class="verdict ok">Invariant held: {{.Check.Reads}} snapshot reads, {{.Check.Transfers}} committed transfers, {{len .Faults}} faults. No money created or lost, no read saw half a transfer.</div>
{{else}}<div class="verdict bad">Invariant violated. See problems below.</div>{{end}}
<div class="cards">
{{range .Kinds}}<div class="card"><b>{{index $.Counts .}}</b>{{.}}</div>{{end}}
<div class="card"><b>{{.Check.Transfers}}</b>transfers committed</div>
<div class="card"><b>{{.Check.Failed}}</b>transfers failed cleanly</div>
<div class="card"><b>{{.Check.Indeterminate}}</b>indeterminate commits</div>
<div class="card"><b>{{.Check.Reads}}</b>reads checked</div>
<div class="card"><b>{{.Check.BadReads}}</b>torn reads</div>
<div class="card"><b>{{.Check.FinalTotal}}</b>final total (want {{.Check.ExpectedTotal}})</div>
</div>
<h2>Throughput and faults</h2>
<svg viewBox="0 -24 {{.Width}} {{.Height}}" preserveAspectRatio="none" role="img" aria-label="transfers per second with fault windows">
{{range .Bars}}<rect x="{{.X}}" y="-24" width="{{.W}}" height="{{$.Height}}" fill="{{.Color}}" opacity="0.13"/><rect x="{{.X}}" y="-24" width="{{.W}}" height="6" fill="{{.Color}}"/>{{end}}
<polyline points="{{.OKLine}}" fill="none" stroke="#30a46c" stroke-width="1.6"/>
<polyline points="{{.FailLine}}" fill="none" stroke="#e5484d" stroke-width="1.2"/>
{{range .Ticks}}<text x="{{.X}}" y="{{$.Height}}" dy="-4" font-size="11" fill="currentColor" opacity=".6">{{.Label}}</text>{{end}}
<text x="4" y="-8" font-size="11" fill="currentColor" opacity=".6">max {{.MaxY}}/s</text>
</svg>
<div class="legend muted"><span><i class="sw" style="background:#30a46c"></i>committed transfers/s</span><span><i class="sw" style="background:#e5484d"></i>failed + indeterminate/s</span>
{{range $k, $c := .Colors}}<span><i class="sw" style="background:{{$c}}"></i>{{$k}}</span>{{end}}</div>
<h2>Checks</h2>
<table>
<tr><th>Invariant</th><th>Result</th></tr>
<tr><td>Every snapshot read sums to {{.Check.ExpectedTotal}}, no negative balance</td><td>{{if eq .Check.BadReads 0}}✓ {{.Check.Reads}} reads{{else}}✗ {{.Check.BadReads}} bad reads (first at {{.Check.FirstBadReadAt}}){{end}}</td></tr>
<tr><td>Final balances sum to {{.Check.ExpectedTotal}}</td><td>{{if eq .Check.FinalTotal .Check.ExpectedTotal}}✓{{else}}✗ {{.Check.FinalTotal}}{{end}}</td></tr>
<tr><td>Final state = committed transfers + some subset of indeterminate ones</td><td>{{if .Check.Explained}}✓{{else}}✗{{end}} <span class="muted">({{.Check.ExplainMethod}})</span></td></tr>
</table>
{{if .Check.Problems}}<h2>Problems</h2><ul>{{range .Check.Problems}}<li>{{.}}</li>{{end}}</ul>{{end}}
<h2>Faults</h2>
<table><tr><th>#</th><th>Kind</th><th>Target</th><th>From</th><th>To</th></tr>
{{range $i, $f := .Faults}}<tr><td>{{$i}}</td><td>{{$f.Kind}}</td><td>{{$f.Detail}}</td><td>{{dur $f.Start}}</td><td>{{dur $f.End}}</td></tr>{{end}}
</table>
<h2>Errors seen by clients</h2>
<table><tr><th>Error</th><th>Count</th></tr>{{range .ErrorKeys}}<tr><td><code>{{.}}</code></td><td>{{index $.Errors .}}</td></tr>{{end}}</table>
<h2>Final balances</h2><p><code>{{.Check.FinalBalances}}</code></p>
<p class="muted">Workload: {{.Config.Accounts}} accounts × {{.Config.Initial}}, {{.Config.Writers}} writers (SERIALIZABLE transfers), {{.Config.Readers}} readers (REPEATABLE READ snapshots), op timeout {{.Config.OpTimeout}}.</p>
</main></body></html>`))

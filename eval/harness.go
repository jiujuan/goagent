package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/jiujuan/goagent/agent"
)

// harness.go is closed loop B — offline regression. A Harness runs every Case in
// a Dataset through the agent under test, records each trajectory, scores it with
// a fixed set of scorers, and aggregates a Report suitable for a CI gate.

// Case is one evaluation example.
type Case struct {
	Name      string
	Input     string
	Reference string
	Meta      map[string]any
}

// Dataset is an ordered set of cases.
type Dataset []Case

// Harness evaluates one agent against a dataset with a fixed set of scorers.
type Harness struct {
	Agent   *agent.Agent
	Scorers []Scorer
}

// CaseResult is the per-case outcome: the produced answer and its scores.
type CaseResult struct {
	Case   Case
	Output string
	Scores []Score
	Err    error
}

// Report aggregates case results.
type Report struct {
	Cases    []CaseResult
	Mean     map[string]float64 // mean Value per scorer name
	PassRate float64            // fraction of (case,scorer) pairs that passed
}

// Run evaluates every case on its own fresh thread and returns the aggregated
// report. A case whose run or a scorer errors is recorded with that error and
// excluded from the aggregates; Run itself returns an error only on misuse.
func (h *Harness) Run(ctx context.Context, ds Dataset) (Report, error) {
	if h.Agent == nil {
		return Report{}, fmt.Errorf("eval: Harness needs an Agent")
	}
	if len(h.Scorers) == 0 {
		return Report{}, fmt.Errorf("eval: Harness needs at least one Scorer")
	}

	rep := Report{Mean: map[string]float64{}}
	sums := map[string]float64{}
	counts := map[string]int{}
	var totalPairs, passedPairs int

	for _, c := range ds {
		cr := CaseResult{Case: c}

		traj, res, err := Record(h.Agent.Stream(ctx, c.Input))
		if err != nil {
			cr.Err = err
			rep.Cases = append(rep.Cases, cr)
			continue
		}
		traj.Input = c.Input
		cr.Output = res.Message.Text()

		sample := Sample{Input: c.Input, Output: cr.Output, Reference: c.Reference, Traj: traj, Meta: c.Meta}
		for _, sc := range h.Scorers {
			s, serr := sc.Score(ctx, sample)
			if serr != nil {
				cr.Err = fmt.Errorf("scorer %q: %w", sc.Name(), serr)
				break
			}
			cr.Scores = append(cr.Scores, s)
			sums[s.Name] += s.Value
			counts[s.Name]++
			totalPairs++
			if s.Passed {
				passedPairs++
			}
		}
		rep.Cases = append(rep.Cases, cr)
	}

	for name, sum := range sums {
		rep.Mean[name] = sum / float64(counts[name])
	}
	if totalPairs > 0 {
		rep.PassRate = float64(passedPairs) / float64(totalPairs)
	}
	return rep, nil
}

// scorerNames returns the scorer names seen across the report, sorted.
func (r Report) scorerNames() []string {
	seen := map[string]bool{}
	for _, c := range r.Cases {
		for _, s := range c.Scores {
			seen[s.Name] = true
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Print writes a human-readable table to stdout: one row per case with each
// scorer's value, then aggregate means and the overall pass rate.
func (r Report) Print() {
	names := r.scorerNames()
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)

	header := "CASE\t" + strings.Join(names, "\t")
	fmt.Fprintln(w, header)
	for _, c := range r.Cases {
		cells := make([]string, 0, len(names)+1)
		name := c.Case.Name
		if name == "" {
			name = truncate(c.Case.Input, 24)
		}
		cells = append(cells, name)
		if c.Err != nil {
			cells = append(cells, "ERR: "+truncate(c.Err.Error(), 40))
			fmt.Fprintln(w, strings.Join(cells, "\t"))
			continue
		}
		byName := map[string]Score{}
		for _, s := range c.Scores {
			byName[s.Name] = s
		}
		for _, n := range names {
			s, ok := byName[n]
			if !ok {
				cells = append(cells, "-")
				continue
			}
			mark := "✗"
			if s.Passed {
				mark = "✓"
			}
			cells = append(cells, fmt.Sprintf("%.2f%s", s.Value, mark))
		}
		fmt.Fprintln(w, strings.Join(cells, "\t"))
	}

	meanCells := []string{"MEAN"}
	for _, n := range names {
		meanCells = append(meanCells, fmt.Sprintf("%.2f", r.Mean[n]))
	}
	fmt.Fprintln(w, strings.Join(meanCells, "\t"))
	w.Flush()
	fmt.Printf("PASS RATE: %.0f%%\n", r.PassRate*100)
}

// reportWire is the JSON shape of a report (errors reduced to strings).
type reportWire struct {
	Cases []struct {
		Name   string  `json:"name"`
		Output string  `json:"output,omitempty"`
		Scores []Score `json:"scores,omitempty"`
		Err    string  `json:"err,omitempty"`
	} `json:"cases"`
	Mean     map[string]float64 `json:"mean"`
	PassRate float64            `json:"pass_rate"`
}

// JSON renders the report as a CI artifact.
func (r Report) JSON() []byte {
	var w reportWire
	w.Mean = r.Mean
	w.PassRate = r.PassRate
	for _, c := range r.Cases {
		name := c.Case.Name
		if name == "" {
			name = c.Case.Input
		}
		entry := struct {
			Name   string  `json:"name"`
			Output string  `json:"output,omitempty"`
			Scores []Score `json:"scores,omitempty"`
			Err    string  `json:"err,omitempty"`
		}{Name: name, Output: c.Output, Scores: c.Scores}
		if c.Err != nil {
			entry.Err = c.Err.Error()
		}
		w.Cases = append(w.Cases, entry)
	}
	b, _ := json.MarshalIndent(w, "", "  ")
	return b
}

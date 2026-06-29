package eval

import (
	"context"
	"sort"
	"strconv"
	"strings"
)

// composite.go combines scorers into one. Composites are themselves Scorers, so
// they nest freely (a Weighted of Alls of Rubrics is fine).

// Not inverts a scorer: Value becomes 1-Value and Passed is negated. Useful for
// "must NOT contain / match" checks, e.g. Not(Contains("联系客服")).
func Not(s Scorer) Scorer {
	return newScorer("not_"+s.Name(), func(ctx context.Context, sample Sample) (Score, error) {
		sc, err := s.Score(ctx, sample)
		if err != nil {
			return Score{}, err
		}
		sc.Name = "not_" + sc.Name
		sc.Value = clamp01(1 - sc.Value)
		sc.Passed = !sc.Passed
		return sc, nil
	})
}

// Named overrides a scorer's reported name (on both Name() and Score.Name), so
// two scorers of the same kind — e.g. two Contains — get distinct columns in a
// Report instead of merging. Reports and composites key on the name.
func Named(name string, s Scorer) Scorer {
	return newScorer(name, func(ctx context.Context, sample Sample) (Score, error) {
		sc, err := s.Score(ctx, sample)
		sc.Name = name
		return sc, err
	})
}

// WeightedScorer pairs a scorer with its weight for Weighted. Use Weight to
// build one. (A slice — not a map keyed by Scorer — because Scorer values are
// not guaranteed hashable.)
type WeightedScorer struct {
	Scorer Scorer
	Weight float64
}

// Weight pairs a scorer with a weight.
func Weight(s Scorer, w float64) WeightedScorer { return WeightedScorer{Scorer: s, Weight: w} }

// Weighted aggregates scorers by weight into one normalized score; Sub holds the
// per-scorer breakdown (sorted by scorer name for stable output). Passed when
// the weighted value reaches 0.5.
func Weighted(items ...WeightedScorer) Scorer {
	return newScorer("weighted", func(ctx context.Context, s Sample) (Score, error) {
		subs, totalW, acc := make([]Score, 0, len(items)), 0.0, 0.0
		for _, it := range items {
			sub, err := it.Scorer.Score(ctx, s)
			if err != nil {
				return Score{Name: "weighted"}, err
			}
			subs = append(subs, sub)
			acc += it.Weight * sub.Value
			totalW += it.Weight
		}
		sortScores(subs)
		value := 0.0
		if totalW > 0 {
			value = clamp01(acc / totalW)
		}
		return Score{Name: "weighted", Value: value, Passed: value >= 0.5, Reason: summarize(subs), Sub: subs}, nil
	})
}

// All passes only when every sub-scorer passes; Value is the mean.
func All(scorers ...Scorer) Scorer {
	return newScorer("all", func(ctx context.Context, s Sample) (Score, error) {
		subs, sum, pass := make([]Score, 0, len(scorers)), 0.0, true
		for _, sc := range scorers {
			sub, err := sc.Score(ctx, s)
			if err != nil {
				return Score{Name: "all"}, err
			}
			subs = append(subs, sub)
			sum += sub.Value
			pass = pass && sub.Passed
		}
		sortScores(subs)
		value := 0.0
		if len(subs) > 0 {
			value = sum / float64(len(subs))
		}
		return Score{Name: "all", Value: clamp01(value), Passed: pass && len(subs) > 0, Reason: summarize(subs), Sub: subs}, nil
	})
}

// Any passes when at least one sub-scorer passes; Value is the max.
func Any(scorers ...Scorer) Scorer {
	return newScorer("any", func(ctx context.Context, s Sample) (Score, error) {
		subs, max, pass := make([]Score, 0, len(scorers)), 0.0, false
		for _, sc := range scorers {
			sub, err := sc.Score(ctx, s)
			if err != nil {
				return Score{Name: "any"}, err
			}
			subs = append(subs, sub)
			if sub.Value > max {
				max = sub.Value
			}
			pass = pass || sub.Passed
		}
		sortScores(subs)
		return Score{Name: "any", Value: clamp01(max), Passed: pass, Reason: summarize(subs), Sub: subs}, nil
	})
}

// sortScores orders sub-scores by name (then reason) for deterministic output,
// since the scorers come from a map in Weighted.
func sortScores(ss []Score) {
	sort.SliceStable(ss, func(i, j int) bool {
		if ss[i].Name != ss[j].Name {
			return ss[i].Name < ss[j].Name
		}
		return ss[i].Reason < ss[j].Reason
	})
}

// summarize renders a one-line breakdown like "rubric=0.80✓ contains=0.00✗".
func summarize(ss []Score) string {
	var b strings.Builder
	for i, s := range ss {
		if i > 0 {
			b.WriteByte(' ')
		}
		mark := "✗"
		if s.Passed {
			mark = "✓"
		}
		b.WriteString(s.Name)
		b.WriteByte('=')
		b.WriteString(strconv.FormatFloat(s.Value, 'f', 2, 64))
		b.WriteString(mark)
	}
	return b.String()
}

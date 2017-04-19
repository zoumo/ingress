package controller

import (
	"reflect"
	"sort"

	"github.com/denverdino/aliyungo/slb"
)

// sets.Rule is a set of slb.Rule, implemented via map[slb.Rule]struct{} for minimal memory consumption.
type Rule map[string]slb.Rule

// New creates a Rule from a list of values.
func NewRule(items ...slb.Rule) Rule {
	ss := Rule{}
	ss.Insert(items...)
	return ss
}

// keyForRule returns rule key
func keyForRule(r slb.Rule) string {
	return r.VServerGroupId + ":" + r.RuleName + ":" + r.Domain + r.Url
}

// RuleKeySet creates a Rule from a keys of a map[string](? extends interface{}).
// If the value passed in is not actually a map, this will panic.
func RuleKeySet(theMap interface{}) Rule {
	v := reflect.ValueOf(theMap)
	ret := Rule{}

	for _, keyValue := range v.MapKeys() {
		ret.Insert(keyValue.Interface().(slb.Rule))
	}
	return ret
}

// Insert adds items to the set.
func (s Rule) Insert(items ...slb.Rule) {
	for _, item := range items {
		s[keyForRule(item)] = item
	}
}

// Delete removes all items from the set.
func (s Rule) Delete(items ...slb.Rule) {
	for _, item := range items {
		delete(s, keyForRule(item))
	}
}

// Has returns true if and only if item is contained in the set.
func (s Rule) Has(item slb.Rule) bool {
	_, contained := s[keyForRule(item)]
	return contained
}

// HasAll returns true if and only if all items are contained in the set.
func (s Rule) HasAll(items ...slb.Rule) bool {
	for _, item := range items {
		if !s.Has(item) {
			return false
		}
	}
	return true
}

// HasAny returns true if any items are contained in the set.
func (s Rule) HasAny(items ...slb.Rule) bool {
	for _, item := range items {
		if s.Has(item) {
			return true
		}
	}
	return false
}

// Difference returns a set of objects that are not in s2
// For example:
// s1 = {a1, a2, a3}
// s2 = {a1, a2, a4, a5}
// s1.Difference(s2) = {a3}
// s2.Difference(s1) = {a4, a5}
func (s Rule) Difference(s2 Rule) Rule {
	result := NewRule()
	for _, value := range s {
		if !s2.Has(value) {
			result.Insert(value)
		}
	}
	return result
}

// Union returns a new set which includes items in either s1 or s2.
// For example:
// s1 = {a1, a2}
// s2 = {a3, a4}
// s1.Union(s2) = {a1, a2, a3, a4}
// s2.Union(s1) = {a1, a2, a3, a4}
func (s1 Rule) Union(s2 Rule) Rule {
	result := NewRule()
	for _, value := range s1 {
		result.Insert(value)
	}
	for _, value := range s2 {
		result.Insert(value)
	}
	return result
}

// Intersection returns a new set which includes the item in BOTH s1 and s2
// For example:
// s1 = {a1, a2}
// s2 = {a2, a3}
// s1.Intersection(s2) = {a2}
func (s1 Rule) Intersection(s2 Rule) Rule {
	var walk, other Rule
	result := NewRule()
	if s1.Len() < s2.Len() {
		walk = s1
		other = s2
	} else {
		walk = s2
		other = s1
	}
	for _, value := range walk {
		if other.Has(value) {
			result.Insert(value)
		}
	}
	return result
}

// IsSuperset returns true if and only if s1 is a superset of s2.
func (s1 Rule) IsSuperset(s2 Rule) bool {
	for _, item := range s2 {
		if !s1.Has(item) {
			return false
		}
	}
	return true
}

// Equal returns true if and only if s1 is equal (as a set) to s2.
// Two sets are equal if their membership is identical.
// (In practice, this means same elements, order doesn't matter)
func (s1 Rule) Equal(s2 Rule) bool {
	return len(s1) == len(s2) && s1.IsSuperset(s2)
}

type sortableSliceOfRule []slb.Rule

func (s sortableSliceOfRule) Len() int           { return len(s) }
func (s sortableSliceOfRule) Less(i, j int) bool { return lessRule(s[i], s[j]) }
func (s sortableSliceOfRule) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// List returns the contents as a sorted string slice.
func (s Rule) List() []slb.Rule {
	res := make(sortableSliceOfRule, 0, len(s))
	for _, value := range s {
		res = append(res, value)
	}
	sort.Sort(res)
	return []slb.Rule(res)
}

// UnsortedList returns the slice with contents in random order.
func (s Rule) UnsortedList() []slb.Rule {
	res := make([]slb.Rule, 0, len(s))
	for _, value := range s {
		res = append(res, value)
	}
	return res
}

// Returns a single element from the set.
func (s Rule) PopAny() (slb.Rule, bool) {
	for _, value := range s {
		s.Delete(value)
		return value, true
	}
	var zeroValue slb.Rule
	return zeroValue, false
}

// Len returns the size of the set.
func (s Rule) Len() int {
	return len(s)
}

func lessRule(lhs, rhs slb.Rule) bool {
	return lhs.RuleName < rhs.RuleName
}

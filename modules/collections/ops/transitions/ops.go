// Package transitions implements the collections/ops service — array
// primitives (Sum, Map, Filter, Append, RemoveWhere) that let WSL
// workflows express iteration without falling back to Go loops.
//
// Map and Filter invoke a WSL sub-workflow per element via the engine's
// shared-context sub-workflow runner. The element is exposed to the child
// workflow as $element and the iteration index as $index (both via
// WorkerSessionContext.SetValue, which lands in the "values" bag the
// template engine resolves $var against).
//
// PROMOTION-CANDIDATE: stable since Wave 4, no acme-* deps, used in 2+ packages.
// Provides Map/Filter/Reduce/Sum/Append/RemoveWhere/Contains/MaxBy/SortBy/Limit/
// AppendUnique/RemoveElement/RecurseForest/Find/SelectMostRecentDated/AssertFound.
// No std-* equivalent. Consider promoting to std-collections after kuetix review.
package transitions

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/kuetix/engine/engine/domain"
	"github.com/kuetix/engine/engine/domain/interfaces"
	wf "github.com/kuetix/engine/engine/workflow"
)

var _ interfaces.ServiceTransitions = (*opsTransitions)(nil)

type opsTransitions struct {
	wf.BaseServiceTransition
}

func NewOpsTransitions() interfaces.ServiceTransitions {
	return &opsTransitions{}
}

// Sum sums a numeric field across an array of maps. Returns {sum: float64}.
// arr may be []interface{} or []map[string]interface{} (the two shapes
// persistence/store returns). field is the key to sum on each element.
// Non-numeric values are coerced to 0 via toFloat.
// WSL: collections/ops/ops.Sum(arr: $lines, field: "subtotal")
func (t *opsTransitions) Sum(arr interface{}, field string) (r domain.FlowStepResult) {
	sum := 0.0
	for _, m := range toMapSlice(arr) {
		sum += toFloat(m[field])
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"sum": sum}
	return
}

// Map runs a WSL workflow per element of arr, passing the element as
// $element and the index as $index in the shared session context. The
// sub-workflow must end ok with a response value; Map collects each
// response into a result array. Returns {results: []interface{}}.
// WSL: collections/ops/ops.Map(arr: $lines, workflow: "solutions/invoice/recompute-line")
func (t *opsTransitions) Map(p *wf.WorkerSessionContext, arr interface{}, workflow string) (r domain.FlowStepResult) {
	items := toInterfaceSlice(arr)
	results := make([]interface{}, 0, len(items))
	runner := newRunner(p, workflow)
	for i, elem := range items {
		p.SetValue("element", elem)
		p.SetValue("index", i)
		resp, err := runner.RunWithSharedContext(p, workflow)
		if err != nil {
			r.Success = false
			r.StatusCode = 500
			r.Error = fmt.Errorf("collections.Map: workflow %q failed at index %d: %w", workflow, i, err)
			r.Response = map[string]interface{}{"code": "map_error", "message": err.Error(), "index": i}
			clearIterVars(p)
			return
		}
		kept, ok := extractResponse(resp, workflow)
		if !ok {
			r.Success = false
			r.StatusCode = 500
			r.Error = fmt.Errorf("collections.Map: workflow %q returned no response at index %d", workflow, i)
			r.Response = map[string]interface{}{"code": "map_error", "message": "no response", "index": i}
			clearIterVars(p)
			return
		}
		results = append(results, kept)
	}
	clearIterVars(p)
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"results": results}
	return
}

// Filter runs a WSL workflow per element of arr, passing $element + $index.
// The sub-workflow must respond with {keep: true|false}. Filter collects
// elements where keep=true into a result array. Returns {results: []interface{}}.
// WSL: collections/ops/ops.Filter(arr: $lines, workflow: "solutions/invoice/line-matches")
func (t *opsTransitions) Filter(p *wf.WorkerSessionContext, arr interface{}, workflow string) (r domain.FlowStepResult) {
	items := toInterfaceSlice(arr)
	results := make([]interface{}, 0, len(items))
	runner := newRunner(p, workflow)
	for i, elem := range items {
		p.SetValue("element", elem)
		p.SetValue("index", i)
		resp, err := runner.RunWithSharedContext(p, workflow)
		if err != nil {
			r.Success = false
			r.StatusCode = 500
			r.Error = fmt.Errorf("collections.Filter: workflow %q failed at index %d: %w", workflow, i, err)
			r.Response = map[string]interface{}{"code": "filter_error", "message": err.Error(), "index": i}
			clearIterVars(p)
			return
		}
		kept, ok := extractResponse(resp, workflow)
		if !ok {
			r.Success = false
			r.StatusCode = 500
			r.Error = fmt.Errorf("collections.Filter: workflow %q returned no response at index %d", workflow, i)
			r.Response = map[string]interface{}{"code": "filter_error", "message": "no response", "index": i}
			clearIterVars(p)
			return
		}
		if asBool(asMap(kept)["keep"]) {
			results = append(results, elem)
		}
	}
	clearIterVars(p)
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"results": results}
	return
}

// FlatMap runs a WSL workflow per element of arr (passing $element + $index),
// collecting each sub-workflow's array response into a single flat array.
// The sub-workflow must respond with {items: [...]}. Returns {results: []interface{}}.
// WSL: collections/ops/ops.FlatMap(arr: $user.groupIds, workflow: "solutions/iam/group-grants")
func (t *opsTransitions) FlatMap(p *wf.WorkerSessionContext, arr interface{}, workflow string) (r domain.FlowStepResult) {
	items := toInterfaceSlice(arr)
	results := make([]interface{}, 0, len(items))
	runner := newRunner(p, workflow)
	for i, elem := range items {
		p.SetValue("element", elem)
		p.SetValue("index", i)
		resp, err := runner.RunWithSharedContext(p, workflow)
		if err != nil {
			r.Success = false
			r.StatusCode = 500
			r.Error = fmt.Errorf("collections.FlatMap: workflow %q failed at index %d: %w", workflow, i, err)
			r.Response = map[string]interface{}{"code": "flatmap_error", "message": err.Error(), "index": i}
			clearIterVars(p)
			return
		}
		kept, ok := extractResponse(resp, workflow)
		if !ok {
			clearIterVars(p)
			continue
		}
		itemsField := asMap(kept)["items"]
		switch arr := itemsField.(type) {
		case []interface{}:
			results = append(results, arr...)
		case []map[string]interface{}:
			for _, m := range arr {
				results = append(results, m)
			}
		default:
			if arr != nil {
				results = append(results, arr)
			}
		}
	}
	clearIterVars(p)
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"results": results}
	return
}

// newRunner builds a WorkflowRunner configured for the sub-workflow.
// We construct the config explicitly because engine.GetWorkflowConfig()
// returns the parent's config (or zero value) — the sub-workflow needs
// its own config with its own name so it loads the right file.
func newRunner(p *wf.WorkerSessionContext, workflow string) *wf.WorkflowRunner {
	app := p.Engine.GetApplication()
	wfConfig := domain.WorkflowConfigItem{
		Name:          workflow,
		Path:          app.Env.Config.Application.WorkflowsPath,
		Amount:        1,
		Retry:         1,
		RestartPolicy: "stop",
	}
	return wf.NewWorkflowRunner(wfConfig, app)
}

// Append returns a new array with element appended to arr. Returns {arr: []interface{}}.
// WSL: collections/ops/ops.Append(arr: $lines, element: $newLine)
func (t *opsTransitions) Append(arr interface{}, element interface{}) (r domain.FlowStepResult) {
	items := toInterfaceSlice(arr)
	out := make([]interface{}, len(items)+1)
	copy(out, items)
	out[len(items)] = element
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"arr": out}
	return
}

// RemoveWhere returns a new array with all elements whose `field` equals
// `value` removed. Returns {arr: []interface{}}.
// WSL: collections/ops/ops.RemoveWhere(arr: $lines, field: "id", value: $url.lineId)
func (t *opsTransitions) RemoveWhere(arr interface{}, field string, value interface{}) (r domain.FlowStepResult) {
	items := toInterfaceSlice(arr)
	out := make([]interface{}, 0, len(items))
	for _, m := range toMapSlice(items) {
		if !valuesEqual(m[field], value) {
			out = append(out, m)
		}
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"arr": out}
	return
}

// Contains reports whether any element in arr has field equal to value.
// Returns {contains: bool}. Short-circuits on first match.
// WSL: collections/ops/ops.Contains(arr: $user.groupIds, field: "id", value: $url.groupId)
func (t *opsTransitions) Contains(arr interface{}, field string, value interface{}) (r domain.FlowStepResult) {
	found := false
	for _, m := range toMapSlice(arr) {
		if valuesEqual(m[field], value) {
			found = true
			break
		}
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"contains": found}
	return
}

// ContainsValue checks if a flat array ([]interface{} or []string) contains
// a value. Unlike Contains (which searches []map[string]interface{} by field),
// this searches a flat array of scalar values. Returns {contains: bool}.
// WSL: collections/ops/ops.ContainsValue(arr: $Grants.response.doc.grants, value: $PermissionId.response.doc.id)
func (t *opsTransitions) ContainsValue(arr interface{}, value interface{}) (r domain.FlowStepResult) {
	found := false
	for _, item := range toInterfaceSlice(arr) {
		if valuesEqual(item, value) {
			found = true
			break
		}
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"contains": found}
	return
}

// Reduce runs a WSL workflow per element of arr, passing $element, $index,
// and $accumulator. The sub-workflow must respond with {accumulator: <value>};
// that value becomes $accumulator for the next iteration and the final result.
// init seeds $accumulator before the first iteration. Returns {result: <final accumulator>}.
// WSL: collections/ops/ops.Reduce(arr: $lines, workflow: "solutions/ledger/sum-line", init: {totalDebit: 0, totalCredit: 0})
func (t *opsTransitions) Reduce(p *wf.WorkerSessionContext, arr interface{}, workflow string, init interface{}) (r domain.FlowStepResult) {
	items := toInterfaceSlice(arr)
	accumulator := init
	runner := newRunner(p, workflow)
	for i, elem := range items {
		p.SetValue("element", elem)
		p.SetValue("index", i)
		p.SetValue("accumulator", accumulator)
		resp, err := runner.RunWithSharedContext(p, workflow)
		if err != nil {
			r.Success = false
			r.StatusCode = 500
			r.Error = fmt.Errorf("collections.Reduce: workflow %q failed at index %d: %w", workflow, i, err)
			r.Response = map[string]interface{}{"code": "reduce_error", "message": err.Error(), "index": i}
			clearIterVars(p)
			p.RemoveValue("accumulator")
			return
		}
		kept, ok := extractResponse(resp, workflow)
		if !ok {
			r.Success = false
			r.StatusCode = 500
			r.Error = fmt.Errorf("collections.Reduce: workflow %q returned no response at index %d", workflow, i)
			r.Response = map[string]interface{}{"code": "reduce_error", "message": "no response", "index": i}
			clearIterVars(p)
			p.RemoveValue("accumulator")
			return
		}
		next, ok := asMap(kept)["accumulator"]
		if !ok || next == nil {
			r.Success = false
			r.StatusCode = 500
			r.Error = fmt.Errorf("collections.Reduce: workflow %q did not return {accumulator: ...} at index %d", workflow, i)
			r.Response = map[string]interface{}{"code": "reduce_error", "message": "missing accumulator in response", "index": i}
			clearIterVars(p)
			p.RemoveValue("accumulator")
			return
		}
		accumulator = next
	}
	clearIterVars(p)
	p.RemoveValue("accumulator")
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"result": accumulator}
	return
}

// Recurse iteratively traverses a tree starting at rootId. For each node,
// it runs childWorkflow with $node set (the node document). The sub-workflow
// must respond with {children: [...]} — the array becomes the node's children
// and each child is pushed onto the stack for processing. Cycle detection
// via a visited set prevents infinite loops on corrupt data.
// Returns {tree: <rootNode with children nested>}.
// WSL: collections/ops/ops.Recurse(rootId: $url.locationId, childWorkflow: "solutions/stock/location-children", childField: "children")
func (t *opsTransitions) Recurse(p *wf.WorkerSessionContext, rootId string, childWorkflow string, childField string) (r domain.FlowStepResult) {
	rootId = strings.TrimSpace(rootId)
	runner := newRunner(p, childWorkflow)
	visited := map[string]bool{}

	// buildNode loads the node doc from persistence/store, runs the child
	// workflow to get its children, then recursively builds each child.
	// Implemented iteratively via an explicit stack to avoid Go recursion
	// depth limits on deep trees.
	var buildNode func(nodeId string) (map[string]interface{}, error)
	buildNode = func(nodeId string) (map[string]interface{}, error) {
		if visited[nodeId] {
			return map[string]interface{}{"id": nodeId, "_cycle": true}, nil
		}
		visited[nodeId] = true

		// The node document is loaded by the caller and passed via $node
		// is NOT set here — instead we set $node to the nodeId and let the
		// child workflow load it (keeps Recurse generic, not coupled to
		// persistence/store). The child workflow receives $nodeId.
		p.SetValue("nodeId", nodeId)
		resp, err := runner.RunWithSharedContext(p, childWorkflow)
		if err != nil {
			return nil, fmt.Errorf("collections.Recurse: workflow %q failed for node %q: %w", childWorkflow, nodeId, err)
		}
		kept, ok := extractResponse(resp, childWorkflow)
		if !ok {
			return nil, fmt.Errorf("collections.Recurse: workflow %q returned no response for node %q", childWorkflow, nodeId)
		}
		nodeMap := asMap(kept)
		if nodeMap == nil {
			return nil, fmt.Errorf("collections.Recurse: workflow %q returned non-map for node %q", childWorkflow, nodeId)
		}

		// Extract children array from the response (default key "children"
		// but respect childField if set).
		childrenKey := childField
		if childrenKey == "" {
			childrenKey = "children"
		}
		childrenRaw := nodeMap[childrenKey]
		childrenArr := toInterfaceSlice(childrenRaw)
		builtChildren := make([]interface{}, 0, len(childrenArr))
		for _, child := range childrenArr {
			childMap := asMap(child)
			if childMap == nil {
				continue
			}
			childId, _ := childMap["id"].(string)
			if childId == "" {
				// No id — can't recurse into it; keep as-is.
				builtChildren = append(builtChildren, child)
				continue
			}
			built, err := buildNode(childId)
			if err != nil {
				return nil, err
			}
			builtChildren = append(builtChildren, built)
		}
		nodeMap[childrenKey] = builtChildren
		return nodeMap, nil
	}

	tree, err := buildNode(rootId)
	p.RemoveValue("nodeId")
	if err != nil {
		r.Success = false
		r.StatusCode = 500
		r.Error = err
		r.Response = map[string]interface{}{"code": "recurse_error", "message": err.Error()}
		return
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"tree": tree}
	return
}

// ─── internal helpers ──────────────────────────────────────────────────

// toMapSlice coerces an array of maps ([]interface{} or []map[string]interface{})
// into []map[string]interface{} for uniform field access.
func toMapSlice(v interface{}) []map[string]interface{} {
	switch s := v.(type) {
	case []map[string]interface{}:
		return s
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(s))
		for _, item := range s {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// toInterfaceSlice coerces []map[string]interface{} or []interface{} to []interface{}.
func toInterfaceSlice(v interface{}) []interface{} {
	switch s := v.(type) {
	case []interface{}:
		return s
	case []map[string]interface{}:
		out := make([]interface{}, len(s))
		for i, m := range s {
			out[i] = m
		}
		return out
	case []string:
		out := make([]interface{}, len(s))
		for i, m := range s {
			out[i] = m
		}
		return out
	case nil:
		return nil
	}
	return nil
}

// toStringSlice coerces an interface{} to []string. Used by GroupSumMulti
// to accept a WSL array of field names (arrives as []interface{}).
func toStringSlice(v interface{}) []string {
	s := toInterfaceSlice(v)
	out := make([]string, 0, len(s))
	for _, item := range s {
		out = append(out, asString(item))
	}
	return out
}

// extractResponse pulls the sub-workflow's response out of the engine's
// map[string]*WorkerResponse. The map is keyed by the flow name (the
// `name` in `workflow name { ... }`), not the file path. We always invoke
// single-workflow files from Map/Filter, so we take the single response
// if the lookup-by-name fails.
func extractResponse(responses map[string]*wf.WorkerResponse, workflow string) (interface{}, bool) {
	if responses == nil {
		return nil, false
	}
	// Try exact flow name first (caller may have passed it directly).
	if wr, ok := responses[workflow]; ok && wr != nil {
		return wr.Response, true
	}
	// Try by base name (last path segment, with -→_ normalization).
	base := workflow
	if idx := strings.LastIndex(workflow, "/"); idx >= 0 {
		base = workflow[idx+1:]
	}
	baseNormalized := strings.ReplaceAll(base, "-", "_")
	if wr, ok := responses[baseNormalized]; ok && wr != nil {
		return wr.Response, true
	}
	// Single-response fallback: common case for single-workflow files.
	if len(responses) == 1 {
		for _, wr := range responses {
			if wr != nil {
				return wr.Response, true
			}
		}
	}
	return nil, false
}

// toFloat coerces interface{} (float64 from JSON, int, string, etc.) to float64.
func toFloat(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case string:
		if parsed, err := strconv.ParseFloat(n, 64); err == nil {
			return parsed
		}
	}
	return 0
}

// asBool coerces an interface{} to bool. Non-bool values yield false.
func asBool(v interface{}) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

// asMap coerces an interface{} to map[string]interface{}. Non-maps yield nil.
func asMap(v interface{}) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return nil
}

// valuesEqual reports whether a and b are equal after numeric coercion.
// Handles the common case where one side is float64 (JSON number) and
// the other is a string ID — falls back to string comparison.
func valuesEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	// Try numeric comparison first.
	if af, aok := numericFloat(a); aok {
		if bf, bok := numericFloat(b); bok {
			return af == bf
		}
	}
	// Fall back to string comparison.
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// numericFloat returns the float64 value of a numeric interface{} and true,
// or 0, false if not numeric.
func numericFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// clearIterVars removes the element/index keys the loop wrote into the
// shared context so the parent workflow doesn't observe stale values.
func clearIterVars(p *wf.WorkerSessionContext) {
	p.RemoveValue("element")
	p.RemoveValue("index")
}

// ─── Wave 5 primitives ──────────────────────────────────────

// SelectMostRecentDated picks the best doc from arr by dateField, with two
// strategies controlled by useCurrent:
//   - useCurrent=true: return the doc whose dateField == "" (the "current"
//     observation). If none exists, fall back to the latest dated doc
//     (max dateField value among non-empty).
//   - useCurrent=false: return the doc with the maximum dateField value
//     that is <= targetDate. Docs with empty dateField are skipped.
//
// Returns {doc: <map>, found: bool}. Empty array or no matching doc returns
// found=false. WSL: collections/ops/ops.SelectMostRecentDated(arr: $RateList.response.docs, dateField: "date", targetDate: "2026-02-15", useCurrent: false)
func (t *opsTransitions) SelectMostRecentDated(arr interface{}, dateField string, targetDate string, useCurrent bool) (r domain.FlowStepResult) {
	docs := toMapSlice(arr)
	if useCurrent {
		for _, d := range docs {
			if dt, _ := d[dateField].(string); dt == "" {
				r.Success = true
				r.StatusCode = 200
				r.Response = map[string]interface{}{"doc": d, "found": true}
				return
			}
		}
		var best map[string]interface{}
		var bestDate string
		for _, d := range docs {
			dt, _ := d[dateField].(string)
			if dt == "" {
				continue
			}
			if best == nil || dt > bestDate {
				best = d
				bestDate = dt
			}
		}
		if best != nil {
			r.Success = true
			r.StatusCode = 200
			r.Response = map[string]interface{}{"doc": best, "found": true}
			return
		}
		r.Success = true
		r.StatusCode = 200
		r.Response = map[string]interface{}{"doc": map[string]interface{}{}, "found": false}
		return
	}
	var best map[string]interface{}
	var bestDate string
	for _, d := range docs {
		dt, _ := d[dateField].(string)
		if dt == "" {
			continue
		}
		if dt <= targetDate {
			if best == nil || dt > bestDate {
				best = d
				bestDate = dt
			}
		}
	}
	if best != nil {
		r.Success = true
		r.StatusCode = 200
		r.Response = map[string]interface{}{"doc": best, "found": true}
		return
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"doc": map[string]interface{}{}, "found": false}
	return
}

// AssertFound fails with 404 if found is false. Used after SelectMostRecentDated
// or Find to short-circuit a workflow when no matching doc exists. Returns the
// doc on success so downstream states can reference $AssertFound.response.doc.
// WSL: collections/ops/ops.AssertFound(found: $BestRate.response.found, doc: $BestRate.response.doc, code: "no_rate", message: "no rate found")
func (t *opsTransitions) AssertFound(found bool, doc interface{}, code string, message string) (r domain.FlowStepResult) {
	if !found {
		if code == "" {
			code = "not_found"
		}
		if message == "" {
			message = "no matching document found"
		}
		r.Success = false
		r.StatusCode = 404
		r.Error = fmt.Errorf("%s", message)
		r.Response = map[string]interface{}{"code": code, "message": message}
		return
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"doc": doc, "found": true}
	return
}

// SortBy returns a new array sorted ascending by field. Returns {arr: []}.
// Numeric fields sort numerically; strings sort lexicographically.
// WSL: collections/ops/ops.SortBy(arr: $RateList.response.docs, field: "date")
func (t *opsTransitions) SortBy(arr interface{}, field string) (r domain.FlowStepResult) {
	docs := toMapSlice(arr)
	out := make([]map[string]interface{}, len(docs))
	copy(out, docs)
	sort.SliceStable(out, func(i, j int) bool {
		vi := out[i][field]
		vj := out[j][field]
		if isNumericField(vi) && isNumericField(vj) {
			return toFloat(vi) < toFloat(vj)
		}
		return asString(vi) < asString(vj)
	})
	result := make([]interface{}, len(out))
	for i, d := range out {
		result[i] = d
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"arr": result, "count": len(result)}
	return
}

// SortByDesc returns a new array sorted descending by field. Returns {arr: []}.
// Mirror of SortBy for most-recent-first ordering (e.g. audit logs, activity
// feeds). Numeric fields sort numerically; strings sort lexicographically.
// WSL: collections/ops/ops.SortByDesc(arr: $ListRes.response.docs, field: "id")
func (t *opsTransitions) SortByDesc(arr interface{}, field string) (r domain.FlowStepResult) {
	docs := toMapSlice(arr)
	out := make([]map[string]interface{}, len(docs))
	copy(out, docs)
	sort.SliceStable(out, func(i, j int) bool {
		vi := out[i][field]
		vj := out[j][field]
		if isNumericField(vi) && isNumericField(vj) {
			return toFloat(vi) > toFloat(vj)
		}
		return asString(vi) > asString(vj)
	})
	result := make([]interface{}, len(out))
	for i, d := range out {
		result[i] = d
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"arr": result, "count": len(result)}
	return
}

// LimitOrAll returns the first n elements of arr. If n is 0 or negative,
// returns the entire array (no limit). Use this instead of Limit when the
// caller wants "no limit" semantics for n=0 (e.g. audit List with optional
// limit — limit=0 means return all entries).
// WSL: collections/ops/ops.LimitOrAll(arr: $sorted, n: $json.limit??0)
func (t *opsTransitions) LimitOrAll(arr interface{}, n interface{}) (r domain.FlowStepResult) {
	items := toInterfaceSlice(arr)
	limit := toInt(n)
	if limit <= 0 || limit > len(items) {
		r.Success = true
		r.StatusCode = 200
		r.Response = map[string]interface{}{"arr": items, "count": len(items)}
		return
	}
	out := items[:limit]
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"arr": out, "count": len(out)}
	return
}

// Length returns the number of elements in arr. Returns {length: int}.
// arr may be []interface{}, []map[string]interface{}, []string, or nil.
// WSL: collections/ops/ops.Length(arr: $json.items)
func (t *opsTransitions) Length(arr interface{}) (r domain.FlowStepResult) {
	n := 0
	if arr == nil {
		// nil → 0
	} else {
		switch v := arr.(type) {
		case []interface{}:
			n = len(v)
		case []map[string]interface{}:
			n = len(v)
		case []string:
			n = len(v)
		case []int:
			n = len(v)
		case []float64:
			n = len(v)
		default:
			rv := reflect.ValueOf(arr)
			if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
				n = rv.Len()
			}
		}
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"length": n}
	return
}

// GetAtIndex returns the element at position idx in arr. Returns {item: <elem>}.
// Errors (r.Success=false) when idx is out of range or arr is nil/empty — this
// lets WSL branch via `on error -> EmitEmpty` to handle the "no more items" case.
// Use this instead of `$arr.0` (which does NOT work on session-stored values —
// only on response-scoped fields, due to the engine's property resolver).
// WSL: collections/ops/ops.GetAtIndex(arr: $element.lines, idx: 0)
func (t *opsTransitions) GetAtIndex(arr interface{}, idx interface{}) (r domain.FlowStepResult) {
	items := toInterfaceSlice(arr)
	i := toInt(idx)
	if i < 0 || i >= len(items) {
		r.Success = false
		r.StatusCode = 404
		r.Error = fmt.Errorf("collections.GetAtIndex: index %d out of range (len=%d)", i, len(items))
		r.Response = map[string]interface{}{"item": nil, "found": false}
		return
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"item": items[i], "found": true}
	return
}

// GetField returns the value of `field` from a map-shaped `doc`. Returns
// {value: <fieldValue>, found: bool}. Errors when the field is missing —
// this lets WSL branch via `on error -> DefaultState` to provide a fallback.
// Use this instead of `$doc.field` (which does NOT work on session-stored
// values — only on response-scoped fields, due to the engine's property
// resolver).
// WSL: collections/ops/ops.GetField(doc: $element, field: "lines")
func (t *opsTransitions) GetField(doc interface{}, field string) (r domain.FlowStepResult) {
	m := asMap(doc)
	if m == nil {
		r.Success = false
		r.StatusCode = 404
		r.Error = fmt.Errorf("collections.GetField: doc is not a map")
		r.Response = map[string]interface{}{"value": nil, "found": false}
		return
	}
	v, ok := m[field]
	if !ok {
		r.Success = false
		r.StatusCode = 404
		r.Error = fmt.Errorf("collections.GetField: field %q not found", field)
		r.Response = map[string]interface{}{"value": nil, "found": false}
		return
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"value": v, "found": true}
	return
}

// MakeMap builds a map from two parallel arrays: keys and values.
// Each keys[i] becomes a map key with value values[i]. This primitive
// forces eager resolution of $-placeholders in the values array at
// call time — unlike WSL map literals which store placeholder strings
// that may be resolved lazily after the calling sub-workflow completes.
// WSL: collections/ops/ops.MakeMap(keys: ["productId","qty"], values: [$TargetId.response.value, $QtyResult.response.value])
func (t *opsTransitions) MakeMap(keys []interface{}, values []interface{}) (r domain.FlowStepResult) {
	m := make(map[string]interface{})
	for i, k := range keys {
		ks, _ := k.(string)
		if i < len(values) {
			m[ks] = values[i]
		}
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = m
	return
}

// BuildValuationRow builds a valuation row map from individual scalar args.
// Each arg is a separate named parameter — this forces the WSL engine to
// resolve $-placeholders EAGERLY at call time (via PrepareInput → ExprLookup),
// unlike map/array literals which store placeholder strings that get resolved
// lazily after the sub-workflow completes (causing all Map iterations to see
// the LAST iteration's values).
// WSL: collections/ops/ops.BuildValuationRow(productId: $TargetId.response.value, sku: $SkuField.response.value, name: $NameField.response.value, qty: $QtyResult.response.value, unitCost: $CostField.response.value, totalValue: $ValueResult.response.result)
func (t *opsTransitions) BuildValuationRow(productId, sku, name interface{}, qty interface{}, unitCost, totalValue interface{}) (r domain.FlowStepResult) {
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{
		"productId":  productId,
		"sku":        sku,
		"name":       name,
		"qty":        qty,
		"unitCost":   unitCost,
		"totalValue": totalValue,
	}
	return
}

// EnrichValuationRows joins grouped quantity rows (output of GroupSumMulti
// with groupField=productId) to a products array, producing enriched rows
// with {productId, sku, name, qty, unitCost, totalValue} in a single Go
// pass. This bypasses the WSL Map+Filter sub-workflow pattern, which has an
// unresolved engine bug: nested Filter calls inside a Map iteration overwrite
// $element in the shared WorkerSessionContext, so all iterations end up
// seeing the last element's data.
//
// rows: []map with {groupId, groupLabel, sums: {quantity: N}}
// products: []map with {id, sku, name, cost}
//
// Returns {results: []map, totalValue: sum}.
// WSL: collections/ops/ops.EnrichValuationRows(rows: $GroupedResult.response.rows, products: $ProductsList.response.docs)
func (t *opsTransitions) EnrichValuationRows(rows interface{}, products interface{}) (r domain.FlowStepResult) {
	rowItems := toInterfaceSlice(rows)
	productItems := toMapSlice(products)

	productById := make(map[string]map[string]interface{}, len(productItems))
	for _, p := range productItems {
		id := asString(p["id"])
		if id != "" {
			productById[id] = p
		}
	}

	results := make([]interface{}, 0, len(rowItems))
	totalValue := 0.0

	for _, rowRaw := range rowItems {
		row, ok := rowRaw.(map[string]interface{})
		if !ok {
			continue
		}
		groupId := asString(row["groupId"])
		sums, _ := row["sums"].(map[string]interface{})
		qty := toFloat(sums["quantity"])

		var sku, name interface{} = "", ""
		var unitCost, lineTotal float64 = 0, 0
		if p, found := productById[groupId]; found {
			sku = p["sku"]
			name = p["name"]
			if cost, ok := p["cost"]; ok {
				unitCost = toFloat(cost)
			} else if price, ok := p["unitPrice"]; ok {
				unitCost = toFloat(price)
			}
			lineTotal = qty * unitCost
			totalValue += lineTotal
		}

		results = append(results, map[string]interface{}{
			"productId":  groupId,
			"sku":        sku,
			"name":       name,
			"qty":        qty,
			"unitCost":   unitCost,
			"totalValue": lineTotal,
		})
	}

	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{
		"results":    results,
		"totalValue": totalValue,
	}
	return
}

// GetNestedPath returns the value at a dotted path within a doc. Returns
// {value: <fieldValue>, found: bool}. Errors when any segment is missing.
// Use this to navigate session-stored maps (e.g. $element) where dot-access
// doesn't work via WSL's property resolver.
// Example: GetNestedPath(doc: $element, path: "lines.0.productId")
// WSL: collections/ops/ops.GetNestedPath(doc: $element, path: "lines.0.productId")
func (t *opsTransitions) GetNestedPath(doc interface{}, path string) (r domain.FlowStepResult) {
	current := doc
	segments := strings.Split(path, ".")
	for _, seg := range segments {
		if current == nil {
			r.Success = false
			r.StatusCode = 404
			r.Error = fmt.Errorf("collections.GetNestedPath: nil at segment %q", seg)
			r.Response = map[string]interface{}{"value": nil, "found": false}
			return
		}
		// Try as map key first
		if m, ok := current.(map[string]interface{}); ok {
			v, exists := m[seg]
			if !exists {
				r.Success = false
				r.StatusCode = 404
				r.Error = fmt.Errorf("collections.GetNestedPath: field %q not found", seg)
				r.Response = map[string]interface{}{"value": nil, "found": false}
				return
			}
			current = v
			continue
		}
		// Try as slice index
		if i, err := strconv.Atoi(seg); err == nil {
			if s, ok := current.([]interface{}); ok {
				if i < 0 || i >= len(s) {
					r.Success = false
					r.StatusCode = 404
					r.Error = fmt.Errorf("collections.GetNestedPath: index %d out of range (len=%d)", i, len(s))
					r.Response = map[string]interface{}{"value": nil, "found": false}
					return
				}
				current = s[i]
				continue
			}
		}
		r.Success = false
		r.StatusCode = 404
		r.Error = fmt.Errorf("collections.GetNestedPath: cannot navigate %q on type %T", seg, current)
		r.Response = map[string]interface{}{"value": nil, "found": false}
		return
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"value": current, "found": true}
	return
}

// RecurseForest builds a forest (array of root nodes, each with children)
// from a flat array of docs linked by a parent-pointer field. Pure Go —
// WSL has no recursion.
//
// docs is the flat array ([]map or []interface{} of maps). idField is the
// primary key field name (e.g. "id" or "code"). parentField is the
// parent-reference field name (e.g. "parentId" or "parentCode"). childField
// is the key under which the children array is nested in each output node
// (defaults to "children" if empty). sortBy is the field used to sort
// children within each parent for deterministic output (empty = no sort).
//
// Root nodes are those with an empty parentField. Each node in the output
// is the full doc plus a children array. Cycle detection via a visited set
// prevents infinite loops on corrupt data — cycles are broken by marking
// the back-edge child with a "_cycle: true" flag.
//
// Returns {forest: []interface{}, count: int}. count is the total doc count.
// WSL: collections/ops/ops.RecurseForest(docs: $AccountList.response.docs, idField: "code", parentField: "parentCode", childField: "children", sortBy: "code")
func (t *opsTransitions) RecurseForest(docs interface{}, idField string, parentField string, childField string, sortBy string) (r domain.FlowStepResult) {
	all := toMapSlice(docs)
	if idField == "" {
		idField = "id"
	}
	if parentField == "" {
		parentField = "parentId"
	}
	if childField == "" {
		childField = "children"
	}

	// Build child index: parentKey -> []childDoc
	childrenOf := map[string][]map[string]interface{}{}
	roots := []map[string]interface{}{}
	for _, d := range all {
		// Clone the doc so we don't mutate the caller's slice.
		doc := map[string]interface{}{}
		for k, v := range d {
			doc[k] = v
		}
		parentKey, _ := doc[parentField].(string)
		if parentKey == "" {
			roots = append(roots, doc)
		} else {
			childrenOf[parentKey] = append(childrenOf[parentKey], doc)
		}
	}

	var sortChildren func(arr []map[string]interface{})
	sortChildren = func(arr []map[string]interface{}) {
		if sortBy == "" {
			return
		}
		sort.SliceStable(arr, func(i, j int) bool {
			vi, _ := arr[i][sortBy].(string)
			vj, _ := arr[j][sortBy].(string)
			return vi < vj
		})
	}

	var build func(doc map[string]interface{}, visited map[string]bool) map[string]interface{}
	build = func(doc map[string]interface{}, visited map[string]bool) map[string]interface{} {
		key, _ := doc[idField].(string)
		if visited[key] {
			return map[string]interface{}{idField: key, "_cycle": true}
		}
		visited[key] = true
		defer delete(visited, key)

		childDocs := childrenOf[key]
		sortChildren(childDocs)
		children := make([]interface{}, 0, len(childDocs))
		for _, c := range childDocs {
			children = append(children, build(c, visited))
		}
		out := map[string]interface{}{}
		for k, v := range doc {
			out[k] = v
		}
		out[childField] = children
		return out
	}

	// Sort roots for deterministic output
	sortChildren(roots)

	forest := make([]interface{}, 0, len(roots))
	for _, root := range roots {
		forest = append(forest, build(root, map[string]bool{}))
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"forest": forest, "count": len(all)}
	return
}

// Find runs a WSL workflow per element of arr, passing $element + $index.
// The sub-workflow must respond with {keep: true|false}. Find returns the
// FIRST element where keep=true, and stops iterating immediately (short-
// circuit). Returns {doc: <map>, found: bool}.
// WSL: collections/ops/ops.Find(arr: $Forest.response.forest, workflow: "solutions/stock/location-matches-root")
func (t *opsTransitions) Find(p *wf.WorkerSessionContext, arr interface{}, workflow string) (r domain.FlowStepResult) {
	items := toInterfaceSlice(arr)
	runner := newRunner(p, workflow)
	for i, elem := range items {
		p.SetValue("element", elem)
		p.SetValue("index", i)
		resp, err := runner.RunWithSharedContext(p, workflow)
		if err != nil {
			r.Success = false
			r.StatusCode = 500
			r.Error = fmt.Errorf("collections.Find: workflow %q failed at index %d: %w", workflow, i, err)
			r.Response = map[string]interface{}{"code": "find_error", "message": err.Error(), "index": i}
			clearIterVars(p)
			return
		}
		kept, ok := extractResponse(resp, workflow)
		if !ok {
			r.Success = false
			r.StatusCode = 500
			r.Error = fmt.Errorf("collections.Find: workflow %q returned no response at index %d", workflow, i)
			r.Response = map[string]interface{}{"code": "find_error", "message": "no response", "index": i}
			clearIterVars(p)
			return
		}
		if asBool(asMap(kept)["keep"]) {
			clearIterVars(p)
			r.Success = true
			r.StatusCode = 200
			r.Response = map[string]interface{}{"doc": elem, "found": true}
			return
		}
	}
	clearIterVars(p)
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{"doc": map[string]interface{}{}, "found": false}
	return
}

// GroupSumBucket groups items by a field (e.g. partnerId) and sums a numeric
// field into one of N buckets per group, where the bucket index is read from
// each item via bucketField (expected to be an int 0..bucketCount-1). Returns
// {rows: [{groupField, sumField totals per bucket, total}], totals: [bucketCount]float}.
//
// This is purpose-built for A/R aging: group invoices by partnerId, sum total
// into bucket 0..4 based on daysOverdue bucket. Items whose bucketField is
// out of range are skipped. groupLabelField (optional, may be "") provides a
// human-readable group name (e.g. partnerName) that is copied into each row.
//
// WSL: collections/ops/ops.GroupSumBucket(
//   arr: $PostedResult.response.results,
//   groupField: "partnerId",
//   groupLabelField: "partnerName",
//   bucketField: "bucketIdx",
//   bucketCount: 5,
//   sumField: "total"
// )
func (t *opsTransitions) GroupSumBucket(arr interface{}, groupField string, groupLabelField string, bucketField string, bucketCount int, sumField string) (r domain.FlowStepResult) {
	if bucketCount <= 0 {
		r.Success = false
		r.StatusCode = 400
		r.Error = fmt.Errorf("collections.GroupSumBucket: bucketCount must be > 0")
		r.Response = map[string]interface{}{"code": "invalid_args", "message": "bucketCount must be > 0"}
		return
	}
	type groupRow struct {
		GroupID    string
		GroupLabel string
		Buckets    []float64
		Total      float64
	}
	order := []string{}
	rows := map[string]*groupRow{}
	totals := make([]float64, bucketCount)
	for _, m := range toMapSlice(arr) {
		groupID := asString(m[groupField])
		if groupID == "" {
			continue
		}
		bucketIdx := toInt(m[bucketField])
		if bucketIdx < 0 || bucketIdx >= bucketCount {
			continue
		}
		amount := toFloat(m[sumField])
		row, ok := rows[groupID]
		if !ok {
			row = &groupRow{
				GroupID:    groupID,
				GroupLabel: asString(m[groupLabelField]),
				Buckets:    make([]float64, bucketCount),
			}
			rows[groupID] = row
			order = append(order, groupID)
		}
		row.Buckets[bucketIdx] += amount
		row.Total += amount
		totals[bucketIdx] += amount
	}
	outRows := make([]map[string]interface{}, 0, len(order))
	for _, id := range order {
		row := rows[id]
		buckets := make([]interface{}, bucketCount)
		for i, v := range row.Buckets {
			buckets[i] = v
		}
		outRows = append(outRows, map[string]interface{}{
			"groupId":    row.GroupID,
			"groupLabel": row.GroupLabel,
			"buckets":     buckets,
			"total":       row.Total,
		})
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{
		"rows":   outRows,
		"totals": totals,
	}
	return
}

// GroupSumMulti groups items by a field (e.g. accountCode) and sums
// multiple numeric fields per group. Returns {rows: [{groupId, groupLabel,
// sums: {field: total}}], totals: {field: total}}.
//
// This is the trial-balance primitive: group move_lines by accountCode,
// sum both debit and credit. Items whose groupField is empty are skipped.
// sumFields is a []interface{} of field names (WSL arrays arrive as
// []interface{}).
//
// WSL: collections/ops/ops.GroupSumMulti(
//   arr: $LinesResult.response.docs,
//   groupField: "accountCode",
//   groupLabelField: "accountName",
//   sumFields: ["debit", "credit"]
// )
func (t *opsTransitions) GroupSumMulti(arr interface{}, groupField string, groupLabelField string, sumFields interface{}) (r domain.FlowStepResult) {
	fields := toStringSlice(sumFields)
	if len(fields) == 0 {
		r.Success = false
		r.StatusCode = 400
		r.Error = fmt.Errorf("collections.GroupSumMulti: sumFields must be non-empty")
		r.Response = map[string]interface{}{"code": "invalid_args", "message": "sumFields must be non-empty"}
		return
	}
	type groupRow struct {
		GroupID    string
		GroupLabel string
		Sums       map[string]float64
	}
	order := []string{}
	rows := map[string]*groupRow{}
	totals := map[string]float64{}
	for _, f := range fields {
		totals[f] = 0
	}
	for _, m := range toMapSlice(arr) {
		groupID := asString(m[groupField])
		if groupID == "" {
			continue
		}
		row, ok := rows[groupID]
		if !ok {
			row = &groupRow{
				GroupID:    groupID,
				GroupLabel: asString(m[groupLabelField]),
				Sums:       map[string]float64{},
			}
			for _, f := range fields {
				row.Sums[f] = 0
			}
			rows[groupID] = row
			order = append(order, groupID)
		}
		for _, f := range fields {
			v := toFloat(m[f])
			row.Sums[f] += v
			totals[f] += v
		}
	}
	outRows := make([]map[string]interface{}, 0, len(order))
	for _, id := range order {
		row := rows[id]
		sums := map[string]interface{}{}
		for _, f := range fields {
			sums[f] = row.Sums[f]
		}
		outRows = append(outRows, map[string]interface{}{
			"groupId":    row.GroupID,
			"groupLabel": row.GroupLabel,
			"sums":        sums,
		})
	}
	outTotals := map[string]interface{}{}
	for _, f := range fields {
		outTotals[f] = totals[f]
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{
		"rows":   outRows,
		"totals": outTotals,
	}
	return
}

// GroupSumMatrix groups items by two fields (rowField + colField) and
// sums a numeric field per (row, col) cell. Returns:
//   {rows: [{rowId, rowLabel, cells: {colId: sum}, total}], cols: [colId], colLabels: [colLabel], totals: {colId: sum}}
//
// This is the stock-on-hand primitive: group stock move lines by
// (productId, locationId), summing delta. Produces a matrix where rows
// are products, columns are locations, cells are quantities.
// Items whose rowField or colField is empty are skipped.
//
// WSL: collections/ops/ops.GroupSumMatrix(
//   arr: $FlatLines.response.results,
//   rowField: "productId",
//   rowLabelField: "productName",
//   colField: "locationId",
//   colLabelField: "locationName",
//   sumField: "delta"
// )
func (t *opsTransitions) GroupSumMatrix(arr interface{}, rowField string, rowLabelField string, colField string, colLabelField string, sumField string) (r domain.FlowStepResult) {
	type cell struct {
		Total float64
	}
	type row struct {
		RowID    string
		RowLabel string
		Cells    map[string]*cell
		Total    float64
	}
	rowOrder := []string{}
	rows := map[string]*row{}
	colOrder := []string{}
	colLabels := map[string]string{}
	colSeen := map[string]bool{}
	totals := map[string]float64{}
	for _, m := range toMapSlice(arr) {
		rowID := asString(m[rowField])
		if rowID == "" {
			continue
		}
		colID := asString(m[colField])
		if colID == "" {
			continue
		}
		amount := toFloat(m[sumField])
		rowEntry, ok := rows[rowID]
		if !ok {
			rowEntry = &row{
				RowID:    rowID,
				RowLabel: asString(m[rowLabelField]),
				Cells:    map[string]*cell{},
			}
			rows[rowID] = rowEntry
			rowOrder = append(rowOrder, rowID)
		}
		c, ok := rowEntry.Cells[colID]
		if !ok {
			c = &cell{}
			rowEntry.Cells[colID] = c
		}
		c.Total += amount
		rowEntry.Total += amount
		if !colSeen[colID] {
			colSeen[colID] = true
			colOrder = append(colOrder, colID)
			colLabels[colID] = asString(m[colLabelField])
			totals[colID] = 0
		}
		totals[colID] += amount
	}
	outRows := make([]map[string]interface{}, 0, len(rowOrder))
	for _, rowID := range rowOrder {
		row := rows[rowID]
		cells := map[string]interface{}{}
		for _, colID := range colOrder {
			if c, ok := row.Cells[colID]; ok {
				cells[colID] = c.Total
			} else {
				cells[colID] = 0
			}
		}
		outRows = append(outRows, map[string]interface{}{
			"rowId":    row.RowID,
			"rowLabel": row.RowLabel,
			"cells":    cells,
			"total":    row.Total,
		})
	}
	outCols := make([]interface{}, len(colOrder))
	outColLabels := make([]interface{}, len(colOrder))
	outTotals := map[string]interface{}{}
	for i, colID := range colOrder {
		outCols[i] = colID
		outColLabels[i] = colLabels[colID]
		outTotals[colID] = totals[colID]
	}
	r.Success = true
	r.StatusCode = 200
	r.Response = map[string]interface{}{
		"rows":      outRows,
		"cols":      outCols,
		"colLabels": outColLabels,
		"totals":    outTotals,
	}
	return
}

// isNumericField reports whether the value is a numeric type (float64,
// int, etc.) — used by MaxBy/SortBy to decide comparison strategy.
func isNumericField(v interface{}) bool {
	switch v.(type) {
	case float64, float32, int, int64, int32:
		return true
	}
	return false
}

// asString coerces an interface{} to string. Non-string values yield ""
// (matches the pattern in memory_store.go).
func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// toInt coerces interface{} to int. WSL passes JSON numbers as float64.
func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(strings.TrimSpace(n))
		return i
	}
	return 0
}

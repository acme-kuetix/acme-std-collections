package transitions

import (
	"testing"
)

func TestOpsSortBy(t *testing.T) {
	tr := &opsTransitions{}
	docs := []interface{}{
		map[string]interface{}{"id": "3", "date": "2026-03-01"},
		map[string]interface{}{"id": "1", "date": "2026-01-01"},
		map[string]interface{}{"id": "2", "date": "2026-02-01"},
	}

	r := tr.SortBy(docs, "date")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	arr := r.Response.(map[string]interface{})["arr"].([]interface{})
	if len(arr) != 3 {
		t.Fatalf("arr len = %d, want 3", len(arr))
	}
	first := arr[0].(map[string]interface{})
	if first["date"] != "2026-01-01" {
		t.Errorf("first date = %v, want 2026-01-01", first["date"])
	}
	last := arr[2].(map[string]interface{})
	if last["date"] != "2026-03-01" {
		t.Errorf("last date = %v, want 2026-03-01", last["date"])
	}
}

func TestOpsSum(t *testing.T) {
	tr := &opsTransitions{}
	docs := []interface{}{
		map[string]interface{}{"id": "1", "amount": 100.0},
		map[string]interface{}{"id": "2", "amount": 50.0},
		map[string]interface{}{"id": "3", "amount": 25.5},
	}

	r := tr.Sum(docs, "amount")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	sum := r.Response.(map[string]interface{})["sum"].(float64)
	if sum != 175.5 {
		t.Errorf("sum = %v, want 175.5", sum)
	}

	r = tr.Sum([]interface{}{}, "amount")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	sum = r.Response.(map[string]interface{})["sum"].(float64)
	if sum != 0 {
		t.Errorf("sum = %v, want 0", sum)
	}
}

func TestOpsAppend(t *testing.T) {
	tr := &opsTransitions{}
	arr := []interface{}{"a", "b"}

	r := tr.Append(arr, "c")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	out := r.Response.(map[string]interface{})["arr"].([]interface{})
	if len(out) != 3 {
		t.Fatalf("len = %d, want 3", len(out))
	}
	if out[2] != "c" {
		t.Errorf("out[2] = %v, want c", out[2])
	}

	r = tr.Append([]interface{}{}, "first")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	out = r.Response.(map[string]interface{})["arr"].([]interface{})
	if len(out) != 1 || out[0] != "first" {
		t.Errorf("out = %v, want [first]", out)
	}
}

func TestOpsRemoveWhere(t *testing.T) {
	tr := &opsTransitions{}
	docs := []interface{}{
		map[string]interface{}{"id": "1", "status": "active"},
		map[string]interface{}{"id": "2", "status": "deleted"},
		map[string]interface{}{"id": "3", "status": "active"},
		map[string]interface{}{"id": "4", "status": "deleted"},
	}

	r := tr.RemoveWhere(docs, "status", "deleted")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	out := r.Response.(map[string]interface{})["arr"].([]interface{})
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2", len(out))
	}
	for _, doc := range out {
		if doc.(map[string]interface{})["status"] != "active" {
			t.Errorf("expected all active, got %v", doc)
		}
	}

	r = tr.RemoveWhere(docs, "status", "archived")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	out = r.Response.(map[string]interface{})["arr"].([]interface{})
	if len(out) != 4 {
		t.Errorf("len = %d, want 4 (no removals)", len(out))
	}
}

func TestOpsContains(t *testing.T) {
	tr := &opsTransitions{}
	docs := []interface{}{
		map[string]interface{}{"id": "G1", "name": "Admins"},
		map[string]interface{}{"id": "G2", "name": "Users"},
	}

	r := tr.Contains(docs, "id", "G1")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	if !r.Response.(map[string]interface{})["contains"].(bool) {
		t.Error("expected contains=true for G1")
	}

	r = tr.Contains(docs, "id", "G99")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	if r.Response.(map[string]interface{})["contains"].(bool) {
		t.Error("expected contains=false for G99")
	}

	r = tr.Contains([]interface{}{}, "id", "G1")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	if r.Response.(map[string]interface{})["contains"].(bool) {
		t.Error("expected contains=false for empty array")
	}
}

func TestOpsSortByDesc(t *testing.T) {
	tr := &opsTransitions{}
	docs := []interface{}{
		map[string]interface{}{"id": "1", "date": "2026-01-01"},
		map[string]interface{}{"id": "2", "date": "2026-03-01"},
		map[string]interface{}{"id": "3", "date": "2026-02-01"},
	}

	r := tr.SortByDesc(docs, "date")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	arr := r.Response.(map[string]interface{})["arr"].([]interface{})
	if len(arr) != 3 {
		t.Fatalf("arr len = %d, want 3", len(arr))
	}
	first := arr[0].(map[string]interface{})
	if first["date"] != "2026-03-01" {
		t.Errorf("first date = %v, want 2026-03-01 (desc)", first["date"])
	}
	last := arr[2].(map[string]interface{})
	if last["date"] != "2026-01-01" {
		t.Errorf("last date = %v, want 2026-01-01 (desc)", last["date"])
	}

	numDocs := []interface{}{
		map[string]interface{}{"id": "1", "rate": 0.9},
		map[string]interface{}{"id": "2", "rate": 0.92},
		map[string]interface{}{"id": "3", "rate": 0.91},
	}
	r = tr.SortByDesc(numDocs, "rate")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	arr = r.Response.(map[string]interface{})["arr"].([]interface{})
	first = arr[0].(map[string]interface{})
	if first["id"] != "2" {
		t.Errorf("first rate id = %v, want 2", first["id"])
	}
}

func TestOpsLimitOrAll(t *testing.T) {
	tr := &opsTransitions{}
	arr := []interface{}{"a", "b", "c", "d", "e"}

	r := tr.LimitOrAll(arr, 3)
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	out := r.Response.(map[string]interface{})["arr"].([]interface{})
	if len(out) != 3 {
		t.Errorf("limit 3 → len = %d, want 3", len(out))
	}

	r = tr.LimitOrAll(arr, 0)
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	out = r.Response.(map[string]interface{})["arr"].([]interface{})
	if len(out) != 5 {
		t.Errorf("limit 0 → len = %d, want 5 (all)", len(out))
	}

	r = tr.LimitOrAll(arr, 100)
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	out = r.Response.(map[string]interface{})["arr"].([]interface{})
	if len(out) != 5 {
		t.Errorf("limit 100 → len = %d, want 5 (all)", len(out))
	}
}

func TestOpsAssertFound(t *testing.T) {
	tr := &opsTransitions{}

	doc := map[string]interface{}{"id": "1", "name": "test"}
	r := tr.AssertFound(true, doc, "", "")
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	if r.StatusCode != 200 {
		t.Errorf("status = %d, want 200", r.StatusCode)
	}
	resp := r.Response.(map[string]interface{})
	if resp["doc"] == nil {
		t.Error("expected doc in response")
	}
	if !resp["found"].(bool) {
		t.Error("expected found=true in response")
	}

	r = tr.AssertFound(false, nil, "", "")
	if r.Success {
		t.Error("expected failure when found=false")
	}
	if r.StatusCode != 404 {
		t.Errorf("status = %d, want 404", r.StatusCode)
	}
	resp = r.Response.(map[string]interface{})
	if resp["code"] != "not_found" {
		t.Errorf("code = %v, want not_found", resp["code"])
	}

	r = tr.AssertFound(false, nil, "year_not_found", "fiscal year 2025 not found")
	if r.Success {
		t.Error("expected failure when found=false")
	}
	if r.StatusCode != 404 {
		t.Errorf("status = %d, want 404", r.StatusCode)
	}
	resp = r.Response.(map[string]interface{})
	if resp["code"] != "year_not_found" {
		t.Errorf("code = %v, want year_not_found", resp["code"])
	}
	if resp["message"] != "fiscal year 2025 not found" {
		t.Errorf("message = %v, want fiscal year 2025 not found", resp["message"])
	}
}

func TestOpsSelectMostRecentDated(t *testing.T) {
	tr := &opsTransitions{}
	docs := []interface{}{
		map[string]interface{}{"id": "1", "date": "2026-01-01", "rate": 0.9},
		map[string]interface{}{"id": "2", "date": "2026-02-15", "rate": 0.92},
		map[string]interface{}{"id": "3", "date": "", "rate": 0.85},
		map[string]interface{}{"id": "4", "date": "2026-01-15", "rate": 0.91},
	}

	r := tr.SelectMostRecentDated(docs, "date", "", true)
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	doc := r.Response.(map[string]interface{})["doc"].(map[string]interface{})
	if doc["id"] != "3" {
		t.Errorf("expected id=3 (current), got %v", doc["id"])
	}

	noCurrent := []interface{}{
		map[string]interface{}{"id": "1", "date": "2026-01-01"},
		map[string]interface{}{"id": "2", "date": "2026-02-15"},
		map[string]interface{}{"id": "3", "date": "2026-03-01"},
	}
	// useCurrent=true but no empty-date doc → picks max date overall (Mar 1).
	r = tr.SelectMostRecentDated(noCurrent, "date", "2026-02-20", true)
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	doc = r.Response.(map[string]interface{})["doc"].(map[string]interface{})
	if doc["id"] != "3" {
		t.Errorf("expected id=3 (max date), got %v", doc["id"])
	}

	// useCurrent=false → picks max date ≤ target.
	r = tr.SelectMostRecentDated(noCurrent, "date", "2026-02-20", false)
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	doc = r.Response.(map[string]interface{})["doc"].(map[string]interface{})
	if doc["id"] != "2" {
		t.Errorf("expected id=2 (Feb 15 ≤ Feb 20), got %v", doc["id"])
	}

	r = tr.SelectMostRecentDated(noCurrent, "date", "2025-01-01", false)
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	if r.Response.(map[string]interface{})["found"].(bool) {
		t.Error("expected found=false when no dates match")
	}

	r = tr.SelectMostRecentDated([]interface{}{}, "date", "2026-01-01", false)
	if !r.Success {
		t.Fatalf("expected success, got: %v", r.Error)
	}
	if r.Response.(map[string]interface{})["found"].(bool) {
		t.Error("expected found=false for empty array")
	}
}

func TestLength(t *testing.T) {
	tr := &opsTransitions{}
	r := tr.Length([]interface{}{"a", "b", "c"})
	if n := r.Response.(map[string]interface{})["length"]; n != 3 {
		t.Errorf("length = %v, want 3", n)
	}
	r = tr.Length([]interface{}{})
	if n := r.Response.(map[string]interface{})["length"]; n != 0 {
		t.Errorf("length = %v, want 0 (empty)", n)
	}
	r = tr.Length(nil)
	if n := r.Response.(map[string]interface{})["length"]; n != 0 {
		t.Errorf("length = %v, want 0 (nil)", n)
	}
	r = tr.Length([]string{"x", "y"})
	if n := r.Response.(map[string]interface{})["length"]; n != 2 {
		t.Errorf("length = %v, want 2 ([]string)", n)
	}
}


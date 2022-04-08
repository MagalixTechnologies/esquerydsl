package esquerydsl

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestBogusQueryType(t *testing.T) {
	_, err := json.Marshal(QueryDoc{
		Index: "some_index",
		Sort:  []map[string]string{{"id": "asc"}},
		And: []QueryItem{
			{
				Field: "some_index_id",
				Value: "some-long-key-id-value",
				Type:  100001,
			},
		},
	})

	var queryTypeErr *QueryTypeErr
	if !errors.As(err, &queryTypeErr) {
		t.Errorf("\nUnexpected error: %v", err)
	}
}

func TestQueryStringEsc(t *testing.T) {
	body, _ := json.Marshal(QueryDoc{
		Index: "some_index",
		And: []QueryItem{
			{
				Field: "user.id",
				Value: "kimchy!",
				Type:  QueryString,
			},
		},
	})

	expected := `{"query":{"bool":{"must":[{"query_string":{"analyze_wildcard":true,"fields":["user.id"],"query":"kimchy\\!"}}]}},"size":0}`
	if string(body) != expected {
		t.Errorf("\nWant: %q\nHave: %q", expected, string(body))
	}
}

func TestMultiSearchDoc(t *testing.T) {
	doc, _ := MultiSearchDoc([]QueryDoc{
		{
			Index: "index1",
			And: []QueryItem{
				{
					Field: "user.id",
					Value: "kimchy!",
					Type:  QueryString,
				},
			},
		},
		{
			Index: "index2",
			And: []QueryItem{
				{
					Field: "some_index_id",
					Value: "some-long-key-id-value",
					Type:  Match,
				},
			},
		},
	})

	expected := `{"index":"index1"}
{"query":{"bool":{"must":[{"query_string":{"analyze_wildcard":true,"fields":["user.id"],"query":"kimchy\\!"}}]}},"size":0}
{"index":"index2"}
{"query":{"bool":{"must":[{"match":{"some_index_id":"some-long-key-id-value"}}]}},"size":0}
`
	if string(doc) != expected {
		t.Errorf("\nWant: %q\nHave: %q", expected, string(doc))
	}
}

func TestAndQuery(t *testing.T) {
	body, _ := json.Marshal(QueryDoc{
		Index: "some_index",
		Sort:  []map[string]string{{"id": "asc"}},
		And: []QueryItem{
			{
				Field: "some_index_id",
				Value: "some-long-key-id-value",
				Type:  Match,
			},
		},
	})

	expected := `{"query":{"bool":{"must":[{"match":{"some_index_id":"some-long-key-id-value"}}]}},"size":0,"sort":[{"id":"asc"}]}`
	if string(body) != expected {
		t.Errorf("\nWant: %q\nHave: %q", expected, string(body))
	}
}

func TestFilterQuery(t *testing.T) {
	body, _ := json.Marshal(QueryDoc{
		Index: "some_index",
		Size:  20,
		And: []QueryItem{
			{
				Field: "title",
				Value: "Search",
				Type:  Match,
			},
			{
				Field: "content",
				Value: "Elasticsearch",
				Type:  Match,
			},
		},
		Filter: []QueryItem{
			{
				Field: "status",
				Value: "published",
				Type:  Term,
			},
			{
				Field: "publish_date",
				Value: map[string]string{
					"gte": "2015-01-01",
				},
				Type: Range,
			},
		},
	})

	expected := `{"query":{"bool":{"must":[{"match":{"title":"Search"}},{"match":{"content":"Elasticsearch"}}],"filter":[{"term":{"status":"published"}},{"range":{"publish_date":{"gte":"2015-01-01"}}}]}},"size":20}`
	if string(body) != expected {
		t.Errorf("\nWant: %q\nHave: %q", expected, string(body))
	}
}

func TestTrackTotalHits(t *testing.T) {
	body, _ := json.Marshal(QueryDoc{
		Index: "some_index",
		Sort:  []map[string]string{{"id": "asc"}},
		Size:  100,
		And: []QueryItem{
			{
				Field: "some_index_id",
				Value: "some-long-key-id-value",
				Type:  Match,
			},
		}, TrackTotalHits: true,
	})

	expected := `{"query":{"bool":{"must":[{"match":{"some_index_id":"some-long-key-id-value"}}]}},"size":100,"sort":[{"id":"asc"}],"track_total_hits":true}`
	if string(body) != expected {
		t.Errorf("\nWant: %q\nHave: %q", expected, string(body))
	}
}

func TestOrQuery(t *testing.T) {
	body, _ := json.Marshal(QueryDoc{
		Index: "some_index",
		Sort:  []map[string]string{{"id": "asc"}},
		Size:  10,
		Or: []QueryItem{
			{
				Field: "some_index_id",
				Value: "some-long-key-id-value",
				Type:  Terms,
			},
		},
	})

	expected := `{"query":{"bool":{"should":[{"terms":{"some_index_id":"some-long-key-id-value"}}]}},"size":10,"sort":[{"id":"asc"}]}`
	if string(body) != expected {
		t.Errorf("\nWant: %q\nHave: %q", expected, string(body))
	}
}

func TestMinimumShouldMatch(t *testing.T) {
	body, _ := json.Marshal(QueryDoc{
		Index: "some_index",
		Sort:  []map[string]string{{"id": "asc"}},
		Or: []QueryItem{
			{
				Field: "some_index_id",
				Value: "some-long-key-id-value",
				Type:  Terms,
			},
		},
		MinimumShouldMatch: 1,
	})

	expected := `{"query":{"bool":{"should":[{"terms":{"some_index_id":"some-long-key-id-value"}}],"minimum_should_match":1}},"size":0,"sort":[{"id":"asc"}]}`
	if string(body) != expected {
		t.Errorf("\nWant: %q\nHave: %q", expected, string(body))
	}
}

func TestAggregations(t *testing.T) {
	order := map[string]string{"_count": "desc"}
	size := 100
	body, _ := json.Marshal(QueryDoc{
		Index: "some_index",
		Sort:  []map[string]string{{"id": "asc"}},
		Size:  0,
		And: []QueryItem{
			{
				Field: "some_index_id",
				Value: "some-long-key-id-value",
				Type:  Match,
			},
		},
		Aggregations: []Aggregation{
			{
				Type:  Terms,
				Size:  &size,
				Order: &order,
				Name:  "first_field_agg",
				Field: "first_field.keyword",
			},
			{
				Type:  Terms,
				Name:  "second_field_agg",
				Field: "second_field.keyword",
			},
		},
	})

	expected := `{"aggregations":{"first_field_agg":{"aggregations":{"second_field_agg":{"terms":{"field":"second_field.keyword"}}},"terms":{"field":"first_field.keyword","order":{"_count":"desc"},"size":100}}},"query":{"bool":{"must":[{"match":{"some_index_id":"some-long-key-id-value"}}]}},"size":0,"sort":[{"id":"asc"}]}`
	if string(body) != expected {
		t.Errorf("\nWant: %q\nHave: %q", expected, string(body))
	}
}

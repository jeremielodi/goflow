package api

import "testing"

func TestLikeMatch(t *testing.T) {
	cases := []struct {
		s, pattern string
		want       bool
	}{
		{"orderId", "", true},
		{"orderId", "orderId", true},
		{"orderId", "order", false},
		{"orderId", "order%", true},
		{"orderId", "%Id", true},
		{"orderId", "%der%", true},
		{"orderId", "order_d", true},
		{"orderId", "Order%", false}, // case-sensitive, as in Camunda 7
		{"", "%", true},
		{"a", "_", true},
		{"", "_", false},
	}
	for _, c := range cases {
		if got := likeMatch(c.s, c.pattern); got != c.want {
			t.Errorf("likeMatch(%q, %q) = %v, want %v", c.s, c.pattern, got, c.want)
		}
	}
}

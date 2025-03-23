package evaluator

import (
	"reflect"
	"testing"
)

func TestExpressionEvaluator(t *testing.T) {
	evaluator := New()

	tests := []struct {
		name     string
		expr     string
		from     map[string]interface{}
		expected interface{}
	}{
		{
			name:     "simple variable",
			expr:     "foo",
			from:     map[string]interface{}{"foo": "bar"},
			expected: "bar",
		},
		{
			name:     "nested property",
			expr:     "user.name",
			from:     map[string]interface{}{"user": map[string]interface{}{"name": "John"}},
			expected: "John",
		},
		{
			name:     "array indexing",
			expr:     "users[1]",
			from:     map[string]interface{}{"users": []interface{}{"John", "Jane", "Bob"}},
			expected: "Jane",
		},
		{
			name:     "simple addition",
			expr:     "${x + 10}",
			from:     map[string]interface{}{"x": 5},
			expected: 15,
		},
		{
			name:     "complex expression",
			expr:     "${(x + y) * 2}",
			from:     map[string]interface{}{"x": 5, "y": 3},
			expected: 16,
		},
		{
			name:     "nested property access",
			expr:     "${user.age + 5}",
			from:     map[string]interface{}{"user": map[string]interface{}{"age": 25}},
			expected: 30,
		},
		{
			name:     "boolean comparison",
			expr:     "${x > y}",
			from:     map[string]interface{}{"x": 10, "y": 5},
			expected: true,
		},
		{
			name:     "len function with string",
			expr:     "${len(name)}",
			from:     map[string]interface{}{"name": "John Doe"},
			expected: 8,
		},
		{
			name:     "len function with array",
			expr:     "${len(users)}",
			from:     map[string]interface{}{"users": []interface{}{"John", "Jane", "Bob"}},
			expected: 3,
		},
		{
			name:     "len function with map",
			expr:     "${len(data)}",
			from:     map[string]interface{}{"data": map[string]interface{}{"a": 1, "b": 2}},
			expected: 2,
		},
		{
			name:     "len function with nil",
			expr:     "${len(missing)}",
			from:     map[string]interface{}{},
			expected: 0,
		},
		{
			name:     "is nil function with nil",
			expr:     "${is nil(missing)}",
			from:     map[string]interface{}{},
			expected: true,
		},
		{
			name:     "is nil function with non-nil",
			expr:     "${is nil(name)}",
			from:     map[string]interface{}{"name": "John"},
			expected: false,
		},
		{
			name:     "regex match positive",
			expr:     "${~/username [a-z]+/}",
			from:     map[string]interface{}{"username": "john"},
			expected: true,
		},
		{
			name:     "regex match negative",
			expr:     "${~/username [0-9]+/}",
			from:     map[string]interface{}{"username": "john"},
			expected: false,
		},
		{
			name:     "regex match with variable",
			expr:     "${~/text [A-Z]+/}",
			from:     map[string]interface{}{"text": "HELLO"},
			expected: true,
		},
		{
			name:     "comparison with braces less than",
			expr:     "{x < 10}",
			from:     map[string]interface{}{"x": 5},
			expected: true,
		},
		{
			name:     "comparison with braces less than false",
			expr:     "{x < 10}",
			from:     map[string]interface{}{"x": 15},
			expected: false,
		},
		{
			name:     "comparison with braces greater than",
			expr:     "{x > 10}",
			from:     map[string]interface{}{"x": 15},
			expected: true,
		},
		{
			name:     "comparison with braces equal to",
			expr:     "{x == 10}",
			from:     map[string]interface{}{"x": 10},
			expected: true,
		},
		{
			name:     "comparison with braces not equal to",
			expr:     "{x != 10}",
			from:     map[string]interface{}{"x": 5},
			expected: true,
		},
		{
			name:     "comparison with braces less than or equal to",
			expr:     "{x <= 10}",
			from:     map[string]interface{}{"x": 10},
			expected: true,
		},
		{
			name:     "comparison with braces greater than or equal to",
			expr:     "{x >= 10}",
			from:     map[string]interface{}{"x": 10},
			expected: true,
		},
		{
			name:     "complex comparison with braces",
			expr:     "{(x + 5) > (y * 2)}",
			from:     map[string]interface{}{"x": 10, "y": 3},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := evaluator.Evaluate(tc.expr, tc.from)
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("Evaluate(%q, %v) = %v, expected %v", tc.expr, tc.from, result, tc.expected)
			}
		})
	}
}

func TestSpecialFunctions(t *testing.T) {
	evaluator := New()

	t.Run("len function", func(t *testing.T) {
		tests := []struct {
			value    interface{}
			expected int
		}{
			{[]interface{}{"a", "b", "c"}, 3},
			{"hello", 5},
			{map[string]interface{}{"a": 1, "b": 2}, 2},
			{nil, 0},
		}

		for _, tc := range tests {
			variables := map[string]interface{}{"value": tc.value}
			result := evaluator.Evaluate("${len(value)}", variables)
			if result != tc.expected {
				t.Errorf("len(%v) = %v, expected %v", tc.value, result, tc.expected)
			}
		}
	})

	t.Run("is nil function", func(t *testing.T) {
		var nilPointer *int
		tests := []struct {
			value    interface{}
			expected bool
		}{
			{nil, true},
			{nilPointer, true},
			{"hello", false},
			{42, false},
		}

		for _, tc := range tests {
			variables := map[string]interface{}{"value": tc.value}
			result := evaluator.Evaluate("${is nil(value)}", variables)
			if result != tc.expected {
				t.Errorf("is nil(%v) = %v, expected %v", tc.value, result, tc.expected)
			}
		}
	})

	t.Run("regex function", func(t *testing.T) {
		tests := []struct {
			expr     string
			value    string
			expected bool
		}{
			{"${~/value [0-9]+/}", "123", true},
			{"${~/value [a-z]+/}", "abc", true},
			{"${~/value [A-Z]+/}", "abc", false},
			{"${~/value \\d+/}", "123", true},
			{"${~/value \\s+/}", "   ", true},
		}

		for _, tc := range tests {
			variables := map[string]interface{}{"value": tc.value}
			result := evaluator.Evaluate(tc.expr, variables)
			if result != tc.expected {
				t.Errorf("regex(%s, %v) = %v, expected %v", tc.expr, tc.value, result, tc.expected)
			}
		}
	})

	t.Run("comparison operators with braces", func(t *testing.T) {
		tests := []struct {
			expr     string
			vars     map[string]interface{}
			expected bool
		}{
			{"{value < 10}", map[string]interface{}{"value": 5}, true},
			{"{value < 10}", map[string]interface{}{"value": 15}, false},
			{"{value > 10}", map[string]interface{}{"value": 15}, true},
			{"{value > 10}", map[string]interface{}{"value": 5}, false},
			{"{value == 10}", map[string]interface{}{"value": 10}, true},
			{"{value == 10}", map[string]interface{}{"value": 5}, false},
			{"{value != 10}", map[string]interface{}{"value": 5}, true},
			{"{value != 10}", map[string]interface{}{"value": 10}, false},
			{"{value <= 10}", map[string]interface{}{"value": 10}, true},
			{"{value <= 10}", map[string]interface{}{"value": 11}, false},
			{"{value >= 10}", map[string]interface{}{"value": 10}, true},
			{"{value >= 10}", map[string]interface{}{"value": 9}, false},
			{"{value == 'test'}", map[string]interface{}{"value": "test"}, true},
			{"{value != 'test'}", map[string]interface{}{"value": "other"}, true},
			{"{a < b}", map[string]interface{}{"a": 5, "b": 10}, true},
			{"{a < b}", map[string]interface{}{"a": 15, "b": 10}, false},
			{"{a + b < c * d}", map[string]interface{}{"a": 2, "b": 3, "c": 2, "d": 3}, true},
			{"{a + b > c * d}", map[string]interface{}{"a": 10, "b": 10, "c": 2, "d": 3}, true},
		}

		for _, tc := range tests {
			result := evaluator.Evaluate(tc.expr, tc.vars)
			if result != tc.expected {
				t.Errorf("%s with %v = %v, expected %v", tc.expr, tc.vars, result, tc.expected)
			}
		}
	})
}

package execution

import (
	"reflect"
	"testing"
)

func TestExpand(t *testing.T) {
	type testCase struct {
		name   string
		value  string
		from   map[string]interface{}
		expect interface{}
	}

	tests := []testCase{
		{
			name:   "simple variable",
			value:  "$foo",
			from:   map[string]interface{}{"foo": "bar"},
			expect: "bar",
		},
		{
			name:   "variable with curly braces",
			value:  "${foo}",
			from:   map[string]interface{}{"foo": "bar"},
			expect: "bar",
		},
		{
			name:   "text with embedded variable",
			value:  "Hello, ${name}!",
			from:   map[string]interface{}{"name": "World"},
			expect: "Hello, World!",
		},
		{
			name:   "multiple variables in text",
			value:  "${greeting}, ${name}!",
			from:   map[string]interface{}{"greeting": "Hello", "name": "World"},
			expect: "Hello, World!",
		},
		{
			name:   "nested object property",
			value:  "${user.name}",
			from:   map[string]interface{}{"user": map[string]interface{}{"name": "John"}},
			expect: "John",
		},
		{
			name:  "deeply nested object property",
			value: "${user.profile.contact.email}",
			from: map[string]interface{}{
				"user": map[string]interface{}{
					"profile": map[string]interface{}{
						"contact": map[string]interface{}{
							"email": "john@example.com",
						},
					},
				},
			},
			expect: "john@example.com",
		},
		{
			name:  "array element",
			value: "${users[0]}",
			from: map[string]interface{}{
				"users": []interface{}{"John", "Jane", "Bob"},
			},
			expect: "John",
		},
		{
			name:  "array nested property",
			value: "${users[1].name}",
			from: map[string]interface{}{
				"users": []interface{}{
					map[string]interface{}{"name": "John"},
					map[string]interface{}{"name": "Jane"},
				},
			},
			expect: "Jane",
		},
		{
			name:   "variable not found",
			value:  "${notFound}",
			from:   map[string]interface{}{"foo": "bar"},
			expect: "",
		},
		{
			name:   "non-string value",
			value:  "$number",
			from:   map[string]interface{}{"number": 42},
			expect: 42,
		},
		{
			name:   "boolean value",
			value:  "$flag",
			from:   map[string]interface{}{"flag": true},
			expect: true,
		},
		{
			name:   "no variable reference",
			value:  "Hello, World!",
			from:   map[string]interface{}{"name": "John"},
			expect: "Hello, World!",
		},
		{
			name:  "struct field access",
			value: "${person.Name}",
			from: map[string]interface{}{
				"person": struct {
					Name string
					Age  int
				}{"Alice", 30},
			},
			expect: "Alice",
		},
		{
			name:  "pointer to struct access",
			value: "${person.Name}",
			from: map[string]interface{}{
				"person": &struct {
					Name string
					Age  int
				}{"Bob", 35},
			},
			expect: "Bob",
		},
		{
			name:   "escaping - incomplete variable",
			value:  "Hello $",
			from:   map[string]interface{}{"name": "World"},
			expect: "Hello $",
		},
		{
			name:   "multiple variable substitutions of the same variable",
			value:  "${name} ${name} ${name}",
			from:   map[string]interface{}{"name": "echo"},
			expect: "echo echo echo",
		},
		{
			name:  "complex nested structure",
			value: "${data.users[1].contacts[0].email}",
			from: map[string]interface{}{
				"data": map[string]interface{}{
					"users": []interface{}{
						map[string]interface{}{"name": "John"},
						map[string]interface{}{
							"name": "Jane",
							"contacts": []interface{}{
								map[string]interface{}{"email": "jane@example.com"},
								map[string]interface{}{"email": "jane2@example.com"},
							},
						},
					},
				},
			},
			expect: "jane@example.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := expand(tc.value, tc.from)
			if !reflect.DeepEqual(result, tc.expect) {
				t.Errorf("expand(%q, %v) = %v, want %v", tc.value, tc.from, result, tc.expect)
			}
		})
	}
}

func TestExpandExpression(t *testing.T) {
	type testCase struct {
		name   string
		expr   string
		from   map[string]interface{}
		expect interface{}
	}

	tests := []testCase{
		{
			name:   "simple variable",
			expr:   "foo",
			from:   map[string]interface{}{"foo": "bar"},
			expect: "bar",
		},
		{
			name:   "nested property",
			expr:   "user.name",
			from:   map[string]interface{}{"user": map[string]interface{}{"name": "John"}},
			expect: "John",
		},
		{
			name:   "array indexing",
			expr:   "users[1]",
			from:   map[string]interface{}{"users": []interface{}{"John", "Jane", "Bob"}},
			expect: "Jane",
		},
		{
			name:   "array and nested property",
			expr:   "users[0].name",
			from:   map[string]interface{}{"users": []interface{}{map[string]interface{}{"name": "John"}}},
			expect: "John",
		},
		{
			name:   "non-existent property",
			expr:   "user.address",
			from:   map[string]interface{}{"user": map[string]interface{}{"name": "John"}},
			expect: nil,
		},
		{
			name:   "invalid array index",
			expr:   "users[99]",
			from:   map[string]interface{}{"users": []interface{}{"John", "Jane"}},
			expect: nil,
		},
		{
			name:   "invalid property path",
			expr:   "a.b.c.d",
			from:   map[string]interface{}{"a": 1},
			expect: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := expandExpression(tc.expr, tc.from)
			if !reflect.DeepEqual(result, tc.expect) {
				t.Errorf("expandExpression(%q, %v) = %v, want %v", tc.expr, tc.from, result, tc.expect)
			}
		})
	}
}

func TestRecursiveExpand(t *testing.T) {
	type testCase struct {
		name   string
		value  interface{}
		from   map[string]interface{}
		expect interface{}
	}

	tests := []testCase{
		{
			name: "expand map values",
			value: map[string]interface{}{
				"greeting": "Hello, ${name}!",
				"subject":  "$topic",
			},
			from: map[string]interface{}{
				"name":  "World",
				"topic": "Testing",
			},
			expect: map[string]interface{}{
				"greeting": "Hello, World!",
				"subject":  "Testing",
			},
		},
		{
			name: "expand slice values",
			value: []interface{}{
				"${greeting}, ${name}!",
				"$topic",
				"No variables here",
			},
			from: map[string]interface{}{
				"greeting": "Hello",
				"name":     "World",
				"topic":    "Testing",
			},
			expect: []interface{}{
				"Hello, World!",
				"Testing",
				"No variables here",
			},
		},
		{
			name: "expand map keys",
			value: map[string]interface{}{
				"${prefix}Key": "Value",
				"regularKey":   "${value}",
			},
			from: map[string]interface{}{
				"prefix": "custom",
				"value":  "customValue",
			},
			expect: map[string]interface{}{
				"customKey":  "Value",
				"regularKey": "customValue",
			},
		},
		{
			name: "nested structures",
			value: map[string]interface{}{
				"user": map[string]interface{}{
					"name":    "${username}",
					"profile": []interface{}{"${bio}", "${interests}"},
				},
			},
			from: map[string]interface{}{
				"username":  "John",
				"bio":       "Developer",
				"interests": "Coding",
			},
			expect: map[string]interface{}{
				"user": map[string]interface{}{
					"name":    "John",
					"profile": []interface{}{"Developer", "Coding"},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Expand(tc.value, tc.from)
			if err != nil {
				t.Fatalf("Expand returned error: %v", err)
			}
			if !reflect.DeepEqual(result, tc.expect) {
				t.Errorf("Expand(%v, %v) = %v, want %v", tc.value, tc.from, result, tc.expect)
			}
		})
	}
}

// Test helper functions
func TestHelperFunctions(t *testing.T) {
	t.Run("hasExpr", func(t *testing.T) {
		tests := []struct {
			value  string
			expect bool
		}{
			{"Hello, $name", true},
			{"Hello, ${name}", true},
			{"No variables here", false},
			{"$ dollar sign alone", true},
			{"", false},
		}

		for _, tc := range tests {
			if result := hasExpr(tc.value); result != tc.expect {
				t.Errorf("hasExpr(%q) = %v, want %v", tc.value, result, tc.expect)
			}
		}
	})

	t.Run("stringify", func(t *testing.T) {
		tests := []struct {
			value  interface{}
			expect string
		}{
			{42, "42"},
			{true, "true"},
			{"hello", "hello"},
			{nil, ""},
		}

		for _, tc := range tests {
			if result := stringify(tc.value); result != tc.expect {
				t.Errorf("stringify(%v) = %v, want %v", tc.value, result, tc.expect)
			}
		}
	})

	t.Run("getProperty", func(t *testing.T) {
		type TestStruct struct {
			Name  string
			Value int
			child struct {
				Data string
			}
		}

		obj := TestStruct{Name: "test", Value: 42}
		obj.child.Data = "hidden"

		tests := []struct {
			obj    interface{}
			prop   string
			expect interface{}
		}{
			{obj, "Name", "test"},
			{obj, "Value", 42},
			{obj, "child", nil}, // unexported field
			{obj, "Missing", nil},
			{&obj, "Name", "test"}, // pointer to struct
			{map[string]interface{}{"key": "value"}, "key", "value"},
			{nil, "anything", nil},
		}

		for _, tc := range tests {
			result := getProperty(tc.obj, tc.prop)
			if !reflect.DeepEqual(result, tc.expect) {
				t.Errorf("getProperty(%v, %q) = %v, want %v", tc.obj, tc.prop, result, tc.expect)
			}
		}
	})

	t.Run("getArrayElement", func(t *testing.T) {
		tests := []struct {
			obj    interface{}
			index  int
			expect interface{}
		}{
			{[]interface{}{"a", "b", "c"}, 1, "b"},
			{[]int{1, 2, 3}, 2, 3},
			{[]string{"hello", "world"}, 0, "hello"},
			{[]interface{}{"a", "b", "c"}, 10, nil}, // out of bounds
			{[]interface{}{"a", "b", "c"}, -1, nil}, // negative index
			{"not an array", 0, nil},                // not an array or slice
			{nil, 0, nil},                           // nil input
		}

		for _, tc := range tests {
			result := getArrayElement(tc.obj, tc.index)
			if !reflect.DeepEqual(result, tc.expect) {
				t.Errorf("getArrayElement(%v, %d) = %v, want %v", tc.obj, tc.index, result, tc.expect)
			}
		}
	})
}

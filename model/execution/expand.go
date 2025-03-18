package execution

import (
	"fmt"
	"github.com/viant/structology/visitor"
	"reflect"
	"strings"
)

// expand expands variable references in a string using values from the provided map.
// It supports both $var and ${var} syntax, as well as nested property access like $obj.prop1.prop2
func expand(value string, from map[string]interface{}) interface{} {
	// Check if using ${...} syntax
	if strings.Contains(value, "${") && strings.Contains(value, "}") {
		result := value
		for {
			start := strings.Index(result, "${")
			if start == -1 {
				break
			}
			end := strings.Index(result[start:], "}")
			if end == -1 {
				break
			}
			end = start + end + 1

			expr := result[start+2 : end-1]
			replacement := expandExpression(expr, from)

			// Replace the entire ${...} expression with the evaluated value
			if str, ok := replacement.(string); ok {
				result = result[:start] + str + result[end:]
			} else if replacement != nil {
				// If it's not a string, return the object directly
				// but only if the entire value is just this expression
				if start == 0 && end == len(result) {
					return replacement
				}
				// Otherwise convert to string for interpolation
				result = result[:start] + stringify(replacement) + result[end:]
			} else {
				// If no replacement found, replace with empty string
				result = result[:start] + result[end:]
			}
		}
		return result
	}

	// Handle simple $var syntax
	if strings.HasPrefix(value, "$") && !strings.Contains(value, " ") {
		expr := strings.TrimPrefix(value, "$")
		return expandExpression(expr, from)
	}

	return value
}

// expandExpression handles dot notation to navigate through objects
// For example: "user.profile.name" will navigate through nested structures
func expandExpression(expr string, from map[string]interface{}) interface{} {
	// Check if this is an array access path
	if strings.Contains(expr, "[") && strings.Contains(expr, "]") {
		return handleNestedExpression(expr, from)
	}

	// Handle simple dot notation without array access
	parts := strings.Split(expr, ".")
	var current interface{}
	var ok bool

	// First part must be in the 'from' map
	if current, ok = from[parts[0]]; !ok {
		return nil
	}

	// Navigate through the nested properties
	for i := 1; i < len(parts); i++ {
		switch c := current.(type) {
		case map[string]interface{}:
			if current, ok = c[parts[i]]; !ok {
				return nil
			}
		default:
			// Try to use reflection for structs and other types
			current = getProperty(current, parts[i])
			if current == nil {
				return nil
			}
		}
	}

	return current
}

// handleNestedExpression handles complex expressions with array indexing
func handleNestedExpression(expr string, from map[string]interface{}) interface{} {
	// Get the root object name (before first dot or bracket)
	var rootName string
	firstDot := strings.Index(expr, ".")
	firstBracket := strings.Index(expr, "[")

	if firstDot < 0 && firstBracket < 0 {
		// No dots or brackets - simple variable
		return from[expr]
	} else if firstDot < 0 || (firstBracket >= 0 && firstBracket < firstDot) {
		// Bracket appears first
		rootName = expr[:firstBracket]
	} else {
		// Dot appears first
		rootName = expr[:firstDot]
	}

	// Get the root object
	rootObj, exists := from[rootName]
	if !exists {
		return nil
	}

	// Remove the root name from the expression
	pathExpr := expr[len(rootName):]

	// Process the path
	return processPath(rootObj, pathExpr)
}

// processPath evaluates a path expression like ".users[1].name" or "[0].email"
func processPath(obj interface{}, path string) interface{} {
	if path == "" {
		return obj
	}

	// Initialize with the current object
	current := obj

	// Parse the path segment by segment
	i := 0
	for i < len(path) {
		// Skip leading dots
		if path[i] == '.' {
			i++
			continue
		}

		// Handle array access
		if path[i] == '[' {
			// Find the closing bracket
			closeBracket := strings.Index(path[i:], "]")
			if closeBracket < 0 {
				return nil // Malformed path
			}
			closeBracket += i

			// Extract the index
			indexStr := path[i+1 : closeBracket]
			index := 0
			for _, ch := range indexStr {
				if ch < '0' || ch > '9' {
					return nil // Not a numeric index
				}
				index = index*10 + int(ch-'0')
			}

			// Access the array element
			current = getArrayElement(current, index)
			if current == nil {
				return nil // Element not found
			}

			// Move past the closing bracket
			i = closeBracket + 1
		} else {
			// Handle property access
			nextDot := strings.Index(path[i:], ".")
			nextBracket := strings.Index(path[i:], "[")

			var propEnd int
			if nextDot < 0 && nextBracket < 0 {
				// No more segments
				propEnd = len(path)
			} else if nextDot < 0 {
				// Next segment is a bracket
				propEnd = i + nextBracket
			} else if nextBracket < 0 {
				// Next segment is a dot
				propEnd = i + nextDot
			} else {
				// Both exist, take the nearest
				propEnd = i + min(nextDot, nextBracket)
			}

			// Extract property name
			propName := path[i:propEnd]

			// Access the property
			switch c := current.(type) {
			case map[string]interface{}:
				var ok bool
				if current, ok = c[propName]; !ok {
					return nil
				}
			default:
				current = getProperty(current, propName)
				if current == nil {
					return nil
				}
			}

			// Move past this property
			i = propEnd
		}
	}

	return current
}

// getProperty uses reflection to get a property from a struct or map
func getProperty(obj interface{}, prop string) interface{} {
	if obj == nil {
		return nil
	}

	// Handle maps
	if mapObj, ok := obj.(map[string]interface{}); ok {
		if val, exists := mapObj[prop]; exists {
			return val
		}
		return nil
	}

	// Use reflection for structs
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil
	}

	// Try to get the field (supports exported fields only)
	field := val.FieldByName(prop)
	if !field.IsValid() {
		// Try case-insensitive match
		typ := val.Type()
		for i := 0; i < typ.NumField(); i++ {
			if strings.EqualFold(typ.Field(i).Name, prop) {
				field = val.Field(i)
				break
			}
		}
		if !field.IsValid() {
			return nil
		}
	}

	if !field.CanInterface() {
		return nil // Unexported field
	}

	return field.Interface()
}

// getArrayElement extracts an element from an array or slice using reflection
func getArrayElement(obj interface{}, index int) interface{} {
	if obj == nil {
		return nil
	}

	switch arr := obj.(type) {
	case []interface{}:
		if index >= 0 && index < len(arr) {
			return arr[index]
		}
	case []string:
		if index >= 0 && index < len(arr) {
			return arr[index]
		}
	case []int:
		if index >= 0 && index < len(arr) {
			return arr[index]
		}
	default:
		// Use reflection for other array types
		val := reflect.ValueOf(obj)
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				return nil
			}
			val = val.Elem()
		}

		if val.Kind() != reflect.Array && val.Kind() != reflect.Slice {
			return nil
		}

		if index < 0 || index >= val.Len() {
			return nil // Index out of bounds
		}

		elementVal := val.Index(index)
		if !elementVal.CanInterface() {
			return nil
		}

		return elementVal.Interface()
	}

	return nil
}

// stringify converts a value to a string representation
func stringify(val interface{}) string {
	if val == nil {
		return ""
	}
	return reflect.ValueOf(val).String()
}

// hasExpr checks if a string contains any variable expression.
func hasExpr(value string) bool {
	return strings.Contains(value, "$")
}

// Expand recursively traverses maps and slices, expanding any string containing variable references.
func Expand(value interface{}, from map[string]interface{}) (interface{}, error) {
	var err error
	switch actual := value.(type) {
	case map[string]interface{}:
		expandedMap := make(map[string]interface{})
		visit := visitor.MapVisitorOf[string, interface{}](actual)
		err = visit(func(key string, element interface{}) (bool, error) {
			var expandedKey = key
			if hasExpr(key) {
				expanded := expand(key, from)
				if str, ok := expanded.(string); ok {
					expandedKey = str
				} else {
					// Skip this entry if the key doesn't expand to a string
					return true, nil
				}
			}

			if text, ok := element.(string); ok && hasExpr(text) {
				element = expand(text, from)
				fmt.Printf("expanded %s  %+v %v\n", text, element, from)
			} else {
				// Recursively expand nested structures
				element, err = Expand(element, from)
				if err != nil {
					return false, err
				}
			}

			expandedMap[expandedKey] = element
			return true, nil
		})
		return expandedMap, err

	case []interface{}:
		expandedSlice := make([]interface{}, len(actual))
		for i, item := range actual {
			if text, ok := item.(string); ok && hasExpr(text) {
				item = expand(text, from)
			} else {
				// Recursively expand nested items
				item, err = Expand(item, from)
				if err != nil {
					return nil, err
				}
			}
			expandedSlice[i] = item
		}
		return expandedSlice, nil

	case string:
		if hasExpr(actual) {
			return expand(actual, from), nil
		}
		return actual, nil

	default:
		// For other types, return as is
		return actual, nil
	}
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

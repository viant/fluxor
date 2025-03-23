package expander

import (
	"fmt"
	"github.com/viant/fluxor/model/evaluator"
	"github.com/viant/structology/visitor"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// expand expands variable references in a string using values from the provided map.
// It supports both $var and ${var} syntax, as well as nested property access like $obj.prop1.prop2
// It also supports expressions like ${i + 1} or ${bar.goo / foo.z}
func expand(value string, from map[string]interface{}) interface{} {
	if value == "" {
		return value
	}

	// First, check if the entire string is a variable reference
	if (strings.HasPrefix(value, "$") && !strings.Contains(value[1:], "$") && !strings.Contains(value, " ")) ||
		(strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") && !strings.Contains(value[2:len(value)-1], "${")) {
		// Pure variable or expression replacement
		if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
			expr := value[2 : len(value)-1]
			if containsExpressionOperators(expr) {
				return evaluator.Evaluate(expr, from)
			}
			return expandExpression(expr, from)
		}
		if strings.HasPrefix(value, "$") {
			expr := value[1:]
			return expandExpression(expr, from)
		}
	}

	// Handle embedded variables in text

	// Process ${...} expressions first
	result := value
	for {
		start := strings.Index(result, "${")
		if start == -1 {
			break
		}
		end := findMatchingClosingBrace(result[start:])
		if end == -1 {
			break
		}
		end = start + end + 1

		expr := result[start+2 : end-1]
		var replacement interface{}

		if containsExpressionOperators(expr) {
			replacement = evaluator.Evaluate(expr, from)
		} else {
			replacement = expandExpression(expr, from)
		}

		// Convert the replacement to string for interpolation
		replacementStr := stringifyValue(replacement)
		result = result[:start] + replacementStr + result[end:]
	}

	// Then process $var references
	// Use regex to find variables of the form $varname
	re := regexp.MustCompile(`\$([a-zA-Z_][a-zA-Z0-9_.]*)`)
	result = re.ReplaceAllStringFunc(result, func(match string) string {
		varName := match[1:] // Remove $ prefix
		replacement := expandExpression(varName, from)
		if replacement == nil {
			return match // Keep original if not found
		}
		return stringifyValue(replacement)
	})

	return result
}

// findMatchingClosingBrace finds the position of the matching closing brace
// for an expression starting with "${". It accounts for nested braces.
func findMatchingClosingBrace(s string) int {
	if !strings.HasPrefix(s, "${") {
		return -1
	}

	depth := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '{' {
			depth++
		} else if s[i] == '}' {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// containsExpressionOperators checks if the string contains math or logical operators
func containsExpressionOperators(s string) bool {
	operators := []string{"+", "-", "*", "/", "%", "==", "!=", ">", "<", ">=", "<=", "&&", "||"}
	for _, op := range operators {
		// Skip minus sign at beginning (negative numbers) or after other operators
		if op == "-" && (strings.HasPrefix(s, "-") || strings.Contains(s, "+-") ||
			strings.Contains(s, "*-") || strings.Contains(s, "/-") ||
			strings.Contains(s, "=-")) {
			continue
		}
		if strings.Contains(s, op) {
			return true
		}
	}
	return false
}

// isIntType checks if the value is an integer type
func isIntType(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	}
	return false
}

// stringifyValue converts a value to its string representation for interpolation
func stringifyValue(val interface{}) string {
	if val == nil {
		return ""
	}

	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case reflect.Bool:
		return strconv.FormatBool(v.Bool())
	case reflect.String:
		return v.String()
	default:
		return fmt.Sprintf("%v", val)
	}
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
	return stringifyValue(val)
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

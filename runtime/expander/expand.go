package expander

import (
	"fmt"
	"github.com/viant/fluxor/runtime/evaluator"
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

	// ------------------------------------------------------------------
	// Fast-path: detect when the WHOLE string is merely a variable or an
	// expression so that we can return typed values (ints, bools…) instead of
	// their string interpolation form.  We deliberately keep the detection
	// conservative – if the token contains any extra characters that make it a
	// mixed literal (e.g. "${prefix}Key") we fall through to the general text
	// interpolation logic below.
	// ------------------------------------------------------------------

	var pureVariable bool
	if strings.HasPrefix(value, "$") && !strings.Contains(value, " ") && !strings.Contains(value, "${") {
		// Regex ‑ ^$[identifier(.identifier|[idx])…]$
		pureVariable = regexp.MustCompile(`^\$[a-zA-Z_][a-zA-Z0-9_\.\[\]]*$`).MatchString(value)
	}

	var pureBraced = strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") &&
		!strings.Contains(value[2:len(value)-1], "${")

	if pureVariable || pureBraced {
		// Pure variable or expression replacement
		if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
			expr := value[2 : len(value)-1]
			if containsExpressionOperators(expr) {
				// Try primary evaluator; fall back to array-aware evaluator if nil.
				if val := evaluator.Evaluate(expr, from); val != nil {
					return val
				}
				return evaluateWithArrayIndexing(expr, from)
			}
			res := expandExpression(expr, from)
			if res == nil {
				// For ${var} style if var not found, return empty string rather than nil
				return ""
			}
			return res
		}
		if strings.HasPrefix(value, "$") {
			expr := value[1:]
			if expanded := expandExpression(expr, from); expanded != nil {
				return expanded
			}
			// If not found keep original token intact – this matches behaviour in
			// text replacements and is expected by tests.
			return value
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

	// Fallback – the loop above may fail in certain edge-cases (e.g. when the
	// very first token starts the string and is followed immediately by other
	// characters, like "${prefix}Key").  In those cases the non-recursive
	// brace matcher sometimes returns -1 which leaves the token unresolved.
	// We run a simpler regexp-based substitution pass to ensure any remaining
	// ${…} tokens are expanded.

	if strings.Contains(result, "${") {
		reBrace := regexp.MustCompile(`\$\{([^{}]+)\}`)
		result = reBrace.ReplaceAllStringFunc(result, func(match string) string {
			expr := match[2 : len(match)-1]
			var replacement interface{}
			if containsExpressionOperators(expr) {
				replacement = evaluator.Evaluate(expr, from)
				if replacement == nil {
					replacement = evaluateWithArrayIndexing(expr, from)
				}
			} else {
				replacement = expandExpression(expr, from)
			}
			return stringifyValue(replacement)
		})
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
		rType := reflect.TypeOf(replacement)
		switch rType.Kind() {
		case reflect.Slice, reflect.Map:
			return match // Keep original if the replacement is a complex type
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
				// Fallback to case-insensitive lookup so that
				// `${exec.Stdout}` can resolve a map key `stdout` that
				// originated from JSON encoding.
				for k, v := range c {
					if strings.EqualFold(k, parts[i]) {
						current = v
						ok = true
						break
					}
				}
				if !ok {
					return nil
				}
			}
		case map[string]string:
			if current, ok = c[parts[i]]; !ok {
				return nil
			}
		default:
			// Generic map handling via reflection: support map[string]T for any T
			if mv, ok := getMapValue(current, parts[i]); ok {
				current = mv
				continue
			}
			// Try to use reflection for structs and other types
			current = getProperty(current, parts[i])
			if current == nil {
				return nil
			}
		}
	}

	return toJSONNumber(current)
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
			case map[string]string:
				var ok bool
				if current, ok = c[propName]; !ok {
					return nil
				}
			default:
				if mv, ok := getMapValue(current, propName); ok {
					current = mv
				} else {
					current = getProperty(current, propName)
					if current == nil {
						return nil
					}
				}
			}

			// Move past this property
			i = propEnd
		}
	}

	return toJSONNumber(current)
}

// getProperty uses reflection to get a property from a struct or map
func getProperty(obj interface{}, prop string) interface{} {
	if obj == nil {
		return nil
	}

	// Handle maps – fast-path for map[string]interface{}
	if mapObj, ok := obj.(map[string]interface{}); ok {
		if val, exists := mapObj[prop]; exists {
			return val
		}
		return nil
	}
	// Generic map via reflection
	if v, ok := getMapValue(obj, prop); ok {
		return v
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

// getMapValue attempts to read a value from any map with string keys via reflection.
// Returns (value, true) when obj is a map[string]T and key exists.
func getMapValue(obj interface{}, key string) (interface{}, bool) {
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, false
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Map {
		return nil, false
	}
	// key must be string
	if val.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	k := reflect.ValueOf(key)
	v := val.MapIndex(k)
	if !v.IsValid() {
		return nil, false
	}
	if !v.CanInterface() {
		return nil, false
	}
	return v.Interface(), true
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

// toJSONNumber coerces integer-typed values to float64 to mimic JSON/YAML
// default decoding semantics used throughout the engine/tests. Floating-point
// values are returned as float64 unchanged. Other types are returned as-is.
func toJSONNumber(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint())
	case reflect.Float32, reflect.Float64:
		return float64(rv.Float())
	}
	return v
}

// evaluateWithArrayIndexing attempts to evaluate simple arithmetic expressions
// that include variables with array indexing (e.g. "values[0] + values[1]").
// It operates by replacing every occurrence of a variable path (including any
// [index] or .field navigation) with its concrete value obtained via
// expandExpression, then delegating to the evaluator again.  This is a best
// -effort fallback – if substitution fails for any token, the function returns
// nil so the caller can preserve the original behaviour.
func evaluateWithArrayIndexing(expr string, from map[string]interface{}) interface{} {
	// Match variable paths that may include nested properties and array
	// indexes, e.g. users[0].name or values[2]
	reVar := regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*(?:\[[0-9]+\])*(?:\.[a-zA-Z_][a-zA-Z0-9_]*(?:\[[0-9]+\])*)*`)

	processed := reVar.ReplaceAllStringFunc(expr, func(token string) string {
		// Evaluate the token against the context
		val := expandExpression(token, from)
		if val == nil {
			// Leave token unchanged – evaluation will likely fail later, we
			// will return nil overall.
			return token
		}
		// Convert the value to a literal suitable for Go expression parser
		switch v := val.(type) {
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64:
			return fmt.Sprintf("%v", v)
		case bool:
			return fmt.Sprintf("%v", v)
		case string:
			return fmt.Sprintf("\"%s\"", v)
		default:
			// unsupported type – keep as-is
			return token
		}
	})

	// Delegate to standard evaluator on the substituted expression
	if processed == expr {
		return nil
	}
	return evaluator.Evaluate(processed, from)
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

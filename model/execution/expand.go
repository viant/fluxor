package execution

import (
	"fmt"
	"github.com/viant/structology/visitor"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// expand expands variable references in a string using values from the provided map.
// It supports both $var and ${var} syntax, as well as nested property access like $obj.prop1.prop2
// It also supports expressions like ${i + 1} or ${bar.goo / foo.z}
func expand(value string, from map[string]interface{}) interface{} {
	// Check if using ${...} syntax
	if strings.Contains(value, "${") && strings.Contains(value, "}") {
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

			// Check if expr contains math or logical operators indicating an expression
			if containsExpressionOperators(expr) {
				replacement = evaluateExpression(expr, from)
			} else {
				replacement = expandExpression(expr, from)
			}

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
				result = result[:start] + stringifyValue(replacement) + result[end:]
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

// evaluateExpression evaluates a mathematical or logical expression
// For example: "i + 1" or "bar.goo / foo.z" or "foo.z * (1 + bar.zoo)"
func evaluateExpression(expr string, from map[string]interface{}) interface{} {
	// Replace variable references with their values
	processedExpr := processExpressionVariables(expr, from)

	// Parse the expression
	e, err := parser.ParseExpr(processedExpr)
	if err != nil {
		return nil
	}

	// Evaluate the expression
	result := evaluateAst(e)
	return result
}

// processExpressionVariables replaces all variable references in the expression
// with their actual values from the context
func processExpressionVariables(expr string, from map[string]interface{}) string {
	// Find all variable references in the expression
	//varPattern := `[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*`
	parts := strings.FieldsFunc(expr, func(c rune) bool {
		return !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.')
	})

	processedExpr := expr
	for _, part := range parts {
		if isVariableReference(part) {
			value := expandExpression(part, from)
			if value != nil {
				// Convert value to string representation for the expression
				valueStr := ""
				switch v := value.(type) {
				case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
					valueStr = fmt.Sprintf("%v", v)
				case bool:
					valueStr = strconv.FormatBool(v)
				case string:
					valueStr = fmt.Sprintf("\"%s\"", v)
				default:
					valueStr = fmt.Sprintf("%v", v)
				}

				// Replace all occurrences of the variable with its value
				// we need to be careful to replace only the whole variable, not substrings
				parts := strings.Split(processedExpr, part)
				processedExpr = strings.Join(parts, valueStr)
			}
		}
	}

	return processedExpr
}

// isVariableReference checks if a string is a valid variable reference
func isVariableReference(s string) bool {
	// Must start with a letter or underscore
	if len(s) == 0 || !((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') || s[0] == '_') {
		return false
	}

	// Rest can contain letters, numbers, underscores, and dots
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.') {
			return false
		}
	}

	return true
}

// evaluateAst evaluates an AST expression
func evaluateAst(node ast.Expr) interface{} {
	switch n := node.(type) {
	case *ast.BasicLit:
		// Handle literals (numbers, strings, etc.)
		switch n.Kind {
		case token.INT:
			val, _ := strconv.Atoi(n.Value)
			return val
		case token.FLOAT:
			val, _ := strconv.ParseFloat(n.Value, 64)
			return val
		case token.STRING:
			// Remove quotes from string
			return strings.Trim(n.Value, "\"'")
		}

	case *ast.BinaryExpr:
		// Handle binary operations (+, -, *, /, etc.)
		x := evaluateAst(n.X)
		y := evaluateAst(n.Y)

		// Convert values to appropriate types for operation
		xVal, yVal := convertToCompatibleTypes(x, y)

		switch n.Op {
		case token.ADD:
			return performAddition(xVal, yVal)
		case token.SUB:
			return performSubtraction(xVal, yVal)
		case token.MUL:
			return performMultiplication(xVal, yVal)
		case token.QUO:
			return performDivision(xVal, yVal)
		case token.REM:
			return performModulo(xVal, yVal)
		case token.EQL:
			return reflect.DeepEqual(xVal, yVal)
		case token.NEQ:
			return !reflect.DeepEqual(xVal, yVal)
		case token.LSS:
			return compareValues(xVal, yVal) < 0
		case token.GTR:
			return compareValues(xVal, yVal) > 0
		case token.LEQ:
			return compareValues(xVal, yVal) <= 0
		case token.GEQ:
			return compareValues(xVal, yVal) >= 0
		}

	case *ast.ParenExpr:
		// Handle parenthesized expressions
		return evaluateAst(n.X)

	case *ast.UnaryExpr:
		// Handle unary operations (-, !, etc.)
		operand := evaluateAst(n.X)

		switch n.Op {
		case token.SUB:
			// Negate the numeric value
			switch v := operand.(type) {
			case int:
				return -v
			case float64:
				return -v
			}
		case token.NOT:
			// Logical NOT
			if b, ok := operand.(bool); ok {
				return !b
			}
		}
	}

	return nil
}

// convertToCompatibleTypes converts x and y to compatible numeric types
func convertToCompatibleTypes(x, y interface{}) (interface{}, interface{}) {
	// If both are integers, keep them as integers
	if isIntType(x) && isIntType(y) {
		return toInt(x), toInt(y)
	}

	// If either is float, convert both to float
	if isFloatType(x) || isFloatType(y) {
		return toFloat64(x), toFloat64(y)
	}

	// Otherwise just return the original values
	return x, y
}

// isIntType checks if the value is an integer type
func isIntType(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	}
	return false
}

// isFloatType checks if the value is a float type
func isFloatType(v interface{}) bool {
	switch v.(type) {
	case float32, float64:
		return true
	}
	return false
}

// toInt converts a value to int
func toInt(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int8:
		return int(val)
	case int16:
		return int(val)
	case int32:
		return int(val)
	case int64:
		return int(val)
	case uint:
		return int(val)
	case uint8:
		return int(val)
	case uint16:
		return int(val)
	case uint32:
		return int(val)
	case uint64:
		return int(val)
	case float32:
		return int(val)
	case float64:
		return int(val)
	case string:
		i, _ := strconv.Atoi(val)
		return i
	}
	return 0
}

// toFloat64 converts a value to float64
func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	}
	return 0
}

// performAddition performs addition on two values
func performAddition(x, y interface{}) interface{} {
	// Handle string concatenation
	if strX, okX := x.(string); okX {
		if strY, okY := y.(string); okY {
			return strX + strY
		}
		return strX + stringifyValue(y)
	}
	if strY, okY := y.(string); okY {
		return stringifyValue(x) + strY
	}

	// Handle numeric addition
	if isIntType(x) && isIntType(y) {
		return toInt(x) + toInt(y)
	}
	return toFloat64(x) + toFloat64(y)
}

// performSubtraction performs subtraction
func performSubtraction(x, y interface{}) interface{} {
	if isIntType(x) && isIntType(y) {
		return toInt(x) - toInt(y)
	}
	return toFloat64(x) - toFloat64(y)
}

// performMultiplication performs multiplication
func performMultiplication(x, y interface{}) interface{} {
	if isIntType(x) && isIntType(y) {
		return toInt(x) * toInt(y)
	}
	return toFloat64(x) * toFloat64(y)
}

// performDivision performs division
func performDivision(x, y interface{}) interface{} {
	// Check for division by zero
	if toFloat64(y) == 0 {
		return math.Inf(1) // Positive infinity
	}

	// Always return float for division to avoid truncation
	return toFloat64(x) / toFloat64(y)
}

// performModulo performs modulo operation
func performModulo(x, y interface{}) interface{} {
	if isIntType(x) && isIntType(y) && toInt(y) != 0 {
		return toInt(x) % toInt(y)
	}
	yFloat := toFloat64(y)
	if yFloat == 0 {
		return math.NaN() // Not a Number
	}
	return math.Mod(toFloat64(x), yFloat)
}

// compareValues compares two values and returns:
// -1 if x < y
//
//	0 if x == y
//	1 if x > y
func compareValues(x, y interface{}) int {
	if isIntType(x) && isIntType(y) {
		xInt, yInt := toInt(x), toInt(y)
		if xInt < yInt {
			return -1
		} else if xInt > yInt {
			return 1
		}
		return 0
	}

	xFloat, yFloat := toFloat64(x), toFloat64(y)
	if xFloat < yFloat {
		return -1
	} else if xFloat > yFloat {
		return 1
	}
	return 0
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

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

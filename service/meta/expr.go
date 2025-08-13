package meta

import (
	"os"
	"strings"
	"unicode"
)

// expandEnvExpr replaces all occurrences of ${env.KEY} in the input
// with the value of the environment variable KEY (or "" if unset).
func expandEnvExpr(value string) string {
	const prefix = "${env."
	var b strings.Builder
	i := 0
	for {
		// Find next prefix in the remaining string
		idx := strings.Index(value[i:], prefix)
		if idx < 0 {
			// No more; append rest and break
			b.WriteString(value[i:])
			break
		}

		// Write everything up to the prefix
		b.WriteString(value[i : i+idx])
		// Move i to just after the prefix
		startKey := i + idx + len(prefix)

		// Find the closing '}'
		endKey := strings.IndexByte(value[startKey:], '}')
		if endKey < 0 {
			// No closing brace—treat the rest as literal
			b.WriteString(value[i+idx:])
			break
		}

		// Extract the env var name candidate
		key := value[startKey : startKey+endKey]

		// Validate key – must consist solely of letters, digits or '_' (allow empty key).
		valid := true
		for _, r := range key {
			if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
				valid = false
				break
			}
		}

		if valid {
			// Lookup and write its value (empty if unset or key is empty)
			b.WriteString(os.Getenv(key))
		} else {
			// Invalid expression – treat the detected prefix as literal and
			// continue scanning from immediately after it so that any nested
			// expressions are still processed.
			b.WriteString(value[i+idx : startKey])
			// Move i right after the prefix, effectively consuming it but
			// leaving the remainder (including the characters that caused the
			// invalidation) to be re-evaluated in the next iteration.
			i = startKey
			continue
		}

		// Advance i past the closing '}' for valid expressions.
		i = startKey + endKey + 1
	}

	return b.String()
}

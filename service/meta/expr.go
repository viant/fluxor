package meta

import (
	"os"
	"strings"
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

		// Extract the env var name
		key := value[startKey : startKey+endKey]
		// Lookup and write its value
		b.WriteString(os.Getenv(key))

		// Advance i past the closing '}'
		i = startKey + endKey + 1
	}

	return b.String()
}

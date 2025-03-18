package parameters

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

// Token codes
const (
	whitespaceCode = iota
	identifierCode
	openSquareBracketCode
	closeSquareBracketCode
	openParenCode
	closeParenCode
	slashCode
	qualifiedTypeCode
	kindCode
	locationCode
)

// Token definitions
var (
	whitespaceToken         = parsly.NewToken(whitespaceCode, "Whitespace", matcher.NewWhiteSpace())
	identifierToken         = parsly.NewToken(identifierCode, "Identifier", newIdentifierMatcher())
	openSquareBracketToken  = parsly.NewToken(openSquareBracketCode, "[", matcher.NewByte('['))
	closeSquareBracketToken = parsly.NewToken(closeSquareBracketCode, "]", matcher.NewByte(']'))
	openParenToken          = parsly.NewToken(openParenCode, "(", matcher.NewByte('('))
	closeParenToken         = parsly.NewToken(closeParenCode, ")", matcher.NewByte(')'))
	slashToken              = parsly.NewToken(slashCode, "/", matcher.NewByte('/'))
	qualifiedTypeToken      = parsly.NewToken(qualifiedTypeCode, "QualifiedType", newQualifiedTypeMatcher())
	kindToken               = parsly.NewToken(kindCode, "Kind", newKindMatcher())
	locationToken           = parsly.NewToken(locationCode, "Location", newLocationMatcher())
)

// Custom matchers
func newIdentifierMatcher() parsly.Matcher {
	return &identifierMatcher{}
}

func newQualifiedTypeMatcher() parsly.Matcher {
	return &qualifiedTypeMatcher{}
}

func newKindMatcher() parsly.Matcher {
	return &kindMatcher{}
}

func newLocationMatcher() parsly.Matcher {
	return &locationMatcher{}
}

// identifierMatcher matches valid identifier names
type identifierMatcher struct{}

func (m *identifierMatcher) Match(cursor *parsly.Cursor) int {
	input := cursor.Input
	pos := cursor.Pos
	size := cursor.InputSize

	if pos >= size {
		return 0
	}

	// First character must be a letter or underscore
	if !isLetter(input[pos]) && input[pos] != '_' {
		return 0
	}

	matched := 1
	for i := pos + 1; i < size; i++ {
		if isLetter(input[i]) || isDigit(input[i]) || input[i] == '_' {
			matched++
			continue
		}
		break
	}

	return matched
}

// qualifiedTypeMatcher matches fully qualified type names (e.g., com.package.Type)
type qualifiedTypeMatcher struct{}

func (m *qualifiedTypeMatcher) Match(cursor *parsly.Cursor) int {
	input := cursor.Input
	pos := cursor.Pos
	size := cursor.InputSize

	if pos >= size {
		return 0
	}
	deps := 0

	matched := 0

	// Capture everything until the closing Square bracket
	for i := pos; i < size; i++ {
		if input[i] == '[' {
			deps++
		}

		if input[i] == ']' {
			if deps == 0 {
				break
			}

			deps--
		}
		matched++
	}

	if matched == 0 {
		return 0
	}

	return matched
}

// kindMatcher matches the kind part (before the slash)
type kindMatcher struct{}

func (m *kindMatcher) Match(cursor *parsly.Cursor) int {
	input := cursor.Input
	pos := cursor.Pos
	size := cursor.InputSize

	if pos >= size {
		return 0
	}

	matched := 0

	// Capture everything until the slash or closing parenthesis
	for i := pos; i < size; i++ {
		if input[i] == '/' || input[i] == ')' {
			break
		}
		matched++
	}

	if matched == 0 {
		return 0
	}

	return matched
}

// locationMatcher matches the location part (after the slash)
type locationMatcher struct{}

func (m *locationMatcher) Match(cursor *parsly.Cursor) int {
	input := cursor.Input
	pos := cursor.Pos
	size := cursor.InputSize

	if pos >= size {
		return 0
	}

	matched := 0

	// Capture everything until the closing parenthesis
	for i := pos; i < size; i++ {
		if input[i] == ')' {
			break
		}
		matched++
	}

	if matched == 0 {
		return 0
	}

	return matched
}

// Helper functions
func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

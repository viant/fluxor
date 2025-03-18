package parameters

import (
	bstate "github.com/viant/bindly/state"
	"github.com/viant/fluxor/model/state"
	"github.com/viant/parsly"
)

// Parse parses a variable with type information in the format: parameterName[fully qualified type name](kind/location)
func Parse(input []byte) (*state.Parameter, error) {
	cursor := parsly.NewCursor("", input, 0)
	parameter := &state.Parameter{Location: &bstate.Location{}}

	// Match the parameter name (identifier)
	matched := cursor.MatchOne(identifierToken)
	if matched.Code != identifierToken.Code {
		return nil, cursor.NewError(identifierToken)
	}
	parameter.Name = matched.Text(cursor)

	// Match the opening Square bracket for type
	matched = cursor.MatchOne(openSquareBracketToken)
	if matched.Code != openSquareBracketToken.Code {
		return nil, cursor.NewError(openSquareBracketToken)
	}

	// Match the fully qualified type name
	matched = cursor.MatchOne(qualifiedTypeToken)
	if matched.Code != qualifiedTypeToken.Code {
		return nil, cursor.NewError(qualifiedTypeToken)
	}
	parameter.DataType = matched.Text(cursor)

	// Match the closing Square bracket
	matched = cursor.MatchOne(closeSquareBracketToken)
	if matched.Code != closeSquareBracketToken.Code {
		return nil, cursor.NewError(closeSquareBracketToken)
	}

	// Match the opening parenthesis for location
	matched = cursor.MatchAfterOptional(whitespaceToken, openParenToken)
	if matched.Code != openParenToken.Code {
		return nil, cursor.NewError(openParenToken)
	}

	// Parse the kind/location part
	matched = cursor.MatchAny(kindToken, closeParenToken)
	switch matched.Code {
	case kindToken.Code:
	case closeParenToken.Code:
		return parameter, nil
	default:
		return nil, cursor.NewError(kindToken)
	}
	kindText := matched.Text(cursor)

	// Check for the separator (/)
	matched = cursor.MatchOne(slashToken)
	if matched.Code != slashToken.Code {
		parameter.Location.Kind = kindText
		matched = cursor.MatchOne(closeParenToken)
		if matched.Code != closeParenToken.Code {
			return nil, cursor.NewError(closeParenToken)
		}
		return parameter, nil
	}

	parameter.Location.Kind = kindText

	// Match the location
	matched = cursor.MatchOne(locationToken)
	if matched.Code != locationToken.Code {
		return nil, cursor.NewError(locationToken)
	}
	parameter.Location.In = matched.Text(cursor)

	// Match the closing parenthesis
	matched = cursor.MatchOne(closeParenToken)
	if matched.Code != closeParenToken.Code {
		return nil, cursor.NewError(closeParenToken)
	}
	return parameter, nil
}

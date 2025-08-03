package cmd

import (
	"testing"
)

func TestParseArg(t *testing.T) {
	var tests = []struct {
		name, field, arg, value string
		operator                Operator
	}{
		{"default", "", "abc", "abc", Equal},
		{"equal", "", "=abc", "abc", Equal},
		{"greaterThan", "", ">abc", "abc", GreaterThan},
		{"greaterThanEqual", "", ">=abc", "abc", GreaterThanEqual},
		{"lessThan", "", "<abc", "abc", LessThan},
		{"lessThanEqual", "", "<=abc", "abc", LessThanEqual},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field, value, operator := ParseArg(test.arg)
			if field != test.field || value != test.value || operator != test.operator {
				t.Errorf("got %s, %s, %s want %s, %s, %s", field, value, operator, test.field, test.value, test.operator)
			}
		})
	}
}

package cmd

import (
	"testing"
)

func TestParseArg(t *testing.T) {
	var tests = []struct {
		name, arg, value string
		operator         Operator
	}{
		{"default", "abc", "abc", Equal},
		{"equal", "=abc", "abc", Equal},
		{"greaterThan", ">abc", "abc", GreaterThan},
		{"greaterThanEqual", ">=abc", "abc", GreaterThanEqual},
		{"lessThan", "<abc", "abc", LessThan},
		{"lessThanEqual", "<=abc", "abc", LessThanEqual},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, operator := ParseArg(test.arg)
			if value != test.value || operator != test.operator {
				t.Errorf("got %s, %s want %s, %s", value, operator, test.value, test.operator)
			}
		})
	}
}

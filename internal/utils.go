package internal

import (
	"fmt"

	"github.com/tidwall/pretty"
)

func PrintJson(json []byte, prettyPrint bool, colorOutput bool) {
	formatString := "%s\n"
	if prettyPrint {
		json = pretty.Pretty(json)
		formatString = "%s" // pretty.Pretty adds a newline
	}
	if colorOutput {
		json = pretty.Color(json, nil)
	}
	fmt.Printf(formatString, json)
}

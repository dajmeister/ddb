package internal

import (
	"fmt"

	"github.com/tidwall/pretty"
)

func PrintJson(json []byte, prettyPrint bool, colorOutput bool) {
	var formatString string
	if prettyPrint {
		json = pretty.Pretty(json)
		formatString = "%s"
	} else {
		formatString = "%s\n"
	}
	if colorOutput {
		json = pretty.Color(json, nil)
	}
	fmt.Printf(formatString, json)
}

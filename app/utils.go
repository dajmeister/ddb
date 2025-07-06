package app

import (
	"fmt"

	"github.com/tidwall/pretty"
)

func PrintJson(json []byte, prettyPrint bool, colorOutput bool) {
	if prettyPrint {
		json = pretty.Pretty(json)
	}
	if colorOutput {
		json = pretty.Color(json, nil)
	}
	fmt.Printf("%s\n", json)
}

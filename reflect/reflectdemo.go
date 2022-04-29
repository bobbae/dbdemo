package demo

import (
	"fmt"
	"reflect"
)

func demo() {
	t := reflect.StructOf([]reflect.StructField{
		{
			Name: "A",
			Type: reflect.TypeOf(int(0)),
			Tag:  `json:"a"`,
		},
		{
			Name: "B",
			Type: reflect.TypeOf(""),
			Tag:  `json:"B"`,
		},
	})

	v := reflect.New(t).Elem()
	v.Field(0).SetInt(1234)
	v.Field(1).SetString("hello")
	d := v.Addr().Interface()

	fmt.Printf("value: %+#v\n", d)

}

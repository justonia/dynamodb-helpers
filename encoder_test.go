package dynamodb_test

import (
	. "backflip/aws/dynamodb"
	"backflip/tools/testutils"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	ck "gopkg.in/check.v1"
)

func TestEncoding(t *testing.T) {
	_ = testutils.GetTestFlags()
	Suite(&EncoderSuite{})
	TestingT(t)
}

type EncoderSuite struct {
}

type Foo struct {
	Int1 int `json:",omitempty"`
}

type NestedStringStruct struct {
	String1 string `json:",omitempty"`
}

type NestedStruct struct {
	Next *NestedStruct
}

type Root struct {
	Bool1   bool     `json:",omitempty"`
	Bool2   *bool    `json:",omitempty"`
	Int1    int      `json:",omitempty"`
	Int2    *int     `json:",omitempty"`
	Float1  float64  `json:",omitempty"`
	Float2  *float64 `json:",omitempty"`
	String1 string   `json:",omitempty"`
	String2 *string  `json:",omitempty"`

	NestedStruct1 *NestedStringStruct `json:",omitempty"`

	InlineStruct struct {
		Int3     int
		FooSlice []Foo
	} `json:",omitempty"`

	Map1 map[string]Foo `json:",omitempty"`
	Map2 map[string]int `json:",omitempty"`

	ByteSlice1 []byte `json:",omitempty"`

	Slice1 []Foo `json:",omitempty"`

	Array1 [4]*Foo
	Array2 [2]int

	Generic1 interface{} `json:",omitempty"`
	Generic2 interface{} `json:",omitempty"`

	GenericArray []interface{} `json:",omitempty"`

	DeepNesting *NestedStruct
}

type AliasedString string

func (s AliasedString) String() string {
	return string(s)
}
func (s AliasedString) MarshalText() ([]byte, error) {
	return []byte(string(s) + "-aliased"), nil
}

func (s *AliasedString) UnmarshalText(text []byte) error {
	t := string(text)
	if !strings.HasSuffix(t, "-aliased") {
		return fmt.Errorf("aliased string was marshaled incorretly and is missing suffix")
	}

	*s = AliasedString(t[:len(t)-len("-aliased")] + "-unaliased")

	return nil
}

type checkers struct {
	funcs []func()
}

func (c *checkers) Add(f func()) {
	c.funcs = append(c.funcs, f)
}
func (c *checkers) Run() {
	for _, f := range c.funcs {
		f()
	}
}

func value(c *ck.C, p string, m map[string]interface{}, expectedKeys int) interface{} {
	ps := strings.Split(p, ".")
	container := m
	for i := 0; i < len(ps)-1; i++ {
		container2, ok := container[ps[i]].(map[string]interface{})
		c.Assert(ok, Equals, true)
		container = container2["M"].(map[string]interface{})
	}
	c.Assert(container, HasLen, expectedKeys)
	return container[ps[len(ps)-1]]
}

func decodeJSON(c *ck.C, d []byte) map[string]interface{} {
	result := make(map[string]interface{})
	err := json.Unmarshal(d, &result)
	c.Assert(err, IsNil)
	c.Assert(result["M"], FitsTypeOf, map[string]interface{}{})
	return result["M"].(map[string]interface{})
}

func rmap(key string, value interface{}) map[string]interface{} {
	return map[string]interface{}{key: value}
}

func (s *EncoderSuite) TestEmptyList(c *ck.C) {
	type X struct {
		A []int
	}
	x1 := X{}
	d, err := Encode(&x1)
	c.Assert(err, IsNil)
	result := decodeJSON(c, d)

	c.Assert(string(d), Equals, `{"M":{"A":{"NULL":true}}}`)
	c.Assert(value(c, "A", result, 1), DeepEquals, rmap("NULL", true))

	x1.A = []int{}
	d, err = Encode(&x1)
	c.Assert(err, IsNil)
	result = decodeJSON(c, d)

	c.Assert(value(c, "A", result, 1), DeepEquals, rmap("NULL", true))
}

func (s *EncoderSuite) TestNull(c *ck.C) {
	type Empty struct {
		Empty    []int
		EmptyMap map[string]int
	}
	x := Empty{}
	c.Assert(x.Empty, IsNil)
	c.Assert(x.EmptyMap, IsNil)

	d, err := Encode(&x)
	c.Assert(err, IsNil)
	result := decodeJSON(c, d)

	c.Assert(value(c, "Empty", result, 2), DeepEquals, rmap("NULL", true))
	c.Assert(value(c, "EmptyMap", result, 2), DeepEquals, rmap("NULL", true))
}

func (s *EncoderSuite) TestMarshalTextValue(c *ck.C) {
	type X struct {
		X AliasedString
	}
	x := X{"foo"}
	d, err := Encode(&x)
	c.Assert(err, IsNil)
	c.Assert(string(d), Equals, `{"M":{"X":{"S":"foo-aliased"}}}`)
}

type fakeJSON struct {
	F string
}

func (f *fakeJSON) MarshalJSON() ([]byte, error) {
	return []byte(`"` + f.F + `"`), nil
}
func (f *fakeJSON) UnmarshalJSON(d []byte) error {
	s := string(d)
	f.F = s[1 : len(s)-1]
	return nil
}

func (s *EncoderSuite) TestJSONValue(c *ck.C) {
	type foo struct {
		T1 time.Time
		T2 *time.Time
		T3 *time.Time
		F1 fakeJSON
	}
	t1 := time.Now()
	time.Sleep(1000 * time.Nanosecond)
	t2 := time.Now()
	f := &foo{t1, &t2, nil, fakeJSON{"foo"}}
	d, err := Encode(f)
	c.Assert(err, IsNil)
	result := decodeJSON(c, d)

	f1D := base64.StdEncoding.EncodeToString([]byte(`"` + f.F1.F + `"`))
	c.Assert(value(c, "F1", result, 4), DeepEquals, rmap("B", f1D))

	t1Str := t1.Format(`"` + time.RFC3339Nano + `"`)
	t2Str := t2.Format(`"` + time.RFC3339Nano + `"`)
	t1D := base64.StdEncoding.EncodeToString([]byte(t1Str))
	t2D := base64.StdEncoding.EncodeToString([]byte(t2Str))
	c.Assert(value(c, "T1", result, 4), DeepEquals, rmap("B", t1D))
	c.Assert(value(c, "T2", result, 4), DeepEquals, rmap("B", t2D))
	c.Assert(value(c, "T3", result, 4), DeepEquals, rmap("NULL", true))
}

func (s *EncoderSuite) TestEncodeValue(c *ck.C) {
	ch := &checkers{}

	value := func(p string, m map[string]interface{}, expectedKeys int) interface{} {
		ps := strings.Split(p, ".")
		container := m
		for i := 0; i < len(ps)-1; i++ {
			container2, ok := container[ps[i]].(map[string]interface{})
			c.Assert(ok, Equals, true)
			container = container2["M"].(map[string]interface{})
		}
		c.Assert(container, HasLen, expectedKeys)
		return container[ps[len(ps)-1]]
	}

	result := map[string]interface{}{}
	rmap := func(key string, value interface{}) map[string]interface{} { return map[string]interface{}{key: value} }

	rootNumKeys := reflect.TypeOf(Root{}).NumField()
	rootValue := func(p string, m map[string]interface{}) interface{} { return value(p, m, rootNumKeys) }

	root := Root{}
	root.Bool1 = true
	ch.Add(func() { c.Assert(rootValue("Bool1", result), DeepEquals, rmap("BOOL", true)) })

	b := false
	root.Bool2 = &b
	ch.Add(func() { c.Assert(rootValue("Bool2", result), DeepEquals, rmap("BOOL", false)) })

	root.Int1 = 10
	ch.Add(func() { c.Assert(rootValue("Int1", result), DeepEquals, rmap("N", "10")) })

	n := 20
	root.Int2 = &n
	ch.Add(func() { c.Assert(rootValue("Int2", result), DeepEquals, rmap("N", "20")) })

	root.Float1 = 25.25
	ch.Add(func() { c.Assert(rootValue("Float1", result), DeepEquals, rmap("N", "25.25")) })

	f := 50.5
	root.Float2 = &f
	ch.Add(func() { c.Assert(rootValue("Float2", result), DeepEquals, rmap("N", "50.5")) })

	root.String1 = "foo"
	ch.Add(func() { c.Assert(rootValue("String1", result), DeepEquals, rmap("S", "foo")) })

	str := "bar"
	root.String2 = &str
	ch.Add(func() { c.Assert(rootValue("String2", result), DeepEquals, rmap("S", "bar")) })

	root.NestedStruct1 = &NestedStringStruct{String1: "baz"}
	ch.Add(func() { c.Assert(value("NestedStruct1.String1", result, 1), DeepEquals, rmap("S", "baz")) })

	root.InlineStruct.Int3 = 30
	ch.Add(func() { c.Assert(value("InlineStruct.Int3", result, 2), DeepEquals, rmap("N", "30")) })
	root.InlineStruct.FooSlice = []Foo{
		Foo{Int1: 100},
		Foo{Int1: 200},
	}
	ch.Add(func() {
		c.Assert(value("InlineStruct.FooSlice", result, 2), DeepEquals, rmap("L", []interface{}{
			rmap("M", rmap("Int1", rmap("N", "100"))),
			rmap("M", rmap("Int1", rmap("N", "200"))),
		}))
	})

	root.Map1 = map[string]Foo{
		"a": Foo{Int1: 300},
		"b": Foo{Int1: 400},
	}
	ch.Add(func() {
		c.Assert(rootValue("Map1", result), DeepEquals, rmap("M", map[string]interface{}{
			"a": rmap("M", rmap("Int1", rmap("N", "300"))),
			"b": rmap("M", rmap("Int1", rmap("N", "400"))),
		}))
	})

	root.Map2 = map[string]int{"c": 500, "d": 600, "e": 0}
	ch.Add(func() {
		c.Assert(rootValue("Map2", result), DeepEquals, rmap("M", map[string]interface{}{
			"c": rmap("N", "500"),
			"d": rmap("N", "600"),
			"e": rmap("N", "0"),
		}))
	})

	root.ByteSlice1 = []byte{0x7b, 0x22, 0x53, 0x22, 0x3a, 0x22, 0x66, 0x6f, 0x6f, 0x22, 0x7d}
	ch.Add(func() {
		c.Assert(rootValue("ByteSlice1", result), DeepEquals, rmap("B", base64.StdEncoding.EncodeToString(root.ByteSlice1)))
	})

	root.Slice1 = []Foo{
		Foo{Int1: 700},
		Foo{Int1: 800},
	}
	ch.Add(func() {
		c.Assert(rootValue("Slice1", result), DeepEquals, rmap("L", []interface{}{
			rmap("M", rmap("Int1", rmap("N", "700"))),
			rmap("M", rmap("Int1", rmap("N", "800"))),
		}))
	})

	root.Array1[0] = &Foo{Int1: 900}
	root.Array1[1] = &Foo{Int1: 1000}
	root.Array1[2] = &Foo{Int1: 1100}
	root.Array1[3] = &Foo{}
	ch.Add(func() {
		c.Assert(rootValue("Array1", result), DeepEquals, rmap("L", []interface{}{
			rmap("M", rmap("Int1", rmap("N", "900"))),
			rmap("M", rmap("Int1", rmap("N", "1000"))),
			rmap("M", rmap("Int1", rmap("N", "1100"))),
			rmap("M", map[string]interface{}{}),
		}))
	})

	root.Array2[0] = 1300
	root.Array2[1] = 1400
	ch.Add(func() {
		c.Assert(rootValue("Array2", result), DeepEquals, rmap("L", []interface{}{
			rmap("N", "1300"),
			rmap("N", "1400"),
		}))
	})

	root.Generic1 = 1500
	ch.Add(func() { c.Assert(rootValue("Generic1", result), DeepEquals, rmap("N", "1500")) })

	root.Generic2 = &NestedStringStruct{String1: "nublet"}
	ch.Add(func() {
		c.Assert(rootValue("Generic2", result), DeepEquals, rmap("M", rmap("String1", rmap("S", "nublet"))))
	})

	root.GenericArray = []interface{}{
		&Foo{Int1: 1700},
		"foobar",
		10,
	}
	ch.Add(func() {
		c.Assert(rootValue("GenericArray", result), DeepEquals, rmap("L", []interface{}{
			rmap("M", rmap("Int1", rmap("N", "1700"))),
			rmap("S", "foobar"),
			rmap("N", "10"),
		}))
	})

	root.DeepNesting = &NestedStruct{&NestedStruct{&NestedStruct{&NestedStruct{}}}}
	ch.Add(func() {
		c.Assert(rootValue("DeepNesting", result), DeepEquals,
			rmap("M", rmap("Next",
				rmap("M", rmap("Next",
					rmap("M", rmap("Next",
						rmap("M", rmap("Next",
							rmap("NULL", true))))))))))
	})

	d, err := Encode(&root)
	c.Assert(err, IsNil)
	result = decodeJSON(c, d)

	ch.Run()
}

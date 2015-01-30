package dynamodb_test

import (
	. "backflip/aws/dynamodb"
	"backflip/tools/testutils"
	"encoding/base64"
	"fmt"
	"strconv"
	"testing"
	"time"

	ck "gopkg.in/check.v1"
)

func TestDecoding(t *testing.T) {
	_ = testutils.GetTestFlags()
	Suite(&DecoderSuite{})
	TestingT(t)
}

type DecoderSuite struct {
}

func (s *DecoderSuite) TestJSONValue(c *ck.C) {
	type foo struct {
		T1 time.Time
		T2 *time.Time
		T3 *time.Time
		F1 fakeJSON
	}

	t1 := time.Now()
	time.Sleep(1000 * time.Nanosecond)
	t2 := time.Now()
	t1Str := t1.Format(`"` + time.RFC3339Nano + `"`)
	t2Str := t2.Format(`"` + time.RFC3339Nano + `"`)
	t1D := base64.StdEncoding.EncodeToString([]byte(t1Str))
	t2D := base64.StdEncoding.EncodeToString([]byte(t2Str))

	d := []byte(fmt.Sprintf(`{"M":{"F1":{"B":"ImZvbyI="},"T1":{"B":"%s"},"T2":{"B":"%s"},"T3":{"NULL":true}}}`, t1D, t2D))

	t3 := time.Now()
	f := foo{T3: &t3}
	err := Decode(d, &f)
	c.Assert(err, IsNil)
	c.Assert(f.T1.Equal(t1), Equals, true)
	c.Assert(f.T2.Equal(t2), Equals, true)
	c.Assert(f.T3, IsNil)
	c.Assert(f.F1.F, Equals, "foo") // from encoder test
}

func (s *EncoderSuite) TestUnmarshalTextValue(c *ck.C) {
	type X struct {
		X AliasedString
	}
	x := X{"bar"}
	err := Decode([]byte(`{"M":{"X":{"S":"foo-aliased"}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.X, Equals, AliasedString("foo-unaliased"))
}

func (s *DecoderSuite) TestBasicBool(c *ck.C) {
	type b struct {
		Value bool
	}
	x := b{}
	c.Assert(x.Value, Equals, false)
	err := Decode([]byte(`{"M":{"Value":{"BOOL":true}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, true)
	// should overwrite existing value
	err = Decode([]byte(`{"M":{"Value":{"BOOL":false}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, false)
	// null == false
	err = Decode([]byte(`{"M":{"Value":{"NULL":true}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, false)

	// pointer value
	type bp struct {
		Value *bool
	}
	p := bp{}
	c.Assert(p.Value, Equals, (*bool)(nil))
	err = Decode([]byte(`{"M":{"Value":{"BOOL":true}}}`), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, NotNil)
	c.Assert(*p.Value, Equals, true)
	err = Decode([]byte(`{"M":{"Value":{"BOOL":false}}}`), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, NotNil)
	c.Assert(*p.Value, Equals, false)
	err = Decode([]byte(`{"M":{"Value":{"NULL":true}}}`), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, IsNil)
}

func (s *DecoderSuite) TestBasicInt(c *ck.C) {
	var i64Val int64 = 1234567167890123456
	type b struct {
		Value int64
	}
	x := b{}
	c.Assert(x.Value, Equals, int64(0))
	err := Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatInt(i64Val, 10))), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, i64Val)
	// should overwrite existing value
	i64Val = 10
	err = Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatInt(i64Val, 10))), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, i64Val)
	// null == 0
	err = Decode([]byte(`{"M":{"Value":{"NULL":true}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, int64(0))

	// pointer value
	type bp struct {
		Value *int64
	}
	p := bp{}
	c.Assert(p.Value, Equals, (*int64)(nil))
	i64Val = 1234567167890123456
	err = Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatInt(i64Val, 10))), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, NotNil)
	c.Assert(*p.Value, Equals, i64Val)
	// should overwrite
	i64Val = 10
	err = Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatInt(i64Val, 10))), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, NotNil)
	c.Assert(*p.Value, Equals, i64Val)
	// should set to null
	err = Decode([]byte(`{"M":{"Value":{"NULL":true}}}`), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, IsNil)
}

func (s *DecoderSuite) TestBasicUint(c *ck.C) {
	var u64Val uint64 = 1234567167890123456
	type b struct {
		Value uint64
	}
	x := b{}
	c.Assert(x.Value, Equals, uint64(0))
	err := Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatUint(u64Val, 10))), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, u64Val)
	// should overwrite existing value
	u64Val = 10
	err = Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatUint(u64Val, 10))), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, u64Val)
	// null == 0
	err = Decode([]byte(`{"M":{"Value":{"NULL":true}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, uint64(0))

	// pointer value
	type bp struct {
		Value *uint64
	}
	p := bp{}
	c.Assert(p.Value, Equals, (*uint64)(nil))
	u64Val = 1234567167890123456
	err = Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatUint(u64Val, 10))), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, NotNil)
	c.Assert(*p.Value, Equals, u64Val)
	// should overwrite
	u64Val = 10
	err = Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatUint(u64Val, 10))), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, NotNil)
	c.Assert(*p.Value, Equals, u64Val)
	// should set to null
	err = Decode([]byte(`{"M":{"Value":{"NULL":true}}}`), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, IsNil)
}

func (s *DecoderSuite) TestBasicFloat(c *ck.C) {
	var f64Val float64 = 1234567167890123456
	type b struct {
		Value float64
	}
	x := b{}
	c.Assert(x.Value, Equals, float64(0))
	err := Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatFloat(f64Val, 'g', -1, 64))), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, f64Val)
	// should overwrite existing value
	f64Val = 10
	err = Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatFloat(f64Val, 'g', -1, 64))), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, f64Val)
	// null == 0
	err = Decode([]byte(`{"M":{"Value":{"NULL":true}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, float64(0))

	// pointer value
	type bp struct {
		Value *float64
	}
	p := bp{}
	c.Assert(p.Value, Equals, (*float64)(nil))
	f64Val = 1234567167890123456
	err = Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatFloat(f64Val, 'g', -1, 64))), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, NotNil)
	c.Assert(*p.Value, Equals, f64Val)
	// should overwrite
	f64Val = 10
	err = Decode([]byte(fmt.Sprintf(`{"M":{"Value":{"N":"%s"}}}`, strconv.FormatFloat(f64Val, 'g', -1, 64))), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, NotNil)
	c.Assert(*p.Value, Equals, f64Val)
	// should set to null
	err = Decode([]byte(`{"M":{"Value":{"NULL":true}}}`), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, IsNil)
}

func (s *DecoderSuite) TestBasicString(c *ck.C) {
	type b struct {
		Value string
	}
	x := b{}
	c.Assert(x.Value, Equals, "")
	err := Decode([]byte(`{"M":{"Value":{"S":"foo"}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, "foo")
	// should overwrite existing value
	err = Decode([]byte(`{"M":{"Value":{"S":"bar"}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, "bar")
	// null == false
	err = Decode([]byte(`{"M":{"Value":{"NULL":true}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.Value, Equals, "")

	// pointer value
	type bp struct {
		Value *string
	}
	p := bp{}
	c.Assert(p.Value, Equals, (*string)(nil))
	err = Decode([]byte(`{"M":{"Value":{"S":"foo"}}}`), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, NotNil)
	c.Assert(*p.Value, Equals, "foo")
	err = Decode([]byte(`{"M":{"Value":{"S":"bar"}}}`), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, NotNil)
	c.Assert(*p.Value, Equals, "bar")
	err = Decode([]byte(`{"M":{"Value":{"NULL":true}}}`), &p)
	c.Assert(err, IsNil)
	c.Assert(p.Value, IsNil)
}

func (s *DecoderSuite) TestNestedStruct(c *ck.C) {
	type b struct {
		Value  string
		Custom string `json:"custom"`
	}
	type a struct {
		B      b
		AValue string
	}
	x := a{}
	err := Decode([]byte(`{"M":{"AValue":{"S":"bar"},"B":{"M":{"Value":{"S":"foo"},"custom":{"S":"baz"}}}}}`), &x)
	c.Assert(err, IsNil)
	c.Assert(x.AValue, Equals, "bar")
	c.Assert(x.B.Custom, Equals, "baz")
	c.Assert(x.B.Value, Equals, "foo")

	// from JSON tests
	type S9 struct {
		X int
		Y int
	}

	type S6 struct {
		X int
	}

	type S7 S6

	type S8 struct {
		S9
	}

	type S5 struct {
		S6
		S7
		S8
	}

	in := `{"M":{"X": {"N":"1"},"Y":{"N":"2"}}}`
	s5 := &S5{}
	err = Decode([]byte(in), s5)
	c.Assert(err, IsNil)
	c.Assert(*s5, DeepEquals, S5{S8: S8{S9: S9{Y: 2}}})

	type S11 struct {
		S6
	}

	type S12 struct {
		S6
	}

	type S13 struct {
		S8
	}

	type S10 struct {
		S11
		S12
		S13
	}

	in = `{"M":{"X": {"N":"1"}, "Y":{"N":"2"}}}`
	s10 := &S10{}
	err = Decode([]byte(in), s10)
	c.Assert(err, IsNil)
	c.Assert(*s10, DeepEquals, S10{S13: S13{S8: S8{S9: S9{Y: 2}}}})
}

func (s *DecoderSuite) TestMaps(c *ck.C) {
	x1 := map[string]int{}
	err := Decode([]byte(`{"M":{"A":{"N":"1"},"B":{"N":"2"}}}`), &x1)
	c.Assert(err, IsNil)
	c.Assert(x1["A"], Equals, 1)
	c.Assert(x1["B"], Equals, 2)

	// float
	x2 := map[string]float64{}
	err = Decode([]byte(`{"M":{"A":{"N":"1"},"B":{"N":"2"},"C":{"N":"40.2"}}}`), &x2)
	c.Assert(err, IsNil)
	c.Assert(x2["A"], Equals, float64(1))
	c.Assert(x2["B"], Equals, float64(2))
	c.Assert(x2["C"], Equals, float64(40.2))

	// map of structs
	type foo struct {
		X int
	}
	x3 := map[string]foo{}
	err = Decode([]byte(`{"M":{"A":{"M":{"X":{"N":"10"}}}, "B":{"M":{"X":{"N":"20"}}}}}`), &x3)
	c.Assert(err, IsNil)
	c.Assert(x3["A"].X, Equals, 10)
	c.Assert(x3["B"].X, Equals, 20)

	// no implicit conversion
	x1 = map[string]int{}
	err = Decode([]byte(`{"M":{"C":{"N":"40.2"}}}`), &x1)
	c.Assert(err, ErrorMatches, ".*overflow number.*for type int.*")

	// no maps with non-string keys
	x4 := map[int]int{}
	err = Decode([]byte(`{"M":{"C":{"N":"40.2"}}}`), &x4)
	c.Assert(err, ErrorMatches, ".*cannot decode.*non-string key.*")
}

func (s *DecoderSuite) TestByteArrayLike(c *ck.C) {
	type x struct {
		A []byte
	}
	// string type -> []byte
	x1 := x{}
	err := Decode([]byte(`{"M":{"A":{"S":"aSBhbSB0aGUgdmVyeSBtb2RlbCBvZiBhIG1vZGVybiBtYWpvciBnZW5lcmFs"}}}`), &x1)
	c.Assert(err, IsNil)
	c.Assert(x1.A, NotNil)
	c.Assert(string(x1.A), Equals, "i am the very model of a modern major general")

	// native []byte type
	x1 = x{}
	err = Decode([]byte(`{"M":{"A":{"B":"aSBhbSB0aGUgdmVyeSBtb2RlbCBvZiBhIG1vZGVybiBtYWpvciBnZW5lcmFs"}}}`), &x1)
	c.Assert(err, IsNil)
	c.Assert(x1.A, NotNil)
	c.Assert(string(x1.A), Equals, "i am the very model of a modern major general")

	// set nil
	c.Assert(x1.A, NotNil)
	err = Decode([]byte(`{"M":{"A":{"NULL":true}}}`), &x1)
	c.Assert(err, IsNil)
	c.Assert(x1.A, IsNil)

	// bad string value
	x1 = x{}
	err = Decode([]byte(`{"M":{"A":{"S":"foobar"}}}`), &x1)
	c.Assert(err, ErrorMatches, ".*cannot base64 decode.*")

	// fail silently on other data types
	x1 = x{}
	err = Decode([]byte(`{"M":{"A":{"N":"1"}}}`), &x1)
	c.Assert(err, IsNil)
	c.Assert(x1.A, IsNil)
}

func (s *DecoderSuite) TestOtherArrayLike(c *ck.C) {
	// regular list and overwriting of an existing variable
	type x struct {
		A []int
		B []int
	}
	x1 := x{}
	x1.B = []int{1, 2, 3}
	err := Decode([]byte(`{"M":{"A":{"L":[
		{"N":"100"},
		{"N":"200"}
		]}, "B":{"NULL":true}
	}}`), &x1)
	c.Assert(err, IsNil)
	c.Assert(x1.A, NotNil)
	c.Assert(x1.A, HasLen, 2)
	c.Assert(x1.A[0], Equals, int(100))
	c.Assert(x1.A[1], Equals, int(200))
	c.Assert(x1.B, IsNil)

	// fixed array of pointers
	type v struct {
		A [4]*int
	}
	var a0, a1, a2, a3 = 1, 2, 3, 4
	x0 := v{A: [4]*int{&a0, &a1, &a2, &a3}}
	err = Decode([]byte(`{"M":{"A":{"L":[
		{"N":"100"},
		{"N":"200"}
	]}}}`), &x0)
	c.Assert(err, IsNil)
	c.Assert(x0.A[0], NotNil)
	c.Assert(x0.A[1], NotNil)
	c.Assert(*x0.A[0], Equals, int(100))
	c.Assert(*x0.A[1], Equals, int(200))
	c.Assert(x0.A[2], Equals, (*int)(nil))
	c.Assert(x0.A[3], Equals, (*int)(nil))

	// list of pointers
	type y struct {
		A []*int
	}
	x2 := y{}
	err = Decode([]byte(`{"M":{"A":{"L":[
		{"N":"100"},
		{"N":"200"}
	]}}}`), &x2)
	c.Assert(err, IsNil)
	c.Assert(x2.A, NotNil)
	c.Assert(x2.A, HasLen, 2)
	c.Assert(*x2.A[0], Equals, int(100))
	c.Assert(*x2.A[1], Equals, int(200))

	// generic interface decoding
	type z struct {
		A []interface{}
	}

	x3 := z{}
	err = Decode([]byte(`{"M":{"A":{"L":[
		{"N":"100"},
		{"N":"200"},
		{"S":"foo"},
		{"BOOL":true},
		{"L":[
			{"N":"300"},
			{"S":"bar"}
		]},
		{"NULL":true}
	]}}}`), &x3)
	c.Assert(err, IsNil)
	c.Assert(x3.A, NotNil)
	c.Assert(x3.A, HasLen, 6)

	c.Assert(x3.A[0], FitsTypeOf, float64(0))
	c.Assert(x3.A[0].(float64), Equals, float64(100))

	c.Assert(x3.A[1], FitsTypeOf, float64(0))
	c.Assert(x3.A[1].(float64), Equals, float64(200))

	c.Assert(x3.A[2], FitsTypeOf, "")
	c.Assert(x3.A[2].(string), Equals, "foo")

	c.Assert(x3.A[3], FitsTypeOf, true)
	c.Assert(x3.A[3].(bool), Equals, true)

	c.Assert(x3.A[4], FitsTypeOf, []interface{}{})
	c.Assert(x3.A[4].([]interface{}), DeepEquals, []interface{}{
		float64(300),
		"bar",
	})

	c.Assert(x3.A[5], IsNil)
}

type IFake interface {
	SomeMethod()
}

func (s *DecoderSuite) TestInterface(c *ck.C) {
	// generic interface
	x1 := map[string]interface{}{}
	err := Decode([]byte(`{
		"M":{
			"A":{"BOOL":true},
			"B":{"BOOL":false},
			"C":{"S":"foo"},
			"D":{"N":"1234"},
			"E":{"N":"100.55"},
			"F":{"NULL":true},
			"G":{"M":{
				"1":{"S":"bar"}
			}},
			"H":{"B":"aSBhbSB0aGUgdmVyeSBtb2RlbCBvZiBhIG1vZGVybiBtYWpvciBnZW5lcmFs"},
			"I":{"L":[
				{"N":"200"},
				{"S":"400"},
				{"L":[
					{"N":"500"}
				]}
			]}
		}}`), &x1)
	c.Assert(err, IsNil)
	c.Assert(x1["A"], FitsTypeOf, true)
	c.Assert(x1["A"].(bool), Equals, true)
	c.Assert(x1["B"], FitsTypeOf, true)
	c.Assert(x1["B"].(bool), Equals, false)
	c.Assert(x1["C"], FitsTypeOf, "")
	c.Assert(x1["C"].(string), Equals, "foo")
	c.Assert(x1["D"], FitsTypeOf, float64(0))
	c.Assert(x1["D"].(float64), Equals, float64(1234))
	c.Assert(x1["E"], FitsTypeOf, float64(0))
	c.Assert(x1["E"].(float64), Equals, float64(100.55))
	c.Assert(x1["F"], Equals, nil)
	c.Assert(x1["G"], FitsTypeOf, map[string]interface{}{})

	m := x1["G"].(map[string]interface{})
	c.Assert(m["1"], FitsTypeOf, "")
	c.Assert(m["1"], Equals, "bar")

	c.Assert(x1["H"], FitsTypeOf, []byte{})
	c.Assert(string(x1["H"].([]byte)), Equals, "i am the very model of a modern major general")

	c.Assert(x1["I"], FitsTypeOf, []interface{}{})
	l := x1["I"].([]interface{})
	c.Assert(l, HasLen, 3)
	c.Assert(l[0], Equals, float64(200))
	c.Assert(l[1], Equals, "400")
	c.Assert(l[2], FitsTypeOf, []interface{}{})
	l1 := l[2].([]interface{})
	c.Assert(l1, HasLen, 1)
	c.Assert(l1[0], Equals, float64(500))

	// can't decode into a non-empty interface
	x2 := map[string]IFake{}
	err = Decode([]byte(`{"M":{"A":{"BOOL":true}}}`), &x2)
	c.Assert(err, ErrorMatches, ".*cannot decode.*non-empty interface.*")
}

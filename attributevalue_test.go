package dynamodb_test

import (
	. "backflip/aws/dynamodb"
	"backflip/tools/testutils"
	"encoding/json"
	"testing"

	ck "gopkg.in/check.v1"
)

var TestingT = ck.TestingT

var Equals = ck.Equals
var IsNil = ck.IsNil
var NotNil = ck.NotNil
var Suite = ck.Suite
var DeepEquals = ck.DeepEquals
var HasLen = ck.HasLen
var ErrorMatches = ck.ErrorMatches
var FitsTypeOf = ck.FitsTypeOf
var PanicMatches = ck.PanicMatches

func TestValue(t *testing.T) {
	_ = testutils.GetTestFlags()
	Suite(&AttributeValueSuite{})
	TestingT(t)
}

type AttributeValueSuite struct {
}

func (s *AttributeValueSuite) getValue(c *ck.C, data []byte) *AttributeValue {
	v := &AttributeValue{}
	err := json.Unmarshal(data, v)
	c.Assert(err, IsNil)
	return v
}
func (s *AttributeValueSuite) encodeValue(c *ck.C, item interface{}) []byte {
	d, err := json.Marshal(item)
	c.Assert(err, IsNil)
	return d
}

func (s *AttributeValueSuite) TestInvalid(c *ck.C) {
	a := &AttributeValue{}
	c.Assert(a.Type(), Equals, INVALID_ATTRIBUTEVALUE_TYPE)
	c.Assert(a.IsValid(), Equals, false)
}

func (s *AttributeValueSuite) TestNULLValue(c *ck.C) {
	data := []byte(`{"NULL":true}`)

	v := s.getValue(c, data)

	c.Assert(v.IsValid(), Equals, true)
	c.Assert(v.BOOL, IsNil)
	c.Assert(v.S, IsNil)
	c.Assert(v.M, IsNil)
	c.Assert(v.L, IsNil)
	c.Assert(v.B, IsNil)

	c.Assert(v.NULL, NotNil)
	c.Assert(*v.NULL, Equals, true)

	c.Assert(s.encodeValue(c, v), DeepEquals, data)
}

func (s *AttributeValueSuite) TestBytesValue(c *ck.C) {
	data := []byte(`{"B":"aSBhbSB0aGUgdmVyeSBtb2RlbCBvZiBhIG1vZGVybiBtYWpvciBnZW5lcmFs"}`)

	v := s.getValue(c, data)

	c.Assert(v.IsValid(), Equals, true)
	c.Assert(v.BOOL, IsNil)
	c.Assert(v.S, IsNil)
	c.Assert(v.M, IsNil)
	c.Assert(v.L, IsNil)
	c.Assert(v.NULL, IsNil)

	c.Assert(v.B, NotNil)
	c.Assert(string(v.B), Equals, "i am the very model of a modern major general")

	c.Assert(s.encodeValue(c, v), DeepEquals, data)
}

func (s *AttributeValueSuite) TestBoolValue(c *ck.C) {
	data := []byte(`{"BOOL":true}`)

	v := s.getValue(c, data)
	c.Assert(v.IsValid(), Equals, true)
	c.Assert(v.B, IsNil)
	c.Assert(v.S, IsNil)
	c.Assert(v.M, IsNil)
	c.Assert(v.L, IsNil)
	c.Assert(v.NULL, IsNil)

	c.Assert(v.BOOL, NotNil)
	c.Assert(*v.BOOL, Equals, true)

	c.Assert(s.encodeValue(c, v), DeepEquals, data)
}

func (s *AttributeValueSuite) TestStringValue(c *ck.C) {
	data := []byte(`{"S":"blah blah blah"}`)

	v := s.getValue(c, data)
	c.Assert(v.IsValid(), Equals, true)
	c.Assert(v.B, IsNil)
	c.Assert(v.BOOL, IsNil)
	c.Assert(v.L, IsNil)
	c.Assert(v.M, IsNil)
	c.Assert(v.NULL, IsNil)

	c.Assert(v.S, NotNil)
	c.Assert(*v.S, Equals, "blah blah blah")

	c.Assert(s.encodeValue(c, v), DeepEquals, data)
}

func (s *AttributeValueSuite) TestNumberValue(c *ck.C) {
	data := []byte(`{"N":"42.35"}`)

	v := s.getValue(c, data)
	c.Assert(v.IsValid(), Equals, true)
	c.Assert(v.B, IsNil)
	c.Assert(v.BOOL, IsNil)
	c.Assert(v.L, IsNil)
	c.Assert(v.S, IsNil)
	c.Assert(v.M, IsNil)
	c.Assert(v.NULL, IsNil)

	c.Assert(v.N, NotNil)
	c.Assert(*v.N, Equals, "42.35")

	c.Assert(s.encodeValue(c, v), DeepEquals, data)
}

func (s *AttributeValueSuite) TestMapValue(c *ck.C) {
	data := []byte(`{"M":{"foo":{"S":"foo"}}}`)

	v := s.getValue(c, data)
	c.Assert(v.IsValid(), Equals, true)
	c.Assert(v.B, IsNil)
	c.Assert(v.BOOL, IsNil)
	c.Assert(v.S, IsNil)
	c.Assert(v.L, IsNil)
	c.Assert(v.NULL, IsNil)

	c.Assert(v.M, NotNil)
	v2, ok := v.M["foo"]
	c.Assert(ok, Equals, true)
	c.Assert(v2.S, NotNil)
	c.Assert(*v2.S, Equals, "foo")

	c.Assert(string(s.encodeValue(c, v)), DeepEquals, string(data))
}

func (s *AttributeValueSuite) TestArrayValue(c *ck.C) {
	data := []byte(`{"L":[{"BOOL":true},{"S":"foo"}]}`)

	v := s.getValue(c, data)
	c.Assert(v.IsValid(), Equals, true)
	c.Assert(v.B, IsNil)
	c.Assert(v.BOOL, IsNil)
	c.Assert(v.S, IsNil)
	c.Assert(v.M, IsNil)
	c.Assert(v.NULL, IsNil)

	c.Assert(v.L, NotNil)
	c.Assert(v.L, HasLen, 2)
	c.Assert(v.L[0].BOOL, NotNil)
	c.Assert(*v.L[0].BOOL, Equals, true)
	c.Assert(v.L[1].S, NotNil)
	c.Assert(*v.L[1].S, Equals, "foo")

	c.Assert(string(s.encodeValue(c, v)), DeepEquals, string(data))
}

func (s *AttributeValueSuite) TestEmptyValue(c *ck.C) {
	v := AttributeValue{}
	_, err := json.Marshal(&v)
	c.Assert(err, ErrorMatches, ".*cannot serialize.*with no values.*")
}

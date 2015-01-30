package dynamodb

import (
	"encoding/json"
	"fmt"
)

type AttributeValue struct {
	B    []byte
	BOOL *bool
	S    *string
	N    *string
	NULL *bool
	M    AttributeValueMap
	L    []*AttributeValue
}

func (a *AttributeValue) IsValid() bool {
	return a.B != nil || a.BOOL != nil || a.S != nil || a.N != nil || a.NULL != nil || a.M != nil || a.L != nil
}

func (a *AttributeValue) Type() AttributeValueType {
	switch {
	case a.B != nil:
		return B
	case a.BOOL != nil:
		return BOOL
	case a.S != nil:
		return S
	case a.N != nil:
		return N
	case a.NULL != nil:
		return NULL
	case a.M != nil:
		return M
	case a.L != nil:
		return L
	default:
		return INVALID_ATTRIBUTEVALUE_TYPE
	}
}

func (a *AttributeValue) MarshalJSON() ([]byte, error) {
	switch {
	case a.B != nil:
		return json.Marshal(struct{ B []byte }{a.B})
	case a.BOOL != nil:
		return json.Marshal(struct{ BOOL *bool }{a.BOOL})
	case a.S != nil:
		return json.Marshal(struct{ S *string }{a.S})
	case a.N != nil:
		return json.Marshal(struct{ N *string }{a.N})
	case a.NULL != nil:
		return json.Marshal(struct{ NULL *bool }{a.NULL})
	case a.M != nil:
		// omitempty in a default json encoding will use null in place of an empty map,
		// and if we don't mark it as omitempty, when serializing other fields it will
		// still be included as null (which is a dynamodb format exception).
		if len(a.M) == 0 {
			return []byte(`{"M":{}}`), nil
		} else {
			return json.Marshal(struct{ M AttributeValueMap }{a.M})
		}
	case a.L != nil:
		// Same rationale as for map above.
		if len(a.L) == 0 {
			return []byte(`{"L":[]}`), nil
		} else {
			return json.Marshal(struct{ L []*AttributeValue }{a.L})
		}
	default:
		return nil, fmt.Errorf("cannot serialize an AttributeValue with no values set")
	}
}

type AttributeValueMap map[string]*AttributeValue

type AttributeValueType int

func (a AttributeValueType) String() string {
	switch {
	case a == B:
		return "B"
	case a == BOOL:
		return "BOOL"
	case a == L:
		return "L"
	case a == M:
		return "M"
	case a == N:
		return "N"
	case a == NULL:
		return "NULL"
	case a == S:
		return "S"
	case a == INVALID_ATTRIBUTEVALUE_TYPE:
		return "INVALID"
	default:
		panic(fmt.Errorf("aws.dynamodb: unknown AttributeValueType %#v", a))
	}
}

func (a AttributeValueType) MarshalJSON() ([]byte, error) {
	return []byte(`"` + a.String() + `"`), nil
}

func (a *AttributeValueType) UnmarshalJSON(d []byte) error {
	s := string(d)
	switch s {
	case `"B"`:
		*a = B
	case `"BOOL"`:
		*a = BOOL
	case `"L"`:
		*a = L
	case `"M"`:
		*a = M
	case `"N"`:
		*a = N
	case `"NULL"`:
		*a = NULL
	case `"S"`:
		*a = S
	default:
		*a = INVALID_ATTRIBUTEVALUE_TYPE
		return fmt.Errorf("aws.dynamodb: unknown AttributeValueType %s", s)
	}
	return nil
}

const (
	INVALID_ATTRIBUTEVALUE_TYPE AttributeValueType = iota

	B
	BOOL
	L
	M
	N
	NULL
	S
)

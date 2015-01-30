package dynamodb

import (
	"encoding"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
)

func Encode(item interface{}) ([]byte, error) {
	attr, err := EncodeToAttributeValue(item)
	if err != nil {
		return nil, err
	}

	if d, err := json.Marshal(attr); err != nil {
		return nil, EncodeError{fmt.Sprintf("%s", err.Error())}
	} else {
		return d, nil
	}
}

func EncodeToAttributeValue(item interface{}) (*AttributeValue, error) {
	if av, ok := item.(*AttributeValue); ok {
		return av, nil
	}

	attr, err := convertToAttribute(reflect.ValueOf(item))
	if err != nil {
		return nil, err
	}

	return attr, nil
}

func MustEncodeToAttributeValue(item interface{}) *AttributeValue {
	if av, err := EncodeToAttributeValue(item); err != nil {
		panic(err)
	} else {
		return av
	}
}

type EncodeError struct {
	Message string
}

func (e EncodeError) Error() string {
	return fmt.Sprintf("aws.dynamodb.EncodeError: %s", e.Message)
}

// private
func encodeJSONValue(v reflect.Value) (*AttributeValue, error) {
	d, err := json.Marshal(v.Interface())
	if err != nil {
		return nil, EncodeError{fmt.Sprintf("error encoding json value type: %s", err.Error())}
	}
	if len(d) > 0 {
		return &AttributeValue{B: d}, nil
	} else {
		t := true
		return &AttributeValue{NULL: &t}, nil
	}
}

func encodeTextValue(v reflect.Value) (*AttributeValue, error) {
	b, err := v.Interface().(encoding.TextMarshaler).MarshalText()
	if err != nil {
		return nil, EncodeError{fmt.Sprintf("error encoding opaque value type: %s", err.Error())}
	}
	if len(b) > 0 {
		b2 := string(b)
		return &AttributeValue{S: &b2}, nil
	} else {
		t := true
		return &AttributeValue{NULL: &t}, nil
	}
}

var JSONMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
var TextMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()

func convertToAttribute(v reflect.Value) (*AttributeValue, error) {
	vt := v.Type()
	if vt.NumMethod() > 0 {
		if vt.Implements(JSONMarshalerType) {
			if v.Kind() == reflect.Ptr && v.IsNil() {
				b := true
				return &AttributeValue{NULL: &b}, nil
			}
			return encodeJSONValue(v)
		} else if vt.Implements(TextMarshalerType) {
			if v.Kind() == reflect.Ptr && v.IsNil() {
				b := true
				return &AttributeValue{NULL: &b}, nil
			}
			return encodeTextValue(v)
		}
	}

	if v.Kind() != reflect.Ptr && v.CanAddr() {
		addr := v.Addr()
		if addr.Type().NumMethod() > 0 {
			if addr.Type().Implements(JSONMarshalerType) {
				if addr.IsNil() {
					b := true
					return &AttributeValue{NULL: &b}, nil
				}
				return encodeJSONValue(addr)
			} else if addr.Type().Implements(TextMarshalerType) {
				if addr.IsNil() {
					b := true
					return &AttributeValue{NULL: &b}, nil
				}
				return encodeTextValue(addr)
			}
		}
	}
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			b := true
			return &AttributeValue{NULL: &b}, nil
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Bool:
		b := v.Bool()
		return &AttributeValue{BOOL: &b}, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64:
		n := convertToNumericString(v)
		return &AttributeValue{N: &n}, nil

	case reflect.String:
		s := v.String()
		if len(s) == 0 {
			b := true
			return &AttributeValue{NULL: &b}, nil
		} else {
			return &AttributeValue{S: &s}, nil
		}

	case reflect.Struct:
		var err error
		var out AttributeValueMap
		if out, err = encodeStruct(v); err != nil {
			return nil, err
		}
		return &AttributeValue{M: out}, nil

	case reflect.Map:
		if v.IsNil() {
			b := true
			return &AttributeValue{NULL: &b}, nil
		}

		if v.Type().Key().Kind() != reflect.String {
			return nil, EncodeError{fmt.Sprintf("only maps with string keys are supported")}
		}

		containerOut := AttributeValueMap{}
		for _, key := range v.MapKeys() {
			v2, err := convertToAttribute(v.MapIndex(key))
			if err != nil {
				return nil, err
			}
			if v2 != nil {
				containerOut[key.String()] = v2
			} else {
				b := true
				containerOut[key.String()] = &AttributeValue{NULL: &b}
			}
		}
		return &AttributeValue{M: containerOut}, nil

	case reflect.Slice:
		// empty lists are not supported in dynamo, kinda sucks we can't
		// differentiate nil slices from empty slices...
		if v.IsNil() || v.Len() == 0 {
			b := true
			return &AttributeValue{NULL: &b}, nil
		}

		// Special-case, byte blob, binary can't be nil...
		if v.Type().Elem().Kind() == reflect.Uint8 {
			if v.Len() == 0 {
				b := true
				return &AttributeValue{NULL: &b}, nil
			} else {
				return &AttributeValue{B: v.Bytes()}, nil
			}
		}

		fallthrough

	case reflect.Array:
		arrayLength := v.Len()
		containerOut := make([]*AttributeValue, arrayLength)
		for i := 0; i < arrayLength; i++ {
			v2, err := convertToAttribute(v.Index(i))
			if err != nil {
				return nil, err
			}
			if v2 != nil {
				containerOut[i] = v2
			} else {
				b := true
				containerOut[i] = &AttributeValue{NULL: &b}
			}
		}
		return &AttributeValue{L: containerOut}, nil

	case reflect.Ptr, reflect.Interface:
		// should have had indirection taken care of above
		panic(EncodeError{fmt.Sprintf("could not handle multiple layers of pointers")})

	default:
		return nil, EncodeError{fmt.Sprintf("aws.dynamodb.EncodeError: unsupported type for field: %#v", v.Type())}
	}
}

func encodeStruct(v reflect.Value) (AttributeValueMap, error) {
	out := AttributeValueMap{}
	for _, f := range cachedTypeFields(v.Type()) { // loop on each field
		fv := fieldByIndex(v, f.index)
		if !fv.IsValid() || (f.omitEmpty && isEmptyValue(fv)) {
			continue
		}

		attr, err := convertToAttribute(fv)
		if err != nil {
			return nil, err
		}

		if attr != nil {
			out[f.name] = attr
		}
	}
	return out, nil
}

func convertToNumericString(v reflect.Value) string {
	switch v.Kind() {
	case reflect.Bool:
		x := v.Bool()
		if x {
			return "1"
		} else {
			return "0"
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(v.Uint(), 10)

	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if math.IsInf(f, 0) || math.IsNaN(f) {
			panic(fmt.Errorf("aws.dynamodb.convertToNumericString: NaN and infinite floats not supported"))
		}
		return strconv.FormatFloat(f, 'g', -1, v.Type().Bits())
	default:
		panic(fmt.Errorf("aws.dynamodb.convertToNumericString: unsupported type %#v", v.Type()))
	}
}

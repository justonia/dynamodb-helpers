package dynamodb

import (
	"encoding"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

func Decode(data []byte, item interface{}) error {
	root, err := DecodeToAttributeValue(data)
	if err != nil {
		return err
	}
	err = DecodeAttributeValueToInterface(root, item)
	return err
}

func DecodeToAttributeValue(data []byte) (*AttributeValue, error) {
	root := &AttributeValue{}
	if err := json.Unmarshal(data, root); err != nil {
		return nil, DecodeError{err.Error(), false}
	}
	return root, nil
}

func DecodeAttributeValueToInterface(attr *AttributeValue, item interface{}) error {
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		return decodeAttribute(attr, v.Elem())
	} else {
		return decodeAttribute(attr, v)
	}
}

type DecodeError struct {
	Message           string
	IsNumericOverflow bool
}

func (e DecodeError) Error() string {
	return fmt.Sprintf("aws.dynamodb.DecodeError: %s", e.Message)
}

// private
func decodeStruct(attrs AttributeValueMap, v reflect.Value) error {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	fields := cachedTypeFields(v.Type())
	for k, attr := range attrs {
		// find actual key name since a json specifier can override the
		// go structure's field name.
		var fv reflect.Value
		for _, f := range fields {
			if f.name == k {
				subv := v
				for _, i := range f.index {
					if subv.Kind() == reflect.Ptr {
						if subv.IsNil() {
							subv.Set(reflect.New(subv.Type().Elem()))
						}
						subv = subv.Elem()
					}
					subv = subv.Field(i)
				}
				fv = subv
				break
			}
		}
		if !fv.IsValid() {
			continue
		}
		if err := decodeAttribute(attr, fv); err != nil {
			return err
		}
	}
	return nil
}

func decodeMap(attr AttributeValueMap, v reflect.Value) error {
	// map must have string kind
	if !v.IsValid() {
		v.Set(reflect.MakeMap(v.Type()))
	}
	elemType := v.Type().Elem()
	for key, subAttr := range attr {
		value := reflect.New(elemType).Elem()
		if err := decodeAttribute(subAttr, value); err != nil {
			return err
		}
		kv := reflect.ValueOf(key).Convert(v.Type().Key())
		v.SetMapIndex(kv, value)
	}
	return nil
}

func decodeArray(attr *AttributeValue, v reflect.Value) error {
	t := v.Type()

	if attr.NULL != nil || attr.L == nil {
		v.Set(reflect.Zero(t))
		return nil
	}

	if t.Kind() == reflect.Slice {
		v.Set(reflect.MakeSlice(v.Type(), len(attr.L), len(attr.L)))
	}

	vlen := v.Len()

	if vlen == 0 {
		return nil
	}

	switch t.Elem().Kind() {
	case reflect.Interface:
		if t.Elem().NumMethod() != 0 {
			// TODO: If custom decoding hooks can be provided, support this
			return DecodeError{fmt.Sprintf("cannot decode into array of non-empty interface types: %s", v.Type().String()), false}
		}

		i := 0
		alen := len(attr.L)
		for ; i < vlen && i < alen; i++ {
			if err := decodeAttribute(attr.L[i], v.Index(i)); err != nil {
				return err
			}
		}

		// zero out the rest
		for ; i < vlen; i++ {
			v.Index(i).Set(reflect.Zero(t.Elem()))
		}

		return nil

	default:
		i := 0
		alen := len(attr.L)
		for ; i < vlen && i < alen; i++ {
			av := v.Index(i)
			if err := decodeAttribute(attr.L[i], av); err != nil {
				return err
			}
			if !av.IsValid() || !av.Type().AssignableTo(t.Elem()) {
				return DecodeError{fmt.Sprintf("could not assign list value %s to array element type %s", v.Type().String(), t.Elem().String()), false}
			}
		}

		for ; i < vlen; i++ {
			v.Index(i).Set(reflect.Zero(t.Elem()))
		}
	}
	return nil
}

var imapType = reflect.TypeOf(map[string]interface{}{})
var ilistType = reflect.TypeOf([]interface{}{})

func decodeJSONValue(attr *AttributeValue, v reflect.Value) error {
	if attr.B != nil {
		if v.Kind() != reflect.Ptr && v.CanAddr() {
			v = v.Addr()
		}
		if err := json.Unmarshal(attr.B, v.Interface()); err != nil {
			return DecodeError{fmt.Sprintf("error decoding json value type: %s", err.Error()), false}
		}
	} else if attr.NULL != nil {
		v.Set(reflect.Zero(v.Type()))
	}
	return nil
}

func decodeTextValue(attr *AttributeValue, v reflect.Value) error {
	if attr.S != nil {
		if v.Kind() != reflect.Ptr && v.CanAddr() {
			v = v.Addr()
		}
		if err := v.Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(*attr.S)); err != nil {
			return DecodeError{fmt.Sprintf("error decoding text value type: %s", err.Error()), false}
		}
	} else if attr.NULL != nil {
		v.Set(reflect.Zero(v.Type()))
	}
	return nil
}

var JSONUnmarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
var TextUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

func decodeAttribute(attr *AttributeValue, v reflect.Value) error {
	if v.Kind() == reflect.Ptr {
		if attr.NULL != nil {
			v.Set(reflect.Zero(v.Type()))
			return nil
		} else {
			v.Set(reflect.New(v.Type().Elem()))
			v = v.Elem()
		}
	}

	if v.Type().NumMethod() > 0 {
		if v.Type().Implements(JSONUnmarshalerType) {
			return decodeJSONValue(attr, v)
		} else if v.Type().Implements(TextUnmarshalerType) {
			return decodeTextValue(attr, v)
		}
	}

	if v.Kind() != reflect.Ptr && v.CanAddr() {
		addr := v.Addr()
		if addr.Type().NumMethod() > 0 {
			if addr.Type().Implements(JSONUnmarshalerType) {
				return decodeJSONValue(attr, addr)
			} else if addr.Type().Implements(TextUnmarshalerType) {
				return decodeTextValue(attr, v)
			}
		}
	}

	switch v.Kind() {
	case reflect.Bool:
		if attr.BOOL != nil {
			v.Set(reflect.ValueOf(*attr.BOOL))
		} else if attr.NULL != nil {
			v.Set(reflect.ValueOf(false))
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if attr.N != nil {
			n, err := strconv.ParseInt(*attr.N, 10, 64)
			if err != nil || v.OverflowInt(n) {
				return DecodeError{fmt.Sprintf("overflow number %s for type %s", *attr.N, v.Type().String()), true}
			}
			v.SetInt(n)
		} else if attr.NULL != nil {
			v.Set(reflect.Zero(v.Type()))
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if attr.N != nil {
			n, err := strconv.ParseUint(*attr.N, 10, 64)
			if err != nil || v.OverflowUint(n) {
				return DecodeError{fmt.Sprintf("overflow number %s for type %s", *attr.N, v.Type().String()), true}
			}
			v.SetUint(n)
		} else if attr.NULL != nil {
			v.Set(reflect.Zero(v.Type()))
		}

	case reflect.Float32, reflect.Float64:
		if attr.N != nil {
			n, err := strconv.ParseFloat(*attr.N, v.Type().Bits())
			if err != nil || v.OverflowFloat(n) {
				return DecodeError{fmt.Sprintf("overflow number %s for type %s", *attr.N, v.Type().String()), true}
			}
			v.SetFloat(n)
		} else if attr.NULL != nil {
			v.Set(reflect.Zero(v.Type()))
		}

	case reflect.String:
		if attr.S != nil {
			v.SetString(*attr.S)
		} else if attr.NULL != nil {
			v.Set(reflect.Zero(v.Type()))
		}

	case reflect.Struct:
		if attr.M != nil {
			if err := decodeStruct(attr.M, v); err != nil {
				return err
			}
		} else if attr.NULL != nil {
			v.Set(reflect.Zero(v.Type()))
		}

	case reflect.Map:
		if attr.M != nil {
			// map must have string kind
			t := v.Type()
			if t.Key().Kind() != reflect.String {
				return DecodeError{fmt.Sprintf("cannot decode a map with a non-string key: %s", t.Key().String()), false}
			}
			if v.IsNil() {
				v.Set(reflect.MakeMap(t))
			}
			if err := decodeMap(attr.M, v); err != nil {
				return err
			}
		} else if attr.NULL != nil {
			v.Set(reflect.Zero(v.Type()))
		}

	case reflect.Slice:
		// []byte handling
		if v.Type().Elem().Kind() == reflect.Uint8 {
			switch {
			case attr.B != nil:
				v.Set(reflect.ValueOf(attr.B[0:len(attr.B)]))

			case attr.S != nil:
				d, err := base64.StdEncoding.DecodeString(*attr.S)
				if err != nil {
					return DecodeError{fmt.Sprintf("cannot base64 decode string: %s", err.Error()), false}
				}
				v.Set(reflect.ValueOf(d))

			case attr.NULL != nil:
				v.Set(reflect.Zero(v.Type()))

			default:
				// nothing to do, silently ignore failed coercion
			}
			return nil
		}

		fallthrough

	case reflect.Array:
		return decodeArray(attr, v)

	case reflect.Interface:
		if v.NumMethod() != 0 {
			// TODO: Might be worth adding a custom demarshalling hook
			return DecodeError{fmt.Sprintf("cannot decode into non-empty interface type: %s", v.Type().String()), false}
		}

		switch {
		case attr.B != nil:
			v.Set(reflect.ValueOf(attr.B[0:len(attr.B)]))
			break

		case attr.BOOL != nil:
			v.Set(reflect.ValueOf(*attr.BOOL))

		case attr.S != nil:
			v.Set(reflect.ValueOf(*attr.S))

		case attr.N != nil:
			var n float64
			var err error
			n, err = strconv.ParseFloat(*attr.N, 64)
			if err != nil {
				return DecodeError{fmt.Sprintf("error parsing number %s into type float64", *attr.N), false}
			}
			v.Set(reflect.ValueOf(n))

		case attr.NULL != nil:
			v.Set(reflect.Zero(v.Type()))
			break

		case attr.M != nil:
			m := reflect.MakeMap(imapType)
			if err := decodeMap(attr.M, m); err != nil {
				return err
			}
			v.Set(m)

		case attr.L != nil:
			l := reflect.New(ilistType)
			if err := decodeArray(attr, l.Elem()); err != nil {
				return err
			}
			v.Set(l.Elem())

		default:
			panic(DecodeError{"unknown error decoding interface value", false})
		}

	case reflect.Ptr:
		// should have had indirection taken care of above
		panic(DecodeError{fmt.Sprintf("could not handle multiple layers of pointers"), false})
	default:
		return DecodeError{fmt.Sprintf("aws.dynamodb.EncodeError: unsupported type for field: %#v", v.Type()), false}
	}
	return nil
}

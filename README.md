# dynamodb-helpers
DynamoDB AttributeValue encode/decode adapted from encoding/json

This is currently used in production in a private AWS library.

```
    type MyNestedStruct struct {
        Field3 int
    }

    type MyStruct struct {
        Field1 int
        Field2 map[string]MyNestedStruct
    }

    m1 := &MyStruct{
        Field1: 10,
        Field2: map[string]MyNestedStruct{
            "foo": MyNestedStruct{10},
            "bar": MyNestedStruct{20},
        },
    }

    // attrValue is now a complete AttributeValue object
    attrValue, err := EncodeToAttributeValue(m1)
    
    m2 := &MyStruct{}
    err = DecodeAttributeValueToInterface(attrValue, m2)

    // m1 == m2

```

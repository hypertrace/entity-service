# IMPORTANT NOTE

`DocStoreJsonFormat` should be used to generate JSON that
will be sent to Doc Store.

The source code for `DocStoreJsonFormat` has been picked from 
`com.google.protobuf.util.JsonFormat` of the `protobuf-java-util:3.12.2` library.

64 bit number data types are generated as string by `com.google.protobuf.util.JsonFormat`
and will be interpreted as string by Doc Store. See how Mongo interprets data types
based on JSON syntax here: https://docs.mongodb.com/manual/reference/mongodb-extended-json/ 

The only modification made is in the `private void printSingleFieldValue(final FieldDescriptor field, final Object value, boolean alwaysWithQuotes)` method

Code diff:

```
case INT64:
case SINT64:
case SFIXED64:
  Long longVal = ((Long) value);
  if (alwaysWithQuotes) {
    generator.print("\"");
    generator.print(longVal.toString());
    generator.print("\"");
  }
  else {
    generator.print(longVal.toString());
  }
  break;
```
// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bsoncodec // import "go.mongodb.org/mongo-driver/bson/bsoncodec"

import (
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	emptyValue = reflect.Value{}
)

// Marshaler is an interface implemented by types that can marshal themselves
// into a BSON document represented as bytes. The bytes returned must be a valid
// BSON document if the error is nil.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Marshaler] instead.
type Marshaler interface {
	MarshalBSON() ([]byte, error)
}

// ValueMarshaler is an interface implemented by types that can marshal
// themselves into a BSON value as bytes. The type must be the valid type for
// the bytes returned. The bytes and byte type together must be valid if the
// error is nil.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.ValueMarshaler] instead.
type ValueMarshaler interface {
	MarshalBSONValue() (bsontype.Type, []byte, error)
}

// Unmarshaler is an interface implemented by types that can unmarshal a BSON
// document representation of themselves. The BSON bytes can be assumed to be
// valid. UnmarshalBSON must copy the BSON bytes if it wishes to retain the data
// after returning.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Unmarshaler] instead.
type Unmarshaler interface {
	UnmarshalBSON([]byte) error
}

// ValueUnmarshaler is an interface implemented by types that can unmarshal a
// BSON value representation of themselves. The BSON bytes and type can be
// assumed to be valid. UnmarshalBSONValue must copy the BSON value bytes if it
// wishes to retain the data after returning.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.ValueUnmarshaler] instead.
type ValueUnmarshaler interface {
	UnmarshalBSONValue(bsontype.Type, []byte) error
}

// ValueEncoderError is an error returned from a ValueEncoder when the provided value can't be
// encoded by the ValueEncoder.
type ValueEncoderError struct {
	Name     string
	Types    []reflect.Type
	Kinds    []reflect.Kind
	Received reflect.Value
}

func (vee ValueEncoderError) Error() string {
	typeKinds := make([]string, 0, len(vee.Types)+len(vee.Kinds))
	for _, t := range vee.Types {
		typeKinds = append(typeKinds, t.String())
	}
	for _, k := range vee.Kinds {
		if k == reflect.Map {
			typeKinds = append(typeKinds, "map[string]*")
			continue
		}
		typeKinds = append(typeKinds, k.String())
	}
	received := vee.Received.Kind().String()
	if vee.Received.IsValid() {
		received = vee.Received.Type().String()
	}
	return fmt.Sprintf("%s can only encode valid %s, but got %s", vee.Name, strings.Join(typeKinds, ", "), received)
}

// ValueDecoderError is an error returned from a ValueDecoder when the provided value can't be
// decoded by the ValueDecoder.
type ValueDecoderError struct {
	Name     string
	Types    []reflect.Type
	Kinds    []reflect.Kind
	Received reflect.Value
}

func (vde ValueDecoderError) Error() string {
	typeKinds := make([]string, 0, len(vde.Types)+len(vde.Kinds))
	for _, t := range vde.Types {
		typeKinds = append(typeKinds, t.String())
	}
	for _, k := range vde.Kinds {
		if k == reflect.Map {
			typeKinds = append(typeKinds, "map[string]*")
			continue
		}
		typeKinds = append(typeKinds, k.String())
	}
	received := vde.Received.Kind().String()
	if vde.Received.IsValid() {
		received = vde.Received.Type().String()
	}
	return fmt.Sprintf("%s can only decode valid and settable %s, but got %s", vde.Name, strings.Join(typeKinds, ", "), received)
}

// TypeDecoderError is an error returned from a TypeDecoder when the provided type can't be
// decoded by the TypeDecoder.
type TypeDecoderError struct {
	Name     string
	Types    []reflect.Type
	Kinds    []reflect.Kind
	Received bsontype.Type
}

// Error implements the error interface.
func (tde TypeDecoderError) Error() string {
	typeKinds := make([]string, 0, len(tde.Types)+len(tde.Kinds))
	for _, t := range tde.Types {
		typeKinds = append(typeKinds, t.String())
	}
	for _, k := range tde.Kinds {
		if k == reflect.Map {
			typeKinds = append(typeKinds, "map[string]*")
			continue
		}
		typeKinds = append(typeKinds, k.String())
	}
	return fmt.Sprintf("cannot decode %s into a %s", tde.Received.String(), strings.Join(typeKinds, ", "))
}

// MultiDecodeError represents mutliple wrapped DecodeError that occurs when unmarshalling BSON bytes into a native Go type.
type MultiDecodeError struct {
	wrapped []error
}

// Error implements the error interface.
func (de *MultiDecodeError) Error() string {
	ret := ""
	if len(de.wrapped) > 0 {
		ret = "Decoding errors => \n"
		for i, err := range de.wrapped {
			de, ok := err.(*DecodeError)
			if ok {
				if i > 0 {
					ret = fmt.Sprintf("%s\n\t%s", ret, de.Error())
				} else {
					ret = fmt.Sprintf("%s\t%s", ret, de.Error())
				}

			}
		}
	}
	return ret
}

// Append a bson key name on a decode error
func (de *MultiDecodeError) setWrappedDecodeErrorKey(errKey string) {
	if len(errKey) <= 0 {
		return
	}
	for _, err := range de.wrapped {
		de, ok := err.(*DecodeError)
		if ok {
			de.setWrappedDecodeErrorKey(errKey)
		}
	}
}

// Create a Wrapped decoder error, setting the errKey on all child errors
// If there is only one child errors, return this one
func newMultiDecodeError(errKey string, childErrors ...error) error {
	if len(childErrors) == 0 {
		return nil
	} else if len(childErrors) == 1 {
		if de, ok := childErrors[0].(*DecodeError); ok {
			de.setWrappedDecodeErrorKey(errKey)
			return de
		} else if mde, ok := childErrors[0].(*MultiDecodeError); ok {
			mde.setWrappedDecodeErrorKey(errKey)
			return mde
		} else {
			ret := &DecodeError{
				wrapped: childErrors[0],
			}
			ret.setWrappedDecodeErrorKey(errKey)
			return ret
		}
	}
	errs := make([]error, 0)
	for _, err := range childErrors {
		if de, ok := err.(*DecodeError); ok {
			de.setWrappedDecodeErrorKey(errKey)
			errs = append(errs, de)
		} else if mde, ok := err.(*MultiDecodeError); ok {
			umde := mde.Unwrap()
			if de, ok := umde.(*DecodeError); ok {
				de.setWrappedDecodeErrorKey(errKey)
				errs = append(errs, de)
			} else if mde, ok := umde.(*MultiDecodeError); ok {
				mde.setWrappedDecodeErrorKey(errKey)
				errs = append(errs, mde.wrapped...)
			} else {
				de := &DecodeError{
					wrapped: umde,
				}
				de.setWrappedDecodeErrorKey(errKey)
				errs = append(errs, de)
			}
		} else {
			de := &DecodeError{
				wrapped: err,
			}
			de.setWrappedDecodeErrorKey(errKey)
			errs = append(errs, de)
		}
	}
	return &MultiDecodeError{
		wrapped: errs,
	}
}

// Unwrap returns the underlying errors
func (de *MultiDecodeError) Unwrap() error {
	// If we have no errors then we do nothing
	if de == nil || de.wrapped == nil {
		return nil
	}
	// If we have exactly one error, we can just return that directly.
	if len(de.wrapped) == 1 {
		return de.wrapped[0]
	}

	errs := make([]error, 0)
	for _, err := range de.wrapped {
		if de, ok := err.(*DecodeError); ok {
			errs = append(errs, de)
		} else if mde, ok := err.(*MultiDecodeError); ok {
			umde := mde.Unwrap()
			if de, ok := umde.(*DecodeError); ok {
				errs = append(errs, de)
			} else if mde, ok := umde.(*MultiDecodeError); ok {
				errs = append(errs, mde.wrapped...)
			}
		}
	}
	return &MultiDecodeError{
		wrapped: errs,
	}
}

// DecodeError represents an error that occurs when unmarshalling BSON bytes into a native Go type.
type DecodeError struct {
	keys    []string
	wrapped error
}

// Error implements the error interface.
func (de *DecodeError) Error() string {
	if de.keys != nil && len(de.keys) > 0 {
		keyPath := strings.Join(de.Keys(), ".")
		return fmt.Sprintf("error decoding key \"%s\" : %s", keyPath, de.Unwrap())
	} else {
		return fmt.Sprintf("decoding error : %s", de.Unwrap())
	}
}

// Append a bson key name on a decode error
func (de *DecodeError) setWrappedDecodeErrorKey(errKey string) {
	if len(errKey) <= 0 {
		return
	}
	de.keys = append(de.keys, errKey)
}

// Unwrap returns the underlying errors
func (de *DecodeError) Unwrap() error {
	if de == nil || de.wrapped == nil {
		return nil
	}

	return de.wrapped
}

// Keys returns the BSON key path that caused an error as a slice of strings. The keys in the slice are in top-down
// order. For example, if the document being unmarshalled was {a: {b: {c: 1}}} and the value for c was supposed to be
// a string, the keys slice will be ["a", "b", "c"].
func (de *DecodeError) Keys() []string {
	reversedKeys := make([]string, 0, len(de.keys))
	for idx := len(de.keys) - 1; idx >= 0; idx-- {
		reversedKeys = append(reversedKeys, de.keys[idx])
	}

	return reversedKeys
}

// EncodeContext is the contextual information required for a Codec to encode a
// value.
type EncodeContext struct {
	*Registry

	// MinSize causes the Encoder to marshal Go integer values (int, int8, int16, int32, int64,
	// uint, uint8, uint16, uint32, or uint64) as the minimum BSON int size (either 32 or 64 bits)
	// that can represent the integer value.
	//
	// Deprecated: Use bson.Encoder.IntMinSize instead.
	MinSize bool

	errorOnInlineDuplicates bool
	stringifyMapKeysWithFmt bool
	nilMapAsEmpty           bool
	nilSliceAsEmpty         bool
	nilByteSliceAsEmpty     bool
	omitZeroStruct          bool
	useJSONStructTags       bool
}

// ErrorOnInlineDuplicates causes the Encoder to return an error if there is a duplicate field in
// the marshaled BSON when the "inline" struct tag option is set.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Encoder.ErrorOnInlineDuplicates] instead.
func (ec *EncodeContext) ErrorOnInlineDuplicates() {
	ec.errorOnInlineDuplicates = true
}

// StringifyMapKeysWithFmt causes the Encoder to convert Go map keys to BSON document field name
// strings using fmt.Sprintf() instead of the default string conversion logic.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Encoder.StringifyMapKeysWithFmt] instead.
func (ec *EncodeContext) StringifyMapKeysWithFmt() {
	ec.stringifyMapKeysWithFmt = true
}

// NilMapAsEmpty causes the Encoder to marshal nil Go maps as empty BSON documents instead of BSON
// null.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Encoder.NilMapAsEmpty] instead.
func (ec *EncodeContext) NilMapAsEmpty() {
	ec.nilMapAsEmpty = true
}

// NilSliceAsEmpty causes the Encoder to marshal nil Go slices as empty BSON arrays instead of BSON
// null.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Encoder.NilSliceAsEmpty] instead.
func (ec *EncodeContext) NilSliceAsEmpty() {
	ec.nilSliceAsEmpty = true
}

// NilByteSliceAsEmpty causes the Encoder to marshal nil Go byte slices as empty BSON binary values
// instead of BSON null.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Encoder.NilByteSliceAsEmpty] instead.
func (ec *EncodeContext) NilByteSliceAsEmpty() {
	ec.nilByteSliceAsEmpty = true
}

// OmitZeroStruct causes the Encoder to consider the zero value for a struct (e.g. MyStruct{})
// as empty and omit it from the marshaled BSON when the "omitempty" struct tag option is set.
//
// Note that the Encoder only examines exported struct fields when determining if a struct is the
// zero value. It considers pointers to a zero struct value (e.g. &MyStruct{}) not empty.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Encoder.OmitZeroStruct] instead.
func (ec *EncodeContext) OmitZeroStruct() {
	ec.omitZeroStruct = true
}

// UseJSONStructTags causes the Encoder to fall back to using the "json" struct tag if a "bson"
// struct tag is not specified.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Encoder.UseJSONStructTags] instead.
func (ec *EncodeContext) UseJSONStructTags() {
	ec.useJSONStructTags = true
}

// DecodeContext is the contextual information required for a Codec to decode a
// value.
type DecodeContext struct {
	*Registry

	// Truncate, if true, instructs decoders to to truncate the fractional part of BSON "double"
	// values when attempting to unmarshal them into a Go integer (int, int8, int16, int32, int64,
	// uint, uint8, uint16, uint32, or uint64) struct field. The truncation logic does not apply to
	// BSON "decimal128" values.
	//
	// Deprecated: Use bson.Decoder.AllowTruncatingDoubles instead.
	Truncate bool

	// Ancestor is the type of a containing document. This is mainly used to determine what type
	// should be used when decoding an embedded document into an empty interface. For example, if
	// Ancestor is a bson.M, BSON embedded document values being decoded into an empty interface
	// will be decoded into a bson.M.
	//
	// Deprecated: Use bson.Decoder.DefaultDocumentM or bson.Decoder.DefaultDocumentD instead.
	Ancestor reflect.Type

	// defaultDocumentType specifies the Go type to decode top-level and nested BSON documents into. In particular, the
	// usage for this field is restricted to data typed as "interface{}" or "map[string]interface{}". If DocumentType is
	// set to a type that a BSON document cannot be unmarshaled into (e.g. "string"), unmarshalling will result in an
	// error. DocumentType overrides the Ancestor field.
	defaultDocumentType reflect.Type

	binaryAsSlice     bool
	useJSONStructTags bool
	useLocalTimeZone  bool
	zeroMaps          bool
	zeroStructs       bool
}

// BinaryAsSlice causes the Decoder to unmarshal BSON binary field values that are the "Generic" or
// "Old" BSON binary subtype as a Go byte slice instead of a primitive.Binary.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Decoder.BinaryAsSlice] instead.
func (dc *DecodeContext) BinaryAsSlice() {
	dc.binaryAsSlice = true
}

// UseJSONStructTags causes the Decoder to fall back to using the "json" struct tag if a "bson"
// struct tag is not specified.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Decoder.UseJSONStructTags] instead.
func (dc *DecodeContext) UseJSONStructTags() {
	dc.useJSONStructTags = true
}

// UseLocalTimeZone causes the Decoder to unmarshal time.Time values in the local timezone instead
// of the UTC timezone.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Decoder.UseLocalTimeZone] instead.
func (dc *DecodeContext) UseLocalTimeZone() {
	dc.useLocalTimeZone = true
}

// ZeroMaps causes the Decoder to delete any existing values from Go maps in the destination value
// passed to Decode before unmarshaling BSON documents into them.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Decoder.ZeroMaps] instead.
func (dc *DecodeContext) ZeroMaps() {
	dc.zeroMaps = true
}

// ZeroStructs causes the Decoder to delete any existing values from Go structs in the destination
// value passed to Decode before unmarshaling BSON documents into them.
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Decoder.ZeroStructs] instead.
func (dc *DecodeContext) ZeroStructs() {
	dc.zeroStructs = true
}

// DefaultDocumentM causes the Decoder to always unmarshal documents into the primitive.M type. This
// behavior is restricted to data typed as "interface{}" or "map[string]interface{}".
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Decoder.DefaultDocumentM] instead.
func (dc *DecodeContext) DefaultDocumentM() {
	dc.defaultDocumentType = reflect.TypeOf(primitive.M{})
}

// DefaultDocumentD causes the Decoder to always unmarshal documents into the primitive.D type. This
// behavior is restricted to data typed as "interface{}" or "map[string]interface{}".
//
// Deprecated: Use [go.mongodb.org/mongo-driver/bson.Decoder.DefaultDocumentD] instead.
func (dc *DecodeContext) DefaultDocumentD() {
	dc.defaultDocumentType = reflect.TypeOf(primitive.D{})
}

// ValueCodec is an interface for encoding and decoding a reflect.Value.
// values.
//
// Deprecated: Use [ValueEncoder] and [ValueDecoder] instead.
type ValueCodec interface {
	ValueEncoder
	ValueDecoder
}

// ValueEncoder is the interface implemented by types that can handle the encoding of a value.
type ValueEncoder interface {
	EncodeValue(EncodeContext, bsonrw.ValueWriter, reflect.Value) error
}

// ValueEncoderFunc is an adapter function that allows a function with the correct signature to be
// used as a ValueEncoder.
type ValueEncoderFunc func(EncodeContext, bsonrw.ValueWriter, reflect.Value) error

// EncodeValue implements the ValueEncoder interface.
func (fn ValueEncoderFunc) EncodeValue(ec EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	return fn(ec, vw, val)
}

// ValueDecoder is the interface implemented by types that can handle the decoding of a value.
type ValueDecoder interface {
	DecodeValue(DecodeContext, bsonrw.ValueReader, reflect.Value) error
}

// ValueDecoderFunc is an adapter function that allows a function with the correct signature to be
// used as a ValueDecoder.
type ValueDecoderFunc func(DecodeContext, bsonrw.ValueReader, reflect.Value) error

// DecodeValue implements the ValueDecoder interface.
func (fn ValueDecoderFunc) DecodeValue(dc DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	return fn(dc, vr, val)
}

// typeDecoder is the interface implemented by types that can handle the decoding of a value given its type.
type typeDecoder interface {
	decodeType(DecodeContext, bsonrw.ValueReader, reflect.Type) (reflect.Value, error)
}

// typeDecoderFunc is an adapter function that allows a function with the correct signature to be used as a typeDecoder.
type typeDecoderFunc func(DecodeContext, bsonrw.ValueReader, reflect.Type) (reflect.Value, error)

func (fn typeDecoderFunc) decodeType(dc DecodeContext, vr bsonrw.ValueReader, t reflect.Type) (reflect.Value, error) {
	return fn(dc, vr, t)
}

// decodeAdapter allows two functions with the correct signatures to be used as both a ValueDecoder and typeDecoder.
type decodeAdapter struct {
	ValueDecoderFunc
	typeDecoderFunc
}

var _ ValueDecoder = decodeAdapter{}
var _ typeDecoder = decodeAdapter{}

// decodeTypeOrValue calls decoder.decodeType is decoder is a typeDecoder. Otherwise, it allocates a new element of type
// t and calls decoder.DecodeValue on it.
func decodeTypeOrValue(decoder ValueDecoder, dc DecodeContext, vr bsonrw.ValueReader, t reflect.Type) (reflect.Value, error) {
	td, _ := decoder.(typeDecoder)
	return decodeTypeOrValueWithInfo(decoder, td, dc, vr, t, true)
}

func decodeTypeOrValueWithInfo(vd ValueDecoder, td typeDecoder, dc DecodeContext, vr bsonrw.ValueReader, t reflect.Type, convert bool) (reflect.Value, error) {
	if td != nil {
		val, err := td.decodeType(dc, vr, t)
		if err != nil || !val.IsValid() {
			return val, err
		}
		if err == nil && convert && val.Type() != t {
			// This conversion step is necessary for slices and maps. If a user declares variables like:
			//
			// type myBool bool
			// var m map[string]myBool
			//
			// and tries to decode BSON bytes into the map, the decoding will fail if this conversion is not present
			// because we'll try to assign a value of type bool to one of type myBool.
			val = val.Convert(t)
		}
		return val, err
	}

	val := reflect.New(t).Elem()
	err := vd.DecodeValue(dc, vr, val)
	return val, err
}

// CodecZeroer is the interface implemented by Codecs that can also determine if
// a value of the type that would be encoded is zero.
//
// Deprecated: Defining custom rules for the zero/empty value will not be supported in Go Driver
// 2.0. Users who want to omit empty complex values should use a pointer field and set the value to
// nil instead.
type CodecZeroer interface {
	IsTypeZero(interface{}) bool
}

// Code generated by protoc-gen-go.
// source: database.proto
// DO NOT EDIT!

/*
Package siriusdb is a generated protocol buffer package.

It is generated from these files:
	database.proto

It has these top-level messages:
*/
package siriusdb

import proto "code.google.com/p/goprotobuf/proto"
import math "math"
import google_protobuf "seasun.com/sirius/proto/google/protobuf/descriptor.pb"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

var E_Index = &proto.ExtensionDesc{
	ExtendedType:  (*google_protobuf.FieldOptions)(nil),
	ExtensionType: (*bool)(nil),
	Field:         52234,
	Name:          "siriusdb.index",
	Tag:           "varint,52234,opt,name=index",
}

var E_IndexUnique = &proto.ExtensionDesc{
	ExtendedType:  (*google_protobuf.FieldOptions)(nil),
	ExtensionType: (*bool)(nil),
	Field:         52235,
	Name:          "siriusdb.index_unique",
	Tag:           "varint,52235,opt,name=index_unique",
}

var E_MysqlCreateOption = &proto.ExtensionDesc{
	ExtendedType:  (*google_protobuf.FieldOptions)(nil),
	ExtensionType: (*string)(nil),
	Field:         52236,
	Name:          "siriusdb.mysql_create_option",
	Tag:           "bytes,52236,opt,name=mysql_create_option",
}

var E_Orm = &proto.ExtensionDesc{
	ExtendedType:  (*google_protobuf.MessageOptions)(nil),
	ExtensionType: (*bool)(nil),
	Field:         51236,
	Name:          "siriusdb.orm",
	Tag:           "varint,51236,opt,name=orm",
}

var E_Indexfield = &proto.ExtensionDesc{
	ExtendedType:  (*google_protobuf.MessageOptions)(nil),
	ExtensionType: (*string)(nil),
	Field:         51237,
	Name:          "siriusdb.indexfield",
	Tag:           "bytes,51237,opt,name=indexfield",
}

var E_MysqlCreateTableOption = &proto.ExtensionDesc{
	ExtendedType:  (*google_protobuf.MessageOptions)(nil),
	ExtensionType: (*string)(nil),
	Field:         51238,
	Name:          "siriusdb.mysql_create_table_option",
	Tag:           "bytes,51238,opt,name=mysql_create_table_option",
}

func init() {
	proto.RegisterExtension(E_Index)
	proto.RegisterExtension(E_IndexUnique)
	proto.RegisterExtension(E_MysqlCreateOption)
	proto.RegisterExtension(E_Orm)
	proto.RegisterExtension(E_Indexfield)
	proto.RegisterExtension(E_MysqlCreateTableOption)
}

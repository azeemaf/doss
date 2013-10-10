/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package doss.net;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatResponse implements org.apache.thrift.TBase<StatResponse, StatResponse._Fields>, java.io.Serializable, Cloneable, Comparable<StatResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("StatResponse");

  private static final org.apache.thrift.protocol.TField BLOB_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("blobId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField SIZE_FIELD_DESC = new org.apache.thrift.protocol.TField("size", org.apache.thrift.protocol.TType.I64, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new StatResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new StatResponseTupleSchemeFactory());
  }

  public long blobId; // required
  public long size; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BLOB_ID((short)1, "blobId"),
    SIZE((short)2, "size");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // BLOB_ID
          return BLOB_ID;
        case 2: // SIZE
          return SIZE;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __BLOBID_ISSET_ID = 0;
  private static final int __SIZE_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BLOB_ID, new org.apache.thrift.meta_data.FieldMetaData("blobId", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.SIZE, new org.apache.thrift.meta_data.FieldMetaData("size", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(StatResponse.class, metaDataMap);
  }

  public StatResponse() {
  }

  public StatResponse(
    long blobId,
    long size)
  {
    this();
    this.blobId = blobId;
    setBlobIdIsSet(true);
    this.size = size;
    setSizeIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public StatResponse(StatResponse other) {
    __isset_bitfield = other.__isset_bitfield;
    this.blobId = other.blobId;
    this.size = other.size;
  }

  public StatResponse deepCopy() {
    return new StatResponse(this);
  }

  @Override
  public void clear() {
    setBlobIdIsSet(false);
    this.blobId = 0;
    setSizeIsSet(false);
    this.size = 0;
  }

  public long getBlobId() {
    return this.blobId;
  }

  public StatResponse setBlobId(long blobId) {
    this.blobId = blobId;
    setBlobIdIsSet(true);
    return this;
  }

  public void unsetBlobId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __BLOBID_ISSET_ID);
  }

  /** Returns true if field blobId is set (has been assigned a value) and false otherwise */
  public boolean isSetBlobId() {
    return EncodingUtils.testBit(__isset_bitfield, __BLOBID_ISSET_ID);
  }

  public void setBlobIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __BLOBID_ISSET_ID, value);
  }

  public long getSize() {
    return this.size;
  }

  public StatResponse setSize(long size) {
    this.size = size;
    setSizeIsSet(true);
    return this;
  }

  public void unsetSize() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __SIZE_ISSET_ID);
  }

  /** Returns true if field size is set (has been assigned a value) and false otherwise */
  public boolean isSetSize() {
    return EncodingUtils.testBit(__isset_bitfield, __SIZE_ISSET_ID);
  }

  public void setSizeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __SIZE_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case BLOB_ID:
      if (value == null) {
        unsetBlobId();
      } else {
        setBlobId((Long)value);
      }
      break;

    case SIZE:
      if (value == null) {
        unsetSize();
      } else {
        setSize((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case BLOB_ID:
      return Long.valueOf(getBlobId());

    case SIZE:
      return Long.valueOf(getSize());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case BLOB_ID:
      return isSetBlobId();
    case SIZE:
      return isSetSize();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof StatResponse)
      return this.equals((StatResponse)that);
    return false;
  }

  public boolean equals(StatResponse that) {
    if (that == null)
      return false;

    boolean this_present_blobId = true;
    boolean that_present_blobId = true;
    if (this_present_blobId || that_present_blobId) {
      if (!(this_present_blobId && that_present_blobId))
        return false;
      if (this.blobId != that.blobId)
        return false;
    }

    boolean this_present_size = true;
    boolean that_present_size = true;
    if (this_present_size || that_present_size) {
      if (!(this_present_size && that_present_size))
        return false;
      if (this.size != that.size)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(StatResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetBlobId()).compareTo(other.isSetBlobId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBlobId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.blobId, other.blobId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSize()).compareTo(other.isSetSize());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSize()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.size, other.size);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("StatResponse(");
    boolean first = true;

    sb.append("blobId:");
    sb.append(this.blobId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("size:");
    sb.append(this.size);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'blobId' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'size' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class StatResponseStandardSchemeFactory implements SchemeFactory {
    public StatResponseStandardScheme getScheme() {
      return new StatResponseStandardScheme();
    }
  }

  private static class StatResponseStandardScheme extends StandardScheme<StatResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, StatResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // BLOB_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.blobId = iprot.readI64();
              struct.setBlobIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SIZE
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.size = iprot.readI64();
              struct.setSizeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetBlobId()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'blobId' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetSize()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'size' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, StatResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(BLOB_ID_FIELD_DESC);
      oprot.writeI64(struct.blobId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(SIZE_FIELD_DESC);
      oprot.writeI64(struct.size);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class StatResponseTupleSchemeFactory implements SchemeFactory {
    public StatResponseTupleScheme getScheme() {
      return new StatResponseTupleScheme();
    }
  }

  private static class StatResponseTupleScheme extends TupleScheme<StatResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, StatResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI64(struct.blobId);
      oprot.writeI64(struct.size);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, StatResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.blobId = iprot.readI64();
      struct.setBlobIdIsSet(true);
      struct.size = iprot.readI64();
      struct.setSizeIsSet(true);
    }
  }

}


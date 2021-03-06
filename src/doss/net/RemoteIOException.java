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

public class RemoteIOException extends TException implements org.apache.thrift.TBase<RemoteIOException, RemoteIOException._Fields>, java.io.Serializable, Cloneable, Comparable<RemoteIOException> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RemoteIOException");

  private static final org.apache.thrift.protocol.TField BLOB_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("blobId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("type", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField MESSSAGE_FIELD_DESC = new org.apache.thrift.protocol.TField("messsage", org.apache.thrift.protocol.TType.STRING, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new RemoteIOExceptionStandardSchemeFactory());
    schemes.put(TupleScheme.class, new RemoteIOExceptionTupleSchemeFactory());
  }

  public long blobId; // optional
  public String type; // optional
  public String messsage; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BLOB_ID((short)1, "blobId"),
    TYPE((short)2, "type"),
    MESSSAGE((short)3, "messsage");

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
        case 2: // TYPE
          return TYPE;
        case 3: // MESSSAGE
          return MESSSAGE;
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
  private byte __isset_bitfield = 0;
  private _Fields optionals[] = {_Fields.BLOB_ID,_Fields.TYPE,_Fields.MESSSAGE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BLOB_ID, new org.apache.thrift.meta_data.FieldMetaData("blobId", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "BlobId")));
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData("type", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.MESSSAGE, new org.apache.thrift.meta_data.FieldMetaData("messsage", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RemoteIOException.class, metaDataMap);
  }

  public RemoteIOException() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RemoteIOException(RemoteIOException other) {
    __isset_bitfield = other.__isset_bitfield;
    this.blobId = other.blobId;
    if (other.isSetType()) {
      this.type = other.type;
    }
    if (other.isSetMesssage()) {
      this.messsage = other.messsage;
    }
  }

  public RemoteIOException deepCopy() {
    return new RemoteIOException(this);
  }

  @Override
  public void clear() {
    setBlobIdIsSet(false);
    this.blobId = 0;
    this.type = null;
    this.messsage = null;
  }

  public long getBlobId() {
    return this.blobId;
  }

  public RemoteIOException setBlobId(long blobId) {
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

  public String getType() {
    return this.type;
  }

  public RemoteIOException setType(String type) {
    this.type = type;
    return this;
  }

  public void unsetType() {
    this.type = null;
  }

  /** Returns true if field type is set (has been assigned a value) and false otherwise */
  public boolean isSetType() {
    return this.type != null;
  }

  public void setTypeIsSet(boolean value) {
    if (!value) {
      this.type = null;
    }
  }

  public String getMesssage() {
    return this.messsage;
  }

  public RemoteIOException setMesssage(String messsage) {
    this.messsage = messsage;
    return this;
  }

  public void unsetMesssage() {
    this.messsage = null;
  }

  /** Returns true if field messsage is set (has been assigned a value) and false otherwise */
  public boolean isSetMesssage() {
    return this.messsage != null;
  }

  public void setMesssageIsSet(boolean value) {
    if (!value) {
      this.messsage = null;
    }
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

    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((String)value);
      }
      break;

    case MESSSAGE:
      if (value == null) {
        unsetMesssage();
      } else {
        setMesssage((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case BLOB_ID:
      return Long.valueOf(getBlobId());

    case TYPE:
      return getType();

    case MESSSAGE:
      return getMesssage();

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
    case TYPE:
      return isSetType();
    case MESSSAGE:
      return isSetMesssage();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof RemoteIOException)
      return this.equals((RemoteIOException)that);
    return false;
  }

  public boolean equals(RemoteIOException that) {
    if (that == null)
      return false;

    boolean this_present_blobId = true && this.isSetBlobId();
    boolean that_present_blobId = true && that.isSetBlobId();
    if (this_present_blobId || that_present_blobId) {
      if (!(this_present_blobId && that_present_blobId))
        return false;
      if (this.blobId != that.blobId)
        return false;
    }

    boolean this_present_type = true && this.isSetType();
    boolean that_present_type = true && that.isSetType();
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (!this.type.equals(that.type))
        return false;
    }

    boolean this_present_messsage = true && this.isSetMesssage();
    boolean that_present_messsage = true && that.isSetMesssage();
    if (this_present_messsage || that_present_messsage) {
      if (!(this_present_messsage && that_present_messsage))
        return false;
      if (!this.messsage.equals(that.messsage))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(RemoteIOException other) {
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
    lastComparison = Boolean.valueOf(isSetType()).compareTo(other.isSetType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.type, other.type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMesssage()).compareTo(other.isSetMesssage());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMesssage()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.messsage, other.messsage);
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
    StringBuilder sb = new StringBuilder("RemoteIOException(");
    boolean first = true;

    if (isSetBlobId()) {
      sb.append("blobId:");
      sb.append(this.blobId);
      first = false;
    }
    if (isSetType()) {
      if (!first) sb.append(", ");
      sb.append("type:");
      if (this.type == null) {
        sb.append("null");
      } else {
        sb.append(this.type);
      }
      first = false;
    }
    if (isSetMesssage()) {
      if (!first) sb.append(", ");
      sb.append("messsage:");
      if (this.messsage == null) {
        sb.append("null");
      } else {
        sb.append(this.messsage);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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

  private static class RemoteIOExceptionStandardSchemeFactory implements SchemeFactory {
    public RemoteIOExceptionStandardScheme getScheme() {
      return new RemoteIOExceptionStandardScheme();
    }
  }

  private static class RemoteIOExceptionStandardScheme extends StandardScheme<RemoteIOException> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, RemoteIOException struct) throws org.apache.thrift.TException {
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
          case 2: // TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.type = iprot.readString();
              struct.setTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // MESSSAGE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.messsage = iprot.readString();
              struct.setMesssageIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, RemoteIOException struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.isSetBlobId()) {
        oprot.writeFieldBegin(BLOB_ID_FIELD_DESC);
        oprot.writeI64(struct.blobId);
        oprot.writeFieldEnd();
      }
      if (struct.type != null) {
        if (struct.isSetType()) {
          oprot.writeFieldBegin(TYPE_FIELD_DESC);
          oprot.writeString(struct.type);
          oprot.writeFieldEnd();
        }
      }
      if (struct.messsage != null) {
        if (struct.isSetMesssage()) {
          oprot.writeFieldBegin(MESSSAGE_FIELD_DESC);
          oprot.writeString(struct.messsage);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RemoteIOExceptionTupleSchemeFactory implements SchemeFactory {
    public RemoteIOExceptionTupleScheme getScheme() {
      return new RemoteIOExceptionTupleScheme();
    }
  }

  private static class RemoteIOExceptionTupleScheme extends TupleScheme<RemoteIOException> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RemoteIOException struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetBlobId()) {
        optionals.set(0);
      }
      if (struct.isSetType()) {
        optionals.set(1);
      }
      if (struct.isSetMesssage()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetBlobId()) {
        oprot.writeI64(struct.blobId);
      }
      if (struct.isSetType()) {
        oprot.writeString(struct.type);
      }
      if (struct.isSetMesssage()) {
        oprot.writeString(struct.messsage);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RemoteIOException struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.blobId = iprot.readI64();
        struct.setBlobIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.type = iprot.readString();
        struct.setTypeIsSet(true);
      }
      if (incoming.get(2)) {
        struct.messsage = iprot.readString();
        struct.setMesssageIsSet(true);
      }
    }
  }

}


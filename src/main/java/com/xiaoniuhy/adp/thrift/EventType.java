/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.xiaoniuhy.adp.thrift;


@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2020-11-20")
public enum EventType implements org.apache.thrift.TEnum {
  Request(0),
  Impression(1),
  Click(2),
  Landing(3);

  private final int value;

  private EventType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static EventType findByValue(int value) { 
    switch (value) {
      case 0:
        return Request;
      case 1:
        return Impression;
      case 2:
        return Click;
      case 3:
        return Landing;
      default:
        return null;
    }
  }
}

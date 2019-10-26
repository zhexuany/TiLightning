package com.pingcap.tikv.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StoreInfo {
  private final String addr;

  @JsonCreator
  public StoreInfo(@JsonProperty("addr") String addr) {
    this.addr = addr;
  }

  public String getAddr() {
    return addr;
  }
}

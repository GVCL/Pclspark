package com.pclspark.model;

public class ProcessedRawPoint {
  public int getRegionid() {
    return regionid;
  }

  public void setRegionid(int regionid) {
    this.regionid = regionid;
  }

  public int getTileid() {
    return tileid;
  }

  public void setTileid(int tileid) {
    this.tileid = tileid;
  }

  public float getX() {
    return x;
  }

  public void setX(float x) {
    this.x = x;
  }

  public float getY() {
    return y;
  }

  public void setY(float y) {
    this.y = y;
  }

  public float getZ() {
    return z;
  }

  public void setZ(float z) {
    this.z = z;
  }

  public int getC() {
    return c;
  }

  public void setC(int c) {
    this.c = c;
  }

  public int regionid;
  public int tileid;
  public float x;
  public float y;
  public float z;
  public int c;
}
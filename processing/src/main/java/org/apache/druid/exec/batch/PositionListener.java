package org.apache.druid.exec.batch;

public interface PositionListener
{
  void updatePosition(int posn);
}
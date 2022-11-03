package org.apache.druid.exec.batch;

public interface BindingListener
{
  void batchBound(int size);
}
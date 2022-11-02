package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.ColumnReaderProvider;

public class Exp
{
//  public interface ReaderIndex
//  {
//    int index();
//    boolean isValid();
//  }
//
//  public interface ReaderBounds
//  {
//    int size();
//    void positioner(ReaderPositioner positioner);
//    boolean canSeek();
//  }

  public interface RowSequencer
  {
    boolean next();
    boolean isEof();
    boolean isValid();
  }

  public interface SequencerBinding
  {
    void bind(StepListener listener);
  }

  public interface RowPositioner extends RowSequencer
  {
    int index();
    int size();
    boolean seek(int i);
  }

  public interface PositionerBinding
  {
    void bind(PositionListener listener);
  }

  public interface StepListener
  {
    boolean next();
    boolean isValid();
  }

  public interface SimpleCursor
  {
    BatchSchema schema();
    ColumnReaderProvider columns();
    RowSequencer sequencer();
  }

  public interface PositionListener
  {
    // -1 is invalid, other is valid
    void position(int posn);
  }

  public interface SeekableCursor extends SimpleCursor
  {
    RowPositioner positioner();
  }
}

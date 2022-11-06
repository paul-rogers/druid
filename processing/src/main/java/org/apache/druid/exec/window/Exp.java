package org.apache.druid.exec.window;

public class Exp
{
  // TODO: Legacy nulls wrapper

  public static class WindowDefinition
  {
    private int maxLag;
    private int maxLead;
    // Offset groups
  }

  public interface WindowState
  {

  }



//  public static class ResultSequencer implements RowSequencer
//  {
//    private final Partitioner partitionState;
//    private final PartitionSequencer primarySequencer;
//    private final List<PartitionSequencer> followerSequencers;
//
//    public ResultSequencer(Partitioner partitionState, PartitionSequencer primarySequencer, List<PartitionSequencer> followerSequencers)
//    {
//      this.partitionState = partitionState;
//      this.primarySequencer = primarySequencer;
//      this.followerSequencers = followerSequencers;
//    }
//
//    @Override
//    public boolean next()
//    {
//      partitionState.next();
//      if (!primarySequencer.next()) {
//        return false;
//      }
//      for (PartitionSequencer seq : followerSequencers) {
//        seq.next();
//      }
//      return true;
//    }
//
//    @Override
//    public boolean isEOF()
//    {
//      // TODO Auto-generated method stub
//      return primarySequencer.isEOF() && partitionState.isEOF();
//    }
//
//    @Override
//    public boolean isValid()
//    {
//      throw new UOE("Not valid");
//    }
//  }

}

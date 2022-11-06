package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class SequencerBuilder
{
  private static class ReaderInfo
  {
    protected final BatchReader reader;
    protected final int offset;
    protected PartitionSequencer sequencer;

    public ReaderInfo(final BatchReader reader, final int offset)
    {
      this.reader = reader;
      this.offset = offset;
    }

    public void buildSequencer(BatchBuffer buffer)
    {
      if (offset == 0) {
        sequencer = new PartitionSequencer.PrimarySequencer(buffer, reader);
      } else if (offset < 0) {
        sequencer = new PartitionSequencer.LagSequencer(buffer, reader, -offset);
      } else { // offset > 0
        sequencer = new PartitionSequencer.LeadSequencer(buffer, reader, -offset);
      }
    }
  }

  private final ProjectionBuilder projectionBuilder;
  private final BatchBuffer buffer;
  private final Partitioner.Builder partitionBuilder;
  private SequencerBuilder.ReaderInfo primaryReader;
  private final List<SequencerBuilder.ReaderInfo> secondaryReaders = new ArrayList<>();
  private SequencerBuilder.ReaderInfo leadMostReader;
  private SequencerBuilder.ReaderInfo lagMostReader;

  public SequencerBuilder(ProjectionBuilder projectionBuilder, Partitioner.Builder partitionBuilder)
  {
    this.projectionBuilder = projectionBuilder;
    this.buffer = projectionBuilder.buffer();
    this.partitionBuilder = partitionBuilder;
  }

  public Partitioner build()
  {
    primaryReader = new ReaderInfo(projectionBuilder.primaryReader(), 0);
    for (Entry<Integer, BatchReader> entry : projectionBuilder.offsetReaders().entrySet()) {
      secondaryReaders.add(new ReaderInfo(entry.getValue(), entry.getKey()));
    }
    buildSequencers();
    findLead();
    findLag();
    assignListeners();
    return buildPartitionState();
  }

  private void buildSequencers()
  {
    primaryReader.buildSequencer(buffer);
    for (SequencerBuilder.ReaderInfo reader : secondaryReaders) {
      reader.buildSequencer(buffer);
    }
  }

  private void findLead()
  {
    for (SequencerBuilder.ReaderInfo reader : secondaryReaders) {
      if (reader.offset <= 0) {
        continue;
      }
      if (leadMostReader == null || reader.offset > leadMostReader.offset) {
        leadMostReader = reader;
      }
    }
  }

  private void findLag()
  {
    for (SequencerBuilder.ReaderInfo reader : secondaryReaders) {
      if (reader.offset >= 0) {
        continue;
      }
      if (leadMostReader == null || reader.offset < lagMostReader.offset) {
        lagMostReader = reader;
      }
    }
  }

  private void assignListeners()
  {
    boolean needsLead = !partitionBuilder.isPartitioned();

    // If a lead exists, then the largest lead will load batches.
    if (needsLead && leadMostReader != null) {
      leadMostReader.sequencer.bindBatchListener(true, false);
      needsLead = false;
    }

    // If a lag exists, then the largest lag will release batches.
    boolean needsLag = true;
    if (lagMostReader != null) {
      lagMostReader.sequencer.bindBatchListener(false, true);
      needsLag = false;
    }

    primaryReader.sequencer.bindBatchListener(needsLead, needsLag);
  }

  private Partitioner buildPartitionState()
  {
    List<PartitionSequencer> sequencers = new ArrayList<>();
    sequencers.add(primaryReader.sequencer);
    for (SequencerBuilder.ReaderInfo reader : secondaryReaders) {
      sequencers.add(reader.sequencer);
    }
    return partitionBuilder.build(sequencers, projectionBuilder.columnReaders());
  }
}
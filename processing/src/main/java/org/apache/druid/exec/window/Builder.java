package org.apache.druid.exec.window;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class Builder
{
  protected static class ReaderInfo
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
        sequencer = new PartitionSequencer.LeadSequencer(buffer, reader, offset);
      }
    }
  }

  protected final BatchBuffer buffer;
  protected final ProjectionBuilder projectionBuilder;
  protected final List<String> partitionKeys;
  protected BatchWriter<?> writer;
  protected ReaderInfo primaryReader;
  private final List<ReaderInfo> secondaryReaders = new ArrayList<>();
  private ReaderInfo leadMostReader;
  private ReaderInfo lagMostReader;
  protected List<PartitionSequencer> sequencers;

  public Builder(BatchBuffer batchBuffer, ProjectionBuilder projectionBuilder, List<String> partitionKeys)
  {
    this.projectionBuilder = projectionBuilder;
    this.buffer = projectionBuilder.buffer();
    this.partitionKeys = partitionKeys;
  }

  public Partitioner build(int outputBatchSize)
  {
    buildReaders();
    buildWriter(outputBatchSize);

    findLead();
    findLag();
    assignListeners();
    buildSequencerList();
    return buildPartitioner();
  }

  /**
   * Build the set of input readers. There is at least a primary reader
   * for the current row. There may be any number of lead and lag readers.
   * For each, also builds the sequencer which steps through the rows of
   * the reader.
   */
  private void buildReaders()
  {
    primaryReader = new ReaderInfo(projectionBuilder.primaryReader(), 0);
    primaryReader.buildSequencer(buffer);

    for (Entry<Integer, BatchReader> entry : projectionBuilder.offsetReaders().entrySet()) {
      Builder.ReaderInfo reader = new ReaderInfo(entry.getValue(), entry.getKey());
      reader.buildSequencer(buffer);
      secondaryReaders.add(reader);
    }
  }

  /**
   * Create the writer used for the output. The writer is for the projected
   * schema, as determined by the projection list.
   */
  private void buildWriter(int outputBatchSize)
  {
    RowSchema outputSchema = projectionBuilder.schema();
    writer = buffer.inputSchema.type().newWriter(outputSchema, outputBatchSize);

    // Sanity check
    Preconditions.checkState(outputSchema.size() == projectionBuilder.columnReaders().size());
  }

  private void findLead()
  {
    for (Builder.ReaderInfo reader : secondaryReaders) {
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
    for (Builder.ReaderInfo reader : secondaryReaders) {
      if (reader.offset >= 0) {
        continue;
      }
      if (lagMostReader == null || reader.offset < lagMostReader.offset) {
        lagMostReader = reader;
      }
    }
  }

  private boolean isPartitioned()
  {
    return !CollectionUtils.isNullOrEmpty(partitionKeys);
  }

  /**
   * Determine which of the readers should load batches and which should
   * release them.
   */
  private void assignListeners()
  {
    boolean needsLead = !isPartitioned();

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

  /**
   * Determine the order that sequencers should be called to ensure
   * batches are loaded and released in the proper order.
   */
  private void buildSequencerList()
  {
    sequencers = new ArrayList<>();

    // Lead-most reader first
    if (leadMostReader != null) {
      sequencers.add(leadMostReader.sequencer);
    }
    // Primary second (if there is a lead) else first (since the primary is lead).
    sequencers.add(primaryReader.sequencer);

    // Others, except lag-most
    for (Builder.ReaderInfo reader : secondaryReaders) {
      if (reader != leadMostReader && reader != lagMostReader) {
        sequencers.add(reader.sequencer);
      }
    }
    // Lag-most reader last
    if (lagMostReader != null) {
      sequencers.add(lagMostReader.sequencer);
    }
  }

  private Partitioner buildPartitioner()
  {
    if (isPartitioned()) {
      return new Partitioner.Multiple(this);
    } else {
      return new Partitioner.Single(this);
    }
  }
}

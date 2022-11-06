/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.exec.window;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

/**
 * Builder for the window function for everything except projections, which are handled
 * in {@link ProjectionBuilder}. The projection builder determines the set of offsets
 * referenced in the window spec and builds readers for each offset. The "offset 0" reader
 * is the primary reader: it represents the row copied from input to output. The other
 * readers are "offset readers": they access rows ahead of the current row (lead) or
 * behind the current row (lag).
 * <p>
 * Buffer management is integrated in with the readers. The "lead-most" reader (the one
 * with the largest lead offset) triggers load of batches from the upstream operator.
 * Then, the "lag-most" reader (the one with the largest lag offset), discards batches
 * which are no longer needed.
 * <p>
 * The window function can be partitioned or unpartitioned. An unpartitioned window
 * can be thought of as putting all data in a single, unnamed, partition. These two cases
 * are quite different, and so have different "drivers". This builder sets things up as
 * needed for these two cases.
 */
public class Builder
{
  /**
   * Temporary bundle of information about each reader: the reader, its offset
   * (0 for primary, <0 for lag, >0 for lead), and the sequencer which positions
   * the reader.
   */
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
  private BatchLoader leadBatchLoader;

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
    buildSequencerList();
    final Partitioner partitioner = buildPartitioner();
    assignListeners();
    return partitioner;
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
    for (ReaderInfo reader : secondaryReaders) {
      if (reader.offset <= 0) {
        continue;
      }
      if (leadMostReader == null || reader.offset > leadMostReader.offset) {
        leadMostReader = reader;
      }
    }
    if (leadMostReader == null) {
      leadMostReader = primaryReader;
    }
  }

  private void findLag()
  {
    for (ReaderInfo reader : secondaryReaders) {
      if (reader.offset >= 0) {
        continue;
      }
      if (lagMostReader == null || reader.offset < lagMostReader.offset) {
        lagMostReader = reader;
      }
    }
    if (lagMostReader == null) {
      lagMostReader = primaryReader;
    }
  }

  private boolean isPartitioned()
  {
    return !CollectionUtils.isNullOrEmpty(partitionKeys);
  }

  /**
   * Determine the order that sequencers should be called to ensure
   * batches are loaded and released in the proper order.
   */
  private void buildSequencerList()
  {
    if (secondaryReaders.isEmpty()) {
      sequencers = Collections.singletonList(primaryReader.sequencer);
      return;
    }

    List<ReaderInfo> rawList = new ArrayList<>(secondaryReaders);
    rawList.add(primaryReader);

    sequencers = new ArrayList<>();

    // Lead-most reader first
    sequencers.add(leadMostReader.sequencer);

    // Others, except lag-most
    for (ReaderInfo reader : rawList) {
      if (reader != leadMostReader && reader != lagMostReader) {
        sequencers.add(reader.sequencer);
      }
    }

    // Lag-most reader last
    sequencers.add(lagMostReader.sequencer);
  }

  private Partitioner buildPartitioner()
  {
    if (isPartitioned()) {
      Partitioner.Multiple partitioner = new Partitioner.Multiple(this);
      leadBatchLoader = partitioner;
      return partitioner;
    } else {
      leadBatchLoader = buffer;
      return new Partitioner.Single(this);
    }
  }

  /**
   * Determine which of the readers should load batches and which should
   * release them.
   */
  private void assignListeners()
  {
    assignLoadersFor(primaryReader);
    for (ReaderInfo reader : secondaryReaders) {
      assignLoadersFor(reader);
    }
  }

  private static final BatchUnloader NO_OP_UNLOADER = (from, to) -> { };

  /**
   * Assign the bit of code which does buffer loading and unloading. The lead-most
   * loads batches. It loads directly from the buffer if unpartitioned, but from
   * the partitioner if partitioned. The lag-most unloads batches. The others read
   * batches from the buffer, and do nothing when moving past a batch.
   */
  private void assignLoadersFor(ReaderInfo reader)
  {
    reader.sequencer.bindBatchLoader(reader == leadMostReader ? leadBatchLoader : buffer);
    reader.sequencer.bindBatchUnloader(reader == lagMostReader ? buffer : NO_OP_UNLOADER);
  }
}

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureBuilder;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

/**
 * Outbound state transfer task. Pushes data segments to another cluster member on request. Instances of
 * OutboundTransferTask are created and managed by StateTransferManagerImpl. There should be at most
 * one such task per destination at any time.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class OutboundTransferTask implements Runnable {

   private static final Log log = LogFactory.getLog(OutboundTransferTask.class);

   private final boolean trace = log.isTraceEnabled();

   private StateProviderImpl stateProvider;

   private final int topologyId;

   private final Address destination;

   private final Set<Integer> segments = new CopyOnWriteArraySet<Integer>();

   private final int stateTransferChunkSize;

   private final Configuration configuration;

   private final ConsistentHash readCh;

   private final DataContainer dataContainer;

   private final CacheLoaderManager cacheLoaderManager;

   private final RpcManager rpcManager;

   private final CommandsFactory commandsFactory;

   private final long timeout;

   private final Map<Integer, List<InternalCacheEntry>> entriesBySegment = ConcurrentMapFactory.makeConcurrentMap();

   /**
    * The total number of entries from all segments accumulated in entriesBySegment.
    */
   private int accumulatedEntries;

   /**
    * This is used with RpcManager.invokeRemotelyInFuture() to be able to cancel message sending if the task needs to be canceled.
    */
   private final NotifyingNotifiableFuture<Object> sendFuture = new AggregatingNotifyingFutureBuilder(null);

   /**
    * The Future obtained from submitting this task to an executor service. This is used for cancellation.
    */
   private FutureTask runnableFuture;

   public OutboundTransferTask(Address destination, Set<Integer> segments, int stateTransferChunkSize,
                               int topologyId, ConsistentHash readCh, StateProviderImpl stateProvider, DataContainer dataContainer,
                               CacheLoaderManager cacheLoaderManager, RpcManager rpcManager, Configuration configuration,
                               CommandsFactory commandsFactory, long timeout) {
      if (segments == null || segments.isEmpty()) {
         throw new IllegalArgumentException("Segments must not be null or empty");
      }
      if (destination == null) {
         throw new IllegalArgumentException("Destination address cannot be null");
      }
      if (stateTransferChunkSize <= 0) {
         throw new IllegalArgumentException("stateTransferChunkSize must be greater than 0");
      }
      this.stateProvider = stateProvider;
      this.destination = destination;
      this.segments.addAll(segments);
      this.stateTransferChunkSize = stateTransferChunkSize;
      this.topologyId = topologyId;
      this.readCh = readCh;
      this.dataContainer = dataContainer;
      this.cacheLoaderManager = cacheLoaderManager;
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.commandsFactory = commandsFactory;
      this.timeout = timeout;
   }

   public void execute(ExecutorService executorService) {
      if (runnableFuture != null) {
         throw new IllegalStateException("This task was already submitted");
      }
      runnableFuture = new FutureTask<Void>(this, null) {
         @Override
         protected void done() {
            stateProvider.onTaskCompletion(OutboundTransferTask.this);
         }
      };
      executorService.submit(runnableFuture);
   }

   public Address getDestination() {
      return destination;
   }

   public Set<Integer> getSegments() {
      return segments;
   }

   //todo [anistor] check thread interrupt status in loops to implement faster cancellation
   public void run() {
      try {
         // send data container entries
         for (InternalCacheEntry ice : dataContainer) {
            Object key = ice.getKey();
            int segmentId = readCh.getSegment(key);
            if (segments.contains(segmentId)) {
               sendEntry(ice, segmentId);
            }
         }

         // send cache store entries if needed
         CacheStore cacheStore = getCacheStore();
         if (cacheStore != null) {
            try {
               //todo [anistor] need to extend CacheStore interface to be able to specify a filter when loading keys (ie. keys should belong to desired segments)
               Set<Object> storedKeys = cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer));
               for (Object key : storedKeys) {
                  int segmentId = readCh.getSegment(key);
                  if (segments.contains(segmentId)) {
                     try {
                        InternalCacheEntry ice = cacheStore.load(key);
                        if (ice != null) { // check entry still exists
                           sendEntry(ice, segmentId);
                        }
                     } catch (CacheLoaderException e) {
                        log.failedLoadingValueFromCacheStore(key, e);
                     }
                  }
               }
            } catch (CacheLoaderException e) {
               log.failedLoadingKeysFromCacheStore(e);
            }
         } else {
            if (trace) {
               log.tracef("No cache store or the cache store is shared, no need to send any stored cache entries for segments: %s", segments);
            }
         }

         // send the last chunk of all segments
         sendEntries(true);
      } catch (Throwable t) {
         // ignore eventual exceptions caused by cancellation (have InterruptedException as the root cause)
         if (!runnableFuture.isCancelled()) {
            log.error("Failed to execute outbound transfer", t);
         }
      }
      if (trace) {
         log.tracef("Outbound transfer of segments %s to %s is complete", segments, destination);
      }
   }

   /**
    * Obtains the CacheStore that will be used for pulling segments that will be sent to other new owners on request.
    * The CacheStore is ignored if it is disabled or if it is shared or if fetchPersistentState is disabled.
    */
   private CacheStore getCacheStore() {
      if (cacheLoaderManager != null && cacheLoaderManager.isEnabled() && !cacheLoaderManager.isShared() && cacheLoaderManager.isFetchPersistentState()) {
         return cacheLoaderManager.getCacheStore();
      }
      return null;
   }

   private void sendEntry(InternalCacheEntry ice, int segmentId) {
      // send if we have a full chunk
      if (accumulatedEntries >= stateTransferChunkSize) {
         sendEntries(false);
         entriesBySegment.clear();
         accumulatedEntries = 0;
      }

      List<InternalCacheEntry> entries = entriesBySegment.get(segmentId);
      if (entries == null) {
         entries = new ArrayList<InternalCacheEntry>();
         entriesBySegment.put(segmentId, entries);
      }
      entries.add(ice);
      accumulatedEntries++;
   }

   private void sendEntries(boolean isLast) {
      List<StateChunk> chunks = new ArrayList<StateChunk>();
      if (isLast) {
         for (int segmentId : segments) {
            List<InternalCacheEntry> entries = entriesBySegment.get(segmentId);
            if (entries == null) {
               entries = Collections.emptyList();
            }
            chunks.add(new StateChunk(segmentId, entries, isLast));
         }
      } else {
         for (Map.Entry<Integer, List<InternalCacheEntry>> e : entriesBySegment.entrySet()) {
            List<InternalCacheEntry> entries = e.getValue();
            if (!entries.isEmpty()) {
               chunks.add(new StateChunk(e.getKey(), entries, isLast));
            }
         }
      }

      if (!chunks.isEmpty() || isLast) {
         if (trace) {
            log.tracef("Sending %d cache entries from segments %s to node %s", accumulatedEntries, entriesBySegment.keySet(), destination);
         }

         //todo [anistor] send back the received topologyId or my local one?
         StateResponseCommand cmd = commandsFactory.buildStateResponseCommand(rpcManager.getAddress(), topologyId, chunks);
         // send synchronously, in FIFO mode. it is important that the last chunk is received last in order to correctly detect completion of the stream of chunks
         rpcManager.invokeRemotelyInFuture(Collections.singleton(destination), cmd, false, sendFuture, timeout);
      }
   }

   /**
    * Cancel some of the segments. If all segments get cancelled then the whole task will be cancelled.
    *
    * @param cancelledSegments segments to cancel.
    */
   public void cancelSegments(Set<Integer> cancelledSegments) {
      if (trace) {
         log.tracef("Cancelling outbound transfer of segments %s to %s", cancelledSegments, destination);
      }
      if (segments.removeAll(cancelledSegments)) {
         entriesBySegment.keySet().removeAll(cancelledSegments);  // here we do not update accumulatedEntries but this inaccuracy does not cause any harm
         if (segments.isEmpty()) {
            cancel();
         }
      }
   }

   /**
    * Cancel the whole task.
    */
   public void cancel() {
      if (runnableFuture != null && !runnableFuture.isCancelled()) {
         runnableFuture.cancel(true);
         sendFuture.cancel(true);
      }
   }

   public boolean isCancelled() {
      return runnableFuture != null && runnableFuture.isCancelled();
   }
}

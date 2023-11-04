﻿using J2N.Threading.Atomic;
using Lucene.Net.Diagnostics;
using Lucene.Net.Search;
using Lucene.Net.Support;
using Lucene.Net.Support.Threading;
using System;
using System.Threading;

namespace Lucene.Net.Index
{
    /*
     * Licensed to the Apache Software Foundation (ASF) under one or more
     * contributor license agreements.  See the NOTICE file distributed with
     * this work for additional information regarding copyright ownership.
     * The ASF licenses this file to You under the Apache License, Version 2.0
     * (the "License"); you may not use this file except in compliance with
     * the License.  You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */

    /// <summary>
    /// <see cref="DocumentsWriterDeleteQueue"/> is a non-blocking linked pending deletes
    /// queue. In contrast to other queue implementation we only maintain the
    /// tail of the queue. A delete queue is always used in a context of a set of
    /// DWPTs and a global delete pool. Each of the DWPT and the global pool need to
    /// maintain their 'own' head of the queue (as a <see cref="DeleteSlice"/> instance per DWPT).
    /// The difference between the DWPT and the global pool is that the DWPT starts
    /// maintaining a head once it has added its first document since for its segments
    /// private deletes only the deletes after that document are relevant. The global
    /// pool instead starts maintaining the head once this instance is created by
    /// taking the sentinel instance as its initial head.
    /// <para/>
    /// Since each <see cref="DeleteSlice"/> maintains its own head and the list is only
    /// single linked the garbage collector takes care of pruning the list for us.
    /// All nodes in the list that are still relevant should be either directly or
    /// indirectly referenced by one of the DWPT's private <see cref="DeleteSlice"/> or by
    /// the global <see cref="BufferedUpdates"/> slice.
    /// <para/>
    /// Each DWPT as well as the global delete pool maintain their private
    /// DeleteSlice instance. In the DWPT case updating a slice is equivalent to
    /// atomically finishing the document. The slice update guarantees a "happens
    /// before" relationship to all other updates in the same indexing session. When a
    /// DWPT updates a document it:
    ///
    /// <list type="number">
    ///     <item><description>consumes a document and finishes its processing</description></item>
    ///     <item><description>updates its private <see cref="DeleteSlice"/> either by calling
    ///     <see cref="UpdateSlice(DeleteSlice)"/> or <see cref="Add(Term, DeleteSlice)"/> (if the
    ///         document has a delTerm)</description></item>
    ///     <item><description>applies all deletes in the slice to its private <see cref="BufferedUpdates"/>
    ///         and resets it</description></item>
    ///     <item><description>increments its internal document id</description></item>
    /// </list>
    ///
    /// The DWPT also doesn't apply its current documents delete term until it has
    /// updated its delete slice which ensures the consistency of the update. If the
    /// update fails before the <see cref="DeleteSlice"/> could have been updated the deleteTerm
    /// will also not be added to its private deletes neither to the global deletes.
    ///
    /// </summary>
    internal sealed class DocumentsWriterDeleteQueue
    {
        // the current end (latest delete operation) in the delete queue:
        private Node tail; // LUCENENET NOTE: can't use type without specifying type parameter, also not volatile due to Interlocked

        /* Used to record deletes against all prior (already written to disk) segments.  Whenever any segment flushes, we bundle up this set of
        *  deletes and insert into the buffered updates stream before the newly flushed segment(s). */
        private readonly DeleteSlice globalSlice; // LUCENENET NOTE: no need for AtomicReferenceFieldUpdater, we can use Interlocked instead

        private readonly BufferedUpdates globalBufferedUpdates;

        /* only acquired to update the global deletes */
        private readonly ReentrantLock globalBufferLock = new ReentrantLock();

        internal readonly long generation;

        /* Generates the sequence number that IW returns to callers changing the index, showing the effective serialization of all operations. */
        private readonly AtomicInt64 nextSeqNo;

        // for asserts
        internal long maxSeqNo = long.MaxValue;

        // seqNo must start at 1 because some APIs negate this to encode a boolean
        internal DocumentsWriterDeleteQueue()
            : this(0, 1)
        {
        }

        internal DocumentsWriterDeleteQueue(long generation, long startSeqNo)
            : this(new BufferedUpdates("global"), generation, startSeqNo)
        {
        }

        internal DocumentsWriterDeleteQueue(BufferedUpdates globalBufferedUpdates, long generation, long startSeqNo)
        {
            this.globalBufferedUpdates = globalBufferedUpdates;
            this.generation = generation;
            this.nextSeqNo = new AtomicInt64(startSeqNo);
            /*
             * we use a sentinel instance as our initial tail. No slice will ever try to
             * apply this tail since the head is always omitted.
             */
            tail = new Node(null); // sentinel
            globalSlice = new DeleteSlice(tail);
        }

        internal long AddDelete(params Query[] queries)
        {
            long seqNo = Add(new QueryArrayNode(queries));
            TryApplyGlobalSlice();
            return seqNo;
        }

        internal long AddDelete(params Term[] terms)
        {
            long seqNo = Add(new TermArrayNode(terms));
            TryApplyGlobalSlice();
            return seqNo;
        }

        internal long AddNumericUpdate(NumericDocValuesUpdate update)
        {
            long seqNo = Add(new NumericUpdateNode(update));
            TryApplyGlobalSlice();
            return seqNo;
        }

        internal long AddBinaryUpdate(BinaryDocValuesUpdate update)
        {
            long seqNo = Add(new BinaryUpdateNode(update));
            TryApplyGlobalSlice();
            return seqNo;
        }

        /// <summary>
        /// invariant for document update
        /// </summary>
        internal long Add(Term term, DeleteSlice slice)
        {
            TermNode termNode = new TermNode(term);
            long seqNo = Add(termNode);
            /*
             * this is an update request where the term is the updated documents
             * delTerm. in that case we need to guarantee that this insert is atomic
             * with regards to the given delete slice. this means if two threads try to
             * update the same document with in turn the same delTerm one of them must
             * win. By taking the node we have created for our del term as the new tail
             * it is guaranteed that if another thread adds the same right after us we
             * will apply this delete next time we update our slice and one of the two
             * competing updates wins!
             */
            slice.sliceTail = termNode;
            if (Debugging.AssertsEnabled) Debugging.Assert(slice.sliceHead != slice.sliceTail, "slice head and tail must differ after add");
            TryApplyGlobalSlice(); // TODO doing this each time is not necessary maybe
            // we can do it just every n times or so?
            return seqNo;
        }

        // nocommit can we remove the sync'd
        internal long Add(Node newNode)
        {
            UninterruptableMonitor.Enter(this);
            /* Earlier/old implementation
             * this non-blocking / 'wait-free' linked list add was inspired by Apache
             * Harmony's ConcurrentLinkedQueue Implementation.
             */
            try
            {
                tail.next = newNode;
                tail = newNode;
                return NextSequenceNumber;
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        internal bool AnyChanges()
        {
            globalBufferLock.@Lock();
            try
            {
                /*
                 * check if all items in the global slice were applied
                 * and if the global slice is up-to-date
                 * and if globalBufferedUpdates has changes
                 */
                return globalBufferedUpdates.Any() || !globalSlice.IsEmpty || globalSlice.sliceTail != tail || tail.next != null;
            }
            finally
            {
                globalBufferLock.Unlock();
            }
        }

        internal void TryApplyGlobalSlice()
        {
            if (globalBufferLock.TryLock())
            {
                /*
                 * The global buffer must be locked but we don't need to update them if
                 * there is an update going on right now. It is sufficient to apply the
                 * deletes that have been added after the current in-flight global slices
                 * tail the next time we can get the lock!
                 */
                try
                {
                    if (UpdateSliceNoSeqNo(globalSlice))
                    {
                        globalSlice.Apply(globalBufferedUpdates, BufferedUpdates.MAX_INT32);
                    }
                }
                finally
                {
                    globalBufferLock.Unlock();
                }
            }
        }

        internal FrozenBufferedUpdates FreezeGlobalBuffer(DeleteSlice callerSlice)
        {
            globalBufferLock.@Lock();
            /*
             * Here we freeze the global buffer so we need to lock it, apply all
             * deletes in the queue and reset the global slice to let the GC prune the
             * queue.
             */
            Node currentTail = tail; // take the current tail make this local any
            // Changes after this call are applied later
            // and not relevant here
            if (callerSlice != null)
            {
                // Update the callers slices so we are on the same page
                callerSlice.sliceTail = currentTail;
            }
            try
            {
                if (globalSlice.sliceTail != currentTail)
                {
                    globalSlice.sliceTail = currentTail;
                    globalSlice.Apply(globalBufferedUpdates, BufferedUpdates.MAX_INT32);
                }

                FrozenBufferedUpdates packet = new FrozenBufferedUpdates(globalBufferedUpdates, false);
                globalBufferedUpdates.Clear();
                return packet;
            }
            finally
            {
                globalBufferLock.Unlock();
            }
        }

        internal DeleteSlice NewSlice()
        {
            return new DeleteSlice(tail);
        }

        /* Negative result means there were new deletes since we last applied*/
        internal long UpdateSlice(DeleteSlice slice)
        {
            UninterruptableMonitor.Enter(this);
            long seqNo = NextSequenceNumber;
            try
            {
                if (slice.sliceTail != tail)
                {
                    // new deletes arrived since we last checked
                    slice.sliceTail = tail;
                    seqNo = -seqNo;
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
            return seqNo;
        }

        /** Just like updateSlice, but does not assign a sequence number */
        internal bool UpdateSliceNoSeqNo(DeleteSlice slice)
        {
            if (slice.sliceTail != tail)
            {
                // new deletes arrived since we last checked
                slice.sliceTail = tail;
                return true;
            }
            return false;
        }

        internal class DeleteSlice
        {
            // No need to be volatile, slices are thread captive (only accessed by one thread)!
            internal Node sliceHead; // we don't apply this one

            internal Node sliceTail;

            internal DeleteSlice(Node currentTail)
            {
                if (Debugging.AssertsEnabled) Debugging.Assert(currentTail != null);
                /*
                 * Initially this is a 0 length slice pointing to the 'current' tail of
                 * the queue. Once we update the slice we only need to assign the tail and
                 * have a new slice
                 */
                sliceHead = sliceTail = currentTail;
            }

            internal virtual void Apply(BufferedUpdates del, int docIDUpto)
            {
                if (sliceHead == sliceTail)
                {
                    // 0 length slice
                    return;
                }
                /*
                 * When we apply a slice we take the head and get its next as our first
                 * item to apply and continue until we applied the tail. If the head and
                 * tail in this slice are not equal then there will be at least one more
                 * non-null node in the slice!
                 */
                Node current = sliceHead;
                do
                {
                    current = current.next;
                    if (Debugging.AssertsEnabled) Debugging.Assert(current != null, "slice property violated between the head on the tail must not be a null node");
                    current.Apply(del, docIDUpto);
                    //        System.out.println(Thread.currentThread().getName() + ": pull " + current + " docIDUpto=" + docIDUpto);
                } while (current != sliceTail);
                Reset();
            }

            internal virtual void Reset()
            {
                // Reset to a 0 length slice
                sliceHead = sliceTail;
            }

            /// <summary>
            /// Returns <code>true</code> iff the given item is identical to the item
            /// hold by the slices tail, otherwise <code>false</code>.
            /// </summary>
            internal virtual bool IsTailItem(object item)
            {
                return sliceTail.item == item;
            }

            internal virtual bool IsEmpty => sliceHead == sliceTail;
        }

        public int NumGlobalTermDeletes => globalBufferedUpdates.numTermDeletes;

        internal void Clear()
        {
            globalBufferLock.@Lock();
            try
            {
                Node currentTail = tail;
                globalSlice.sliceHead = globalSlice.sliceTail = currentTail;
                globalBufferedUpdates.Clear();
            }
            finally
            {
                globalBufferLock.Unlock();
            }
        }

        internal class Node // LUCENENET specific - made internal instead of private because it is used in internal APIs
        {
            internal /*volatile*/ Node next;
            internal readonly object item;

            internal Node(object item)
            {
                this.item = item;
            }

            //internal static readonly AtomicReferenceFieldUpdater<Node, Node> NextUpdater = AtomicReferenceFieldUpdater.newUpdater(typeof(Node), typeof(Node), "next");

            internal virtual void Apply(BufferedUpdates bufferedDeletes, int docIDUpto)
            {
                throw IllegalStateException.Create("sentinel item must never be applied");
            }

            internal virtual bool CasNext(Node cmp, Node val)
            {
                // LUCENENET NOTE: Interlocked.CompareExchange(location, value, comparand) is backwards from
                // AtomicReferenceFieldUpdater.compareAndSet(obj, expect, update), so swapping val and cmp.
                // Return true if the result of the CompareExchange is the same as the comparison.
                return ReferenceEquals(Interlocked.CompareExchange(ref next, val, cmp), cmp);
            }
        }

        private sealed class TermNode : Node
        {
            internal TermNode(Term term)
                : base(term)
            {
            }

            internal override void Apply(BufferedUpdates bufferedDeletes, int docIDUpto)
            {
                bufferedDeletes.AddTerm((Term)item, docIDUpto);
            }

            public override string ToString()
            {
                return "del=" + item;
            }
        }

        private sealed class QueryArrayNode : Node
        {
            internal QueryArrayNode(Query[] query)
                : base(query)
            {
            }

            internal override void Apply(BufferedUpdates bufferedUpdates, int docIDUpto)
            {
                foreach (Query query in (Query[])item)
                {
                    bufferedUpdates.AddQuery(query, docIDUpto);
                }
            }
        }

        private sealed class TermArrayNode : Node
        {
            internal TermArrayNode(Term[] term)
                : base(term)
            {
            }

            internal override void Apply(BufferedUpdates bufferedUpdates, int docIDUpto)
            {
                foreach (Term term in (Term[])item)
                {
                    bufferedUpdates.AddTerm(term, docIDUpto);
                }
            }

            public override string ToString()
            {
                return "dels=" + Arrays.ToString((Term[])item);
            }
        }

        private sealed class NumericUpdateNode : Node
        {
            internal NumericUpdateNode(NumericDocValuesUpdate update)
                : base(update)
            {
            }

            internal override void Apply(BufferedUpdates bufferedUpdates, int docIDUpto)
            {
                bufferedUpdates.AddNumericUpdate((NumericDocValuesUpdate)item, docIDUpto);
            }

            public override string ToString()
            {
                return "update=" + item;
            }
        }

        private sealed class BinaryUpdateNode : Node
        {
            internal BinaryUpdateNode(BinaryDocValuesUpdate update)
                : base(update)
            {
            }

            internal override void Apply(BufferedUpdates bufferedUpdates, int docIDUpto)
            {
                bufferedUpdates.AddBinaryUpdate((BinaryDocValuesUpdate)item, docIDUpto);
            }

            public override string ToString()
            {
                return "update=" + (BinaryDocValuesUpdate)item;
            }
        }

        private bool ForceApplyGlobalSlice()
        {
            globalBufferLock.@Lock();
            Node currentTail = tail;
            try
            {
                if (globalSlice.sliceTail != currentTail)
                {
                    globalSlice.sliceTail = currentTail;
                    globalSlice.Apply(globalBufferedUpdates, BufferedUpdates.MAX_INT32);
                }
                return globalBufferedUpdates.Any();
            }
            finally
            {
                globalBufferLock.Unlock();
            }
        }

        public int BufferedUpdatesTermsSize
        {
            get
            {
                globalBufferLock.@Lock();
                try
                {
                    ForceApplyGlobalSlice();
                    return globalBufferedUpdates.terms.Count;
                }
                finally
                {
                    globalBufferLock.Unlock();
                }
            }
        }

        public long BytesUsed => globalBufferedUpdates.bytesUsed;

        public override string ToString()
        {
            return "DWDQ: [ generation: " + generation + " ]";
        }

        public long NextSequenceNumber
        {
            get
            {
               long seqNo =nextSeqNo.GetAndIncrement();
                Debugging.Assert( seqNo <= maxSeqNo , "seqNo=" + seqNo + " vs maxSeqNo=" + maxSeqNo);
                return seqNo;
            }
        }

        public long LastSequenceNumber => nextSeqNo - 1;

        /* Inserts a gap in the sequence numbers.  This is used by IW during flush or commit to ensure any in-flight threads get sequence numbers
         *  inside the gap */
        public void SkipSequenceNumbers(long jump)
        {
            nextSeqNo.AddAndGet(jump);
        }

    }
}
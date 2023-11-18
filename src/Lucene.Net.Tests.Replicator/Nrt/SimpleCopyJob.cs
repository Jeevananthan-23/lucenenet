using J2N;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Support.Threading;
using Lucene.Net.Util;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    /** Handles one set of files that need copying, either because we have a
 *  new NRT point, or we are pre-copying merged files for merge warming. */
    class SimpleCopyJob : CopyJob
    {
        readonly Connection c;

        readonly byte[] copyBuffer = new byte[65536];
        readonly CopyState copyState;

        private IEnumerator<Dictionary<string, FileMetaData>> iter;

        public SimpleCopyJob(string reason, Connection c, CopyState copyState, SimpleReplicaNode dest, IDictionary<string, FileMetaData> files, bool highPriority, IOnceDone onceDone)
            : base(reason, files, dest, highPriority, onceDone)
        {

            dest.Message("create SimpleCopyJob o" + ord);
            this.c = c;
            this.copyState = copyState;
        }
        public override void Start()
        {
            if (iter == null)
            {
                iter = (IEnumerator<Dictionary<string, FileMetaData>>)toCopy.GetEnumerator();

                // Send all file names / offsets up front to avoid ping-ping latency:
                try
                {

                    // This means we resumed an already in-progress copy; we do this one first:
                    if (current != null)
                    {
                        c.@out.WriteByte(0);
                        c.@out.WriteString(current.name);
                        c.@out.WriteVInt64(current.GetBytesCopied());
                        totBytes += current.metaData.length;
                    }

                    foreach (KeyValuePair<string, FileMetaData> ent in toCopy)
                    {
                        string fileName = ent.Key;
                        FileMetaData metaData = ent.Value;
                        totBytes += metaData.length;
                        c.@out.WriteByte(0);
                        c.@out.WriteString(fileName);
                        c.@out.WriteVInt64(0);
                    }
                    c.@out.WriteByte(1);
                    c.Flush();
                    c.s.Shutdown(System.Net.Sockets.SocketShutdown.Both);

                    if (current != null)
                    {
                        // Do this only at the end, after sending all requested files, so we don't deadlock due to socket buffering waiting for primary to
                        // send us this length:
                        long len = c.@in.ReadVInt64();
                        if (len != current.metaData.length)
                        {
                            throw IllegalStateException.Create("file " + current.name + ": meta data says length=" + current.metaData.length + " but c.in says " + len);
                        }
                    }

                    dest.Message("SimpleCopyJob.init: done start files count=" + toCopy.size() + " totBytes=" + totBytes);

                }
                catch (Exception t)
                {
                    Cancel("exc during start", t);
                    throw new NodeCommunicationException("exc during start", t);
                }
            }
            else
            {
                throw IllegalStateException.Create("already started");
            }
        }
        public override long GetTotalBytesCopied() => totBytesCopied;
        public override ISet<string> GetFileNamesToCopy()
        {
            ISet<string> fileNames = new HashSet<string>();
            foreach (var ent in toCopy)
            {
                fileNames.add(ent.Key);
            }
            return fileNames;
        }

        public override ISet<string> GetFileNames() => files.Keys.ToHashSet();

        /** Higher priority and then "first come first serve" order. */

        public override int CompareTo(CopyJob _other)
        {
            SimpleCopyJob other = (SimpleCopyJob)_other;
            if (highPriority != other.highPriority)
            {
                return highPriority ? -1 : 1;
            }
            else
            {
                return ord < other.ord ? -1 : 0;
            }
        }

        public override void Finish()
        {
            dest.Message(string.Format(
                                   "top: file copy done; took {0} msec to copy {1} bytes; now rename {2} tmp files",
                                   (Time.NanoTime() - startNS) / 1000000.0,
                                   totBytesCopied,
                                   copiedFiles.size()));

            // NOTE: if any of the files we copied overwrote a file in the current commit point, we (ReplicaNode) removed the commit point up
            // front so that the commit is not corrupt.  This way if we hit exc here, or if we crash here, we won't leave a corrupt commit in
            // the index:
            foreach (KeyValuePair<string, string> ent in copiedFiles)
            {
                string tmpFileName = ent.Value;
                string fileName = ent.Key;

                if (Node.VERBOSE_FILES)
                {
                    dest.Message("rename file " + tmpFileName + " to " + fileName);
                }

                // NOTE: if this throws exception, then some files have been moved to their true names, and others are leftover .tmp files.  I don't
                // think heroic exception handling is necessary (no harm will come, except some leftover files),  nor warranted here (would make the
                // code more complex, for the exceptional cases when something is wrong w/ your IO system):
                dest.dir.RenameFile(tmpFileName, fileName);
            }

            copiedFiles.Clear();
        }

        /** Do an iota of work; returns true if all copying is done */
        internal bool Visit()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (exc != null)
                {
                    // We were externally cancelled:
                    return true;
                }

                if (current == null)
                {
                    if (iter.MoveNext() == false)
                    {
                        c.Dispose();
                        return true;
                    }
                    //check how to do this
                    Dictionary<string, FileMetaData> next = iter.Current;
                    FileMetaData metaData = next.Values.First();
                    string fileName = next.Keys.First();
                    long len = c.@in.ReadVInt64();
                    if (len != metaData.length)
                    {
                        throw IllegalStateException.Create("file " + fileName + ": meta data says length=" + metaData.length + " but c.in says " + len);
                    }
                    current = new CopyOneFile(c.@in, dest, fileName, metaData, copyBuffer);
                }

                if (current.Visit())
                {
                    // This file is done copying
                    copiedFiles.Add(current.name, current.tmpName);
                    totBytesCopied += current.GetBytesCopied();
                    Debug.Assert(totBytesCopied <= totBytes, "totBytesCopied=" + totBytesCopied + " totBytes=" + totBytes);
                    current = null;
                    return false;
                }

                return false;
            }
            finally { UninterruptableMonitor.Exit(this); }
        }

        protected override CopyOneFile NewCopyOneFile(CopyOneFile prev) => new CopyOneFile(prev, c.@in);

        public override void TransferAndCancel(CopyJob prevJob)
        {
            try
            {
                base.TransferAndCancel(prevJob);
            }
            finally
            {
                IOUtils.CloseWhileHandlingException(((SimpleCopyJob)prevJob).c);
            }
        }

        public void Cancel(string reason, Exception exc)
        {
            try
            {
                base.Cancel(reason, exc);
            }
            finally
            {
                IOUtils.CloseWhileHandlingException(c);
            }
        }
        public override bool GetFailed() => exc != null;
        public override string ToString() => "SimpleCopyJob(ord=" + ord + " " + reason + " highPriority=" + highPriority + " files count=" + files.size() + " bytesCopied=" + totBytesCopied + " (of " + totBytes + ") filesCopied=" + copiedFiles.size() + ")";

        public override void RunBlocking()
        {
            while (Visit() == false) ;

            if (GetFailed())
            {
                throw RuntimeException.Create("copy failed: " + cancelReason, exc);
            }
        }
        public override CopyState GetCopyState() => copyState;

        public override bool Conflicts(CopyJob _other)
        {
            ISet<string> filesToCopy = new HashSet<string>();
            foreach (KeyValuePair<string, FileMetaData> ent in toCopy)
            {
                filesToCopy.add(ent.Key);
            }

            SimpleCopyJob other = (SimpleCopyJob)_other;
            lock (other)
            {
                foreach (KeyValuePair<string, FileMetaData> ent in other.toCopy)
                {
                    if (filesToCopy.contains(ent.Key))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        internal class SimpleCopyJobQueue : PriorityQueue<CopyJob>
        {
            public SimpleCopyJobQueue(int maxSize) : base(maxSize)
            {
            }

            protected internal override bool LessThan(CopyJob a, CopyJob b) => throw new NotImplementedException();
        }
    }
}

using J2N.Threading;
using J2N.Threading.Atomic;
using Lucene.Net.Documents;
using Lucene.Net.Support.Threading;
using Lucene.Net.Util;
using System;
using System.Diagnostics;
using System.Net;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    /** Parent JVM hold this "wrapper" to refer to each child JVM.  This is roughly equivalent e.g. to a client-side "sugar" API. */
    internal class NodeProcess : IDisposable
    {
        internal readonly Process p;

        // Port sub-process is listening on
        internal readonly int tcpPort;

        internal readonly int id;

        internal readonly ThreadJob pumper;

        // Acquired when searching or indexing wants to use this node:
        internal readonly ReentrantLock rlock;

        internal readonly bool isPrimary;

        // Version in the commit point we opened on init:
        internal readonly long initCommitVersion;

        // SegmentInfos.version, which can be higher than the initCommitVersion
        internal readonly long initInfosVersion;

        internal volatile bool isOpen = true;

        internal readonly AtomicBoolean nodeIsClosing;

        public NodeProcess(Process p, int id, int tcpPort, ThreadJob pumper, bool isPrimary, long initCommitVersion, long initInfosVersion, AtomicBoolean nodeIsClosing)
        {
            this.p = p;
            this.id = id;
            this.tcpPort = tcpPort;
            this.pumper = pumper;
            this.isPrimary = isPrimary;
            this.initCommitVersion = initCommitVersion;
            this.initInfosVersion = initInfosVersion;
            this.nodeIsClosing = nodeIsClosing;
            Debug.Assert(initInfosVersion >= initCommitVersion, "initInfosVersion=" + initInfosVersion + " initCommitVersion=" + initCommitVersion);
            rlock = new ReentrantLock();
        }
        public override string ToString()
        {
            if (isPrimary)
            {
                return "P" + id + " tcpPort=" + tcpPort;
            }
            else
            {
                return "R" + id + " tcpPort=" + tcpPort;
            }
        }

        public void Crash()
        {
            UninterruptableMonitor.Enter(this);
            try
            {

                if (isOpen)
                {
                    isOpen = false;
                    p.Dispose(); //destory
                    try
                    {
                        p.WaitForExit();
                        pumper.Join();
                    }
                    catch (ThreadInterruptedException ie)
                    {
                        ThreadJob.CurrentThread.Interrupt();
                        throw RuntimeException.Create(ie);
                    }
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        public bool Commit()
        {
            using Connection c = new Connection(tcpPort);
            c.@out.WriteByte(SimplePrimaryNode.CMD_COMMIT);
            c.Flush();
            c.s.Shutdown(System.Net.Sockets.SocketShutdown.Both); //check
            if (c.@in.ReadByte() != 1)
            {
                throw RuntimeException.Create("commit failed");
            }
            return true;
        }

        public void CommitAsync()
        {
            using Connection c = new Connection(tcpPort);
            c.@out.WriteByte(SimplePrimaryNode.CMD_COMMIT);
            c.Flush();
        }

        public long GetSearchingVersion()
        {
            using Connection c = new Connection(tcpPort);
            c.@out.WriteByte(SimplePrimaryNode.CMD_GET_SEARCHING_VERSION);
            c.Flush();
            c.s.Shutdown(System.Net.Sockets.SocketShutdown.Both);
            return c.@in.ReadVInt64();
        }

        /** Ask the primary node process to flush.  We send it all currently up replicas so it can notify them about the new NRT point.  Returns the newly
         *  flushed version, or a negative (current) version if there were no changes. */
        public long Flush(int atLeastMarkerCount)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                Debug.Assert(isPrimary);
                using Connection c = new Connection(tcpPort);
                c.@out.WriteByte(SimplePrimaryNode.CMD_FLUSH);
                c.@out.WriteVInt32(atLeastMarkerCount);
                c.Flush();
                c.s.Shutdown(System.Net.Sockets.SocketShutdown.Both);
                return c.@in.ReadInt64();
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        public bool Shutdown()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                rlock.Lock();
                try
                {
                    //System.out.println("PARENT: now shutdown node=" + id + " isOpen=" + isOpen);
                    if (isOpen)
                    {
                        // Ask the child process to shutdown gracefully:
                        isOpen = false;
                        //System.out.println("PARENT: send CMD_CLOSE to node=" + id);
                        using Connection c = new Connection(tcpPort);
                        c.@out.WriteByte(SimplePrimaryNode.CMD_CLOSE);
                        c.Flush();
                        if (c.@in.ReadByte() != 1)
                        {
                            throw RuntimeException.Create("shutdown failed");
                        }
                        /*catch (Exception t)
                        {
                        Console.WriteLine("top: shutdown failed; ignoring");
                        t.printStackTrace(Console.Out);*/
                        try
                        {
                            p.WaitForExit();
                            pumper.Join();
                        }
                        catch (ThreadInterruptedException ie)
                        {
                            ThreadJob.CurrentThread.Interrupt();
                            throw RuntimeException.Create(ie);
                        }
                    }
                    return true;
                }
                finally
                {
                    rlock.Unlock();
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        public void NewNRTPoint(long version, long primaryGen, int primaryTCPPort)
        {
            using Connection c = new Connection(tcpPort);
            c.@out.WriteByte(SimpleReplicaNode.CMD_NEW_NRT_POINT);
            c.@out.WriteVInt64(version);
            c.@out.WriteVInt64(primaryGen);
            c.@out.WriteInt32(primaryTCPPort);
            c.Flush();
        }

        public void AddOrUpdateDocument(Connection c, Document doc, bool isUpdate)
        {
            if (isPrimary == false)
            {
                throw IllegalStateException.Create("only primary can index");
            }
            int fieldCount = 0;

            string title = doc.Get("title");
            if (title != null)
            {
                fieldCount++;
            }

            string docid = doc.Get("docid");
            Debug.Assert(docid != null);
            fieldCount++;

            string body = doc.Get("body");
            if (body != null)
            {
                fieldCount++;
            }

            string marker = doc.Get("marker");
            if (marker != null)
            {
                fieldCount++;
            }

            c.@out.WriteByte(isUpdate ? SimplePrimaryNode.CMD_UPDATE_DOC : SimplePrimaryNode.CMD_ADD_DOC);
            c.@out.WriteVInt32(fieldCount);
            c.@out.WriteString("docid");
            c.@out.WriteString(docid);
            if (title != null)
            {
                c.@out.WriteString("title");
                c.@out.WriteString(title);
            }
            if (body != null)
            {
                c.@out.WriteString("body");
                c.@out.WriteString(body);
            }
            if (marker != null)
            {
                c.@out.WriteString("marker");
                c.@out.WriteString(marker);
            }
            c.Flush();
            c.@in.ReadByte();
        }

        public void DeleteDocument(Connection c, string docid)
        {
            if (isPrimary == false)
            {
                throw IllegalStateException.Create("only primary can delete documents");
            }
            c.@out.WriteByte(SimplePrimaryNode.CMD_DELETE_DOC);
            c.@out.WriteString(docid);
            c.Flush();
            c.@in.ReadByte();
        }

        public void DeleteAllDocuments(Connection c)
        {
            if (isPrimary == false)
            {
                throw IllegalStateException.Create("only primary can delete documents");
            }
            c.@out.WriteByte(SimplePrimaryNode.CMD_DELETE_ALL_DOCS);
            c.Flush();
            c.@in.ReadByte();
        }

        public void ForceMerge(Connection c)
        {
            if (isPrimary == false)
            {
                throw IllegalStateException.Create("only primary can force merge");
            }
            c.@out.WriteByte(SimplePrimaryNode.CMD_FORCE_MERGE);
            c.Flush();
            c.@in.ReadByte();
        }

        public void Dispose() => Shutdown();
    }
}

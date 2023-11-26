using J2N.Threading;
using J2N.Threading.Atomic;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using RandomizedTesting.Generators;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    internal class SimpleReplicaNode : ReplicaNode
    {
        internal readonly int tcpPort;
        internal readonly Jobs jobs;

        // Rate limits incoming bytes/sec when fetching files:
        internal readonly RateLimiter fetchRateLimiter;

        internal readonly AtomicInt64 bytesSinceLastRateLimiterCheck = new AtomicInt64();
        internal readonly Random random;

        /** Changes over time, as primary node crashes and moves around */
        private int curPrimaryTCPPort;

        public SimpleReplicaNode(Random random, int id, int tcpPort, string indexPath, long curPrimaryGen, int primaryTCPPort,
                                 SearcherFactory searcherFactory, bool doCheckIndexOnClose)
                : base(id, GetDirectory(random, id, indexPath, doCheckIndexOnClose), searcherFactory, Console.Out)

        {
            this.tcpPort = tcpPort;
            this.random = new Random(random.nextInt());

            // Random IO throttling on file copies: 5 - 20 MB/sec:
            double mbPerSec = 5 * (1.0 + 3 * random.NextDouble());
            Message(string.Format("top: will rate limit file fetch to %.2f MB/sec", mbPerSec));
            fetchRateLimiter = new RateLimiter.SimpleRateLimiter(mbPerSec);
            this.curPrimaryTCPPort = primaryTCPPort;

            Start(curPrimaryGen);

            // Handles fetching files from primary:
            jobs = new Jobs(this);
            jobs.Name = ("R" + id + ".copyJobs");
            //jobs.setDaemon(true);
            jobs.Start();
        }

        protected override void Launch(CopyJob job)
        {
            jobs.Launch(job);
        }

        public override void Dispose()
        {
            // Can't be sync'd when calling jobs since it can lead to deadlock:
            jobs.Dispose();
            Message("top: jobs closed");
            lock (mergeCopyJobs)
            {
                foreach (CopyJob job in mergeCopyJobs)
                {
                    Message("top: cancel merge copy job " + job);
                    job.Cancel("jobs closing", null);
                }
            }
            base.Dispose();
        }

        protected override CopyJob NewCopyJob(string reason, IDictionary<string, FileMetaData> files, IDictionary<string, FileMetaData> prevFiles, bool highPriority, CopyJob.IOnceDone onceDone)
        {
            Connection c;
            CopyState copyState;

            // Exceptions in here mean something went wrong talking over the socket, which are fine (e.g. primary node crashed):
            try
            {
                c = new Connection(curPrimaryTCPPort);
                c.@out.WriteByte(SimplePrimaryNode.CMD_FETCH_FILES);
                c.@out.WriteVInt32(id);
                if (files == null)
                {
                    // No incoming CopyState: ask primary for latest one now
                    c.@out.WriteByte((byte)1);
                    c.Flush();
                    copyState = SimpleServer.ReadCopyState(c.@in);
                    files = copyState.files;
                }
                else
                {
                    c.@out.WriteByte((byte)0);
                    copyState = null;
                }
            }
            catch (Exception t)
            {
                throw new NodeCommunicationException("exc while reading files to copy", t);
            }

            return new SimpleCopyJob(reason, c, copyState, this, files, highPriority, onceDone);
        }

        internal static Directory GetDirectory(Random random, int id, string path, bool doCheckIndexOnClose)
        {
            MockDirectoryWrapper dir = LuceneTestCase.NewMockFSDirectory(LuceneTestCase.CreateTempDir(path));

            dir.AssertNoUnreferencedFilesOnDispose = true;
            dir.CheckIndexOnDispose = doCheckIndexOnClose;

            // Corrupt any index files not referenced by current commit point; this is important (increases test evilness) because we may have done
            // a hard crash of the previous JVM writing to this directory and so MDW's corrupt-unknown-files-on-close never ran:
            Node.NodeMessage(Console.Out, id, "top: corrupt unknown files");
            //dir.CorruptUnknownFiles();

            return dir;
        }

        internal const byte CMD_NEW_NRT_POINT = 0;

        // Sent by primary to replica to pre-copy merge files:
        internal const byte CMD_PRE_COPY_MERGE = 17;

        /** Handles incoming request to the naive TCP server wrapping this node */

        internal void HandleOneConnection(TcpListener ss, AtomicBoolean stop, NetworkStream @is, Socket socket, DataInput @in, DataOutput @out, BufferedStream bos)
        {
        //Message("one connection: " + socket);
        outer:
            while (true)
            {
                byte cmd;
                while (true)
                {
                    if (@is.DataAvailable)
                    {
                        break;
                    }
                    if (stop.Value)
                    {
                        return;
                    }
                    ThreadJob.Sleep(10);
                }

                try
                {
                    cmd = @in.ReadByte();
                }
                catch (EOFException eofe)
                {
                    break;
                }

                switch (cmd)
                {
                    case CMD_NEW_NRT_POINT:
                        {
                            long version = @in.ReadVInt64();
                            long newPrimaryGenz = @in.ReadVInt64();
                            ThreadJob.CurrentThread.Name = ("recv-" + version);
                            curPrimaryTCPPort = @in.ReadInt32();
                            Message("newNRTPoint primaryTCPPort=" + curPrimaryTCPPort + " version=" + version + " newPrimaryGen=" + newPrimaryGenz);
                            NewNRTPoint(newPrimaryGenz, version);
                        }
                        break;

                    case SimplePrimaryNode.CMD_GET_SEARCHING_VERSION:
                        // This is called when primary has crashed and we need to elect a new primary from all the still running replicas:

                        // Tricky: if a sync is just finishing up, i.e. managed to finish copying all files just before we crashed primary, and is now
                        // in the process of opening a new reader, we need to wait for it, to be sure we really pick the most current replica:
                        if (IsCopying())
                        {
                            Message("top: getSearchingVersion: now wait for finish sync");
                            // TODO: use immediate concurrency instead of polling:
                            while (IsCopying() && stop.Value == false)
                            {
                                ThreadJob.Sleep(10);
                                Message("top: curNRTCopy=" + curNRTCopy);
                            }
                            Message("top: getSearchingVersion: done wait for finish sync");
                        }
                        if (stop.Value == false)
                        {
                            @out.WriteVInt64(GetCurrentSearchingVersion());
                        }
                        else
                        {
                            Message("top: getSearchingVersion: stop waiting for finish sync: stop is set");
                        }
                        break;

                    case SimplePrimaryNode.CMD_SEARCH:
                        {
                            ThreadJob.CurrentThread.Name = ("search");
                            IndexSearcher searcher = mgr.Acquire();
                            try
                            {
                                long version = ((DirectoryReader)searcher.IndexReader).Version;
                                int hitCount = searcher.Search(new TermQuery(new Term("body", "the")), 1).TotalHits;
                                //node.Message("version=" + version + " searcher=" + searcher);
                                @out.WriteVInt64(version);
                                @out.WriteVInt32(hitCount);
                                bos.Flush();
                            }
                            finally
                            {
                                mgr.Release(searcher);
                            }
                        }
                        goto outer;

                    case SimplePrimaryNode.CMD_SEARCH_ALL:
                        {
                            ThreadJob.CurrentThread.Name = ("search all");
                            IndexSearcher searcher = mgr.Acquire();
                            try
                            {
                                long version = ((DirectoryReader)searcher.IndexReader).Version;
                                int hitCount = searcher.Search(new MatchAllDocsQuery(), 1).TotalHits;
                                //node.Message("version=" + version + " searcher=" + searcher);
                                @out.WriteVInt64(version);
                                @out.WriteVInt32(hitCount);
                                bos.Flush();
                            }
                            finally
                            {
                                mgr.Release(searcher);
                            }
                        }
                        goto outer;

                    case SimplePrimaryNode.CMD_MARKER_SEARCH:
                        {
                            ThreadJob.CurrentThread.Name = ("msearch");
                            int expectedAtLeastCount = @in.ReadVInt32();
                            IndexSearcher searcher = mgr.Acquire();
                            try
                            {
                                long version = ((DirectoryReader)searcher.IndexReader).Version;
                                int hitCount = searcher.Search(new TermQuery(new Term("marker", "marker")), 1).TotalHits;
                                if (hitCount < expectedAtLeastCount)
                                {
                                    Message("marker search: expectedAtLeastCount=" + expectedAtLeastCount + " but hitCount=" + hitCount);
                                    TopDocs hits = searcher.Search(new TermQuery(new Term("marker", "marker")), expectedAtLeastCount);
                                    List<int> seen = new();
                                    foreach (ScoreDoc hit in hits.ScoreDocs)
                                    {
                                        Document doc = searcher.Doc(hit.Doc);
                                        seen.Add(int.Parse(doc.Get("docid").Substring(1)));
                                    }
                                    seen.Sort();
                                    Message("saw markers:");
                                    foreach (int marker in seen)
                                    {
                                        Message("saw m" + marker);
                                    }
                                }

                                @out.WriteVInt64(version);
                                @out.WriteVInt32(hitCount);
                                bos.Flush();
                            }
                            finally
                            {
                                mgr.Release(searcher);
                            }
                        }
                        goto outer;

                    case SimplePrimaryNode.CMD_COMMIT:
                        ThreadJob.CurrentThread.Name = ("commit");
                        Commit();
                        @out.WriteByte((byte)1);
                        break;

                    case SimplePrimaryNode.CMD_CLOSE:
                        ThreadJob.CurrentThread.Name = ("close");
                        ss.Stop();
                        @out.WriteByte((byte)1);
                        goto outer;

                    case CMD_PRE_COPY_MERGE:
                        ThreadJob.CurrentThread.Name = ("merge copy");

                        long newPrimaryGen = @in.ReadVInt64();
                        curPrimaryTCPPort = @in.ReadVInt32();
                        IDictionary<string, FileMetaData> files = SimpleServer.ReadFilesMetaData(@in);
                        Message("done reading files to copy files=" + files.Keys);
                        AtomicBoolean finished = new();
                        CopyJob job = LaunchPreCopyMerge(finished, newPrimaryGen, files);
                        Message("done launching copy job files=" + files.Keys);

                        // Silly keep alive mechanism, else if e.g. we (replica node) crash, the primary
                        // won't notice for a very long time:
                        bool success = false;
                        try
                        {
                            int count = 0;
                            while (true)
                            {
                                if (finished || stop)
                                {
                                    break;
                                }
                                ThreadJob.Sleep(10);
                                count++;
                                if (count == 100)
                                {
                                    // Once per second or so, we send a keep alive
                                    Message("send merge pre copy keep alive... files=" + files.Keys);

                                    // To be evil, we sometimes fail to keep-alive, e.g. simulating a long GC pausing us:
                                    if (random.NextBoolean())
                                    {
                                        @out.WriteByte((byte)0);
                                        count = 0;
                                    }
                                }
                            }

                            @out.WriteByte((byte)1);
                            bos.Flush();
                            success = true;
                        }
                        finally
                        {
                            Message("done merge copy files=" + files.Keys + " success=" + success);
                        }
                        break;

                    default:
                        throw new IllegalArgumentException("unrecognized cmd=" + cmd);
                }
                bos.Flush();

                break;
            }
        }

        protected override void SendNewReplica()
        {
            Message("send new_replica to primary tcpPort=" + curPrimaryTCPPort);
            using Connection c = new Connection(curPrimaryTCPPort);
            try
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_NEW_REPLICA);
                c.@out.WriteVInt32(tcpPort);
                c.Flush();
                c.s.Shutdown(SocketShutdown.Both);
            }
            catch (Exception t)
            {
                Message("ignoring exc " + t + " sending new_replica to primary tcpPort=" + curPrimaryTCPPort);
            }
        }

        public override IndexOutput CreateTempOutput(string prefix, string suffix, IOContext ioContext)
        {
            return new RateLimitedIndexOutput(fetchRateLimiter, base.CreateTempOutput(prefix, suffix, ioContext));
        }
    }
}
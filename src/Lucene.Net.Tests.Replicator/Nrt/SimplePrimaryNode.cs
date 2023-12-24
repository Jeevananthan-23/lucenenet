using J2N;
using J2N.Threading;
using J2N.Threading.Atomic;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Lucene.Net.Support.Threading;
using Lucene.Net.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    /** A primary node that uses simple TCP connections to send commands and copy files */

    internal class SimplePrimaryNode : PrimaryNode
    {
        internal readonly int tcpPort;

        internal readonly Random random;

        // These are updated by parent test process whenever replicas change:
        private int[] replicaTCPPorts = new int[0];

        private int[] replicaIDs = new int[0];

        // So we only flip a bit once per file name:
        internal readonly ISet<string> bitFlipped = new HashSet<string>();

        internal readonly List<MergePreCopy> warmingSegments = new List<MergePreCopy>();

        internal readonly bool doFlipBitsDuringCopy;

        internal class MergePreCopy
        {
            internal readonly List<Connection> connections = new List<Connection>();
            internal readonly IDictionary<string, FileMetaData> _files;
            private bool finished;

            public MergePreCopy(IDictionary<string, FileMetaData> files)
            {
                _files = files;
            }

            public bool TryAddConnection(Connection c)
            {
                UninterruptableMonitor.Enter(this);
                try
                {
                    if (finished == false)
                    {
                        connections.Add(c);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                finally
                {
                    UninterruptableMonitor.Exit(this);
                }
            }

            public bool Finished()
            {
                UninterruptableMonitor.Enter(this);
                try
                {
                    if (connections.Any())
                    {
                        finished = true;
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                finally
                {
                    UninterruptableMonitor.Exit(this);
                }
            }
        }

        public SimplePrimaryNode(Random random, string indexPath, int id, int tcpPort, long primaryGen, long forcePrimaryVersion, SearcherFactory searcherFactory,
                                 bool doFlipBitsDuringCopy, bool doCheckIndexOnClose)
            : base(InitWriter(id, random, indexPath, doCheckIndexOnClose), id, primaryGen, forcePrimaryVersion, searcherFactory, Console.Out)
        {
            this.tcpPort = tcpPort;
            this.random = new Random(random.nextInt());
            this.doFlipBitsDuringCopy = doFlipBitsDuringCopy;
        }

        /** Records currently alive replicas. */

        public void SetReplicas(int[] replicaIDs, int[] replicaTCPPorts)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                Message("top: set replicasIDs=" + Arrays.ToString(replicaIDs) + " tcpPorts=" + Arrays.ToString(replicaTCPPorts));
                this.replicaIDs = replicaIDs;
                this.replicaTCPPorts = replicaTCPPorts;
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        private static IndexWriter InitWriter(int id, Random random, string indexPath, bool doCheckIndexOnClose)
        {
            Directory dir = SimpleReplicaNode.GetDirectory(random, id, indexPath, doCheckIndexOnClose);

            MockAnalyzer analyzer = new MockAnalyzer(random);
            analyzer.MaxTokenLength = TestUtil.NextInt32(random, 1, IndexWriter.MAX_TERM_LENGTH);
            IndexWriterConfig iwc = LuceneTestCase.NewIndexWriterConfig(random, LuceneVersion.LUCENE_48, analyzer);

            MergePolicy mp = iwc.MergePolicy;
            //iwc.SetInfoStream(new TextWriterInfoStream(Console.Out));

            // Force more frequent merging so we stress merge warming:
            if (mp is TieredMergePolicy)
            {
                TieredMergePolicy tmp = (TieredMergePolicy)mp;
                tmp.SegmentsPerTier = 3;
                tmp.MaxMergeAtOnce = 3;
            }
            else if (mp is LogMergePolicy)
            {
                LogMergePolicy lmp = (LogMergePolicy)mp;
                lmp.MergeFactor = 3;
            }

            IndexWriter writer = new IndexWriter(dir, iwc);
            TestUtil.ReduceOpenFiles(writer);
            return writer;
        }

        public override void PreCopyMergedSegmentFiles(SegmentCommitInfo info, IDictionary<string, FileMetaData> files)
        {
            int[] replicaTCPPorts = this.replicaTCPPorts;
            if (replicaTCPPorts == null)
            {
                Message("no replicas; skip warming " + info);
                return;
            }

            Message("top: warm merge " + info + " to " + replicaTCPPorts.Length + " replicas; tcpPort=" + tcpPort + ": files=" + files.Keys);

            MergePreCopy preCopy = new(files);
            warmingSegments.Add(preCopy);

            try
            {
                ISet<string> fileNames = files.Keys.ToHashSet();

                // Ask all currently known replicas to pre-copy this newly merged segment's files:
                foreach (int replicaTCPPort in replicaTCPPorts)
                {
                    try
                    {
                        Connection c = new Connection(replicaTCPPort);
                        c.@out.WriteByte(SimpleReplicaNode.CMD_PRE_COPY_MERGE);
                        c.@out.WriteVInt64(primaryGen);
                        c.@out.WriteVInt32(tcpPort);
                        SimpleServer.WriteFilesMetaData(c.@out, files);
                        c.Flush();
                        c.s.Shutdown(SocketShutdown.Both);
                        Message("warm connection " + c.s);
                        preCopy.connections.Add(c);
                    }
                    catch (Exception t)
                    {
                        Message("top: ignore exception trying to warm to replica port " + replicaTCPPort + ": " + t);
                        //t.printStackTrace(System.@out);
                    }
                }

                long startNS = Time.NanoTime();
                long lastWarnNS = startNS;

                // TODO: maybe ... place some sort of time limit on how long we are willing to wait for slow replica(s) to finish copying?
                while (preCopy.Finished() == false)
                {
                    try
                    {
                        ThreadJob.Sleep(10);
                    }
                    catch (ThreadInterruptedException ie)
                    {
                        throw new ThreadInterruptedException(ie);
                    }

                    if (IsClosed())
                    {
                        Message("top: primary is closing: now cancel segment warming");
                        lock (preCopy.connections)
                        {
                            IOUtils.CloseWhileHandlingException(preCopy.connections);
                        }
                        return;
                    }

                    long ns = Time.NanoTime();
                    if (ns - lastWarnNS > 1000000000L)
                    {
                        Message(string.Format("top: warning: still warming merge " + info + " to " + preCopy.connections.size() + " replicas for %.1f sec...", (ns - startNS) / 1000000000.0));
                        lastWarnNS = ns;
                    }

                    // Process keep-alives:
                    lock (preCopy.connections)
                    {
                        IEnumerator<Connection> it = (IEnumerator<Connection>)preCopy.connections.AsEnumerable();
                        while (it.MoveNext())
                        {
                            Connection c = it.MoveNext() ? it.Current : null;
                            try
                            {
                                long nowNS = Time.NanoTime();
                                bool done = false;
                                while (c.sockIn.Length > 0)
                                {
                                    byte b = c.@in.ReadByte();
                                    if (b == 0)
                                    {
                                        // keep-alive
                                        c.lastKeepAliveNS = nowNS;
                                        Message("keep-alive for socket=" + c.s + " merge files=" + files.Keys);
                                    }
                                    else
                                    {
                                        // merge is done pre-copying to this node
                                        if (b != 1)
                                        {
                                            throw new IllegalArgumentException();
                                        }
                                        Message("connection socket=" + c.s + " is done warming its merge " + info + " files=" + files.Keys);
                                        IOUtils.CloseWhileHandlingException(c);
                                        it.Reset();
                                        done = true;
                                        break;
                                    }
                                }

                                // If > 2 sec since we saw a keep-alive, assume this replica is dead:
                                if (done == false && nowNS - c.lastKeepAliveNS > 2000000000L)
                                {
                                    Message("top: warning: replica socket=" + c.s + " for segment=" + info + " seems to be dead; closing files=" + files.Keys);
                                    IOUtils.CloseWhileHandlingException(c);
                                    it.Reset();
                                    done = true;
                                }

                                if (done == false && random.nextInt(1000) == 17)
                                {
                                    Message("top: warning: now randomly dropping replica from merge warming; files=" + files.Keys);
                                    IOUtils.CloseWhileHandlingException(c);
                                    it.Reset();
                                    done = true;
                                }
                            }
                            catch (Exception t)
                            {
                                Message("top: ignore exception trying to read byte during warm for segment=" + info + " to replica socket=" + c.s + ": " + t + " files=" + files.Keys);
                                IOUtils.CloseWhileHandlingException(c);
                                it.Reset();
                            }
                        }
                    }
                }
            }
            finally
            {
                warmingSegments.Remove(preCopy);
            }
        }

        /** Flushes all indexing ops to disk and notifies all replicas that they should now copy */

        private void HandleFlush(DataInput topIn, DataOutput topOut, BufferedStream bos)
        {
            ThreadJob.CurrentThread.Name = "flush";

            int atLeastMarkerCount = topIn.ReadVInt32();

            int[] replicaTCPPorts, replicaIDs;

            lock (this)
            {
                replicaTCPPorts = this.replicaTCPPorts;
                replicaIDs = this.replicaIDs;
            }

            Message("now flush; " + replicaIDs.Length + " replicas");

            if (FlushAndRefresh())
            {
                // Something did get flushed (there were indexing ops since the last flush):

                VerifyAtLeastMarkerCount(atLeastMarkerCount, null);

                // Tell caller the version before pushing to replicas, so that even if we crash after this, caller will know what version we
                // (possibly) pushed to some replicas.  Alternatively we could make this 2 separate ops?
                long version = GetCopyStateVersion();
                Message("send flushed version=" + version);
                topOut.WriteInt64(version);
                bos.Flush();

                // Notify current replicas:
                for (int i = 0; i < replicaIDs.Length; i++)
                {
                    int replicaID = replicaIDs[i];
                    using Connection c = new Connection(replicaTCPPorts[i]);
                    try
                    {
                        Message("send NEW_NRT_POINT to R" + replicaID + " at tcpPort=" + replicaTCPPorts[i]);
                        c.@out.WriteByte(SimpleReplicaNode.CMD_NEW_NRT_POINT);
                        c.@out.WriteVInt64(version);
                        c.@out.WriteVInt64(primaryGen);
                        c.@out.WriteInt32(tcpPort);
                        c.Flush();
                        // TODO: we should use multicast to broadcast files @out to replicas
                        // TODO: ... replicas could copy from one another instead of just primary
                        // TODO: we could also prioritize one replica at a time?
                    }
                    catch (Exception t)
                    {
                        Message("top: failed to connect R" + replicaID + " for newNRTPoint; skipping: " + t.Message);
                    }
                }
            }
            else
            {
                // No changes flushed:
                topOut.WriteInt64(-GetCopyStateVersion());
            }
        }

        /** Pushes CopyState on the wire */

        private static void WriteCopyState(CopyState state, DataOutput @out)
        {
            // TODO (opto): we could encode to byte[] once when we created the copyState, and then just send same byts to all replicas...
            @out.WriteVInt32(state.infosBytes.Length);
            @out.WriteBytes(state.infosBytes, 0, state.infosBytes.Length);
            @out.WriteVInt64(state.gen);
            @out.WriteVInt64(state.version);
            SimpleServer.WriteFilesMetaData(@out, state.files);

            @out.WriteVInt32(state.completedMergeFiles.size());
            foreach (string fileName in state.completedMergeFiles)
            {
                @out.WriteString(fileName);
            }
            @out.WriteVInt64(state.primaryGen);
        }

        /** Called when another node (replica) wants to copy files from us */

        private bool HandleFetchFiles(Random random, Socket socket, DataInput destIn, DataOutput destOut, BufferedStream bos)
        {
            ThreadJob.CurrentThread.Name = "send";

            int replicaID = destIn.ReadVInt32();
            Message("top: start fetch for R" + replicaID + " socket=" + socket);
            byte b = destIn.ReadByte();
            CopyState copyState;
            if (b == 0)
            {
                // Caller already has CopyState
                copyState = null;
            }
            else if (b == 1)
            {
                // Caller does not have CopyState; we pull the latest one:
                copyState = GetCopyState();
                ThreadJob.CurrentThread.Name = "send-R" + replicaID + "-" + copyState.version;
            }
            else
            {
                // Protocol error:
                throw new IllegalArgumentException("invalid CopyState byte=" + b);
            }

            try
            {
                if (copyState != null)
                {
                    // Serialize CopyState on the wire to the client:
                    WriteCopyState(copyState, destOut);
                    bos.Flush();
                }

                byte[] buffer = new byte[16384];
                int fileCount = 0;
                long totBytesSent = 0;
                while (true)
                {
                    byte done = destIn.ReadByte();
                    if (done == 1)
                    {
                        break;
                    }
                    else if (done != 0)
                    {
                        throw new IllegalArgumentException("expected 0 or 1 byte but got " + done);
                    }

                    // Name of the file the replica wants us to send:
                    string fileName = destIn.ReadString();

                    // Starting offset in the file we should start sending bytes from:
                    long fpStart = destIn.ReadVInt64();

                    using (IndexInput @in = dir.OpenInput(fileName, IOContext.DEFAULT))
                    {
                        long len = @in.Length;
                        //Message("fetch " + fileName + ": send len=" + len);
                        destOut.WriteVInt64(len);
                        @in.Seek(fpStart);
                        long upto = fpStart;
                        while (upto < len)
                        {
                            int chunk = (int)Math.Min(buffer.Length, len - upto);
                            @in.ReadBytes(buffer, 0, chunk);
                            if (doFlipBitsDuringCopy)
                            {
                                if (random.nextInt(3000) == 17 && bitFlipped.contains(fileName) == false)
                                {
                                    bitFlipped.add(fileName);
                                    Message("file " + fileName + " to R" + replicaID + ": now randomly flipping a bit at byte=" + upto);
                                    int x = random.nextInt(chunk);
                                    int bit = random.nextInt(8);
                                    buffer[x] ^= (byte)(1 << bit); //TODO: Check this the right way?
                                }
                            }
                            destOut.WriteBytes(buffer, 0, chunk);
                            upto += chunk;
                            totBytesSent += chunk;
                        }
                    }

                    fileCount++;
                }

                Message("top: done fetch files for R" + replicaID + ": sent " + fileCount + " files; sent " + totBytesSent + " bytes");
            }
            catch (Exception t)
            {
                Message("top: exception during fetch: " + t.Message + "; now close socket");
                socket.Dispose();
                return false;
            }
            finally
            {
                if (copyState != null)
                {
                    Message("top: fetch: now release CopyState");
                    ReleaseCopyState(copyState);
                }
            }

            return true;
        }

        private static readonly FieldType tokenizedWithTermVectors = new FieldType(TextField.TYPE_STORED)
        {
            IndexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
            StoreTermVectorOffsets = true,
            StoreTermVectors = true,
            StoreTermVectorPositions = true,
        };

        private void HandleIndexing(Socket socket, AtomicBoolean stop, NetworkStream @is, DataInput @in, DataOutput @out, BufferedStream bos)
        {
            ThreadJob.CurrentThread.Name = "indexing";
            Message("start handling indexing socket=" + socket);
            while (true)
            {
                while (true)
                {
                    if (@is.DataAvailable)
                    {
                        break;
                    }
                    if (stop)
                    {
                        return;
                    }
                    ThreadJob.Sleep(10);
                }
                byte cmd;
                try
                {
                    cmd = @in.ReadByte();
                }
                catch (EOFException)
                {
                    // done
                    return;
                }
                //Message("INDEXING OP " + cmd);
                if (cmd == CMD_ADD_DOC)
                {
                    HandleAddDocument(@in, @out);
                    @out.WriteByte(1);
                    bos.Flush();
                }
                else if (cmd == CMD_UPDATE_DOC)
                {
                    HandleUpdateDocument(@in, @out);
                    @out.WriteByte(1);
                    bos.Flush();
                }
                else if (cmd == CMD_DELETE_DOC)
                {
                    HandleDeleteDocument(@in, @out);
                    @out.WriteByte(1);
                    bos.Flush();
                }
                else if (cmd == CMD_DELETE_ALL_DOCS)
                {
                    writer.DeleteAll();
                    @out.WriteByte(1);
                    bos.Flush();
                }
                else if (cmd == CMD_FORCE_MERGE)
                {
                    writer.ForceMerge(1);
                    @out.WriteByte(1);
                    bos.Flush();
                }
                else if (cmd == CMD_INDEXING_DONE)
                {
                    @out.WriteByte(1);
                    bos.Flush();
                    break;
                }
                else
                {
                    throw new IllegalArgumentException("cmd must be add, update or delete; got " + cmd);
                }
            }
        }

        private void HandleAddDocument(DataInput @in, DataOutput @out)
        {
            int fieldCount = @in.ReadVInt32();
            Document doc = new Document();
            for (int i = 0; i < fieldCount; i++)
            {
                string name = @in.ReadString();
                string value = @in.ReadString();
                // NOTE: clearly NOT general!
                if (name.equals("docid") || name.equals("marker"))
                {
                    doc.Add(new StringField(name, value, Field.Store.YES));
                }
                else if (name.equals("title"))
                {
                    doc.Add(new StringField("title", value, Field.Store.YES));
                    doc.Add(new Field("titleTokenized", value, tokenizedWithTermVectors));
                }
                else if (name.equals("body"))
                {
                    doc.Add(new Field("body", value, tokenizedWithTermVectors));
                }
                else
                {
                    throw new IllegalArgumentException("unhandled field name " + name);
                }
            }
            writer.AddDocument(doc);
        }

        private void HandleUpdateDocument(DataInput @in, DataOutput @out)
        {
            int fieldCount = @in.ReadVInt32();
            Document doc = new Document();
            string docid = null;
            for (int i = 0; i < fieldCount; i++)
            {
                string name = @in.ReadString();
                string value = @in.ReadString();
                // NOTE: clearly NOT general!
                if (name.equals("docid"))
                {
                    docid = value;
                    doc.Add(new StringField("docid", value, Field.Store.YES));
                }
                else if (name.equals("marker"))
                {
                    doc.Add(new StringField("marker", value, Field.Store.YES));
                }
                else if (name.equals("title"))
                {
                    doc.Add(new StringField("title", value, Field.Store.YES));
                    doc.Add(new Field("titleTokenized", value, tokenizedWithTermVectors));
                }
                else if (name.equals("body"))
                {
                    doc.Add(new Field("body", value, tokenizedWithTermVectors));
                }
                else
                {
                    throw new IllegalArgumentException("unhandled field name " + name);
                }
            }

            writer.UpdateDocument(new Term("docid", docid), doc);
        }

        private void HandleDeleteDocument(DataInput @in, DataOutput @out)
        {
            string docid = @in.ReadString();
            writer.DeleteDocuments(new Term("docid", docid));
        }

        // Sent to primary to cutover new SIS:
        internal const byte CMD_FLUSH = 10;

        // Sent by replica to primary asking to copy a set of files over:
        internal const byte CMD_FETCH_FILES = 1;

        internal const byte CMD_GET_SEARCHING_VERSION = 12;
        internal const byte CMD_SEARCH = 2;
        internal const byte CMD_MARKER_SEARCH = 3;
        internal const byte CMD_COMMIT = 4;
        internal const byte CMD_CLOSE = 5;
        internal const byte CMD_SEARCH_ALL = 21;

        // Send (to primary) the list of currently running replicas:
        internal const byte CMD_SET_REPLICAS = 16;

        // Multiple indexing ops
        internal const byte CMD_INDEXING = 18;

        internal const byte CMD_ADD_DOC = 6;
        internal const byte CMD_UPDATE_DOC = 7;
        internal const byte CMD_DELETE_DOC = 8;
        internal const byte CMD_INDEXING_DONE = 19;
        internal const byte CMD_DELETE_ALL_DOCS = 22;
        internal const byte CMD_FORCE_MERGE = 23;

        // Sent by replica to primary when replica first starts up, so primary can add it to any warming merges:
        internal const byte CMD_NEW_REPLICA = 20;

        internal void HandleOneConnection(Random random, TcpListener ss, AtomicBoolean stop, NetworkStream @is, Socket socket,
            DataInput @in, DataOutput @out, BufferedStream bos)
        {
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
                    if (stop)
                    {
                        return;
                    }
                    ThreadJob.Sleep(10);
                }

                try
                {
                    cmd = @in.ReadByte();
                }
                catch (EOFException)
                {
                    break;
                }

                switch (cmd)
                {
                    case CMD_FLUSH:
                        HandleFlush(@in, @out, bos);
                        break;

                    case CMD_FETCH_FILES:
                        // Replica (other node) is asking us (primary node) for files to copy
                        HandleFetchFiles(random, socket, @in, @out, bos);
                        break;

                    case CMD_INDEXING:
                        HandleIndexing(socket, stop, @is, @in, @out, bos);
                        break;

                    case CMD_GET_SEARCHING_VERSION:
                        @out.WriteVInt64(GetCurrentSearchingVersion());
                        break;

                    case CMD_SEARCH:
                        {
                            ThreadJob.CurrentThread.Name = "search";
                            IndexSearcher searcher = mgr.Acquire();
                            try
                            {
                                long version = ((DirectoryReader)searcher.IndexReader).Version;
                                int hitCount = searcher.Search(new TermQuery(new Term("body", "the")), 1).TotalHits;
                                //Message("version=" + version + " searcher=" + searcher);
                                @out.WriteVInt64(version);
                                @out.WriteVInt32(hitCount);
                                bos.Flush();
                            }
                            finally
                            {
                                mgr.Release(searcher);
                            }
                            bos.Flush();
                        }
                        goto outer;

                    case CMD_SEARCH_ALL:
                        {
                            ThreadJob.CurrentThread.Name = "search all";
                            IndexSearcher searcher = mgr.Acquire();
                            try
                            {
                                long version = ((DirectoryReader)searcher.IndexReader).Version;
                                int hitCount = searcher.Search(new MatchAllDocsQuery(), 1).TotalHits;
                                //Message("version=" + version + " searcher=" + searcher);
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

                    case CMD_MARKER_SEARCH:
                        {
                            ThreadJob.CurrentThread.Name = "msearch";
                            int expectedAtLeastCount = @in.ReadVInt32();
                            VerifyAtLeastMarkerCount(expectedAtLeastCount, @out);
                            bos.Flush();
                        }
                        goto outer;

                    case CMD_COMMIT:
                        ThreadJob.CurrentThread.Name = "commit";
                        Commit();
                        @out.WriteByte((byte)1);
                        break;

                    case CMD_CLOSE:
                        ThreadJob.CurrentThread.Name = "close";
                        Message("top close: now close server socket");
                        ss.Stop();
                        @out.WriteByte((byte)1);
                        Message("top close: done close server socket");
                        break;

                    case CMD_SET_REPLICAS:
                        ThreadJob.CurrentThread.Name = "set repls";
                        int count = @in.ReadVInt32();
                        int[] replicaIDs = new int[count];
                        int[] replicaTCPPorts = new int[count];
                        for (int i = 0; i < count; i++)
                        {
                            replicaIDs[i] = @in.ReadVInt32();
                            replicaTCPPorts[i] = @in.ReadVInt32();
                        }
                        @out.WriteByte((byte)1);
                        SetReplicas(replicaIDs, replicaTCPPorts);
                        break;

                    case CMD_NEW_REPLICA:
                        ThreadJob.CurrentThread.Name = "new repl";
                        int replicaTCPPort = @in.ReadVInt32();
                        Message("new replica: " + warmingSegments.size() + " current warming merges");
                        // Step through all currently warming segments and try to add this replica if it isn't there already:
                        lock (warmingSegments)
                        {
                            foreach (MergePreCopy preCopy in warmingSegments)
                            {
                                Message("warming segment " + preCopy._files.Keys);
                                bool found = false;
                                lock (preCopy.connections)
                                {
                                    foreach (Connection con in preCopy.connections)
                                    {
                                        if (con.destTCPPort.Port == replicaTCPPort)
                                        {
                                            found = true;
                                            break;
                                        }
                                    }
                                }

                                if (found)
                                {
                                    Message("this replica is already warming this segment; skipping");
                                    // It's possible (maybe) that the replica started up, then a merge kicked off, and it warmed to this new replica, all before the
                                    // replica sent us this command:
                                    continue;
                                }

                                // OK, this new replica is not already warming this segment, so attempt (could fail) to start warming now:

                                Connection c = new Connection(replicaTCPPort);
                                if (preCopy.TryAddConnection(c) == false)
                                {
                                    // This can happen, if all other replicas just now finished warming this segment, and so we were just a bit too late.  In this
                                    // case the segment will be copied over in the next nrt point sent to this replica
                                    Message("failed to add connection to segment warmer (too late); closing");
                                    c.Dispose();
                                }
                                c.@out.WriteByte(SimpleReplicaNode.CMD_PRE_COPY_MERGE);
                                c.@out.WriteVInt64(primaryGen);
                                c.@out.WriteVInt32(tcpPort);
                                SimpleServer.WriteFilesMetaData(c.@out, preCopy._files);
                                c.Flush();
                                c.s.Shutdown(SocketShutdown.Both);
                                Message("successfully started warming");
                            }
                        }
                        break;

                    default:
                        throw new IllegalArgumentException("unrecognized cmd=" + cmd + " via socket=" + socket);
                }
                bos.Flush();
                break;
            }
        }

        private void VerifyAtLeastMarkerCount(int expectedAtLeastCount, DataOutput @out)
        {
            IndexSearcher searcher = base.mgr.Acquire();
            try
            {
                long version = ((DirectoryReader)searcher.IndexReader).Version;
                int hitCount = searcher.Search(new TermQuery(new Term("marker", "marker")), 1).TotalHits;

                if (hitCount < expectedAtLeastCount)
                {
                    Message("marker search: expectedAtLeastCount=" + expectedAtLeastCount + " but hitCount=" + hitCount);
                    TopDocs hits = searcher.Search(new TermQuery(new Term("marker", "marker")), expectedAtLeastCount);
                    List<int> seen = new List<int>();
                    foreach (var hit in hits.ScoreDocs)
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
                    throw IllegalStateException.Create("at flush: marker count " + hitCount + " but expected at least " + expectedAtLeastCount + " version=" + version);
                }

                if (@out != null)
                {
                    @out.WriteVInt64(version);
                    @out.WriteVInt32(hitCount);
                }
            }
            finally
            {
                mgr.Release(searcher);
            }
        }
    }
}
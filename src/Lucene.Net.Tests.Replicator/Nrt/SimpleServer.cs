using J2N.Threading;
using J2N.Threading.Atomic;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Store;
using Lucene.Net.Util;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    /** Child process with silly naive TCP socket server to handle
  *  between-node commands, launched for each node  by TestNRTReplication. */
    /*@SuppressCodecs({ "MockRandom", "Memory", "Direct", "SimpleText"})
@SuppressSysoutChecks(bugUrl = "Stuff gets printed, important stuff for debugging a failure")
@SuppressForbidden(reason = "We need Unsafe to actually crush :-)")*/

    public class SimpleServer : LuceneTestCase
    {
        private static readonly ISet<ThreadJob> clientThreads = new HashSet<ThreadJob>();
        private static readonly AtomicBoolean stop = new AtomicBoolean();

        /** Handles one client connection */

        private class ClientHandler : ThreadJob
        {
            // We hold this just so we can close it to exit the process:
            private readonly TcpListener ss;

            private readonly Socket socket;
            private readonly Node node;
            private readonly int bufferSize;

            public ClientHandler(TcpListener ss, Node node, Socket socket)
            {
                this.ss = ss;
                this.node = node;
                this.socket = socket;
                this.bufferSize = TestUtil.NextInt32(Random, 128, 65536);
                if (Node.VERBOSE_CONNECTIONS)
                {
                    node.Message("new connection socket=" + socket);
                }
            }

            public override void Run()
            {
                bool success = false;
                try
                {
                    //node.message("using stream buffer size=" + bufferSize);
                    var @is = new NetworkStream(socket);
                    DataInput @in = new InputStreamDataInput(@is);
                    BufferedStream bos = new BufferedStream(new NetworkStream(socket), bufferSize);
                    DataOutput @out = new OutputStreamDataOutput(bos);

                    if (node is SimplePrimaryNode)
                    {
                        ((SimplePrimaryNode)node).HandleOneConnection(Random, ss, stop, @is, socket, @in, @out, bos);
                    }
                    else
                    {
                        ((SimpleReplicaNode)node).HandleOneConnection(ss, stop, @is, socket, @in, @out, bos);
                    }

                    bos.Flush();
                    if (Node.VERBOSE_CONNECTIONS)
                    {
                        node.Message("bos.flush done");
                    }

                    success = true;
                }
                catch (Exception t)
                {
                    if ((t is SocketException) == false && (t is NodeCommunicationException) == false)
                    {
                        node.Message("unexpected exception handling client connection; now failing test:");
                        t.printStackTrace(Console.Out);
                        ss.Stop();
                        // Test should fail with this:
                        throw RuntimeException.Create(t);
                    }
                    else
                    {
                        node.Message("exception handling client connection; ignoring:");
                        t.printStackTrace(Console.Out);
                    }
                }
                finally
                {
                    if (success)
                    {
                        try
                        {
                            IOUtils.Close(socket);
                        }
                        catch (IOException ioe)
                        {
                            throw RuntimeException.Create(ioe);
                        }
                    }
                    else
                    {
                        IOUtils.CloseWhileHandlingException(socket);
                    }
                }
                if (Node.VERBOSE_CONNECTIONS)
                {
                    node.Message("socket.close done");
                }
            }
        }

        /**
         * currently, this only works/tested on Sun and IBM.
         */

        // poached from TestIndexWriterOnJRECrash ... should we factor out to TestUtil?  seems dangerous to give it such "publicity"?
        /*        private static void crashJRE()
                {
                     string vendor = Constants.JAVA_VENDOR;
                     bool supportsUnsafeNpeDereference =
                        vendor.startsWith("Oracle") ||
                        vendor.startsWith("Sun") ||
                        vendor.startsWith("Apple");

                    try
                    {
                        if (supportsUnsafeNpeDereference)
                        {
                            try
                            {
                                //Class <?> clazz = Class.forName("sun.misc.Unsafe");
                                //java.lang.reflect.Field field = clazz.getDeclaredField("theUnsafe");
                                //field.setAccessible(true);
                                //Object o = field.get(null);
                                //Method m = clazz.getMethod("putAddress", long.class, long.class);
        m.invoke(o, 0L, 0L);
                } catch (Exception e)
        {
            Console.Out.WriteLine("Couldn't kill the JVM via Unsafe.");
            e.printStackTrace(Console.Out);
        }
              }

              // Fallback attempt to Runtime.halt();
              Runtime.getRuntime().halt(-1);
            } catch (Exception e)
        {
            Console.Out.WriteLine("Couldn't kill the JVM.");
            e.printStackTrace(Console.Out);
        }

        // We couldn't get the JVM to crash for some reason.
        throw new RuntimeException("JVM refuses to die!");
          }*/

        internal static void WriteFilesMetaData(DataOutput @out, IDictionary<string, FileMetaData> files)
        {
            @out.WriteVInt32(files.size());
            foreach (KeyValuePair<string, FileMetaData> ent in files.ToHashSet())
            {
                @out.WriteString(ent.Key);

                FileMetaData fmd = ent.Value;
                @out.WriteVInt64(fmd.length);
                @out.WriteVInt64(fmd.checksum);
                @out.WriteVInt32(fmd.header.Length);
                @out.WriteBytes(fmd.header, 0, fmd.header.Length);
                @out.WriteVInt32(fmd.footer.Length);
                @out.WriteBytes(fmd.footer, 0, fmd.footer.Length);
            }
        }

        internal static IDictionary<string, FileMetaData> ReadFilesMetaData(DataInput @in)
        {
            int fileCount = @in.ReadVInt32();
            //Console.@out.WriteLine("readFilesMetaData: fileCount=" + fileCount);
            IDictionary<string, FileMetaData> files = new Dictionary<string, FileMetaData>();
            for (int i = 0; i < fileCount; i++)
            {
                string fileName = @in.ReadString();
                //Console.@out.WriteLine("readFilesMetaData: fileName=" + fileName);
                long length = @in.ReadVInt64();
                long checksum = @in.ReadVInt64();
                byte[] header = new byte[@in.ReadVInt32()];
                @in.ReadBytes(header, 0, header.Length);
                byte[] footer = new byte[@in.ReadVInt32()];
                @in.ReadBytes(footer, 0, footer.Length);
                files.Add(fileName, new FileMetaData(header, footer, length, checksum));
            }
            return files;
        }

        /** Pulls CopyState off the wire */

        internal static CopyState ReadCopyState(DataInput @in)
        {
            // Decode a new CopyState
            byte[]
            infosBytes = new byte[@in.ReadVInt32()];
            @in.ReadBytes(infosBytes, 0, infosBytes.Length);

            long gen = @in.ReadVInt64();
            long version = @in.ReadVInt64();
            IDictionary<string, FileMetaData> files = ReadFilesMetaData(@in);

            int count = @in.ReadVInt32();
            ISet<string> completedMergeFiles = new HashSet<string>();
            for (int i = 0; i < count; i++)
            {
                completedMergeFiles.Add(@in.ReadString());
            }
            long primaryGen = @in.ReadVInt64();

            return new CopyState(files, version, gen, infosBytes, completedMergeFiles, primaryGen, null);
        }

        [Test]
        public void Test()
        {
            int id = int.Parse(SystemProperties.GetProperty("tests:nrtreplication.nodeid"));
            ThreadJob.CurrentThread.Name = ("main child " + id);
            string indexPath = Path.GetDirectoryName(SystemProperties.GetProperty("tests:nrtreplication.indexpath"));
            bool isPrimary = SystemProperties.GetProperty("tests:nrtreplication.isPrimary") != null;
            int primaryTCPPort;
            long forcePrimaryVersion;
            if (isPrimary == false)
            {
                forcePrimaryVersion = -1;
                primaryTCPPort = int.Parse(SystemProperties.GetProperty("tests:nrtreplication.primaryTCPPort"));
            }
            else
            {
                primaryTCPPort = -1;
                forcePrimaryVersion = long.Parse(SystemProperties.GetProperty("tests:nrtreplication.forcePrimaryVersion"));
            }
            long primaryGen = long.Parse(SystemProperties.GetProperty("tests:nrtreplication.primaryGen"));
            Node.globalStartNS = long.Parse(SystemProperties.GetProperty("tests:nrtreplication.startNS"));

            bool doRandomCrash = "true".equals(SystemProperties.GetProperty("tests:nrtreplication.doRandomCrash"));
            bool doRandomClose = "true".equals(SystemProperties.GetProperty("tests:nrtreplication.doRandomClose"));
            bool doFlipBitsDuringCopy = "true".equals(SystemProperties.GetProperty("tests:nrtreplication.doFlipBitsDuringCopy"));
            bool doCheckIndexOnClose = "true".equals(SystemProperties.GetProperty("tests:nrtreplication.checkonclose"));

            // Create server socket that we listen for incoming requests on:
            TcpListener ss = new(System.Net.IPAddress.Any, 80);
            //TODO this is not tested yet and many changes have to be done
            int tcpPort = 80;
            Console.Out.WriteLine("\nPORT: " + tcpPort);
            Node node;
            if (isPrimary)
            {
                node = new SimplePrimaryNode(Random, indexPath, id, tcpPort, primaryGen, forcePrimaryVersion, null, doFlipBitsDuringCopy, doCheckIndexOnClose);
                Console.Out.WriteLine("\nCOMMIT VERSION: " + ((PrimaryNode)node).GetLastCommitVersion());
            }
            else
            {
                try
                {
                    node = new SimpleReplicaNode(Random, id, tcpPort, indexPath, primaryGen, primaryTCPPort, null, doCheckIndexOnClose);
                }
                catch (RuntimeException re)
                {
                    if (re.Message.StartsWith("replica cannot start"))
                    {
                        // this is "OK": it means MDW's refusal to delete a segments_N commit point means we cannot start:
                        // Assert.True(re.Message, false);
                    }
                    throw re;
                }
            }
            Console.Out.WriteLine("\nINFOS VERSION: " + node.GetCurrentSearchingVersion());

            if (doRandomClose || doRandomCrash)
            {
                int waitForMS = isPrimary ? TestUtil.NextInt32(Random, 20000, 60000) : TestUtil.NextInt32(Random, 5000, 60000);
                bool doClose;
                if (doRandomCrash == false)
                {
                    doClose = true;
                }
                else
                {
                    doClose = doRandomClose && Random.nextBoolean();
                }

                if (doClose)
                {
                    node.Message("top: will close after " + (waitForMS / 1000.0) + " seconds");
                }
                else
                {
                    node.Message("top: will crash after " + (waitForMS / 1000.0) + " seconds");
                }

                ThreadJob t = new ThreadJob()
                {
                };

                if (isPrimary)
                {
                    t.Name = ("crasher P" + id);
                }
                else
                {
                    t.Name = ("crasher R" + id);
                }

                // So that if node exits naturally, this thread won't prevent process exit:
                // t.setDaemon(true);
                t.Start();
            }
            Console.Out.WriteLine("\nNODE STARTED");

            //List<Thread> clientThreads = new ArrayList<>();

            // Naive thread-per-connection server:
            while (true)
            {
                Socket socket;
                try
                {
                    socket = ss.AcceptSocket();
                }
                catch (SocketException)
                {
                    // when ClientHandler closes our ss we will hit this
                    node.Message("top: server socket exc; now exit");
                    break;
                }
                ThreadJob thread = new ClientHandler(ss, node, socket);
                // thread.setDaemon(true);
                thread.Start();

                clientThreads.Add(thread);

                // Prune finished client threads:
                var it = clientThreads.GetEnumerator();
                while (it.MoveNext())
                {
                    ThreadJob t = it.MoveNext() ? it.Current : null;
                    if (t.IsAlive == false)
                    {
                        it.Reset();
                    }
                }
                //node.message(clientThreads.size() + " client threads are still alive");
            }

            stop.GetAndSet(true);

            // Make sure all client threads are done, else we get annoying (yet ultimately "harmless") messages about threads still running /
            // lingering for them to finish from the child processes:
            foreach (var clientThread in clientThreads)
            {
                node.Message("top: join clientThread=" + clientThread);
                clientThread.Join();
                node.Message("top: done join clientThread=" + clientThread);
            }
            node.Message("done join all client threads; now close node");
            node.Dispose();
        }
    }
}
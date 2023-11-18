using J2N;
using J2N.Threading;
using J2N.Threading.Atomic;
using Lucene.Net.Documents;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Util;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace Lucene.Net.Tests.Replicator.Nrt
{


    /*"test C:\\Users\\admin\\Projects\\Dotnet Projects local Repo\\lucenenet\\src\\Lucene.Net.Tests.Replicator\\bin\\Debug\\net7.0\\Lucene.Net.Tests.Replicator.dll --framework net7.0 --filter 
     * TestNRTReplication --logger:\"console;verbosity=normal\" -- RunConfiguration.TargetPlatform=x64 TestRunParameters.Parameter(name=\\\"assert\\\", value=\\\"true\\\") 
     * TestRunParameters.Parameter(name=\\\"tests:seed\\\", value=\\\"0x2e6caf7b36b71a2\\\") TestRunParameters.Parameter(name=\\\"tests:culture\\\", value=\\\"ff-Latn\\\") 
     * TestRunParameters.Parameter(name=\\\"tests:nrtreplication.primaryTCPPort\\\", value=\\\"\\\") TestRunParameters.Parameter(name=\\\"tests:nrtreplication.closeorcrash\\\", value=\\\"false\\\") 
     * TestRunParameters.Parameter(name=\\\"tests:nrtreplication.node\\\", value=\\\"true\\\") TestRunParameters.Parameter(name=\\\"tests:nrtreplication.nodeid\\\", value=\\\"0\\\") 
     * TestRunParameters.Parameter(name=\\\"tests:nrtreplication.startNS\\\", value=\\\"269378253865000\\\") 
     * TestRunParameters.Parameter(name=\\\"tests:nrtreplication.indexpath\\\", value=\\\"C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp\\primary-lfvw102g\\\")
    TestRunParameters.Parameter(name=\\\"tests:nrtreplication.checkonclose\\\", value=\\\"true\\\") TestRunParameters.Parameter(name=\\\"tests:nrtreplication.isPrimary\\\", value=\\\"true\\\")
    TestRunParameters.Parameter(name=\\\"tests:nrtreplication.forcePrimaryVersion\\\", value=\\\"-1\\\") TestRunParameters.Parameter(name=\\\"tests:nightly\\\", value=\\\"true\\\")"




    "test C:\\Users\\admin\\Projects\\Dotnet Projects local Repo\\lucenenet\\src\\Lucene.Net.Tests._I-J\\bin\\Debug\\net7.0\\Lucene.Net.Tests._I-J.dll --framework net7.0 --filter
    TestIndexWriterOnJRECrash --logger:\"console;verbosity=normal\" -- RunConfiguration.TargetPlatform=x64 TestRunParameters.Parameter(name=\\\"assert\\\", value=\\\"true\\\")
    TestRunParameters.Parameter(name=\\\"tests:seed\\\", value=\\\"0x678489da15cb62cc\\\") TestRunParameters.Parameter(name=\\\"tests:culture\\\", value=\\\"hsb-DE\\\")
    TestRunParameters.Parameter(name=\\\"tests:crashmode\\\", value=\\\"true\\\") TestRunParameters.Parameter(name=\\\"tests:nightly\\\", value=\\\"true\\\")
    TestRunParameters.Parameter(name=\\\"tempDir\\\", value=\\\"C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp\\netcrash-ftg32jbd\\\")
    TestRunParameters.Parameter(name=\\\"tests:tempProcessToKillFile\\\", value=\\\"C:\\Users\\admin\\AppData\\Local\\Temp\\netcrash-processToKillycbldjkr.txt\\\")"
*/
    // MockRandom's .sd file has no index header/footer:
    /*    @SuppressCodecs({ "MockRandom", "Memory", "Direct", "SimpleText"})
    @SuppressSysoutChecks(bugUrl = "Stuff gets printed, important stuff for debugging a failure")*/
    [SuppressCodecs("MockRandom", "Memory", "Direct", "SimpleText")]
    [TestFixture]
    public class TestNRTReplication : LuceneTestCase
    {

        /** cwd where we start each child (server) node */
        private DirectoryInfo childTempDir;

        readonly AtomicInt64 nodeStartCounter = new AtomicInt64();
        private long nextPrimaryGen;
        private long lastPrimaryGen;
        LineFileDocs docs;

        /** Launches a child "server" (separate JVM), which is either primary or replica node */
        //@SuppressForbidden(reason = "ProcessBuilder requires java.io.File for CWD")
        private NodeProcess StartNode(int primaryTCPPort, int id, DirectoryInfo indexPath, long forcePrimaryVersion, bool willCrash)
        {
            List<string> cmd = new List<string>();

            //get the full location of the assembly with DaoTests in it
            string testAssemblyPath = Assembly.GetAssembly(typeof(TestNRTReplication)).Location;

            //get the folder that's in
            string theDirectory = Path.GetDirectoryName(testAssemblyPath);

            cmd.AddRange(new[]
            {
                    "test", testAssemblyPath,
                    "--framework", GetTargetFramework(),
                    "--filter", nameof(TestNRTReplication),
                    "--logger:\"console;verbosity=normal\"",
                    "--",
                    $"RunConfiguration.TargetPlatform={GetTargetPlatform()}"
            });


            // LUCENENET NOTE: Since in our CI environment we create a lucene.testSettings.config file
            // for all tests, we need to pass some of these settings as test run parameters to override
            // for this process. These are read as system properties on the inside of the application.
            cmd.Add(TestRunParameter("assert", "true"));
            // Mixin our own counter because this is called from a fresh thread which means the seed otherwise isn't changing each time we spawn a
            // new node:
            long seed = Random.NextInt64() * nodeStartCounter.IncrementAndGet();
            cmd.Add(TestRunParameter("tests:seed", SeedUtils.FormatSeed(seed)));
            cmd.Add(TestRunParameter("tests:culture", Thread.CurrentThread.CurrentCulture.Name));
            long myPrimaryGen;
            if (primaryTCPPort != -1)
            {
                // I am a replica
                cmd.Add(TestRunParameter("tests:nrtreplication.primaryTCPPort" ,primaryTCPPort.ToString()));
                myPrimaryGen = lastPrimaryGen;
            }
            else
            {
                myPrimaryGen = nextPrimaryGen++;
                lastPrimaryGen = myPrimaryGen;
            }
            cmd.Add(TestRunParameter("tests:nrtreplication.primaryGen" , myPrimaryGen.ToString()));
            cmd.Add(TestRunParameter("tests:nrtreplication.closeorcrash","false"));

            cmd.Add(TestRunParameter("tests:nrtreplication.node","true"));
            cmd.Add(TestRunParameter("tests:nrtreplication.nodeid" , id.ToString()));
            cmd.Add(TestRunParameter("tests:nrtreplication.startNS" , Node.globalStartNS.ToString()));
            cmd.Add(TestRunParameter("tests:nrtreplication.indexpath" , indexPath.FullName));
            cmd.Add(TestRunParameter("tests:nrtreplication.checkonclose","true"));

            if (primaryTCPPort == -1)
            {
                // We are the primary node
                cmd.Add(TestRunParameter("tests:nrtreplication.isPrimary","true"));
                cmd.Add(TestRunParameter("tests:nrtreplication.forcePrimaryVersion" , forcePrimaryVersion.ToString()));
            }

           
            // passing NIGHTLY to this test makes it run for much longer, easier to catch it in the act...
            cmd.Add(TestRunParameter("tests:nightly", "true"));
           /* cmd.Add("-ea");
            cmd.Add("-cp");
            cmd.Add(SystemProperties.GetProperty("java.class.var"));
            cmd.Add("org.junit.runner.JUnitCore");*/
           // cmd.Add(getClass().getName().replace(getClass().getSimpleName(), "SimpleServer"));


           
            // Set up the process to run the console app
            ProcessStartInfo startInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = string.Join(" ",cmd),
                WorkingDirectory = theDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            
            Message("child process command: " + cmd);

            Process p = Process.Start(startInfo);

            TextReader r;
            try
            {
                r = IOUtils.GetDecodingReader(p.StandardOutput.BaseStream, Encoding.UTF8);
            }
            catch (UnsupportedOperationException uee)
            {
                throw RuntimeException.Create(uee);
            }

            int tcpPort = -1;
            long initCommitVersion = -1;
            long initInfosVersion = -1;
            Regex logTimeStart = new Regex("^[0-9\\.]+s .*");
            bool sawExistingSegmentsFile = false;

            while (true)
            {
                string l = r.ReadLine();
                if (l == null)
                {
                    Message("top: node=" + id + " failed to start");
                    try
                    {
                        p.WaitForExit();
                    }
                    catch (Util.ThreadInterruptedException ie)
                    {
                        throw RuntimeException.Create(ie);
                    }
                    Message("exit value=" + p.ExitCode);
                    Message("top: now fail test replica R" + id + " failed to start");
                    throw RuntimeException.Create("replica R" + id + " failed to start");
                }

                if (logTimeStart.IsMatch(l))
                {
                    // Already a well-formed log output:
                    Console.WriteLine(l);
                }
                else
                {
                    Message(l);
                }

                if (l.StartsWith("PORT: "))
                {
                    tcpPort = int.Parse(l.Substring(6).Trim());
                }
                else if (l.StartsWith("COMMIT VERSION: "))
                {
                    initCommitVersion = int.Parse(l.Substring(16).Trim());
                }
                else if (l.StartsWith("INFOS VERSION: "))
                {
                    initInfosVersion = int.Parse(l.Substring(15).Trim());
                }
                else if (l.Contains("will crash after"))
                {
                    willCrash = true;
                }
                else if (l.StartsWith("NODE STARTED"))
                {
                    break;
                }
                else if (l.Contains("replica cannot start: existing segments file="))
                {
                    sawExistingSegmentsFile = true;
                }
            }

            bool finalWillCrash = willCrash;

            // Baby sits the child process, pulling its stdout and printing to our stdout:
            AtomicBoolean nodeClosing = new AtomicBoolean();
            ThreadJob pumper = ThreadPumper.Start(new ThreadJobAnonymousClass(p, finalWillCrash, id), r, Console.Out, null, nodeClosing);

            pumper.Name = ("pump" + id);

            Message("top: node=" + id + " started at tcpPort=" + tcpPort + " initCommitVersion=" + initCommitVersion + " initInfosVersion=" + initInfosVersion);
            return new NodeProcess(p, id, tcpPort, pumper, primaryTCPPort == -1, initCommitVersion, initInfosVersion, nodeClosing);
        }

        private string GetTargetFramework()
        {
            var targetFrameworkAttribute = GetType().Assembly.GetAttributes<System.Reflection.AssemblyMetadataAttribute>(inherit: false).Where(a => a.Key == "TargetFramework").FirstOrDefault();
            if (targetFrameworkAttribute is null)
                Assert.Fail("TargetFramework metadata not found in this assembly.");
            return targetFrameworkAttribute.Value;
        }

        private string GetTargetPlatform()
        {
            return Environment.Is64BitProcess ? "x64" : "x86";
        }

        private static string TestRunParameter(string name, string value)
        {
            // See: https://github.com/microsoft/vstest/issues/862#issuecomment-621737720
            return $"TestRunParameters.Parameter(name=\\\"{Escape(name)}\\\", value=\\\"{Escape(value)}\\\")";
        }

        private static string Escape(string value)
            => value.Replace(Space, string.Concat(BackSlash, Space));

        private const string BackSlash = "\\";
        private const string Space = " ";

        private sealed class ThreadJobAnonymousClass : ThreadJob
        {
            Process p;
            bool finalWillCrash;
            int id;

            public ThreadJobAnonymousClass(Process proc, bool finalWillCrash, int id)
            {
                p = proc;
                this.finalWillCrash = finalWillCrash;
                this.id = id;
            }

            public override void Run()
            {
                {
                    Message("now wait for process " + p);
                    try
                    {
                        p.WaitForExit();
                    }
                    catch (Exception t)
                    {
                        throw RuntimeException.Create(t);
                    }

                    Message("done wait for process " + p);
                    int exitValue = p.ExitCode;
                    Message("exit value=" + exitValue + " willCrash=" + finalWillCrash);
                    if (exitValue != 0 && finalWillCrash == false)
                    {
                        // should fail test
                        throw RuntimeException.Create("node " + id + " process had unexpected non-zero exit status=" + exitValue);
                    }
                }
            }
        }

        public override void SetUp()
        {
            base.SetUp();
            Node.globalStartNS = Time.NanoTime();
            childTempDir = CreateTempDir("child");
            docs = new LineFileDocs(Random);
        }


        public override void TearDown()
        {
            base.TearDown();
            docs.Dispose();
        }

        //@Nightly
        [Test]
        public void TestReplicateDeleteAllDocuments()
        {

            var primaryPath = CreateTempDir("primary");
            NodeProcess primary = StartNode(-1, 0, primaryPath, -1, false);

            var replicaPath = CreateTempDir("replica");
            NodeProcess replica = StartNode(primary.tcpPort, 1, replicaPath, -1, false);

            // Tell primary current replicas:
            SendReplicasToPrimary(primary, replica);

            // Index 10 docs into primary:
            LineFileDocs docs = new LineFileDocs(Random);
            Connection primaryC = new Connection(primary.tcpPort);
            primaryC.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
            for (int i = 0; i < 10; i++)
            {
                Document doc = docs.NextDoc();
                primary.AddOrUpdateDocument(primaryC, doc, false);
            }

            // Nothing in replica index yet
            AssertVersionAndHits(replica, 0, 0);

            // Refresh primary, which also pushes to replica:
            long primaryVersion1 = primary.Flush(0);
            assertTrue(primaryVersion1 > 0);

            // Wait for replica to show the change
            WaitForVersionAndHits(replica, primaryVersion1, 10);

            // Delete all docs from primary
            if (Random.nextBoolean())
            {
                // Inefficiently:
                for (int id = 0; id < 10; id++)
                {
                    primary.DeleteDocument(primaryC, J2N.Numerics.Int32.ToString(id));
                }
            }
            else
            {
                // Efficiently:
                primary.DeleteAllDocuments(primaryC);
            }

            // Replica still shows 10 docs:
            AssertVersionAndHits(replica, primaryVersion1, 10);

            // Refresh primary, which also pushes to replica:
            long primaryVersion2 = primary.Flush(0);
            assertTrue(primaryVersion2 > primaryVersion1);

            // Wait for replica to show the change
            WaitForVersionAndHits(replica, primaryVersion2, 0);

            // Index 10 docs again:
            for (int i = 0; i < 10; i++)
            {
                Document doc = docs.NextDoc();
                primary.AddOrUpdateDocument(primaryC, doc, false);
            }

            // Refresh primary, which also pushes to replica:
            long primaryVersion3 = primary.Flush(0);
            assertTrue(primaryVersion3 > primaryVersion2);

            // Wait for replica to show the change
            WaitForVersionAndHits(replica, primaryVersion3, 10);

            primaryC.Dispose();

            replica.Dispose();
            primary.Dispose();
        }

        //@Nightly
        [Test]
        public void TestReplicateForceMerge()
        {

            var primaryPath = CreateTempDir("primary");
            NodeProcess primary = StartNode(-1, 0, primaryPath, -1, false);

            var replicaPath = CreateTempDir("replica");
            NodeProcess replica = StartNode(primary.tcpPort, 1, replicaPath, -1, false);

            SendReplicasToPrimary(primary, replica);

            // Index 10 docs into primary:
            LineFileDocs docs = new LineFileDocs(Random);
            Connection primaryC = new Connection(primary.tcpPort);
            primaryC.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
            for (int i = 0; i < 10; i++)
            {
                Document doc = docs.NextDoc();
                primary.AddOrUpdateDocument(primaryC, doc, false);
            }

            // Refresh primary, which also pushes to replica:
            long primaryVersion1 = primary.Flush(0);
            assertTrue(primaryVersion1 > 0);

            // Index 10 more docs into primary:
            for (int i = 0; i < 10; i++)
            {
                Document doc = docs.NextDoc();
                primary.AddOrUpdateDocument(primaryC, doc, false);
            }

            // Refresh primary, which also pushes to replica:
            long primaryVersion2 = primary.Flush(0);
            assertTrue(primaryVersion2 > primaryVersion1);

            primary.ForceMerge(primaryC);

            // Refresh primary, which also pushes to replica:
            long primaryVersion3 = primary.Flush(0);
            assertTrue(primaryVersion3 > primaryVersion2);

            // Wait for replica to show the change
            WaitForVersionAndHits(replica, primaryVersion3, 20);

            primaryC.Dispose();

            replica.Dispose();
            primary.Dispose();
        }

        // Start up, index 10 docs, replicate, but crash and restart the replica without committing it:
        //@Nightly
        [Test]
        public void TestReplicaCrashNoCommit()
        {

            var primaryPath = CreateTempDir("primary");
            NodeProcess primary = StartNode(-1, 0, primaryPath, -1, false);

            var replicaPath = CreateTempDir("replica");
            NodeProcess replica = StartNode(primary.tcpPort, 1, replicaPath, -1, true);

            SendReplicasToPrimary(primary, replica);

            // Index 10 docs into primary:
            LineFileDocs docs = new LineFileDocs(Random);
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Refresh primary, which also pushes to replica:
            long primaryVersion1 = primary.Flush(0);
            assertTrue(primaryVersion1 > 0);

            // Wait for replica to sync up:
            WaitForVersionAndHits(replica, primaryVersion1, 10);

            // Crash replica:
            replica.Crash();

            // Restart replica:
            replica = StartNode(primary.tcpPort, 1, replicaPath, -1, false);

            // On startup the replica searches the last commit (empty here):
            AssertVersionAndHits(replica, 0, 0);

            // Ask replica to sync:
            replica.NewNRTPoint(primaryVersion1, 0, primary.tcpPort);
            WaitForVersionAndHits(replica, primaryVersion1, 10);

            replica.Dispose();
            primary.Dispose();
        }

        // Start up, index 10 docs, replicate, commit, crash and restart the replica
        //@Nightly
        [Test]
        public void TestReplicaCrashWithCommit()
        {

            var primaryPath = CreateTempDir("primary");
            NodeProcess primary = StartNode(-1, 0, primaryPath, -1, false);

            var replicaPath = CreateTempDir("replica");
            NodeProcess replica = StartNode(primary.tcpPort, 1, replicaPath, -1, true);

            SendReplicasToPrimary(primary, replica);

            // Index 10 docs into primary:
            LineFileDocs docs = new LineFileDocs(Random);
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Refresh primary, which also pushes to replica:
            long primaryVersion1 = primary.Flush(0);
            assertTrue(primaryVersion1 > 0);

            // Wait for replica to sync up:
            WaitForVersionAndHits(replica, primaryVersion1, 10);

            // Commit and crash replica:
            replica.Commit();
            replica.Crash();

            // Restart replica:
            replica = StartNode(primary.tcpPort, 1, replicaPath, -1, false);

            // On startup the replica searches the last commit:
            AssertVersionAndHits(replica, primaryVersion1, 10);

            replica.Dispose();
            primary.Dispose();
        }

        // Start up, index 10 docs, replicate, commit, crash, index more docs, replicate, then restart the replica
        //@Nightly
        [Test]
        public void TestIndexingWhileReplicaIsDown()
        {

            var primaryPath = CreateTempDir("primary");
            NodeProcess primary = StartNode(-1, 0, primaryPath, -1, false);

            var replicaPath = CreateTempDir("replica");
            NodeProcess replica = StartNode(primary.tcpPort, 1, replicaPath, -1, true);

            SendReplicasToPrimary(primary, replica);

            // Index 10 docs into primary:
            LineFileDocs docs = new LineFileDocs(Random);
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Refresh primary, which also pushes to replica:
            long primaryVersion1 = primary.Flush(0);
            assertTrue(primaryVersion1 > 0);

            // Wait for replica to sync up:
            WaitForVersionAndHits(replica, primaryVersion1, 10);

            // Commit and crash replica:
            replica.Commit();
            replica.Crash();

            SendReplicasToPrimary(primary);

            // Index 10 more docs, while replica is down
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // And flush:
            long primaryVersion2 = primary.Flush(0);
            assertTrue(primaryVersion2 > primaryVersion1);

            // Now restart replica:
            replica = StartNode(primary.tcpPort, 1, replicaPath, -1, false);

            SendReplicasToPrimary(primary, replica);

            // On startup the replica still searches its last commit:
            AssertVersionAndHits(replica, primaryVersion1, 10);

            // Now ask replica to sync:
            replica.NewNRTPoint(primaryVersion2, 0, primary.tcpPort);

            WaitForVersionAndHits(replica, primaryVersion2, 20);

            replica.Dispose();
            primary.Dispose();
        }

        // Crash primary and promote a replica
        //@Nightly
        [Test]
        public void TestCrashPrimary1()
        {

            var path1 = CreateTempDir("1");
            NodeProcess primary = StartNode(-1, 0, path1, -1, true);

            var path2 = CreateTempDir("2");
            NodeProcess replica = StartNode(primary.tcpPort, 1, path2, -1, true);

            SendReplicasToPrimary(primary, replica);

            // Index 10 docs into primary:
            LineFileDocs docs = new LineFileDocs(Random);
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Refresh primary, which also pushes to replica:
            long primaryVersion1 = primary.Flush(0);
            assertTrue(primaryVersion1 > 0);

            // Wait for replica to sync up:
            WaitForVersionAndHits(replica, primaryVersion1, 10);

            // Crash primary:
            primary.Crash();

            // Promote replica:
            replica.Commit();
            replica.Dispose();

            primary = StartNode(-1, 1, path2, -1, false);

            // Should still see 10 docs:
            AssertVersionAndHits(primary, primaryVersion1, 10);

            primary.Dispose();
        }

        // Crash primary and then restart it
        //@Nightly
        [Test]
        public void TestCrashPrimary2()
        {

            var path1 = CreateTempDir("1");
            NodeProcess primary = StartNode(-1, 0, path1, -1, true);

            var path2 = CreateTempDir("2");
            NodeProcess replica = StartNode(primary.tcpPort, 1, path2, -1, true);

            SendReplicasToPrimary(primary, replica);

            // Index 10 docs into primary:
            LineFileDocs docs = new LineFileDocs(Random);
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Refresh primary, which also pushes to replica:
            long primaryVersion1 = primary.Flush(0);
            assertTrue(primaryVersion1 > 0);

            // Wait for replica to sync up:
            WaitForVersionAndHits(replica, primaryVersion1, 10);

            primary.Commit();

            // Index 10 docs, but crash before replicating or committing:
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Crash primary:
            primary.Crash();

            // Restart it:
            primary = StartNode(-1, 0, path1, -1, true);

            SendReplicasToPrimary(primary, replica);

            // Index 10 more docs
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            long primaryVersion2 = primary.Flush(0);
            assertTrue(primaryVersion2 > primaryVersion1);

            // Wait for replica to sync up:
            WaitForVersionAndHits(replica, primaryVersion2, 20);

            primary.Dispose();
            replica.Dispose();
        }

        // Crash primary and then restart it, while a replica node is down, then bring replica node back up and make sure it properly "unforks" itself
        //@Nightly
        [Test]
        public void TestCrashPrimary3()
        {

            var path1 = CreateTempDir("1");
            NodeProcess primary = StartNode(-1, 0, path1, -1, true);

            var path2 = CreateTempDir("2");
            NodeProcess replica = StartNode(primary.tcpPort, 1, path2, -1, true);

            SendReplicasToPrimary(primary, replica);

            // Index 10 docs into primary:
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Refresh primary, which also pushes to replica:
            long primaryVersion1 = primary.Flush(0);
            assertTrue(primaryVersion1 > 0);

            // Wait for replica to sync up:
            WaitForVersionAndHits(replica, primaryVersion1, 10);

            replica.Commit();

            replica.Dispose();
            primary.Crash();

            // At this point replica is "in the future": it has 10 docs committed, but the primary crashed before committing so it has 0 docs

            // Restart primary:
            primary = StartNode(-1, 0, path1, -1, true);

            // Index 20 docs into primary:
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 20; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Flush primary, but there are no replicas to sync to:
            long primaryVersion2 = primary.Flush(0);

            // Now restart replica, which on init should detect on a "lost branch" because its 10 docs that were committed came from a different
            // primary node:
            replica = StartNode(primary.tcpPort, 1, path2, -1, true);

            AssertVersionAndHits(replica, primaryVersion2, 20);

            primary.Dispose();
            replica.Dispose();
        }

        //@Nightly
        [Test]
        public void TestCrashPrimaryWhileCopying()
        {

            var path1 = CreateTempDir("1");
            NodeProcess primary = StartNode(-1, 0, path1, -1, true);

            var path2 = CreateTempDir("2");
            NodeProcess replica = StartNode(primary.tcpPort, 1, path2, -1, true);

            SendReplicasToPrimary(primary, replica);

            // Index 100 docs into primary:
            LineFileDocs docs = new LineFileDocs(Random);
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 100; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Refresh primary, which also pushes (async) to replica:
            long primaryVersion1 = primary.Flush(0);
            assertTrue(primaryVersion1 > 0);

            ThreadJob.Sleep(TestUtil.NextInt32(Random, 1, 30));

            // Crash primary, likely/hopefully while replica is still copying
            primary.Crash();

            // Could see either 100 docs (replica finished before crash) or 0 docs:
            using (Connection c = new Connection(replica.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_SEARCH_ALL);
                c.Flush();
                long version = c.@in.ReadVInt64();
                int hitCount = c.@in.ReadVInt32();
                if (version == 0)
                {
                    assertEquals(0, hitCount);
                }
                else
                {
                    assertEquals(primaryVersion1, version);
                    assertEquals(100, hitCount);
                }
            }

            primary.Dispose();
            replica.Dispose();
        }

        [Test]
        public void TestCrashReplica()
        {

            var path1 = CreateTempDir("1");
            NodeProcess primary = StartNode(-1, 0, path1, -1, true);

            var path2 = CreateTempDir("2");
            NodeProcess replica = StartNode(primary.tcpPort, 1, path2, -1, true);

            SendReplicasToPrimary(primary, replica);

            // Index 10 docs into primary:
            LineFileDocs docs = new LineFileDocs(Random);
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Refresh primary, which also pushes to replica:
            long primaryVersion1 = primary.Flush(0);
            assertTrue(primaryVersion1 > 0);

            // Wait for replica to sync up:
            WaitForVersionAndHits(replica, primaryVersion1, 10);

            // Crash replica
            replica.Crash();

            SendReplicasToPrimary(primary);

            // Lots of new flushes while replica is down:
            long primaryVersion2 = 0;
            for (int iter = 0; iter < 10; iter++)
            {
                // Index 10 docs into primary:
                using (Connection c = new Connection(primary.tcpPort))
                {
                    c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                    for (int i = 0; i < 10; i++)
                    {
                        Document doc = docs.NextDoc();
                        primary.AddOrUpdateDocument(c, doc, false);
                    }
                }
                primaryVersion2 = primary.Flush(0);
            }

            // Start up replica again:
            replica = StartNode(primary.tcpPort, 1, path2, -1, true);

            SendReplicasToPrimary(primary, replica);

            // Now ask replica to sync:
            replica.NewNRTPoint(primaryVersion2, 0, primary.tcpPort);

            // Make sure it sees all docs that were indexed while it was down:
            AssertVersionAndHits(primary, primaryVersion2, 110);

            replica.Dispose();
            primary.Dispose();
        }

        //@Nightly
        [Test]
        public void TestFullClusterCrash()
        {

            var path1 = CreateTempDir("1");
            NodeProcess primary = StartNode(-1, 0, path1, -1, true);

            var path2 = CreateTempDir("2");
            NodeProcess replica1 = StartNode(primary.tcpPort, 1, path2, -1, true);

            var path3 = CreateTempDir("3");
            NodeProcess replica2 = StartNode(primary.tcpPort, 2, path3, -1, true);

            SendReplicasToPrimary(primary, replica1, replica2);

            // Index 50 docs into primary:
            LineFileDocs docs = new LineFileDocs(Random);
            long primaryVersion1 = 0;
            for (int iter = 0; iter < 5; iter++)
            {
                using (Connection c = new Connection(primary.tcpPort))
                {
                    c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                    for (int i = 0; i < 10; i++)
                    {
                        Document doc = docs.NextDoc();
                        primary.AddOrUpdateDocument(c, doc, false);
                    }
                }

                // Refresh primary, which also pushes to replicas:
                primaryVersion1 = primary.Flush(0);
                assertTrue(primaryVersion1 > 0);
            }

            // Wait for replicas to sync up:
            WaitForVersionAndHits(replica1, primaryVersion1, 50);
            WaitForVersionAndHits(replica2, primaryVersion1, 50);

            primary.Commit();
            replica1.Commit();
            replica2.Commit();

            // Index 10 more docs, but don't sync to replicas:
            using (Connection c = new Connection(primary.tcpPort))
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
                for (int i = 0; i < 10; i++)
                {
                    Document doc = docs.NextDoc();
                    primary.AddOrUpdateDocument(c, doc, false);
                }
            }

            // Full cluster crash
            primary.Crash();
            replica1.Crash();
            replica2.Crash();

            // Full cluster restart
            primary = StartNode(-1, 0, path1, -1, true);
            replica1 = StartNode(primary.tcpPort, 1, path2, -1, true);
            replica2 = StartNode(primary.tcpPort, 2, path3, -1, true);

            // Only 50 because we didn't commit primary before the crash:

            // It's -1 because it's unpredictable how IW changes segments version on init:
            AssertVersionAndHits(primary, -1, 50);
            AssertVersionAndHits(replica1, primary.initInfosVersion, 50);
            AssertVersionAndHits(replica2, primary.initInfosVersion, 50);

            primary.Dispose();
            replica1.Dispose();
            replica2.Dispose();
        }

        /** Tell primary current replicas. */
        private void SendReplicasToPrimary(NodeProcess primary, params NodeProcess[] replicas)
        {
            using Connection c = new Connection(primary.tcpPort);
            c.@out.WriteByte(SimplePrimaryNode.CMD_SET_REPLICAS);
            c.@out.WriteVInt32(replicas.Length);
            for (int id = 0; id < replicas.Length; id++)
            {
                NodeProcess replica = replicas[id];
                c.@out.WriteVInt32(replica.id);
                c.@out.WriteVInt32(replica.tcpPort);
            }
            c.Flush();
            c.@in.ReadByte();
        }

        /** Verifies this node is currently searching the specified version with the specified total hit count, or that it eventually does when
         *  keepTrying is true. */
        private void AssertVersionAndHits(NodeProcess node, long expectedVersion, int expectedHitCount)
        {
            using Connection c = new Connection(node.tcpPort);
            c.@out.WriteByte(SimplePrimaryNode.CMD_SEARCH_ALL);
            c.Flush();
            long version = c.@in.ReadVInt64();
            int hitCount = c.@in.ReadVInt32();
            if (expectedVersion != -1)
            {
                assertEquals("wrong searcher version, with hitCount=" + hitCount, expectedVersion, version);
            }
            assertEquals(expectedHitCount, hitCount);
        }

        private void WaitForVersionAndHits(NodeProcess node, long expectedVersion, int expectedHitCount)
        {
            using Connection c = new Connection(node.tcpPort);
            while (true)
            {
                c.@out.WriteByte(SimplePrimaryNode.CMD_SEARCH_ALL);
                c.Flush();
                long version = c.@in.ReadVInt64();
                int hitCount = c.@in.ReadVInt32();

                if (version == expectedVersion)
                {
                    assertEquals(expectedHitCount, hitCount);
                    break;
                }

                assertTrue(version < expectedVersion);
                ThreadJob.Sleep(10);
            }
        }

        static void Message(string message)
        {
            long now = Time.NanoTime();
            Console.WriteLine(string.Format(
                                             "%5.3fs       :     parent [%11s] %s",
                                             (now - Node.globalStartNS) / 1000000000,
                                             ThreadJob.CurrentThread.Name,
                                             message));
        }
    }
}
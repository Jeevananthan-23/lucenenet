using J2N;
using J2N.Threading;
using J2N.Threading.Atomic;
using Lucene.Net.Configuration;
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
using System.Text.RegularExpressions;
using System.Threading;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    /*
     * 
     * 
     * 
     * 
     * 
     * 
     * "Microsoft (R) Test Execution Command Line Tool Version 17.4.0 (x64)\r\nCopyright (c) Microsoft Corporation.  All rights reserved.\r\n\r\nStarting test execution, please wait...\r\nA total of 1 test files matched the specified pattern.\r\nNUnit Adapter 3.17.0.0: Test execution started\r\nRunning all tests in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Tests.Replicator\\bin\\Debug\\net7.0\\Lucene.Net.Tests.Replicator.dll\r\n   NUnit3TestExecutor discovered 68 of 68 NUnit test cases\r\nRandomSeed: 0xda6967b17112454b\r\nCulture: mgh-MZ\r\nTime Zone: (UTC-09:00) Coordinated Universal Time-09\r\nDefault Codec: CheapBastard (CheapBastardCodec)\r\nDefault Similarity: DefaultSimilarity\r\nNightly: False\r\nWeekly: False\r\nSlow: True\r\nAwaits Fix: False\r\nDirectory: random\r\nVerbose: True\r\nRandom Multiplier: 1\r\n\r\nPORT: 0\r\n%5.3fs %5.1fs:        
     * N%d [%11s] %s\r\nIFD 1 [29/11/2023 17:26:17; NonParallelWorker]: init: current segments file is \"\"; deletionPolicy=Lucene.Net.Index.KeepOnlyLastCommitDeletionPolicy\r\nIFD 1 [29/11/2023 17:26:17; NonParallelWorker]: now checkpoint \"\" [0 segments ; isCommit = False]\r\nIFD 1 [29/11/2023 17:26:17; NonParallelWorker]: 1 msec to checkpoint\r\nIW 1 [29/11/2023 17:26:17; NonParallelWorker]: init: create=True\r\nIW 1 [29/11/2023 17:26:17; NonParallelWorker]: \r
     * \ndir=MockDirectoryWrapper(NIOFSDirectory@C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-og1h4m4n lockFactory=NativeFSLockFactory@C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-og1h4m4n)\r\nindex=\r\nversion=4.8.0\r\nmatchVersion=LUCENE_48\r\nanalyzer=MockAnalyzer\r\nramBufferSizeMB=16\r\nmaxBufferedDocs=719\r\nmaxBufferedDeleteTerms=-1\r\nmergedSegmentWarmer=\r\nreaderTermsIndexDivisor=4\r\ntermIndexInterval=81\r\ndelPolicy=KeepOnlyLastCommitDeletionPolicy\r\ncommit=null\r\nopenMode=CREATE_OR_APPEND\r\nsimilarity=DefaultSimilarity\r\nmergeScheduler=Lucene.Net.Index.SerialMergeScheduler\r\ndefault WRITE_LOCK_TIMEOUT=1000\r\nwriteLockTimeout=1000\r\ncodec=CheapBastard\r\ninfoStream=ThreadNameFixingPrintStreamInfoStream\r\nmergePolicy=[LogByteSizeMergePolicy: minMergeSize=1677721, mergeFactor=3, maxMergeSize=2147483648, maxMergeSizeForForcedMerge=9223372036854775807, calibrateSizeByDeletes=False, maxMergeDocs=2147483647, maxCFSSegmentSizeMB=8796093022207,999, noCFSRatio=0]\r\nindexerThreadPool=Lucene.Net.Index.DocumentsWriterPerThreadPool\r\nreaderPooling=False\r\nperThreadHardLimitMB=1945\r\nuseCompoundFile=False\r\ncheckIntegrityAtMerge=False\r\n\r\n%5.3fs %5.1fs: %7s %2s [%11s] %s\r\n%5.3fs %5.1fs: %7s %2s [%11s] %s\r\n%5.3fs %5.1fs: %7s %2s [%11s] %s\r\n%5.3fs %5.1fs: %7s %2s [%11s] %s\r\n%5.3fs %5.1fs: %7s %2s [%11s] %s\r\n   at System.Collections.Generic.Dictionary`2.get_Item(TKey key)\r\n   at Lucene.Net.Replicator.Nrt.PrimaryNode..ctor(IndexWriter writer, Int32 id, Int64 primaryGen, Int64 forcePrimaryVersion, SearcherFactory searcherFactory, TextWriter printStream) in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Replicator\\Nrt\\PrimaryNode.cs:line 88\r\nWARNING: Leftover undeleted temporary files Could not remove the following files (in the order of attempts):\r\n 
     * C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-og1h4m4n\\write.lock\r\n   C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-og1h4m4n\r\n\r\nNUnit Adapter 3.17.0.0: Test execution complete\r\n  Failed Test [565 ms]\r\n 
     * Error Message:\r\n   Lucene.Net.Util.LuceneSystemException : The given key '__version' was not present in the dictionary.\r\n  ----> System.Collections.Generic.KeyNotFoundException : 
     * The given key '__version' was not present in the dictionary.\r\n\r\nTo reproduce this test result:\r\n\r\nOption 1:\r\n\r\n 
     * Apply the following assembly-level attributes:\r\n\r\n[assembly: Lucene.Net.Util.RandomSeed(\"0xda6967b17112454b\")]\r\n[assembly: NUnit.Framework.SetCulture(\"mgh-MZ\")]\r\n\r\nOption 2:\r\n\r\n Use the following .runsettings file:\r\n\r\n<RunSettings>\r\n  <TestRunParameters>\r\n    
     * <Parameter name=\"tests:seed\" value=\"0xda6967b17112454b\" />\r\n    <Parameter name=\"tests:culture\" value=\"mgh-MZ\" />\r\n 
     * </TestRunParameters>\r\n</RunSettings>\r\n\r\nSee the .runsettings documentation at: https://docs.microsoft.com/en-us/visualstudio/test/configure-unit-tests-by-using-a-dot-runsettings-file.\r\n  
     * Stack Trace:\r\n     at Lucene.Net.Replicator.Nrt.PrimaryNode..ctor(IndexWriter writer, Int32 id, Int64 primaryGen, Int64 forcePrimaryVersion, SearcherFactory searcherFactory, TextWriter printStream) 
     * in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Replicator\\Nrt\\PrimaryNode.cs:line 126\r\n   
     * at Lucene.Net.Tests.Replicator.Nrt.SimplePrimaryNode..ctor(Random random, String indexPath, Int32 id, Int32 tcpPort, Int64 primaryGen, Int64 forcePrimaryVersion, SearcherFactory searcherFactory, Boolean doFlipBitsDuringCopy, Boolean doCheckIndexOnClose) 
     * in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Tests.Replicator\\Nrt\\SimplePrimaryNode.cs:line 98\r\n  
     * at Lucene.Net.Tests.Replicator.Nrt.SimpleServer.Test() in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Tests.Replicator\\Nrt\\SimpleServer.cs:line 259\r\n   
     * at System.RuntimeMethodHandle.InvokeMethod(Object target, Void** arguments, Signature sig, Boolean isConstructor)\r\n  
     * at System.Reflection.MethodInvoker.Invoke(Object obj, IntPtr* args, BindingFlags invokeAttr)\r\n--KeyNotFoundException\r\n   
     * at System.Collections.Generic.Dictionary`2.get_Item(TKey key)\r\n   at Lucene.Net.Replicator.Nrt.PrimaryNode..ctor(IndexWriter writer, Int32 id, Int64 primaryGen, Int64 forcePrimaryVersion, SearcherFactory searcherFactory, TextWriter printStream) 
     * in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Replicator\\Nrt\\PrimaryNode.cs:line 88\r\n  Standard Output Messages:\r\n RandomSeed: 0xda6967b17112454b\r\n Culture: mgh-MZ\r\n Time Zone: (UTC-09:00) Coordinated Universal Time-09\r\n 
     * Default Codec: CheapBastard (CheapBastardCodec)\r\n Default Similarity: DefaultSimilarity\r\n Nightly: False\r\n Weekly: False\r\n Slow: True\r\n Awaits Fix: False\r\n Directory: random\r\n Verbose: True\r\n Random Multiplier: 1\r\n \r\n PORT: 0\r\n %5.3fs %5.1fs:         
     * N%d [%11s] %s\r\n IFD 1 [29/11/2023 17:26:17; NonParallelWorker]: init: current segments file is \"\"; deletionPolicy=Lucene.Net.Index.KeepOnlyLastCommitDeletionPolicy\r\n IFD 1 [29/11/2023 17:26:17; NonParallelWorker]: now checkpoint \"\" [0 segments ; isCommit = False]\r\n IFD 1 [29/11/2023 17:26:17; NonParallelWorker]: 1 msec to checkpoint\r\n IW 1 [29/11/2023 17:26:17; NonParallelWorker]: init: create=True\r\n IW 1 [29/11/2023 17:26:17; NonParallelWorker]: \r\n 
     * dir=MockDirectoryWrapper(NIOFSDirectory@C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-og1h4m4n lockFactory=NativeFSLockFactory@C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-og1h4m4n)\r\n index=\r\n version=4.8.0\r\n
     * matchVersion=LUCENE_48\r\n analyzer=MockAnalyzer\r\n ramBufferSizeMB=16\r\n maxBufferedDocs=719\r\n maxBufferedDeleteTerms=-1\r\n mergedSegmentWarmer=\r\n readerTermsIndexDivisor=4\r\n termIndexInterval=81\r\n delPolicy=KeepOnlyLastCommitDeletionPolicy\r\n commit=null\r\n 
     * openMode=CREATE_OR_APPEND\r\n similarity=DefaultSimilarity\r\n mergeScheduler=Lucene.Net.Index.SerialMergeScheduler\r\n default WRITE_LOCK_TIMEOUT=1000\r\n writeLockTimeout=1000\r\n codec=CheapBastard\r\n infoStream=ThreadNameFixingPrintStreamInfoStream\r\n
     * mergePolicy=[LogByteSizeMergePolicy: minMergeSize=1677721, mergeFactor=3, maxMergeSize=2147483648, maxMergeSizeForForcedMerge=9223372036854775807, calibrateSizeByDeletes=False, maxMergeDocs=2147483647, maxCFSSegmentSizeMB=8796093022207,999, noCFSRatio=0]\r\n 
     * indexerThreadPool=Lucene.Net.Index.DocumentsWriterPerThreadPool\r\n readerPooling=False\r\n perThreadHardLimitMB=1945\r\n useCompoundFile=False\r\n checkIntegrityAtMerge=False\r\n 
     * \r\n %5.3fs %5.1fs: %7s %2s [%11s] %s\r\n %5.3fs %5.1fs: %7s %2s [%11s] %s\r\n %5.3fs %5.1fs: %7s %2s [%11s] %s\r\n %5.3fs %5.1fs: %7s %2s [%11s] %s\r\n %5.3fs %5.1fs: %7s %2s [%11s] %s\r\n   
     * at System.Collections.Generic.Dictionary`2.get_Item(TKey key)\r\n   
     * at Lucene.Net.Replicator.Nrt.PrimaryNode..ctor(IndexWriter writer, Int32 id, Int64 primaryGen, Int64 forcePrimaryVersion, SearcherFactory searcherFactory, TextWriter printStream) in 
     * C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Replicator\\Nrt\\PrimaryNode.cs:line 88\r\n\r\n\r\nTotal tests: 1\r\n    
     * Failed: 1\r\n Total time: 1.8456 Seconds\r\n"
     * 
     * "Microsoft (R) Test Execution Command Line Tool Version 17.4.0 (x64)\r\nCopyright (c) Microsoft Corporation.  All rights reserved.\r\n\r\nStarting test execution, please wait...\r\nA total of 1 test files matched the specified pattern.\r\nNUnit Adapter 3.17.0.0: Test execution started\r\nRunning all tests in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Tests.Replicator\\bin\\Debug\\net7.0\\Lucene.Net.Tests.Replicator.dll\r\n   NUnit3TestExecutor discovered 68 of 68 NUnit test cases\r\nRandomSeed: 0x26cf1b4cef7d5c27\r\nCulture: sq-AL\r\nTime Zone: (UTC+02:00) Harare, Pretoria\r\nDefault Codec: Lucene46 (RandomCodec)\r\nDefault Similarity: DefaultSimilarity\r\nNightly: False\r\nWeekly: False\r\nSlow: True\r\nAwaits Fix: False\r\nDirectory: random\r\nVerbose: True\r\nRandom Multiplier: 1\r\n\r\nPORT: 0\r\n%5.3fs %5.1fs:         N%d [%11s] %s\r\nIFD 1 [1.12.2023 3:30:44 e pasdites; NonParallelWorker]: init: current segments file is \"\"; deletionPolicy=Lucene.Net.Index.KeepOnlyLastCommitDeletionPolicy\r\nIFD 1 [1.12.2023 3:30:44 e pasdites; NonParallelWorker]: now checkpoint \"\" [0 segments ; isCommit = False]\r\nIFD 1 [1.12.2023 3:30:44 e pasdites; NonParallelWorker]: 1 msec to checkpoint\r\nIW 1 [1.12.2023 3:30:44 e pasdites; NonParallelWorker]: init: create=True\r\nIW 1 [1.12.2023 3:30:44 e pasdites; NonParallelWorker]: \r\ndir=MockDirectoryWrapper(MMapDirectory@C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-iqfzvfc2 lockFactory=NativeFSLockFactory@C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-iqfzvfc2)\r\nindex=\r\nversion=4.8.0\r\nmatchVersion=LUCENE_48\r\nanalyzer=MockAnalyzer\r\nramBufferSizeMB=16\r\nmaxBufferedDocs=-1\r\nmaxBufferedDeleteTerms=-1\r\nmergedSegmentWarmer=\r\nreaderTermsIndexDivisor=2\r\ntermIndexInterval=32\r\ndelPolicy=KeepOnlyLastCommitDeletionPolicy\r\ncommit=null\r\nopenMode=CREATE_OR_APPEND\r\nsimilarity=DefaultSimilarity\r\nmergeScheduler=ConcurrentMergeScheduler: maxThreadCount=1, maxMergeCount=2, mergeThreadPriority=-1\r\ndefault WRITE_LOCK_TIMEOUT=1000\r\nwriteLockTimeout=1000\r\ncodec=Lucene46: {}, docValues:{}\r\ninfoStream=ThreadNameFixingPrintStreamInfoStream\r\nmergePolicy=[TieredMergePolicy: maxMergeAtOnce=3, maxMergeAtOnceExplicit=13, maxMergedSegmentMB=56,92578125, floorSegmentMB=1,4658203125, forceMergeDeletesPctAllowed=3,1933459826266697, segmentsPerTier=3, maxCFSSegmentSizeMB=8796093022207,999, noCFSRatio=0,858049558093831\r\nindexerThreadPool=Lucene.Net.Index.DocumentsWriterPerThreadPool\r\nreaderPooling=True\r\nperThreadHardLimitMB=1945\r\nuseCompoundFile=False\r\ncheckIntegrityAtMerge=True\r\n\r\n%5.3fs %5.1fs: %7s %2s [%11s] %s\r\n%5.3fs %5.1fs: %7s %2s [%11s] %s\r\n%5.3fs %5.1fs: %7s %2s [%11s] %s\r\n%5.3fs %5.1fs: %7s %2s [%11s] %s\r\n%5.3fs %5.1fs: %7s %2s [%11s] %s\r\n   at System.Collections.Generic.Dictionary`2.TryInsert(TKey key, TValue value, InsertionBehavior behavior)\r\n   at System.Collections.Generic.Dictionary`2.Add(TKey key, TValue value)\r\n  
     * at Lucene.Net.Replicator.Nrt.PrimaryNode..ctor(IndexWriter writer, Int32 id, Int64 primaryGen, Int64 forcePrimaryVersion, SearcherFactory searcherFactory, TextWriter printStream) 
     * in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Replicator\\Nrt\\PrimaryNode.cs:line 90\r\nWARNING: Leftover undeleted temporary files Could not remove the following files (in the order of attempts):\r\n  
     * C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-iqfzvfc2\\write.lock\r\n   
     * C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-iqfzvfc2\r\n\r\nNUnit Adapter 3.17.0.0: 
     * Test execution complete\r\n  Failed Test [400 ms]\r\n  Error Message:\r\n  
     * Lucene.Net.Util.LuceneSystemException : An item with the same key has already been added. Key: __version\r\n  ----> System.ArgumentException : An item with the same key has already been added. Key: __version\r\n\r\nTo reproduce this test result:\r\n\r\nOption 1:\r\n\r\n Apply the following assembly-level attributes:\r\n\r\n[assembly: Lucene.Net.Util.RandomSeed(\"0x26cf1b4cef7d5c27\")]\r\n[assembly: NUnit.Framework.SetCulture(\"sq-AL\")]\r\n\r\nOption 2:\r\n\r\n Use the following .runsettings file:\r\n\r\n<RunSettings>\r\n  <TestRunParameters>\r\n    <Parameter name=\"tests:seed\" value=\"0x26cf1b4cef7d5c27\" />\r\n    <Parameter name=\"tests:culture\" value=\"sq-AL\" />\r\n  </TestRunParameters>\r\n</RunSettings>\r\n\r\nSee the .runsettings documentation at: https://docs.microsoft.com/en-us/visualstudio/test/configure-unit-tests-by-using-a-dot-runsettings-file.\r\n  Stack Trace:\r\n     at Lucene.Net.Replicator.Nrt.PrimaryNode..ctor(IndexWriter writer, Int32 id, Int64 primaryGen, Int64 forcePrimaryVersion, SearcherFactory searcherFactory, TextWriter printStream) in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Replicator\\Nrt\\PrimaryNode.cs:line 126\r\n   at Lucene.Net.Tests.Replicator.Nrt.SimplePrimaryNode..ctor(Random random, String indexPath, Int32 id, Int32 tcpPort, Int64 primaryGen, Int64 forcePrimaryVersion, SearcherFactory searcherFactory, Boolean doFlipBitsDuringCopy, Boolean doCheckIndexOnClose) in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Tests.Replicator\\Nrt\\SimplePrimaryNode.cs:line 98\r\n   at Lucene.Net.Tests.Replicator.Nrt.SimpleServer.Test() in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Tests.Replicator\\Nrt\\SimpleServer.cs:line 259\r\n   at System.RuntimeMethodHandle.InvokeMethod(Object target, Void** arguments, Signature sig, Boolean isConstructor)\r\n   at System.Reflection.MethodInvoker.Invoke(Object obj, IntPtr* args, BindingFlags invokeAttr)\r\n--ArgumentException\r\n   at System.Collections.Generic.Dictionary`2.TryInsert(TKey key, TValue value, InsertionBehavior behavior)\r\n   at System.Collections.Generic.Dictionary`2.Add(TKey key, TValue value)\r\n   at Lucene.Net.Replicator.Nrt.PrimaryNode..ctor(IndexWriter writer, Int32 id, Int64 primaryGen, Int64 forcePrimaryVersion, SearcherFactory searcherFactory, TextWriter printStream) in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Replicator\\Nrt\\PrimaryNode.cs:line 90\r\n  Standard Output Messages:\r\n RandomSeed: 0x26cf1b4cef7d5c27\r\n Culture: sq-AL\r\n Time Zone: (UTC+02:00) Harare, Pretoria\r\n Default Codec: Lucene46 (RandomCodec)\r\n Default Similarity: DefaultSimilarity\r\n Nightly: False\r\n Weekly: False\r\n Slow: True\r\n Awaits Fix: False\r\n Directory: random\r\n Verbose: True\r\n Random Multiplier: 1\r\n \r\n PORT: 0\r\n %5.3fs %5.1fs:         N%d [%11s] %s\r\n IFD 1 [1.12.2023 3:30:44 e pasdites; NonParallelWorker]: init: current segments file is \"\"; deletionPolicy=Lucene.Net.Index.KeepOnlyLastCommitDeletionPolicy\r\n IFD 1 [1.12.2023 3:30:44 e pasdites; NonParallelWorker]: now checkpoint \"\" [0 segments ; isCommit = False]\r\n IFD 1 [1.12.2023 3:30:44 e pasdites; NonParallelWorker]: 1 msec to checkpoint\r\n IW 1 [1.12.2023 3:30:44 e pasdites; NonParallelWorker]: init: create=True\r\n IW 1 [1.12.2023 3:30:44 e pasdites; NonParallelWorker]: \r\n dir=MockDirectoryWrapper(MMapDirectory@C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-iqfzvfc2 lockFactory=NativeFSLockFactory@C:\\Users\\admin\\AppData\\Local\\Temp\\LuceneTemp-iqfzvfc2)\r\n index=\r\n version=4.8.0\r\n matchVersion=LUCENE_48\r\n analyzer=MockAnalyzer\r\n ramBufferSizeMB=16\r\n maxBufferedDocs=-1\r\n maxBufferedDeleteTerms=-1\r\n mergedSegmentWarmer=\r\n readerTermsIndexDivisor=2\r\n termIndexInterval=32\r\n delPolicy=KeepOnlyLastCommitDeletionPolicy\r\n commit=null\r\n openMode=CREATE_OR_APPEND\r\n similarity=DefaultSimilarity\r\n mergeScheduler=ConcurrentMergeScheduler: maxThreadCount=1, maxMergeCount=2, mergeThreadPriority=-1\r\n default WRITE_LOCK_TIMEOUT=1000\r\n writeLockTimeout=1000\r\n codec=Lucene46: {}, docValues:{}\r\n infoStream=ThreadNameFixingPrintStreamInfoStream\r\n mergePolicy=[TieredMergePolicy: maxMergeAtOnce=3, maxMergeAtOnceExplicit=13, maxMergedSegmentMB=56,92578125, floorSegmentMB=1,4658203125, forceMergeDeletesPctAllowed=3,1933459826266697, segmentsPerTier=3, maxCFSSegmentSizeMB=8796093022207,999, noCFSRatio=0,858049558093831\r\n indexerThreadPool=Lucene.Net.Index.DocumentsWriterPerThreadPool\r\n readerPooling=True\r\n perThreadHardLimitMB=1945\r\n useCompoundFile=False\r\n checkIntegrityAtMerge=True\r\n \r\n %5.3fs %5.1fs: %7s %2s [%11s] %s\r\n %5.3fs %5.1fs: %7s %2s [%11s] %s\r\n %5.3fs %5.1fs: %7s %2s [%11s] %s\r\n %5.3fs %5.1fs: %7s %2s [%11s] %s\r\n %5.3fs %5.1fs: %7s %2s [%11s] %s\r\n    at System.Collections.Generic.Dictionary`2.TryInsert(TKey key, TValue value, InsertionBehavior behavior)\r\n    at System.Collections.Generic.Dictionary`2.Add(TKey key, TValue value)\r\n    at Lucene.Net.Replicator.Nrt.PrimaryNode..ctor(IndexWriter writer, Int32 id, Int64 primaryGen, Int64 forcePrimaryVersion, SearcherFactory searcherFactory, TextWriter printStream) in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Replicator\\Nrt\\PrimaryNode.cs:line 90\r\n\r\n\r\nTotal tests: 1\r\n     Failed: 1\r\n Total time: 1.6639 Seconds\r\n"
     * 
     * 
     * "Microsoft (R) Test Execution Command Line Tool Version 17.4.0 (x64)\r\nCopyright (c) Microsoft Corporation.  All rights reserved.\r\n\r\nStarting test execution, please wait...\r\nA total of 1 test files matched the specified pattern.\r\nNUnit Adapter 3.17.0.0: Test execution started\r\nRunning all tests in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Tests.Replicator\\bin\\Debug\\net7.0\\Lucene.Net.Tests.Replicator.dll\r\n   NUnit3TestExecutor discovered 68 of 68 NUnit test cases\r\nRandomSeed: 0xb3ef85f835cbbf38\r\nCulture: ar-BH\r\nTime Zone: (UTC+03:00) Moscow, St. Petersburg\r\nDefault Codec: CheapBastard (CheapBastardCodec)\r\nDefault Similarity: DefaultSimilarity\r\nNightly: False\r\nWeekly: False\r\nSlow: True\r\nAwaits Fix: False\r\nDirectory: random\r\nVerbose: True\r\nRandom Multiplier: 1\r\n\r\nNUnit Adapter 3.17.0.0: Test execution complete\r\n  Failed Test [169 ms]\r\n  Error Message:\r\n   System.FormatException : The input string '-1' was not in a correct format.\r\n\r\nTo reproduce this test result:\r\n\r\nOption 1:\r\n\r\n Apply the following assembly-level attributes:\r\n\r\n[assembly: Lucene.Net.Util.RandomSeed(\"0xb3ef85f835cbbf38\")]\r\n[assembly: NUnit.Framework.SetCulture(\"ar-BH\")]\r\n\r\nOption 2:\r\n\r\n Use the following .runsettings file:\r\n\r\n<RunSettings>\r\n  <TestRunParameters>\r\n    <Parameter name=\"tests:seed\" value=\"0xb3ef85f835cbbf38\" />\r\n    <Parameter name=\"tests:culture\" value=\"ar-BH\" />\r\n  </TestRunParameters>\r\n</RunSettings>\r\n\r\nSee the .runsettings documentation at: https://docs.microsoft.com/en-us/visualstudio/test/configure-unit-tests-by-using-a-dot-runsettings-file.\r\n  Stack Trace:\r\n     at System.Number.ThrowOverflowOrFormatException(ParsingStatus status, ReadOnlySpan`1 value, TypeCode type)\r\n   at System.Int64.Parse(String s)\r\n   at Lucene.Net.Tests.Replicator.Nrt.SimpleServer.Test() in C:\\Users\\admin\\Projects\\Dotnet\\lucenenet\\src\\Lucene.Net.Tests.Replicator\\Nrt\\SimpleServer.cs:line 241\r\n   at System.RuntimeMethodHandle.InvokeMethod(Object target, Void** arguments, Signature sig, Boolean isConstructor)\r\n   at System.Reflection.MethodInvoker.Invoke(Object obj, IntPtr* args, BindingFlags invokeAttr)\r\n  Standard Output Messages:\r\n RandomSeed: 0xb3ef85f835cbbf38\r\n Culture: ar-BH\r\n Time Zone: (UTC+03:00) Moscow, St. Petersburg\r\n Default Codec: CheapBastard (CheapBastardCodec)\r\n Default Similarity: DefaultSimilarity\r\n Nightly: False\r\n Weekly: False\r\n Slow: True\r\n Awaits Fix: False\r\n Directory: random\r\n Verbose: True\r\n Random Multiplier: 1\r\n\r\n\r\n\r\nTotal tests: 1\r\n     Failed: 1\r\n Total time: 1.3479 Seconds\r\n"
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

        private readonly AtomicInt64 nodeStartCounter = new AtomicInt64();
        private long nextPrimaryGen;
        private long lastPrimaryGen;
        private LineFileDocs docs;

        /** Launches a child "server" (separate JVM), which is either primary or replica node */

        //@SuppressForbidden(reason = "ProcessBuilder requires java.io.File for CWD")
        private NodeProcess StartNode(int primaryTCPPort, int id, DirectoryInfo indexPath, long forcePrimaryVersion, bool willCrash)
        {
            List<string> cmd = new List<string>();

            //get the full location of the assembly with DaoTests in it
            string testAssemblyPath = Assembly.GetAssembly(typeof(SimpleServer)).Location;

            //get the folder that's in
            string theDirectory = Path.GetDirectoryName(testAssemblyPath);

            cmd.AddRange(new[]
            {
                    "test", testAssemblyPath,
                    "--framework", GetTargetFramework(),
                    "--filter", "FullyQualifiedName~" + typeof(SimpleServer).FullName,
                    "--logger:\"console;verbosity=normal\"",
                    "--",
                    $"RunConfiguration.TargetPlatform={GetTargetPlatform()}"
            });

            // LUCENENET NOTE: Since in our CI environment we create a lucene.testSettings.config file
            // for all tests, we need to pass some of these settings as test run parameters to override
            // for this process. These are read as system properties on the inside of the application.
            //cmd.Add(TestRunParameter("assert", "true"));
            ConfigurationSettings.CurrentConfiguration["assert"] = "true";
            /*StringDictionary stringDictionary = new StringDictionary();
            stringDictionary.Add("assert", "true");*/
            // Mixin our own counter because this is called from a fresh thread which means the seed otherwise isn't changing each time we spawn a
            // new node:
            long seed = Random.NextInt64() * nodeStartCounter.IncrementAndGet();
            //cmd.Add(TestRunParameter("tests:seed", SeedUtils.FormatSeed(seed)));
            ConfigurationSettings.CurrentConfiguration["tests:seed"] = SeedUtils.FormatSeed(seed);
            //cmd.Add($"-d:tests:culture={Thread.CurrentThread.CurrentCulture.Name}");
            ConfigurationSettings.CurrentConfiguration["tests:culture"] = Thread.CurrentThread.CurrentCulture.Name;
            long myPrimaryGen;
            if (primaryTCPPort != -1)
            {
                // I am a replica
                //cmd.Add($"-d:tests:nrtreplication.primaryTCPPort={primaryTCPPort}");
                ConfigurationSettings.CurrentConfiguration["tests:nrtreplication.primaryTCPPort"] = primaryTCPPort.toString();
                myPrimaryGen = lastPrimaryGen;
            }
            else
            {
                myPrimaryGen = nextPrimaryGen++;
                lastPrimaryGen = myPrimaryGen;
            }
            //cmd.Add(TestRunParameter("tests:nrtreplication.primaryGen", myPrimaryGen.ToString()));
            ConfigurationSettings.CurrentConfiguration["tests:nrtreplication.primaryGen"] = myPrimaryGen.ToString();
            //cmd.Add(TestRunParameter("tests:nrtreplication.closeorcrash", "false"));
            ConfigurationSettings.CurrentConfiguration["tests:nrtreplication.closeorcrash"] = "false";

            //cmd.Add(TestRunParameter("tests:nrtreplication.node", "true"));
            ConfigurationSettings.CurrentConfiguration["tests:nrtreplication.node"] = "true";

            //cmd.Add(TestRunParameter("tests:nrtreplication.nodeid", id.ToString()));
            ConfigurationSettings.CurrentConfiguration["tests:nrtreplication.nodeid"] = id.ToString();

            //cmd.Add(TestRunParameter("tests:nrtreplication.startNS", Node.globalStartNS.ToString()));
            ConfigurationSettings.CurrentConfiguration["tests:nrtreplication.startNS"] = Node.globalStartNS.ToString();

            //cmd.Add(TestRunParameter("tests:nrtreplication.indexpath", indexPath.FullName));
            ConfigurationSettings.CurrentConfiguration["tests:nrtreplication.indexpath"] = indexPath.FullName;

            //cmd.Add(TestRunParameter("tests:nrtreplication.checkonclose", "true"));
            ConfigurationSettings.CurrentConfiguration["tests:nrtreplication.checkonclose"] = "true";


            if (primaryTCPPort == -1)
            {
                // We are the primary node
                //cmd.Add(TestRunParameter("tests:nrtreplication.isPrimary", "true"));
                ConfigurationSettings.CurrentConfiguration["tests:nrtreplication.isPrimary"] = "true";

                //cmd.Add(TestRunParameter("tests:nrtreplication.forcePrimaryVersion", forcePrimaryVersion.ToString()));
                ConfigurationSettings.CurrentConfiguration["tests:nrtreplication.forcePrimaryVersion"] = J2N.Numerics.Int64.ToString(forcePrimaryVersion);

            }
            Assert.AreEqual(id, int.Parse(SystemProperties.GetProperty("tests:nrtreplication.nodeid")));
            // passing NIGHTLY to this test makes it run for much longer, easier to catch it in the act...
            //cmd.Add(TestRunParameter("tests:nightly", "true"));
            //cmd.Add(typeof(TestNRTReplication).FullName.Replace(typeof(TestNRTReplication).Name, "SimpleServer"));
            /* cmd.Add("-ea");
             cmd.Add("-cp");
             cmd.Add(SystemProperties.GetProperty("java.class.var"));
             cmd.Add("org.junit.runner.JUnitCore");*/
            // cmd.Add(getClass().getName().replace(getClass().getSimpleName(), "SimpleServer"));

            // Set up the process to run the console app
            ProcessStartInfo startInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = string.Join(" ", cmd),
                WorkingDirectory = theDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                EnvironmentVariables =
                {
                    {"tests:nrtreplication.primaryTCPPort",primaryTCPPort.toString() },
                    { "tests:nrtreplication.node","true"},
                    { "tests:nrtreplication.nodeid", id.ToString() },
                    {"tests:nrtreplication.primaryGen", myPrimaryGen.ToString() },
                    {"tests:nrtreplication.closeorcrash", "true" },
                    {"tests:nrtreplication.startNS", Node.globalStartNS.ToString() },
                    {"tests:nrtreplication.indexpath", indexPath.FullName },
                    {"tests:nrtreplication.checkonclose", "true" },
                    {"tests:nrtreplication.isPrimary", "true" },
                    {"tests:nrtreplication.forcePrimaryVersion", forcePrimaryVersion.ToString() }
                }
            };

            /* var cli = Cli.Wrap("dotnet")
                 .WithArguments(string.Join(" ", cmd))
                 .WithWorkingDirectory(theDirectory);*/


            Message("child process command: " + cmd);

            Process p = Process.Start(startInfo);
            //var result = cli.ExecuteBufferedAsync().GetAwaiter().GetResult();
            StreamReader r;
            try
            {
                r = p.StandardOutput;
                cmd.Clear();
                cmd.Add(r.ReadToEnd());
                cmd.Add(p.StandardError.ReadToEnd());
                cmd.Add(SystemProperties.GetProperty("tests:nrtreplication.primaryGen"));
            }
            catch (UnsupportedOperationException uee)
            {
                throw RuntimeException.Create(uee);
            }

            int tcpPort = -1;
            long initCommitVersion = -1;
            long initInfosVersion = -1;
            Regex logTimeStart = new Regex("^[0-9\\.]+s .*", RegexOptions.Compiled);
            bool sawExistingSegmentsFile = false;

            while (true)
            {
                string l = r.ReadLine();
                cmd.Clear();
                cmd.Add(r.ReadToEnd());
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
            ThreadJob pumper = ThreadPumper.Start(new ThreadJobAnonymousClass(p, finalWillCrash, id), null, Console.Out, null, nodeClosing);

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
            private Process p;
            private bool finalWillCrash;
            private int id;

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
            using Connection primaryC = new Connection(primary.tcpPort);
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

        private static void Message(string message)
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
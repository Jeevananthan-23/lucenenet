﻿using J2N;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.IO;
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Replicator.Nrt
{
    //LUCENENET Only
    internal static class Extensions
    {
        internal static IndexReader GetIndexReader(this IndexSearcher indexSearcher) => indexSearcher.IndexReader;

        internal static Lock ObtainLock(this Directory directory, string lockName) => directory.LockFactory.MakeLock(lockName);

        internal static void PrintStackTrace(this Exception e, TextWriter destination) => destination.WriteLine(e.StackTrace);

        internal static double TimeUnitSecondsToNanos(int num) =>  Time.MillisecondsPerNanosecond;
    }
}
﻿/*
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

// TODO: or ... replica node can do merging locally?  tricky to keep things in sync, when one node merges more slowly than others...

using J2N;
using Lucene.Net.Diagnostics;
using Lucene.Net.Index;
using System.Collections.Generic;

namespace Lucene.Net.Replicator.Nrt
{
    /// <summary>
    /// A merged segment warmer that pre-copies the merged segment out to
    ///  replicas before primary cuts over to the merged segment.  This
    ///  ensures that NRT reopen time on replicas is only in proportion to
    ///  flushed segment sizes, not merged segments.
    ///</summary>
    internal class PreCopyMergedSegmentWarmer : IndexWriter.IndexReaderWarmer
    {
        private readonly PrimaryNode primary;

        public PreCopyMergedSegmentWarmer(PrimaryNode primary)
        {
            this.primary = primary;
        }

        /// <exception cref="System.IO.IOException"/>
        public override void Warm(AtomicReader reader)
        {
            long startNS = Time.NanoTime();
            SegmentCommitInfo info = ((SegmentReader)reader).SegmentInfo;
            //System.out.println("TEST: warm merged segment files " + info);
            IDictionary<string, FileMetaData> filesMetaData = new Dictionary<string, FileMetaData>();
            foreach (string fileName in info.GetFiles())
            {
                FileMetaData metaData = primary.ReadLocalFileMetaData(fileName);
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(metaData != null);
                    Debugging.Assert(filesMetaData.ContainsKey(fileName) == false);
                }
                filesMetaData.Add(fileName, metaData);
                primary.finishedMergedFiles.Add(fileName); //moved here
            }
            //from here to up because we don't have AddAll() in Set
            primary.PreCopyMergedSegmentFiles(info, filesMetaData);
            primary.Message(string.Format("top: done warm merge " + info + ": took %.3f sec, %.1f MB", (Time.NanoTime() - startNS) / 1000000000.0, info.GetSizeInBytes() / 1024 / 1024.0));
        }
    }
}
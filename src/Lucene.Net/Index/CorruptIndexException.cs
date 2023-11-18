﻿using Lucene.Net.Store;
using System;
using System.IO;
#if FEATURE_SERIALIZABLE_EXCEPTIONS
using System.Runtime.Serialization;
using System.Xml.Linq;
#endif

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
    /// This exception is thrown when Lucene detects
    /// an inconsistency in the index.
    /// </summary>
    // LUCENENET: It is no longer good practice to use binary serialization. 
    // See: https://github.com/dotnet/corefx/issues/23584#issuecomment-325724568
#if FEATURE_SERIALIZABLE_EXCEPTIONS
    [Serializable]
#endif
    public class CorruptIndexException : IOException // LUCENENENET specific - made public instead of internal because there are public subclasses
    {

        private readonly string message;
        private readonly string resourceDescription;
        /// <summary>
        /// Constructor. </summary>
        public CorruptIndexException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor. </summary>
        public CorruptIndexException(string message, Exception ex) 
            : base(message, ex)
        {
        }

        /** Create exception with message and root cause. */
        public CorruptIndexException(string message, DataOutput output, Exception cause)
             : base(message, cause)
        {
           
        }

        /** Create exception with message and root cause. */
        public CorruptIndexException(string message, string resourceDescription, Exception cause)
            : base((message) + " (resource=" + resourceDescription + ")", cause)
        {
            this.resourceDescription = resourceDescription;
            this.message = message;
        }

#if FEATURE_SERIALIZABLE_EXCEPTIONS
        /// <summary>
        /// Initializes a new instance of this class with serialized data.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The <see cref="StreamingContext"/> that contains contextual information about the source or destination.</param>
        protected CorruptIndexException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
#endif
    }
}
// Lucene version compatibility level 4.8.1
using Lucene.Net.Analysis.Util;
using NUnit.Framework;
using System;
using System.IO;

namespace Lucene.Net.Analysis.No
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
    /// Simple tests to ensure the Norwegian Light stem factory is working.
    /// </summary>
    public class TestNorwegianLightStemFilterFactory : BaseTokenStreamFactoryTestCase
    {
        [Test]
        public virtual void TestStemming()
        {
            TextReader reader = new StringReader("epler eple");
            TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
            stream = TokenFilterFactory("NorwegianLightStem").Create(stream);
            AssertTokenStreamContents(stream, new string[] { "epl", "epl" });
        }

        /// <summary>
        /// Test stemming with variant set explicitly to Bokmål </summary>
        [Test]
        public virtual void TestBokmaalStemming()
        {
            TextReader reader = new StringReader("epler eple");
            TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
            stream = TokenFilterFactory("NorwegianLightStem", "variant", "nb").Create(stream);
            AssertTokenStreamContents(stream, new string[] { "epl", "epl" });
        }

        /// <summary>
        /// Test stemming with variant set explicitly to Nynorsk </summary>
        [Test]
        public virtual void TestNynorskStemming()
        {
            TextReader reader = new StringReader("gutar gutane");
            TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
            stream = TokenFilterFactory("NorwegianLightStem", "variant", "nn").Create(stream);
            AssertTokenStreamContents(stream, new string[] { "gut", "gut" });
        }

        /// <summary>
        /// Test that bogus arguments result in exception </summary>
        [Test]
        public virtual void TestBogusArguments()
        {
            try
            {
                TokenFilterFactory("NorwegianLightStem", "bogusArg", "bogusValue");
                fail();
            }
            catch (ArgumentException expected)
            {
                assertTrue(expected.Message.Contains("Unknown parameters"));
            }
        }
    }
}
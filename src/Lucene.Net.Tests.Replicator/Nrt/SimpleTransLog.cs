using J2N.IO;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using System;
using System.Diagnostics;
using System.IO;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    /**
 * This is a stupid yet functional transaction log: it never fsync's, never prunes, it's
 * over-synchronized, it hard-wires id field name to "docid", can only handle specific docs/fields
 * used by this test, etc. It's just barely enough to show how a translog could work on top of NRT
 * replication to guarantee no data loss when nodes crash
 */

    internal class SimpleTransLog : IDisposable
    {
        private readonly FileStream channel;
        private readonly RAMOutputStream buffer = new RAMOutputStream();

        private static readonly byte[] intBuffer = new byte[4];
        private readonly MemoryStream intByteBuffer = new MemoryStream(intBuffer);

        private static readonly byte OP_ADD_DOCUMENT = 0;
        private static readonly byte OP_UPDATE_DOCUMENT = 1;
        private static readonly byte OP_DELETE_DOCUMENTS = 2;

        public SimpleTransLog(string path)
        {
            channel = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite);
        }

        public long GetNextLocation()
        {
            lock (this)
            {
                return channel.Position;
            }
        }

        /** Appends an addDocument op */

        public long AddDocument(string id, Document doc)
        {
            Debug.Assert(buffer.Length == 0);
            buffer.WriteByte(OP_ADD_DOCUMENT);
            Encode(id, doc);
            return FlushBuffer();
        }

        /** Appends an updateDocument op */

        public long UpdateDocument(string id, Document doc)
        {
            lock (this)
            {
                Debug.Assert(buffer.Length == 0);
                buffer.WriteByte(OP_UPDATE_DOCUMENT);
                Encode(id, doc);
                return FlushBuffer();
            }
        }

        /** Appends a deleteDocuments op */

        public long DeleteDocuments(string id)
        {
            lock (this)
            {
                Debug.Assert(buffer.Length == 0);
                buffer.WriteByte(OP_DELETE_DOCUMENTS);
                buffer.WriteString(id);
                return FlushBuffer();
            }
        }

        /** Writes buffer to the file and returns the start position. */

        private long FlushBuffer()
        {
            long pos = channel.Position;
            int len = (int)buffer.Length;
            // byte[] bytes = new MemoryStream(buffer).ToArray();
            buffer.Reset();

            // BitUtil.VH_BE_INT.set(intBuffer, 0, len);
            intByteBuffer.Capacity = 4;
            intByteBuffer.Position = 0;

            WriteBytesToChannel(intByteBuffer);
            // WriteBytesToChannel(new MemoryStream(bytes));

            return pos;
        }

        private void WriteBytesToChannel(MemoryStream src)
        {
            int left = src.Capacity;
            while (left != 0)
            {
                channel.Write(src.ToArray());
                left -= (int)channel.Length;
            }
        }

        private void ReadBytesFromChannel(long pos, MemoryStream dest)
        {
            int left = dest.Capacity - (int)dest.Position;
            long end = pos + left;
            while (pos < end)
            {
                int inc = channel.Read(dest.ToArray().AsSpan((int)pos, (int)end)); //each time we should box it ?
                if (inc < 0)
                {
                    throw EOFException.Create();
                }
                pos += inc;
            }
        }

        /**
         * Replays ops between start and end location against the provided writer. Can run concurrently
         * with ongoing operations.
         */

        public void Replay(NodeProcess primary, long start, long end)
        {
            using Connection c = new Connection(primary.tcpPort);
            c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING);
            byte[] intBuffer = new byte[4];
            ByteBuffer intByteBuffer = ByteBuffer.Wrap(intBuffer);
            ByteArrayDataInput @in = new ByteArrayDataInput();

            long pos = start;
            while (pos < end)
            {
                intByteBuffer.Position = 0;
                intByteBuffer.Limit = 4;
                // ReadBytesFromChannel(pos, intByteBuffer);
                pos += 4;
                //  int len = (int)BitUtil.VH_BE_INT.get(intBuffer, 0);
                /*
                                byte[] bytes = new byte[len];
                                ReadBytesFromChannel(pos, ByteBuffer.Wrap(bytes));
                                pos += len;*/

                // @in.Reset(bytes);

                byte op = @in.ReadByte();
                // System.out.println("xlog: replay op=" + op);
                switch (op)
                {
                    case 0:
                        // We replay add as update:
                        ReplayAddDocument(c, primary, @in);
                        break;

                    case 1:
                        // We replay add as update:
                        ReplayAddDocument(c, primary, @in);
                        break;

                    case 2:
                        ReplayDeleteDocuments(c, primary, @in);
                        break;

                    default:
                        throw new CorruptIndexException("invalid operation " + op, @in.ReadString(), null); //this is not what implemeted
                }
                // assert pos == end;
                // System.out.println("xlog: done replay");
                c.@out.WriteByte(SimplePrimaryNode.CMD_INDEXING_DONE);
                c.Flush();
                // System.out.println("xlog: done flush");
                c.@in.ReadByte();
                // System.out.println("xlog: done readByte");
            }
        }

        private void ReplayAddDocument(Connection c, NodeProcess primary, DataInput @in)
        {
            string id = @in.ReadString();

            Document doc = new Document();
            doc.Add(new StringField("docid", id, Field.Store.YES));

            string title = ReadNullableString(@in);
            if (title != null)
            {
                doc.Add(new StringField("title", title, Field.Store.NO));
                doc.Add(new TextField("titleTokenized", title, Field.Store.NO));
            }
            string body = ReadNullableString(@in);
            if (body != null)
            {
                doc.Add(new TextField("body", body, Field.Store.NO));
            }
            string marker = ReadNullableString(@in);
            if (marker != null)
            {
                // TestStressNRTReplication.message("xlog: replay marker=" + id);
                doc.Add(new StringField("marker", marker, Field.Store.YES));
            }

            // For both add and update originally, we use updateDocument to replay,
            // because the doc could @in fact already be @in the index:
            // nocomit what if this fails?
            primary.AddOrUpdateDocument(c, doc, false);
        }

        private void ReplayDeleteDocuments(Connection c, NodeProcess primary, DataInput @in)
        {
            string id = @in.ReadString();
            // nocomit what if this fails?
            primary.DeleteDocument(c, id);
        }

        /**
         * Encodes doc into buffer. NOTE: this is NOT general purpose! It only handles the fields used @in
         * this test!
         */

        private void Encode(string id, Document doc)
        {
            lock (this)
            {
                Debug.Assert(id.equals(doc.Get("docid")), "id=" + id + " vs docid=" + doc.Get("docid"));
                buffer.WriteString(id);
                WriteNullableString(doc.Get("title"));
                WriteNullableString(doc.Get("body"));
                WriteNullableString(doc.Get("marker"));
            }
        }

        private void WriteNullableString(string s)
        {
            if (s == null)
            {
                buffer.WriteByte((byte)0);
            }
            else
            {
                buffer.WriteByte((byte)1);
                buffer.WriteString(s);
            }
        }

        private string ReadNullableString(DataInput @in)
        {
            byte b = @in.ReadByte();
            if (b == 0)
            {
                return null;
            }
            else if (b == 1)
            {
                return @in.ReadString();
            }
            else
            {
                throw new CorruptIndexException("invalid string lead byte " + b, @in.ReadString(), null);
            }
        }

        public void Dispose()
        {
            channel.Dispose();
        }
    }
}
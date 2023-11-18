using Lucene.Net.Support;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lucene.Net.Store
{
    public class ByteArrayIndexInput : IndexInput
    {
        private byte[] bytes;

        private int pos;
        private int limit;

        public ByteArrayIndexInput(string description, byte[] bytes)
            : base(description)
        {

            this.bytes = bytes;
            this.limit = bytes.Length;
        }

        public long GetFilePointer()
        {
            return pos;
        }

        public void Reset(byte[] bytes, int offset, int len)
        {
            this.bytes = bytes;
            pos = offset;
            limit = offset + len;
        }

        public override short ReadInt16() => (short) (((bytes[pos++] & 0xFF) << 8) | (bytes[pos++] & 0xFF));

        public override int ReadInt32() => ((bytes[pos++] & 0xFF) << 24) | ((bytes[pos++] & 0xFF) << 16) | ((bytes[pos++] & 0xFF) << 8) | (bytes[pos++] & 0xFF);

        public override long ReadInt64()
        {
             int i1 = ((bytes[pos++] & 0xff) << 24) | ((bytes[pos++] & 0xff) << 16) | ((bytes[pos++] & 0xff) << 8) | (bytes[pos++] & 0xff);
             int i2 = ((bytes[pos++] & 0xff) << 24) | ((bytes[pos++] & 0xff) << 16) | ((bytes[pos++] & 0xff) << 8) | (bytes[pos++] & 0xff);
            return (((long)i1) << 32) | (i2 & 0xFFFFFFFFL);
        }

        public override int ReadVInt32()
        {
            byte b = bytes[pos++];
            if (b >= 0) return b;
            int i = b & 0x7F;
            b = bytes[pos++];
            i |= (b & 0x7F) << 7;
            if (b >= 0) return i;
            b = bytes[pos++];
            i |= (b & 0x7F) << 14;
            if (b >= 0) return i;
            b = bytes[pos++];
            i |= (b & 0x7F) << 21;
            if (b >= 0) return i;
            b = bytes[pos++];
            // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
            i |= (b & 0x0F) << 28;
            if ((b & 0xF0) == 0) return i;
            throw RuntimeException.Create("Invalid vInt detected (too many bits)");
        }

        public override long ReadVInt64()
        {
            byte b = bytes[pos++];
            if (b >= 0) return b;
            long i = b & 0x7FL;
            b = bytes[pos++];
            i |= (b & 0x7FL) << 7;
            if (b >= 0) return i;
            b = bytes[pos++];
            i |= (b & 0x7FL) << 14;
            if (b >= 0) return i;
            b = bytes[pos++];
            i |= (b & 0x7FL) << 21;
            if (b >= 0) return i;
            b = bytes[pos++];
            i |= (b & 0x7FL) << 28;
            if (b >= 0) return i;
            b = bytes[pos++];
            i |= (b & 0x7FL) << 35;
            if (b >= 0) return i;
            b = bytes[pos++];
            i |= (b & 0x7FL) << 42;
            if (b >= 0) return i;
            b = bytes[pos++];
            i |= (b & 0x7FL) << 49;
            if (b >= 0) return i;
            b = bytes[pos++];
            i |= (b & 0x7FL) << 56;
            if (b >= 0) return i;
            throw RuntimeException.Create("Invalid vLong detected (negative values disallowed)");
        }

        public override void SkipBytes(long count)
        {
            pos += (int)count;
        }

        public bool Eof()
        {
            return pos == limit;
        }

        public override long Position => pos;

        public override long Length => limit;

        // NOTE: AIOOBE not EOF if you read too much
        public override byte ReadByte() => bytes[pos++];

        // NOTE: AIOOBE not EOF if you read too much
        public override void ReadBytes(byte[] b, int offset, int len)
        {
            Arrays.Copy(bytes, pos, b, offset, len);
            pos += len;
        }

        public override void Seek(long pos)
        {
            this.pos = (int)pos;
        }

        protected override void Dispose(bool disposing) => throw new NotImplementedException();
    }
}

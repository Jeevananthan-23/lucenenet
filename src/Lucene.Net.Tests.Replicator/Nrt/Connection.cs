using J2N;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Store;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    /** Simple point-to-point TCP connection */
    class Connection : IDisposable
    {
        public readonly DataInput @in;
        public readonly DataOutput @out;
        public readonly Stream sockIn;
        public readonly BufferedStream bos;
        public readonly Socket s;
        public readonly IPEndPoint destTCPPort;
        public long lastKeepAliveNS = Time.NanoTime();

        public Connection(int port)
        {
            this.destTCPPort.Port = port;
            this.s = new Socket(AddressFamily.InterNetwork,SocketType.Stream, ProtocolType.Tcp);
            s.Connect(destTCPPort);
            this.sockIn = new NetworkStream(s);
            this.@in = new InputStreamDataInput(sockIn);
            this.bos = new BufferedStream(new NetworkStream(s));
            this.@out = new OutputStreamDataOutput(bos);
            if (Node.VERBOSE_CONNECTIONS)
            {
                Console.WriteLine("make new client Connection socket=" + this.s + " destPort=" + port);
            }
        }

        public void Flush() => bos.Flush();

        public void Dispose() => s.Dispose();
    }

}

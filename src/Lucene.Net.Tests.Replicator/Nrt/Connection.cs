using J2N;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Store;
using System;
using System.Net;
using System.Net.Sockets;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    /** Simple point-to-point TCP connection */

    internal class Connection : IDisposable
    {
        internal readonly DataInput @in;
        internal readonly DataOutput @out;
        internal readonly NetworkStream sockIn;
        internal readonly NetworkStream bos;
        internal readonly Socket s;
        internal readonly IPEndPoint destTCPPort = new IPEndPoint(IPAddress.Any, 5000);
        internal long lastKeepAliveNS = Time.NanoTime();

        public Connection(int port)
        {
            this.destTCPPort.Port = port;
            this.s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            s.Connect(destTCPPort);
            this.sockIn = new NetworkStream(s);
            this.@in = new InputStreamDataInput(sockIn);
            this.bos = new NetworkStream(s);
            this.@out = new OutputStreamDataOutput(bos);
            if (Node.VERBOSE_CONNECTIONS)
            {
                Console.WriteLine("make new client Connection socket=" + this.s + " destPort=" + port);
            }
        }

        public void Flush() => bos.Flush();

        public void Dispose()
        {
            Console.WriteLine("Disposing" + destTCPPort);
            sockIn.Dispose();
            bos.Dispose();
            s.Dispose();
            s.Ttl = 0;
        }
    }
}
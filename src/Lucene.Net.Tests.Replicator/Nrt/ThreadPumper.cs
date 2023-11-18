using J2N;
using J2N.Threading;
using J2N.Threading.Atomic;
using Lucene.Net.Util;
using Microsoft.AspNetCore.WebUtilities;
using System;
using System.IO;
using System.Text.RegularExpressions;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    /** A pipe thread. It'd be nice to reuse guava's implementation for this... */
    class ThreadPumper
    {
        public static ThreadJob Start(ThreadJob onExit, TextReader from, TextWriter to, TextWriter toFile, AtomicBoolean nodeClosing)
        {
            ThreadJob t = new ThreadJob(() =>
            {
                try
                {
                    long startTimeNS = Time.NanoTime();
                    Regex logTimeStart = new Regex("^[0-9\\.]+s .*");
                    string line;
                    while ((line = from.ReadLine()) != null)
                    {
                        if (toFile != null)
                        {
                            toFile.Write(line);
                            toFile.Write("\n");
                            toFile.Flush();
                        }
                        else if (logTimeStart.IsMatch(line))
                        {
                            // Already a well-formed log output:
                            Console.WriteLine(line);
                        }
                        else
                        {
                            //TestStressNRTReplication.message(line, startTimeNS);
                        }
                        if (line.Contains("now force close server socket after"))
                        {
                            nodeClosing.Value = true;
                        }
                    }
                    // Sub-process finished
                }
                catch (IOException e)
                {
                    Console.Error.WriteLine("ignore IOExc reading from forked process pipe: " + e);
                }
                finally
                {
                    onExit.Run();
                }
            });
            t.Start();
            return t;
        }
    }
}

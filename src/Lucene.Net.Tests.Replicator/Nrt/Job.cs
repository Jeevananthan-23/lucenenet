using J2N.Threading;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Support.Threading;
using Lucene.Net.Util;
using System;
using System.Diagnostics;
using static Lucene.Net.Tests.Replicator.Nrt.SimpleCopyJob;

namespace Lucene.Net.Tests.Replicator.Nrt
{
    /** Runs CopyJob(s) in background thread; each ReplicaNode has an instance of this
 *  running.  At a given there could be one NRT copy job running, and multiple
 *  pre-warm merged segments jobs. */
    class Jobs : ThreadJob, IDisposable
    {

        private readonly PriorityQueue<CopyJob> queue = new SimpleCopyJobQueue(100);

        private readonly Node node;

        public Jobs(Node node)
        {
            this.node = node;
        }

        private bool finish;

        /** Returns null if we are closing, else, returns the top job or waits for one to arrive if the queue is empty. */
        private SimpleCopyJob GetNextJob()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                while (true)
                {
                    if (finish)
                    {
                        return null;
                    }
                    else if (queue.Count < 0)
                    {
                        try
                        {
                            Suspend();
                        }
                        catch (ThreadInterruptedException ie)
                        {
                            throw RuntimeException.Create(ie);
                        }
                    }
                    else
                    {
                        return (SimpleCopyJob)queue.Top; //poll here
                    }
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }
        public override void Run()
        {
            while (true)
            {
                SimpleCopyJob topJob = GetNextJob();
                if (topJob == null)
                {
                    Debug.Assert( finish);
                    break;
                }

                this.Name = ("jobs o" + topJob.ord);

                Debug.Assert( topJob != null);

                bool result;
                try
                {
                    result = topJob.Visit();
                }
                catch (Exception t)
                {
                    if ((t is AlreadyClosedException) == false)
                    {
                        node.Message("exception during job.visit job=" + topJob + "; now cancel");
                        t.printStackTrace(Console.Out);
                    }
                    else
                    {
                        node.Message("AlreadyClosedException during job.visit job=" + topJob + "; now cancel");
                    }
                    try
                    {
                        topJob.Cancel("unexpected exception in visit", t);
                    }
                    catch (Exception t2)
                    {
                        node.Message("ignore exception calling cancel: " + t2);
                        t2.printStackTrace(Console.Out);
                    }
                    try
                    {
                        topJob.onceDone.Run(topJob);
                    }
                    catch (Exception t2)
                    {
                        node.Message("ignore exception calling OnceDone: " + t2);
                        t2.printStackTrace(Console.Out);
                    }
                    continue;
                }

                if (result == false)
                {
                    // Job isn't done yet; put it back:
                    lock (this)
                    {
                        queue.Add(topJob);
                    }
                }
                else
                {
                    // Job finished, now notify caller:
                    try
                    {
                        topJob.onceDone.Run(topJob);
                    }
                    catch (Exception t)
                    {
                        node.Message("ignore exception calling OnceDone: " + t);
                        t.printStackTrace(Console.Out);
                    }
                }
            }

            node.Message("top: jobs now exit run thread");

            lock (this)
            {
                // Gracefully cancel any jobs we didn't finish:
                while (queue.Count > 0 == false)
                {
                    SimpleCopyJob job = (SimpleCopyJob)queue.Top;
                    node.Message("top: Jobs: now cancel job=" + job);
                    try
                    {
                        job.Cancel("jobs closing", null);
                    }
                    catch (Exception t)
                    {
                        node.Message("ignore exception calling cancel");
                        t.printStackTrace(Console.Out);
                    }
                    try
                    {
                        job.onceDone.Run(job);
                    }
                    catch (Exception t)
                    {
                        node.Message("ignore exception calling OnceDone");
                        t.printStackTrace(Console.Out);
                    }
                }
            }
        }

        public void Launch(CopyJob job)
        {
            if (finish == false)
            {
                queue.Add(job);
                Resume(); // perimity thread fuc
            }
            else
            {
                throw  AlreadyClosedException.Create("closed");
            }
        }

        /** Cancels any existing jobs that are copying the same file names as this one */
        public void CancelConflictingJobs(CopyJob newJob)
        {
            foreach (CopyJob job in queue.HeapArray)
            {
                if (job.Conflicts(newJob))
                {
                    node.Message("top: now cancel existing conflicting job=" + job + " due to newJob=" + newJob);
                    job.Cancel("conflicts with new job", null);
                }
            }
        }

        public void Dispose()
        {
            finish = true;
            Resume();
            try
            {
                Join();
            }
            catch (ThreadInterruptedException ie)
            {
                throw RuntimeException.Create(ie);
            }
        }
    }
}

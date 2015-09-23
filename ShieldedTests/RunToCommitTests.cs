using System;
using System.Threading;
using NUnit.Framework;
using Shielded;

namespace ShieldedTests
{
    [TestFixture]
    public class RunToCommitTests
    {
        [Test]
        public void BasicRunToCommit()
        {
            Shielded<int> a = new Shielded<int>(5);
            CommitContinuation cont = null;
            try
            {
                Shield.RunToCommit(out cont, () => {
                    if (a == 5)
                        a.Value = 20;
                });

                int runCount = 0, insideIfCount = 0;
                var t = new Thread(() => Shield.InTransaction(() => {
                    Interlocked.Increment(ref runCount);
                    if (a == 5) // this will block, and continue after the Commit call below
                    {
                        Interlocked.Increment(ref insideIfCount);
                        a.Value = 10;
                    }
                }));
                t.Start();
                Thread.Sleep(100);
                Assert.AreEqual(1, runCount);
                Assert.AreEqual(0, insideIfCount);

                cont.InContext(_ => Assert.AreEqual(20, a));
                Assert.AreEqual(5, a);
                var t2 = new Thread(cont.Commit);
                t2.Start();
                t2.Join();
                t.Join();
                Assert.AreEqual(1, runCount);
                Assert.AreEqual(0, insideIfCount);
                Assert.AreEqual(20, a);
            }
            finally
            {
                if (cont != null)
                    cont.Dispose();
            }
            Assert.AreEqual(20, a);
        }
    }
}


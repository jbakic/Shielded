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

                var t = new Thread(() => Shield.InTransaction(() => {
                    if (a == 5) // this will block, and retry only after the Commit call below
                        a.Value = 10;
                }));
                t.Start();
                Thread.Sleep(100);
                Assert.IsTrue(t.IsAlive);

                cont.InContext(_ => Assert.AreEqual(20, a));
                Assert.AreEqual(5, a);
                var t2 = new Thread(() => cont.Commit());
                t2.Start();
                t2.Join();
                t.Join();
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


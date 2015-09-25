using System;
using System.Threading;
using NUnit.Framework;
using Shielded;
using System.Diagnostics;

namespace ShieldedTests
{
    [TestFixture]
    public class RunToCommitTests
    {
        [Test]
        public void BasicRunToCommit()
        {
            var a = new Shielded<int>(5);
            using (var cont = Shield.RunToCommit(5000,
                () => {
                    if (a == 5)
                        a.Value = 20;
                }))
            {
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
            Assert.AreEqual(20, a);
        }

        [Test]
        public void RunToCommitAndFail()
        {
            var a = new Shielded<int>(5);
            using (Shield.WhenCommitting(_ => Shield.Rollback()))
            {
                using (var cont = Shield.RunToCommit(5000, () => a.Value = 10))
                {
                    // RunToCommit guarantees that Shielded will allow the commit, but cannot
                    // guarantee for any WhenCommitting subscriptions.
                    Assert.Throws<AggregateException>(cont.Commit);
                    Assert.IsTrue(cont.Completed);
                }
            }
        }

        [Test]
        public void RunToCommitTimeout()
        {
            var a = new Shielded<int>(5);
            bool rollback = false;
            using (var cont = Shield.RunToCommit(200, () => a.Value = 10))
            {
                Assert.IsFalse(cont.Completed);
                // we can still do stuff..
                cont.InContext(_ => {
                    a.Value = 20;
                    // NB that any exception thrown in onRollback would be unhandled in this example!
                    Shield.SideEffect(null, () => rollback = true);
                });

                var sw = Stopwatch.StartNew();
                // this causes a simple deadlock! the a field is fully locked, even reading blocks on it.
                // but the locks are released after 200 ms, and this transaction just continues..
                Shield.InTransaction(() => Assert.AreEqual(5, a));

                var time = sw.ElapsedMilliseconds;
                Assert.Greater(time, 150);
                Assert.Less(time, 250);
                // the continuation has been rolled back.
                Assert.IsTrue(cont.Completed);
                Assert.IsTrue(rollback);
            }
            Assert.AreEqual(5, a);
        }
    }
}


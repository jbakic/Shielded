using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;
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
            using (var cont = Shield.RunToCommit(5000, () => {
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

                cont.InContext(() => Assert.AreEqual(20, a));
                Assert.AreEqual(5, a);
                var t2 = new Thread(cont.Commit);
                t2.Start();
                t2.Join();
                t.Join();
                Assert.AreEqual(1, runCount);
                Assert.AreEqual(0, insideIfCount);
                Assert.AreEqual(20, a);

                Assert.IsFalse(cont.TryCommit());
                Assert.IsFalse(cont.TryRollback());
                Assert.IsTrue(cont.Committed);
            }
            Assert.AreEqual(20, a);
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

                Assert.IsFalse(cont.TryCommit());
                Assert.IsFalse(cont.TryRollback());
                Assert.IsFalse(cont.Committed);
            }
            Assert.AreEqual(5, a);
        }

        [Test]
        public void FieldsTest()
        {
            var a = new Shielded<int>();
            using (var continuation = Shield.RunToCommit(1000, () => { int _ = a.Value; }))
            {
                var fields = continuation.Fields;
                Assert.AreEqual(1, fields.Length);
                Assert.AreSame(a, fields[0].Field);
                Assert.IsFalse(fields[0].HasChanges);

                continuation.InContext(f =>
                    Assert.AreSame(fields, f));
            }
        }

        public class CustomException : Exception {}

        [Test]
        public void ExceptionWhenTryingToCommit()
        {
            var a = new Shielded<int>();
            using (Shield.WhenCommitting(a, _ => { throw new CustomException(); }))
            using (var cont = Shield.RunToCommit(5000, () => a.Value = 5))
            {
                var aggr = Assert.Throws<AggregateException>(() => cont.TryCommit());
                Assert.IsInstanceOf<CustomException>(aggr.InnerExceptions.Single());
            }
        }

        [Test]
        public void TryCommitRollbackAfterTimeout()
        {
            var a = new Shielded<int>();
            using (Shield.WhenCommitting(a, _ => { throw new CustomException(); }))
            using (var cont = Shield.RunToCommit(0, () => a.Value = 5))
            {
                Thread.Sleep(100);

                Assert.IsFalse(cont.TryCommit());
                Assert.Throws<ContinuationCompletedException>(cont.Commit);

                Assert.IsFalse(cont.TryRollback());
                Assert.Throws<ContinuationCompletedException>(cont.Rollback);

                Assert.Throws<ContinuationCompletedException>(() => { var _ = cont.Fields; });
                Assert.IsFalse(cont.TryInContext(() => { }));
                Assert.Throws<ContinuationCompletedException>(() => cont.InContext(() => { }));
                Assert.IsFalse(cont.TryInContext(fields => { }));
                Assert.Throws<ContinuationCompletedException>(() => cont.InContext(fields => { }));
            }
        }
    }
}

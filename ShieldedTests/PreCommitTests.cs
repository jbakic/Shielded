using System;
using NUnit.Framework;
using Shielded;
using System.Threading.Tasks;
using System.Linq;
using System.Threading;

namespace ShieldedTests
{
    [TestFixture]
    public class PreCommitTests
    {
        [Test]
        public void NoOdds()
        {
            var x = new Shielded<int>();

            int preCommitFails = 0;
            // we will not allow any odd number to be committed into x.
            Shield.PreCommit(() => (x.Read & 1) == 1, () => {
                Interlocked.Increment(ref preCommitFails);
                Shield.Rollback(false);
            });

            int transactionCount = 0;
            Task.WaitAll(
                Enumerable.Range(1, 100).Select(i => Task.Factory.StartNew(() =>
                {
                    Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref transactionCount);
                        int a = x;
                        Thread.Sleep(5);
                        x.Assign(a + i);
                    });
                }, TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(50, preCommitFails);
            Assert.AreEqual(2550, x);
            // just to confirm validity of test! not really a fail if this fails.
            Assert.Greater(transactionCount, 100);
        }
    }
}


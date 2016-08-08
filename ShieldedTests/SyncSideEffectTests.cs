using System;
using NUnit.Framework;
using Shielded;
using System.Threading;

namespace ShieldedTests
{
    [TestFixture]
    public class SyncSideEffectTests
    {
        [Test]
        public void BasicSyncSideEffect()
        {
            var a = new Shielded<int>();
            Shield.InTransaction(() => {
                a.Value = 10;
                Shield.SyncSideEffect(() => {
                    var t = new Thread(() => {
                        Assert.IsFalse(Shield.IsInTransaction);
                        // attempting to do this in a transaction would cause a deadlock!
                        Assert.AreEqual(0, a);
                    });
                    t.Start();
                    t.Join();
                    Assert.AreEqual(10, a);
                });
            });
        }
    }
}


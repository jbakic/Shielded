using System;
using NUnit.Framework;
using Shielded;
using Shielded.ProxyGen;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace ShieldedTests
{
    public class TestEntity
    {
        public virtual Guid Id { get; set; }
        public virtual int Counter { get; set; }

        public virtual string Name
        {
            get { return null; }
            set
            {
                if (Name == "conflicting")
                    // see test below.
                    Assert.AreEqual("testing conflict...", value);
                // this should be avoided! see ProxyCommuteTest for reason.
                NameChanges.Commute((ref int i) => i++);
            }
        }

        public readonly Shielded<int> NameChanges = new Shielded<int>();

        // by convention, if this exists, it gets overriden.
        public virtual void Commute(Action a)
        {
            a();
        }
    }

    [TestFixture]
    public class ProxyTests
    {
        [Test]
        public void BasicTest()
        {
            var test = Factory.NewShielded<TestEntity>();

            try
            {
                test.Id = Guid.NewGuid();
                Assert.Fail();
            }
            catch (InvalidOperationException) { }

            var id = Guid.NewGuid();
            Shield.InTransaction(() => test.Id = id);
            Assert.AreEqual(id, test.Id);

            Shield.InTransaction(() => {
                test.Id = Guid.Empty;

                var t = new Thread(() => {
                    Assert.AreEqual(id, test.Id);
                });
                t.Start();
                t.Join();

                Assert.AreEqual(Guid.Empty, test.Id);
            });
            Assert.AreEqual(Guid.Empty, test.Id);

            var t2 = new Thread(() => {
                Assert.AreEqual(Guid.Empty, test.Id);
            });
            t2.Start();
            t2.Join();

            int transactionCount = 0;
            Thread tConflict = null;
            Shield.InTransaction(() => {
                test.Name = "testing conflict...";
                transactionCount++;

                if (tConflict == null)
                {
                    tConflict = new Thread(() =>
                        // property setters are not commutable, and cause conflicts
                        Shield.InTransaction(() => test.Name = "conflicting"));
                    tConflict.Start();
                    tConflict.Join();
                }
            });
            Assert.AreEqual(2, transactionCount);
            Assert.AreEqual("testing conflict...", test.Name);
            // it was first "conflicting", then "testing conflict..."
            Assert.AreEqual(2, test.NameChanges);
        }

        [Test]
        public void ProxyCommuteTest()
        {
            var test = Factory.NewShielded<TestEntity>();

            int transactionCount = 0, commuteCount = 0;
            Task.WaitAll(Enumerable.Range(1, 100).Select(i => Task.Factory.StartNew(() => {
                Shield.InTransaction(() => {
                    Interlocked.Increment(ref transactionCount);
                    test.Commute(() => {
                        Interlocked.Increment(ref commuteCount);
                        test.Counter = test.Counter + i;
                    });
                });
            }, TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(5050, test.Counter);
            // commutes never conflict (!)
            Assert.AreEqual(100, transactionCount);
            Assert.Greater(commuteCount, 100);

            try
            {
                Shield.InTransaction(() => {
                    test.Commute(() => {
                        // this will throw, because it tries to run a commute on NameChanges, which
                        // is a separate shielded obj from the backing storage of the proxy.
                        // test.Commute can only touch the virtual fields, or non-shielded objects.
                        // a setter that commutes over another shielded field should be avoided!
                        test.Name = "something";
                        Assert.Fail();
                    });
                });
            }
            catch (InvalidOperationException) { }
        }
    }
}


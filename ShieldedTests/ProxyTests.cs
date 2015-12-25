using System;
using NUnit.Framework;
using Shielded;
using Shielded.ProxyGen;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using ShieldedTests.ProxyTestEntities;
using System.Reflection;
using ShieldedTests.ProxyTestEntities2;

namespace ShieldedTests
{
    public class TestEntity
    {
        public virtual Guid Id { get; set; }
        public virtual int Counter { get; set; }
        protected virtual int FullyProtected { get; set; }

        public void SetFullyProtected(int x)
        {
            // this is transactional as well.
            FullyProtected = x;
        }

        public virtual string Name
        {
            get { return null; }
            set
            {
                if (Name == "conflicting")
                    // see test below.
                    Assert.AreEqual("testing conflict...", value);
                // this should be avoided! see ProxyCommuteTest. basically, we could be
                // running within a commute, and Shielded throws if a commute touches anything
                // except its field. but changing other properties of this object is always safe.
                NameChanges.Commute((ref int i) => i++);
            }
        }

        public readonly Shielded<int> NameChanges = new Shielded<int>();
        public virtual int AnyPropertyChanges { get; set; }

        // by convention, if this exists, it gets overriden.
        public virtual void Commute(Action a) { a(); }

        // likewise, by convention, this gets called after a property changes. called
        // from commutes too, in which case it may not access any other shielded field.
        protected void OnChanged(string name)
        {
            if (name != "AnyPropertyChanges")
                AnyPropertyChanges += 1;
        }
    }

    public class BadEntity
    {
        // CodeDOM does not support overriding a property with different accessors on get and set,
        // so trying to make a proxy of this class will cause an exception.
        public virtual int X { get; protected set; }
    }

    [TestFixture]
    public class ProxyTests
    {
        [Test]
        public void BasicTest()
        {
            var test = Factory.NewShielded<TestEntity>();
            Assert.IsTrue(Factory.IsProxy(test.GetType()));

            Assert.Throws<InvalidOperationException>(() =>
                test.Id = Guid.NewGuid());

            var id = Guid.NewGuid();
            // the proxy object will, when changed, appear in the list of changed
            // fields in the Shield.WhenCommitting events...
            bool committingFired = false;
            using (Shield.WhenCommitting<TestEntity>(ents => committingFired = true))
            {
                Shield.InTransaction(() => test.Id = id);
            }

            Assert.IsTrue(committingFired);
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
            var newTest = Factory.NewShielded<TestEntity>();
            Shield.InTransaction(() => {
                newTest.Id = Guid.NewGuid();
                newTest.Name = "testing conflict...";
                transactionCount++;

                if (tConflict == null)
                {
                    tConflict = new Thread(() =>
                        // property setters are not commutable, and cause conflicts
                        Shield.InTransaction(() => newTest.Name = "conflicting"));
                    tConflict.Start();
                    tConflict.Join();
                }
            });
            Assert.AreEqual(2, transactionCount);
            Assert.AreEqual("testing conflict...", newTest.Name);
            // it was first "conflicting", then "testing conflict..."
            Assert.AreEqual(2, newTest.NameChanges);
            Assert.AreEqual(3, newTest.AnyPropertyChanges);
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
                        Thread.Sleep(1);
                    });
                });
            }, TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(5050, test.Counter);
            // commutes never conflict (!)
            Assert.AreEqual(100, transactionCount);
            Assert.Greater(commuteCount, 100);

            Assert.Throws<InvalidOperationException>(() =>
                Shield.InTransaction(() => {
                    test.Commute(() => {
                        // this will throw, because it tries to run a commute on NameChanges, which
                        // is a separate shielded obj from the backing storage of the proxy.
                        // test.Commute can only touch the virtual fields, or non-shielded objects.
                        // a setter that commutes over another shielded field should be avoided!
                        test.Name = "something";
                        Assert.Fail();
                    });
                }));
        }

        [Test]
        public void ProtectedSetterTest()
        {
            Assert.Throws<InvalidOperationException>(() => Factory.NewShielded<BadEntity>());

            var test = Factory.NewShielded<TestEntity>();
            Assert.Throws<InvalidOperationException>(() => test.SetFullyProtected(5));
            Shield.InTransaction(() => test.SetFullyProtected(5));
        }

        private void AssertTransactional<T>(IIdentifiable<T> item)
        {
            Assert.IsTrue(Factory.IsProxy(item.GetType()));
            Assert.Throws<InvalidOperationException>(() =>
                item.Id = default(T));

            bool detectable = false;
            using (Shield.WhenCommitting<IIdentifiable<T>>(ts => detectable = true))
                Shield.InTransaction(() => item.Id = default(T));
            Assert.IsTrue(detectable);
        }

        [Test]
        public void PreparationTest()
        {
            // first, let's get one before the preparation
            var e1 = Factory.NewShielded<Entity1>();
            AssertTransactional(e1);

            Factory.PrepareTypes(
                Assembly.GetExecutingAssembly().GetTypes()
                    .Where(t =>
                        t.Namespace != null &&
                        t.Namespace.StartsWith("ShieldedTests.ProxyTestEntities", StringComparison.Ordinal) &&
                        t.IsClass)
                    .ToArray());

            var e2 = Factory.NewShielded<Entity2>();
            AssertTransactional(e2);
            var e3 = Factory.NewShielded<Entity3>();
            AssertTransactional(e3);
            var e4 = Factory.NewShielded<Entity4>();
            AssertTransactional(e4);
        }
    }
}


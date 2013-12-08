using System;
using NUnit.Framework;
using Shielded;
using Shielded.ProxyGen;
using System.Threading;

namespace ShieldedTests
{
    public class Test
    {
        public virtual Guid Id { get; set; }
        public virtual string Name
        {
            get { return null; }
            set
            {
                if (Name == "conflicting")
                    // see test below.
                    Assert.AreEqual("testing conflict...", value);
                NameChanges.Commute((ref int i) => i++);
            }
        }

        public readonly Shielded<int> NameChanges = new Shielded<int>();
    }

    [TestFixture]
    public class ProxyTests
    {
        [Test]
        public void BasicTest()
        {
            var test = Factory.NewShielded<Test>();

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
    }
}


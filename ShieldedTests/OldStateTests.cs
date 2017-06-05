using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Shielded;
using Shielded.ProxyGen;
using ShieldedTests.ProxyTestEntities;

namespace ShieldedTests
{
    [TestFixture]
    public class OldStateTests
    {
        [Test]
        public void BasicTest()
        {
            var a = new Shielded<int>(10);
            Shield.InTransaction(() => {
                a.Value = 20;
                Assert.AreEqual(20, a);
                Shield.ReadOldState(() => {
                    Assert.AreEqual(10, a);
                    a.Value = 30;
                    Assert.AreEqual(10, a);
                    a.Modify((ref int x) =>
                        Assert.AreEqual(30, x));
                });
                Assert.AreEqual(30, a.Value);
            });
            Assert.AreEqual(30, a.Value);
        }

        [Test]
        public void DictionaryTest()
        {
            var dict = Shield.InTransaction(() => new ShieldedDict<int, int>() {
                { 1, 1 },
                { 2, 2 },
                { 3, 3 },
            });
            Shield.InTransaction(() => {
                Assert.AreEqual(1, dict[1]);
                dict[2] = 102;
                Assert.AreEqual(102, dict[2]);
                Shield.ReadOldState(() => {
                    Assert.AreEqual(1, dict[1]);
                    Assert.AreEqual(2, dict[2]);
                    var values = dict.Values;
                    Assert.AreEqual(3, values.Count);
                    Assert.IsFalse(values.Except(new[] { 1, 2, 3 }).Any());
                    dict[2] = 22;
                    Assert.AreEqual(2, dict[2]);
                });
                Assert.AreEqual(1, dict[1]);
                Assert.AreEqual(22, dict[2]);
            });
            Assert.AreEqual(1, dict[1]);
            Assert.AreEqual(22, dict[2]);
        }

        [Test]
        public void WhenCommittingWithProxy()
        {
            var a = Factory.NewShielded<Entity1>();
            var guid1 = Guid.NewGuid();
            Shield.InTransaction(() => {
                a.Id = guid1;
                a.Name = "Test";
            });

            var guid2 = Guid.NewGuid();
            using (Shield.WhenCommitting<Entity1>(es => {
                Shield.ReadOldState(() => {
                    Assert.AreEqual(guid1, a.Id);
                    Assert.AreEqual("Test", a.Name);
                });
                Assert.AreEqual(guid2, a.Id);
                Assert.AreEqual("New name", a.Name);
            }))
            {
                Shield.InTransaction(() => {
                    a.Id = guid2;
                    a.Name = "New name";
                });
            }
        }
    }
}


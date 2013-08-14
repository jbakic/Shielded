using System;
using NUnit.Framework;
using Shielded;
using System.Threading.Tasks;

namespace ShieldedTests
{
    [TestFixture()]
    public class Test
    {
        [Test()]
        public void BasicTest()
        {
            Shielded<int> a = new Shielded<int>(5);
            Assert.AreEqual(5, a.Read);

            Shield.InTransaction(() =>
            {
                a.Modify((ref int n) => n = 20);
                var x = Task<int>.Factory.StartNew(() => a.Read).Result;
                Assert.AreEqual(5, x);
                Assert.AreEqual(20, a.Read);
            });

            Assert.AreEqual(20, a.Read);
        }
    }
}


using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Shielded;
 
namespace ConsoleTests
{
    /// <summary>
    /// Testing speed of one-by-one ops.
    /// 
    /// Copied from https://gist.github.com/ayende/7738436, with the Immutable and
    /// LinkedDictionary tests removed, and ShieldedDict test added.
    /// </summary>
    class SequentialTests
    {
         
        public static void Run()
        {
            //warmup
            MutableDicAdd(10);
            SafeDicAdd(10);
            ShieldedDicAdd(10);
            Console.WriteLine();
             
            // actual test
            var d = MutableDicAdd(50 * 100);
            var sd = SafeDicAdd(50 * 100);
            var shd = ShieldedDicAdd(50 * 100);
             
            ReadItemsMutable(d, 10 * 1000);
            ReadItemsShielded(shd, 10 * 1000);
        }
         
        private static void ReadItemsMutable(Dictionary<long, object> ld, int iterations)
        {
            var sp = Stopwatch.StartNew();
             
            for (int i = 0; i < iterations; i++)
            {
                object value;
                ld.TryGetValue(i, out value);
            }
             
            Console.WriteLine(sp.Elapsed + " Reading values mutable dictionary");
        }
         
        private static void ReadItemsShielded(ShieldedDictNc<long, object> ld, int iterations)
        {
            var sp = Stopwatch.StartNew();
             
            for (int i = 0; i < iterations; i++)
            {
                object value;
                ld.TryGetValue(i, out value);
            }
             
            Console.WriteLine(sp.Elapsed + " Reading values Shielded dictionary");
        }
         
        private static Dictionary<long, object> MutableDicAdd(int iterations)
        {
            var tmp = new Dictionary<long, object>();
             
            var sp = Stopwatch.StartNew();
            var rnd = new Random(32);
            for (int i = 0; i < iterations; i++)
            {
                foreach (var item in Enumerable.Range(rnd.Next(0, i), Math.Max(i * 2, 16)))
                {
                    tmp[item] = null;
                }
            }
            Console.WriteLine(sp.Elapsed + " Adding items, mutable dictionary");
            return tmp;
        }
         
        private static Dictionary<long, object> SafeDicAdd(int iterations)
        {
            var dic = new Dictionary<long, object>();
             
            var sp = Stopwatch.StartNew();
            var rnd = new Random(32);
            for (int i = 0; i < iterations; i++)
            {
                var tmp = new Dictionary<long, object>();
                foreach (var item in Enumerable.Range(rnd.Next(0, i), Math.Max(i * 2, 16)))
                {
                    tmp[item] = null;
                }
                dic = new Dictionary<long, object>(dic);
                foreach (var o in tmp)
                {
                    dic[o.Key] = o.Value;
                }
            }
             
            Console.WriteLine(sp.Elapsed + " Adding items, safe dictionary");
             
            return dic;
        }

        private static ShieldedDictNc<long, object> ShieldedDicAdd(int iterations)
        {
            var tmp = new ShieldedDictNc<long, object>();

            var sp = Stopwatch.StartNew();
            var rnd = new Random(32);
            for (int i = 0; i < iterations; i++)
            {
                Shield.InTransaction(() => {
                    foreach (var item in Enumerable.Range(rnd.Next(0, i), Math.Max(i * 2, 16)))
                    {
                        tmp[item] = null;
                    }
                });
            }
            Console.WriteLine(sp.Elapsed + " Adding items, Shielded dictionary");
            return tmp;
        }
    }
}
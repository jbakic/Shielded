using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using Shielded;
using Shielded.ProxyGen;

namespace ConsoleTests
{
    public class SimpleEntity
    {
        public virtual int Id { get; set; }
        public virtual void Commute(Action a) { a(); }
    }

    public class SmallEntity
    {
        public virtual int Id { get; set; }
        public virtual int Value { get; set; }
    }

    class MainClass
    {
        private static Stopwatch _timer;

        public static long mtTest(string name, int taskCount, Func<int, Task> task)
        {
            if (_timer == null)
                _timer = Stopwatch.StartNew();

            long time;
            Console.Write("Test - {0}...", name);
            time = _timer.ElapsedMilliseconds;
            Task.WaitAll(
                Enumerable.Range(0, taskCount)
                    .Select(task)
                    .ToArray());
            time = _timer.ElapsedMilliseconds - time;
            return time;
        }

        public static void TimeTests()
        {
            var randomizr = new Random();
            int transactionCounter;
            int sleepTime = 1;
            int taskCount = 10000;

            // a little warm up for Shielded
            var warmUp = new Shielded<int>();
            Shield.InTransaction(() => warmUp.Value = warmUp + 1);

            foreach (var i in Enumerable.Repeat(0, 5))
            {
                var x = new int[100];
                transactionCounter = 0;
                var time = mtTest("dirty write", taskCount, _ =>
                {
                    var rnd = randomizr.Next(100);
                    return Task.Factory.StartNew(() =>
                    {
                        Interlocked.Increment(ref transactionCounter);
                        int v = x[rnd];
                        if (sleepTime > 0) Thread.Sleep(sleepTime);
                        x[rnd] = v + 1;
                    },
                    sleepTime > 0 ? TaskCreationOptions.LongRunning : TaskCreationOptions.None
                    );
                });
                var correct = x.Sum() == taskCount;
                Console.WriteLine(" {0} ms with {1} iterations and is {2}.",
                    time, transactionCounter, correct ? "correct" : "incorrect");
            }

            var lockCount = 100;
            foreach (var i in Enumerable.Repeat(0, 5))
            {
                var x = new int[100];
                transactionCounter = 0;
                var l = Enumerable.Repeat(0, lockCount).Select(_ => new object()).ToArray();
                var time = mtTest(string.Format("{0} lock write", lockCount), taskCount, _ =>
                {
                    var rnd = randomizr.Next(100);
                    return Task.Factory.StartNew(() =>
                    {
                        lock (l[rnd % lockCount])
                        {
                            Interlocked.Increment(ref transactionCounter);
                            int v = x[rnd];
                            if (sleepTime > 0) Thread.Sleep(sleepTime);
                            x[rnd] = v + 1;
                        }
                    },
                    sleepTime > 0 ? TaskCreationOptions.LongRunning : TaskCreationOptions.None
                    );
                });
                var correct = x.Sum() == taskCount;
                Console.WriteLine(" {0} ms with {1} iterations and is {2}.",
                    time, transactionCounter, correct ? "correct" : "incorrect");
            }

            foreach (var i in Enumerable.Repeat(0, 5))
            {
                var shx = Enumerable.Repeat(0, 100).Select(n => new Shielded<int>(n)).ToArray();
                transactionCounter = 0;
                var time = mtTest("shielded2 write", taskCount, _ =>
                {
                    var rnd = randomizr.Next(100);
                    return Task.Factory.StartNew(() =>
                    {
                        Shield.InTransaction(() =>
                        {
                            Interlocked.Increment(ref transactionCounter);
                            int v = shx[rnd];
                            if (sleepTime > 0) Thread.Sleep(sleepTime);
                            shx[rnd].Value = v + 1;
                        });
                    },
                    sleepTime > 0 ? TaskCreationOptions.LongRunning : TaskCreationOptions.None
                    );
                });
                var correct = shx.Sum(s => s.Value) == taskCount;
                Console.WriteLine(" {0} ms with {1} iterations and is {2}.",
                    time, transactionCounter, correct ? "correct" : "incorrect");
            }
        }

        static void ParallelAddWithSaving()
        {
            var randomizr = new Random();
            int transactionCounter;
            int taskCount = 1000;
            int sleepTime = 1;
            int commitEventHit = 0;
            SmallEntity[] shx = null;
            int[] shxClone = null;

            using (Shield.WhenCommitting<SmallEntity>(ents => {
                foreach (var ent in ents)
                {
                    Thread.Sleep((ent.Value * Interlocked.Increment(ref commitEventHit) * 31) & 0x3F);
                    shxClone[ent.Id] = ent.Value;
                }
            }))
            {
                foreach (var i in Enumerable.Repeat(0, 10))
                {
                    shxClone = new int[100];
                    shx = Enumerable.Repeat(0, 100)
                        .Select((n, ind) => {
                        var ent = Factory.NewShielded<SmallEntity>();
                        Shield.InTransaction(() => {
                            ent.Id = ind;
                        });
                        return ent;
                    }).ToArray();
                    transactionCounter = 0;
                    commitEventHit = 0;
                    var time = mtTest("write with saving", taskCount, _ => {
                        var rnd = randomizr.Next(100);
                        return Task.Factory.StartNew(() => Shield.InTransaction(() => {
                            Interlocked.Increment(ref transactionCounter);
                            int v = shx[rnd].Value;
                            if (sleepTime > 0)
                                Thread.Sleep(sleepTime);
                            shx[rnd].Value = v + 1;
                        }), TaskCreationOptions.LongRunning);
                    });
                    var correct = shx.Sum(s => s.Value) == taskCount &&
                                  shx.All(s => s.Value == 0 || shxClone[s.Id] == s.Value);
                    Console.WriteLine(" {0} ms - {1} reps, {2} commits, {3}.",
                        time, transactionCounter, commitEventHit, correct ? "correct" : "incorrect");
                }
            }
        }

        static void OneTransaction()
        {
            Shielded<int> sh = new Shielded<int>();
            Shield.InTransaction(() =>
            {
                int x = sh;
                Console.WriteLine("Value: {0}", x);
                sh.Modify((ref int a) => a = x + 1);
                Console.WriteLine("Value after increment: {0}", sh.Value);
            });
        }


        struct Account
        {
            public int Id;
            public decimal Balance;
            // beware - copies of this struct share this reference!
            public List<Transfer> Transfers;
        }

        struct Transfer
        {
            public int OtherId;
            public decimal AmountReceived;
        }

        static void ControlledRace()
        {
            var acc1 = new Shielded<Account>(new Account()
            {
                Id = 1,
                Balance = 1000M,
                Transfers = new List<Transfer>()
            });
            var acc2 = new Shielded<Account>(new Account()
            {
                Id = 2,
                Balance = 1000M,
                Transfers = new List<Transfer>()
            });
            int transactionCount = 0;

            mtTest("controlled race", 20, n =>
            {
                if (n % 2 == 0)
                    return Task.Factory.StartNew(() =>
                    {
                        Shield.InTransaction(() =>
                        {
                            Interlocked.Increment(ref transactionCount);
                            Shield.SideEffect(() => Console.WriteLine("Transferred 100.00 .. acc1 -> acc2"),
                                () => Console.WriteLine("Task 1 rollback!"));
                            acc1.Modify((ref Account a) =>
                            {
                                a.Balance = a.Balance - 100M;
                                var list = a.Transfers;
                                Shield.SideEffect(() => list.Add(
                                    new Transfer() { OtherId = acc2.Value.Id, AmountReceived = -100M }));
                            });
                            Thread.Sleep(100);
                            acc2.Modify((ref Account a) =>
                            {
                                a.Balance = a.Balance + 100M;
                                var list = a.Transfers;
                                Shield.SideEffect(() => list.Add(
                                    new Transfer() { OtherId = acc1.Value.Id, AmountReceived = 100M }));
                            });
                        });
                    }, TaskCreationOptions.LongRunning);
                else
                    return Task.Factory.StartNew(() =>
                    {
                        Shield.InTransaction(() =>
                        {
                            Interlocked.Increment(ref transactionCount);
                            Shield.SideEffect(() => Console.WriteLine("Transferred 200.00 .. acc1 <- acc2"),
                                () => Console.WriteLine("Task 2 rollback!"));
                            acc2.Modify((ref Account a) =>
                            {
                                a.Balance = a.Balance - 200M;
                                var list = a.Transfers;
                                Shield.SideEffect(() => list.Add(
                                    new Transfer() { OtherId = acc1.Value.Id, AmountReceived = -200M }));
                            });
                            Thread.Sleep(250);
                            acc1.Modify((ref Account a) =>
                            {
                                a.Balance = a.Balance + 200M;
                                var list = a.Transfers;
                                Shield.SideEffect(() => list.Add(
                                    new Transfer() { OtherId = acc2.Value.Id, AmountReceived = 200M }));
                            });
                        });
                    }, TaskCreationOptions.LongRunning);
            });
            Console.WriteLine("\nCompleted 20 transactions in {0} total attempts.", transactionCount);
            Console.WriteLine("Account 1 balance: {0}", acc1.Value.Balance);
            foreach (var t in acc1.Value.Transfers)
            {
                Console.WriteLine("  {0:####,00}", t.AmountReceived);
            }
            Console.WriteLine("\nAccount 2 balance: {0}", acc2.Value.Balance);
            foreach (var t in acc2.Value.Transfers)
            {
                Console.WriteLine("  {0:####,00}", t.AmountReceived);
            }
        }

        private static void DictionaryTest()
        {
            var dict = new ShieldedDictNc<int, Shielded<int>>();
            var randomizr = new Random();
            while (true)
            {
                var transactionCounter = 0;
                var time = mtTest("dictionary", 10000, i =>
                {
                    var rnd = randomizr.Next(10);
                    if (i % 2 == 0)
                        // adder task - 500 of these
                        return Task.Factory.StartNew(() =>
                        {
                            Shield.InTransaction(() =>
                            {
                                Interlocked.Increment(ref transactionCounter);
                                var v = dict.ContainsKey(rnd) ? dict[rnd] : null;
                                int? num = v != null ? (int?)v.Value : null;
                                Thread.Sleep(1);
                                if (v == null)
                                    dict[rnd] = new Shielded<int>(1);
                                else if (v.Value == -1)
                                    dict.Remove(rnd);
                                else
                                    v.Modify((ref int a) => a = num.Value + 1);
                            }
                            );
                        },
                        TaskCreationOptions.LongRunning
                        );
                    else
                        // subtractor task - 500 of these
                        return Task.Factory.StartNew(() =>
                        {
                            Shield.InTransaction(() =>
                            {
                                Interlocked.Increment(ref transactionCounter);
                                var v = dict.ContainsKey(rnd) ? dict[rnd] : null;
                                int? num = v != null ? (int?)v.Value : null;
                                Thread.Sleep(1);
                                if (v == null)
                                    dict[rnd] = new Shielded<int>(-1);
                                else if (v.Value == 1)
                                    dict.Remove(rnd);
                                else
                                    v.Modify((ref int a) => a = num.Value - 1);
                            }
                            );
                        },
                        TaskCreationOptions.LongRunning
                        );
                });
                var sum = Enumerable.Range(0, 10).Sum(n => dict.ContainsKey(n) ? dict[n] : 0);
                var zeroes = Shield.InTransaction(() => dict.Any(kvp => kvp.Value == 0));
                Console.WriteLine(" {0} ms with {1} iterations and sum {2}, {3}",
                    time, transactionCounter, sum, zeroes ? "with zeroes!" : "no zeroes.");
            }
        }

        /// <summary>
        /// Creates a BetShop, and tries to buy a large number of random tickets. Afterwards it
        /// checks that the rule limiting same ticket winnings is not violated.
        /// </summary>
        public static void BetShopTest()
        {
            int numEvents = 100;
            var betShop = new BetShop(numEvents);
            var randomizr = new Random();
            int reportEvery = 1000;
            var lastReport = new Shielded<int>(0);
            var lastTime = new Shielded<DateTime>(DateTime.UtcNow);
            long time;

            using (var reportingCond = Shield.Conditional(
                () => betShop.Tickets.Count >= lastReport + reportEvery,
                () => {
                    DateTime newNow = DateTime.UtcNow;
                    int count = betShop.Tickets.Count;
                    int speed = (count - lastReport) * 1000 / (int)newNow.Subtract(lastTime).TotalMilliseconds;
                    lastTime.Value = newNow;
                    lastReport.Modify((ref int n) => n += reportEvery);
                    Shield.SideEffect(() =>
                    {
                        Console.Write("\n{0} at {1} item/s", count, speed);
                    });
                }))
            {
                time = mtTest("bet shop w/ " + numEvents, 50000, i =>
                {
                    decimal payIn = (randomizr.Next(10) + 1m) * 1;
                    int event1Id = randomizr.Next(numEvents) + 1;
                    int event2Id = randomizr.Next(numEvents) + 1;
                    int event3Id = randomizr.Next(numEvents) + 1;
                    int offer1Ind = randomizr.Next(3);
                    int offer2Ind = randomizr.Next(3);
                    int offer3Ind = randomizr.Next(3);
                    return Task.Factory.StartNew(() => Shield.InTransaction(() =>
                    {
                        var offer1 = betShop.Events[event1Id].BetOffers[offer1Ind];
                        var offer2 = betShop.Events[event2Id].BetOffers[offer2Ind];
                        var offer3 = betShop.Events[event3Id].BetOffers[offer3Ind];
                        betShop.BuyTicket(payIn, offer1, offer2, offer3);
                    }));
                });
            }
            //}

            var totalCorrect = betShop.VerifyTickets();
            Console.WriteLine(" {0} ms with {1} tickets paid in and is {2}.",
                time, betShop.Tickets.Count, totalCorrect ? "correct" : "incorrect");
        }

        public static void BetShopPoolTest()
        {
            int numThreads = 3;
            int numTickets = 200000;
            int numEvents = 100;
            var barrier = new Barrier(2);
            var betShop = new BetShop(numEvents);
            var randomizr = new Random();

            var bags = new List<Action>[numThreads];
            var threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++)
            {
                var bag = bags[i] = new List<Action>();
                threads[i] = new Thread(() => {
                    foreach (var a in bag)
                        a();
                });
            }

            var complete = new Shielded<int>();
            IDisposable completeCond = null;
            completeCond = Shield.Conditional(() => complete == numTickets, () => {
                barrier.SignalAndWait();
                completeCond.Dispose();
            });

            var reportEvery = 10000;
            Shielded<int> lastReport = new Shielded<int>(0);
            Shielded<DateTime> lastTime = new Shielded<DateTime>(DateTime.UtcNow);
            using (Shield.Conditional(
                () => betShop.Tickets.Count >= lastReport + reportEvery,
                () => {
                    DateTime newNow = DateTime.UtcNow;
                    int count = betShop.Tickets.Count;
                    int speed = (count - lastReport) * 1000 / (int)newNow.Subtract(lastTime).TotalMilliseconds;
                    lastTime.Value = newNow;
                    lastReport.Modify((ref int n) => n += reportEvery);
                    Shield.SideEffect(() =>
                            Console.Write("\n{0} at {1} item/s", count, speed));
                }))
            {
                foreach (var i in Enumerable.Range(0, numTickets))
                {
                    decimal payIn = (randomizr.Next(10) + 1m) * 1;
                    int event1Id = randomizr.Next(numEvents) + 1;
                    int event2Id = randomizr.Next(numEvents) + 1;
                    int event3Id = randomizr.Next(numEvents) + 1;
                    int offer1Ind = randomizr.Next(3);
                    int offer2Ind = randomizr.Next(3);
                    int offer3Ind = randomizr.Next(3);
                    bags[i % numThreads].Add(() => Shield.InTransaction(() => {
                        var offer1 = betShop.Events[event1Id].BetOffers[offer1Ind];
                        var offer2 = betShop.Events[event2Id].BetOffers[offer2Ind];
                        var offer3 = betShop.Events[event3Id].BetOffers[offer3Ind];
                        betShop.BuyTicket(payIn, offer1, offer2, offer3);
                        complete.Commute((ref int n) => n++);
                    }));
                }
                _timer = Stopwatch.StartNew();
                for (int i = 0; i < numThreads; i++)
                    threads[i].Start();
                barrier.SignalAndWait();
            }

            var time = _timer.ElapsedMilliseconds;
            var totalCorrect = betShop.VerifyTickets();
            Console.WriteLine(" {0} ms with {1} tickets paid in and is {2}.",
                time, betShop.Tickets.Count, totalCorrect ? "correct" : "incorrect");
        }

        class TreeItem
        {
            public Guid Id = Guid.NewGuid();
        }

        public static void TreePoolTest()
        {
            int numThreads = Environment.ProcessorCount;
            int numItems = 1000000;
            var tree = new ShieldedTree<Guid, TreeItem>();
            var barrier = new Barrier(numThreads + 1);
            var counter = 0;
            int reportEvery = 10000;
            var lastReport = 0;
            long time;

            TreeItem x = new TreeItem();

            _timer = new Stopwatch();
            _timer.Start();
            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => {
                    var a = x.Id;
                });
            time = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("Empty transactions in {0} ms.", time);

            var bags = new List<Action>[numThreads];
            var threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++)
            {
                var bag = bags[i] = new List<Action>();
                threads[i] = new Thread(() => {
                    foreach (var a in bag)
                    {
                        try
                        {
                            a();
                        }
                        catch
                        {
                            Console.Write(" * ");
                        }
                    }
                    barrier.SignalAndWait();
                });
            }

            var lastTime = _timer.ElapsedMilliseconds;
            foreach (var i in Enumerable.Range(0, numItems))
            {
                var item1 = new TreeItem();
                bags[i % numThreads].Add(() => Shield.InTransaction(() => {
                    tree.Add(item1.Id, item1);
                    Shield.SideEffect(() => {
                        var last = lastReport;
                        var count = Interlocked.Increment(ref counter);
                        var newNow = _timer.ElapsedMilliseconds;
                        if (count > last + reportEvery &&
                            Interlocked.CompareExchange(ref lastReport, last + reportEvery, last) == last)
                        {
                            var speed = reportEvery * 1000 / (newNow - lastTime);
                            lastTime = newNow; // risky, but safe ;)
                            Console.Write("\n{0} at {1} item/s", last + reportEvery, speed);
                        }
                    });
                }));
            }
            lastTime = _timer.ElapsedMilliseconds;
            for (int i = 0; i < numThreads; i++)
                threads[i].Start();

            barrier.SignalAndWait();
            time = _timer.ElapsedMilliseconds;
            Console.WriteLine(" {0} ms.", time);

            Console.WriteLine("\nReading sequentially...");
            time = _timer.ElapsedMilliseconds;
            var keys = Shield.InTransaction(() => tree.Keys);
            time = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("Keys read in {0} ms.", time);

            time = _timer.ElapsedMilliseconds;
            Shield.InTransaction(() => {
                foreach (var kvp in tree)
                    x = kvp.Value;
            });
            time = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("Items read by enumerator in {0} ms.", time);

            time = _timer.ElapsedMilliseconds;
            Shield.InTransaction(() => {
                foreach (var kvp in tree.OrderBy(kvp => kvp.Key))
                    x = kvp.Value;
            });
            time = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("Items read by sorted enumerator in {0} ms.", time);

            time = _timer.ElapsedMilliseconds;
            Shield.InTransaction(() => {
                foreach (var k in keys)
                    x = tree[k];
            });
            time = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("Items read by key in one trans in {0} ms.", time);

            time = _timer.ElapsedMilliseconds;
            foreach (var k in keys)
                x = tree[k];
            time = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("Items read by key separately in {0} ms.", time);

            time = _timer.ElapsedMilliseconds;
            keys.AsParallel().ForAll(k => x = tree[k]);
            time = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("Items read by key in parallel in {0} ms.", time);

            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => { var a = x.Id; });
            time = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("Empty transactions in {0} ms.", time);
        }

        static long Timed(string name, int numRepeats, Action act)
        {
            var time = _timer.ElapsedMilliseconds;
            act();
            time = _timer.ElapsedMilliseconds - time;
            var front = string.Format("{0} in {1} ms, ", name, time);
            Console.WriteLine("{0}{2}cost {1} us per rep.", front, time * 1000.0 / numRepeats,
                string.Join(string.Empty, Enumerable.Repeat("\t", Math.Max(0, 5 - (front.Length / 8)))));
            return time;
        }

        static void SimpleOps()
        {
            long time;
            _timer = Stopwatch.StartNew();
            var numItems = 200000;
            var repeatsPerTrans = 100;

            Console.WriteLine(
                "Testing simple ops with {0} iterations, and repeats per trans (N) = {1}",
                numItems, repeatsPerTrans);

            var accessTest = new Shielded<int>();

            Timed("WARM UP", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => {
                        accessTest.Value = 3;
                        var a = accessTest.Value;
                        accessTest.Modify((ref int n) => n = 5);
                        a = accessTest.Value;
                    });
            });

            var emptyTime = Timed("empty transactions", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => { });
            });

            var emptyReturningTime = Timed("1 out-of-t. read w/ result", numItems, () => {
                // this version uses the generic, result-returning InTransaction, which involves creation
                // of a closure, i.e. an allocation.
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => 5);
            });


            // the purpose here is to get a better picture of the expense of using Shielded. a more
            // complex project would probably, during one transaction, repeatedly access the same
            // field. does this cost much more than a single-access transaction? if it is the same
            // field, then any significant extra expense is unacceptable.

            var oneReadTime = Timed("1-read transactions", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => {
                        var a = accessTest.Value;
                    });
            });

            var nReadTime = Timed("N-reads transactions", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => {
                        int a;
                        for (int i = 0; i < repeatsPerTrans; i++)
                            a = accessTest.Value;
                    });
            });

            var oneReadModifyTime = Timed("1-read-1-modify tr.", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => {
                        var a = accessTest.Value;
                        accessTest.Modify((ref int n) => n = 1);
                    });
            });

            // Assign is no longer commutable, for performance reasons. It is faster,
            // particularly when repeated (almost 10 times), and you can see the difference
            // that not reading the old value does.
            var oneReadAssignTime = Timed("1-read-1-assign tr.", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => {
                        var a = accessTest.Value;
                        accessTest.Value = 1;
                    });
            });


            var oneModifyTime = Timed("1-modify transactions", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => accessTest.Modify((ref int n) => n = 1));
            });

            var nModifyTime = Timed("N-modify transactions", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => {
                        for (int i = 0; i < repeatsPerTrans; i++)
                            accessTest.Modify((ref int n) => n = 1);
                    });
            });

            var accessTest2 = new Shielded<int>();
            var modifyModifyTime = Timed("modify-modify transactions", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => {
                        accessTest.Modify((ref int n) => n = 1);
                        accessTest2.Modify((ref int n) => n = 2);
                    });
            });

            // here Modify is the first call, making all Reads as fast as can be,
            // reading direct from local storage.
            var oneModifyNReadTime = Timed("1-modify-N-reads tr.", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => {
                        accessTest.Modify((ref int n) => n = 1);
                        int a;
                        for (int i = 0; i < repeatsPerTrans; i++)
                            a = accessTest.Value;
                    });
            });


            var oneAssignTime = Timed("1-assign transactions", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => accessTest.Value = 1);
            });

            var nAssignTime = Timed("N-assigns transactions", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => {
                        for (int i = 0; i < repeatsPerTrans; i++)
                            accessTest.Value = 1;
                    });
            });


            var oneCommuteTime = Timed("1-commute transactions", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => accessTest.Commute((ref int n) => n = 1));
            });

            var nCommuteTime = Timed("N-commute transactions", numItems, () => {
                foreach (var k in Enumerable.Repeat(1, numItems))
                    Shield.InTransaction(() => {
                        for (int i = 0; i < repeatsPerTrans; i++)
                            accessTest.Commute((ref int n) => n = 1);
                    });
            });


            Console.WriteLine("\ncost of empty transaction = {0:0.000} us", emptyTime / (numItems / 1000.0));
            Console.WriteLine("cost of the closure in InTransaction<T> = {0:0.000} us",
                              (emptyReturningTime - emptyTime) / (numItems / 1000.0));
            Console.WriteLine("cost of the first read = {0:0.000} us",
                              (oneReadTime - emptyTime) / (numItems / 1000.0));
            Console.WriteLine("cost of an additional read = {0:0.000} us",
                              (nReadTime - oneReadTime) / ((repeatsPerTrans - 1) * numItems / 1000.0));
            Console.WriteLine("cost of Modify after read = {0:0.000} us",
                              (oneReadModifyTime - oneReadTime) / (numItems / 1000.0));
            Console.WriteLine("cost of Assign after read = {0:0.000} us",
                              (oneReadAssignTime - oneReadTime) / (numItems / 1000.0));
            Console.WriteLine("cost of the first Modify = {0:0.000} us",
                              (oneModifyTime - emptyTime) / (numItems / 1000.0));
            Console.WriteLine("cost of an additional Modify = {0:0.000} us",
                              (nModifyTime - oneModifyTime) / ((repeatsPerTrans - 1) * numItems / 1000.0));
            Console.WriteLine("cost of a second, different Modify = {0:0.000} us",
                              (modifyModifyTime - oneModifyTime) / (numItems / 1000.0));
            Console.WriteLine("cost of a Value after Modify = {0:0.000} us",
                              (oneModifyNReadTime - oneModifyTime) / (repeatsPerTrans * numItems / 1000.0));
            Console.WriteLine("cost of the first Assign = {0:0.000} us",
                              (oneAssignTime - emptyTime) / (numItems / 1000.0));
            Console.WriteLine("cost of an additional Assign = {0:0.000} us",
                              (nAssignTime - oneAssignTime) / ((repeatsPerTrans - 1) * numItems / 1000.0));
            Console.WriteLine("cost of the first commute = {0:0.000} us",
                              (oneCommuteTime - emptyTime) / (numItems / 1000.0));
            Console.WriteLine("cost of an additional commute = {0:0.000} us",
                              (nCommuteTime - oneCommuteTime) / ((repeatsPerTrans - 1) * numItems / 1000.0));
        }

        public static void SimpleProxyOps()
        {
            var entity = Factory.NewShielded<SimpleEntity>();

            long time;
            _timer = Stopwatch.StartNew();
            var numItems = 200000;
            var repeatsPerTrans = 100;

            Console.WriteLine(
                "Testing simple proxy ops with {0} iterations, and repeats per trans (N) = {1}",
                numItems, repeatsPerTrans);

            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => {
                    var a = entity.Id;
                    entity.Id = 3;
                });
            time = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("WARM UP in {0} ms.", time);


            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => { });
            var emptyTime = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("empty transactions in {0} ms.", emptyTime);

            // this version uses the generic, result-returning InTransaction, which involves creation
            // of a closure, i.e. an allocation.
            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => 5);
            var emptyReturningTime = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("1 non-transactional read w/ returning result in {0} ms.", emptyReturningTime);


            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => { var a = entity.Id; });
            var oneReadTime = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("1-read transactions in {0} ms.", oneReadTime);

            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => {
                    int a;
                    for (int i = 0; i < repeatsPerTrans; i++)
                        a = entity.Id;
                });
            var nReadTime = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("N-reads transactions in {0} ms.", nReadTime);

            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => {
                    var a = entity.Id;
                    entity.Id = 1;
                });
            var oneReadWriteTime = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("1-read-1-write transactions in {0} ms.", oneReadWriteTime);

            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => entity.Id = 1);
            var oneWriteTime = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("1-write transactions in {0} ms.", oneWriteTime);

            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => {
                    for (int i = 0; i < repeatsPerTrans; i++)
                        entity.Id = 1;
                });
            var nWriteTime = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("N-write transactions in {0} ms.", nWriteTime);

            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => {
                    entity.Id = 2;
                    int a;
                    for (int i = 0; i < repeatsPerTrans; i++)
                        a = entity.Id;
                });
            var oneWriteNReadTime = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("1-write-N-reads transactions in {0} ms.", oneWriteNReadTime);


            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => entity.Commute(() => entity.Id = 1));
            var oneCommuteTime = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("1-commute transactions in {0} ms.", oneCommuteTime);

            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numItems))
                Shield.InTransaction(() => {
                    for (int i = 0; i < repeatsPerTrans; i++)
                        entity.Commute(() => entity.Id = 1);
                });
            var nCommuteTime = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("N-commute transactions in {0} ms.", nCommuteTime);


            Console.WriteLine("\ncost of empty transaction = {0:0.000} us", emptyTime / (numItems / 1000.0));
            Console.WriteLine("cost of the closure in InTransaction<T> = {0:0.000} us",
                              (emptyReturningTime - emptyTime) / (numItems / 1000.0));
            Console.WriteLine("cost of the first read = {0:0.000} us",
                              (oneReadTime - emptyTime) / (numItems / 1000.0));
            Console.WriteLine("cost of an additional read = {0:0.000} us",
                              (nReadTime - oneReadTime) / ((repeatsPerTrans - 1) * numItems / 1000.0));
            Console.WriteLine("cost of write after read = {0:0.000} us",
                              (oneReadWriteTime - oneReadTime) / (numItems / 1000.0));
            Console.WriteLine("cost of the first write = {0:0.000} us",
                              (oneWriteTime - emptyTime) / (numItems / 1000.0));
            Console.WriteLine("cost of an additional write = {0:0.000} us",
                              (nWriteTime - oneWriteTime) / ((repeatsPerTrans - 1) * numItems / 1000.0));
            Console.WriteLine("cost of a read after write = {0:0.000} us",
                              (oneWriteNReadTime - oneWriteTime) / (repeatsPerTrans * numItems / 1000.0));
            Console.WriteLine("cost of the first commute = {0:0.000} us",
                              (oneCommuteTime - emptyTime) / (numItems / 1000.0));
            Console.WriteLine("cost of an additional commute = {0:0.000} us",
                              (nCommuteTime - oneCommuteTime) / ((repeatsPerTrans - 1) * numItems / 1000.0));
        }

        public static void MultiFieldOps()
        {
            long time;
            _timer = Stopwatch.StartNew();
            var numTrans = 100000;
            var fields = 20;

            Console.WriteLine(
                "Testing multi-field ops with {0} iterations, and nuber of fields (N) = {1}",
                numTrans, fields);

            var accessTest = new Shielded<int>[fields];
            for (int i = 0; i < fields; i++)
                accessTest[i] = new Shielded<int>();
            var dummy = new Shielded<int>();

            time = _timer.ElapsedMilliseconds;
            foreach (var k in Enumerable.Repeat(1, numTrans))
                Shield.InTransaction(() => {
                    dummy.Value = 3;
                    var a = dummy.Value;
                    dummy.Modify((ref int n) => n = 5);
                    a = dummy.Value;
                });
            time = _timer.ElapsedMilliseconds - time;
            Console.WriteLine("WARM UP in {0} ms.", time);


            var results = new long[fields];
            foreach (var i in Enumerable.Range(0, fields))
            {
                time = _timer.ElapsedMilliseconds;
                foreach (var k in Enumerable.Repeat(1, numTrans))
                    Shield.InTransaction(() => {
                        for (int j = 0; j <= i; j++)
                            accessTest[j].Modify((ref int n) => n = 1);
                    });
                results[i] = _timer.ElapsedMilliseconds - time;
                Console.WriteLine("{0} field modifiers in {1} ms.", i + 1, results[i]);
            }
        }

        public static void TreeTest()
        {
            int numTasks = 100000;
            int reportEvery = 1000;

            ShieldedTree<Guid, TreeItem> tree = new ShieldedTree<Guid, TreeItem>();
            int transactionCount = 0;
            Shielded<int> lastReport = new Shielded<int>(0);
            Shielded<int> countComplete = new Shielded<int>(0);
//            Shielded<DateTime> lastTime = new Shielded<DateTime>(DateTime.UtcNow);
//
//            Shield.Conditional(() => countComplete >= lastReport + reportEvery, () =>
//            {
//                DateTime newNow = DateTime.UtcNow;
//                int speed = (countComplete - lastReport) * 1000 / (int)newNow.Subtract(lastTime).TotalMilliseconds;
//                lastTime.Assign(newNow);
//                lastReport.Modify((ref int n) => n += reportEvery);
//                int count = countComplete;
//                Shield.SideEffect(() =>
//                {
//                    Console.Write("\n{0} at {1} item/s", count, speed);
//                }
//                );
//                return true;
//            }
//            );

            if (true)
            {
                var treeTime = mtTest("tree", numTasks, i =>
                {
                    return Task.Factory.StartNew(() =>
                    {
                        var item1 = new TreeItem();
                        Shield.InTransaction(() =>
                        {
                            //Interlocked.Increment(ref transactionCount);
                            tree.Add(item1.Id, item1);
//                            countComplete.Commute((ref int c) => c++);
                        }
                        );
                    }
                    );
                }
                );
                Guid? previous = null;
                bool correct = true;
                Shield.InTransaction(() =>
                {
                    int count = 0;
                    foreach (var item in tree)
                    {
                        count++;
                        if (previous != null && previous.Value.CompareTo(item.Key) > 0)
                        {
                            correct = false;
                            break;
                        }
                        previous = item.Key;
                    }
                    correct = correct && (count == numTasks);
                }
                );
                Console.WriteLine("\n -- {0} ms with {1} iterations and is {2}.",
                    treeTime, transactionCount, correct ? "correct" : "incorrect");
            }

            if (true)
            {
                ShieldedDict<Guid, TreeItem> dict = new ShieldedDict<Guid, TreeItem>();
                transactionCount = 0;
                Shield.InTransaction(() =>
                {
                    countComplete.Value = 0;
                    lastReport.Value = 0;
                }
                );

                var time = mtTest("dictionary", numTasks, i =>
                {
                    return Task.Factory.StartNew(() =>
                    {
                        var item1 = new TreeItem();
                        Shield.InTransaction(() =>
                        {
                            //Interlocked.Increment(ref transactionCount);
                            dict[item1.Id] = item1;
//                            countComplete.Commute((ref int c) => c++);
                        }
                        );
                    }
                    );
                }
                );
                Console.WriteLine("\n -- {0} ms with {1} iterations. Not sorted.",
                time, transactionCount);
            }

            if (true)
            {
                ConcurrentDictionary<Guid, TreeItem> dict = new ConcurrentDictionary<Guid, TreeItem>();

                var time = mtTest("ConcurrentDictionary", numTasks, i =>
                {
                    return Task.Factory.StartNew(() =>
                    {
                        var item1 = new TreeItem();
                        dict[item1.Id] = item1;
                    }
                    );
                }
                );
                Console.WriteLine("\n -- {0} ms with {1} iterations. Not sorted.",
                time, numTasks);
            }
        }

        public static void SkewTest()
        {
            var numTasks = 1000;
            var numFields = 10;
            var transactionCount = 0;
            var randomizr = new Random();
            var shA = Enumerable.Repeat(0, numFields).Select(_ => new Shielded<int>(100)).ToArray();
            var shB = Enumerable.Repeat(0, numFields).Select(_ => new Shielded<int>(100)).ToArray();

            var time = mtTest("skew write", numTasks, i =>
            {
                int index = randomizr.Next(numFields << 1);
                int amount = randomizr.Next(10) + 1;
                return Task.Factory.StartNew(() =>
                {
                    Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref transactionCount);
                        // take one from one of the arrays, but only if it does not take sum under 100
                        var sum = shA[index >> 1] + shB[index >> 1];
                        if (sum - amount >= 100)
                        {
                            //Thread.Sleep(10);
                            if ((index & 1) == 0)
                                shA[index >> 1].Modify((ref int n) => n = n - amount);
                            else
                                shB[index >> 1].Modify((ref int n) => n = n - amount);
                        }
                    }
                    );
                },
                TaskCreationOptions.LongRunning
                );
            }
            );
            // it's correct if no sum is smaller than 100
            var correct = true;
            for (int j = 0; j < numFields; j++)
                if (shA[j] + shB[j] < 100)
                {
                    correct = false;
                    break;
                }
            Console.WriteLine("\n -- {0} ms with {1} iterations and is {2}.",
                time, transactionCount, correct ? "correct" : "incorrect");
        }

        private class Dummy
        {
            public int Value;
        }

        public static void SimpleTreeTest()
        {
            ShieldedTree<int, Dummy> tree = new ShieldedTree<int, Dummy>();
            Shield.InTransaction(() =>
            {
                foreach (int i in Enumerable.Range(1, 2000))
                {
                    tree.Add(1000 - i, new Dummy() { Value = 1000 - i });
                    tree.Add(1000 - i, new Dummy() { Value = 1000 - i });
                }
            });
            Shield.InTransaction(() =>
            {
                foreach (int i in Enumerable.Range(1, 1000).Select(x => x << 1))
                {
                    tree.Remove(1000 - i);
                }
            });
            Shield.InTransaction(() =>
            {
                foreach (var kvp in tree.Range(505, 525))
                    Console.WriteLine("Item: {0}", kvp.Key);
            });
        }

        public static void SimpleCommuteTest()
        {
            var a = new Shielded<int>();

            Shield.InTransaction(() => a.Commute((ref int n) => n++));
            Console.WriteLine(a);

            Shield.InTransaction(() =>
            {
                Console.WriteLine(a);
                a.Commute((ref int n) => n++);
                Console.WriteLine(a);
            });
            Console.WriteLine(a);

            Shield.InTransaction(() =>
            {
                a.Commute((ref int n) => n++);
                Console.WriteLine(a);
            });
            Console.WriteLine(a);
        }

        public static void Main(string[] args)
        {
            //TimeTests();

            //ParallelAddWithSaving();

            //OneTransaction();

            //ControlledRace();

            //DictionaryTest();

            //BetShopTest();

            //BetShopPoolTest();

            //TreeTest();

            //TreePoolTest();

            //SimpleOps();

            //MultiFieldOps();

            //SimpleProxyOps();

            //SkewTest();

            //SimpleTreeTest();

            //SimpleCommuteTest();

            new Queue().Run();

            //SequentialTests.Run();
        }
    }
}

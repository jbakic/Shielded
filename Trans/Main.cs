using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Trans
{
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
            int sleepTime = 10;
            int taskCount = 1000;

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
                        int v = x [rnd];
                        Thread.Sleep(sleepTime);
                        x [rnd] = v + 1;
                    },
                    TaskCreationOptions.LongRunning
                    );
                });
                var correct = x.Sum() == taskCount;
                Console.WriteLine(" {0} ms with {1} iterations and is {2}.",
                    time, transactionCounter, correct ? "correct" : "incorrect");
            }

            var lockCount = 10;
            foreach (var i in Enumerable.Repeat(0, 5))
            {
                var x = new int[100];
                transactionCounter = 0;
                var l = Enumerable.Repeat(0, lockCount).Select (_ => new object()).ToArray();
                var time = mtTest(string.Format("{0} lock write", lockCount), taskCount, _ =>
                {
                    var rnd = randomizr.Next(100);
                    return Task.Factory.StartNew(() =>
                    {
                        lock (l[rnd % lockCount])
                        {
                            Interlocked.Increment(ref transactionCounter);
                            int v = x [rnd];
                            Thread.Sleep(sleepTime);
                            x [rnd] = v + 1;
                        }
                    },
                    TaskCreationOptions.LongRunning
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
                        try
                        {
                        Shield.InTransaction(() =>
                        {
                            Interlocked.Increment(ref transactionCounter);
                            int v = shx[rnd].Read;
                            Thread.Sleep(sleepTime);
                            shx[rnd].Modify((ref int a) => a = v + 1);

//                            if (rnd == 45)
//                                Shield.SideEffect(() =>
//                                    {
//                                        throw new InvalidCastException();
//                                    });
                        });
                        }
                        catch (InvalidCastException)
                        {
                            Console.Write("*");
                        }
                    },
                    TaskCreationOptions.LongRunning
                    );
                });
                var correct = shx.Sum(s => s.Read) == taskCount;
                Console.WriteLine(" {0} ms with {1} iterations and is {2}.",
                    time, transactionCounter, correct ? "correct" : "incorrect");
            }
        }

        static void OneTransaction()
        {
            Shielded<int> sh = new Shielded<int>();
            Shield.InTransaction(() => 
            {
                int x = sh.Read;
                Console.WriteLine("Read: {0}", x);
                sh.Modify((ref int a) => a = x + 1);
                Console.WriteLine("Read after increment: {0}", sh.Read);
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
                                    new Transfer() { OtherId = acc2.Read.Id, AmountReceived = -100M }));
                            });
                            Thread.Sleep(100);
                            acc2.Modify((ref Account a) =>
                            {
                                a.Balance = a.Balance + 100M;
                                var list = a.Transfers;
                                Shield.SideEffect(() => list.Add(
                                    new Transfer() { OtherId = acc1.Read.Id, AmountReceived = 100M }));
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
                                    new Transfer() { OtherId = acc1.Read.Id, AmountReceived = -200M }));
                            });
                            Thread.Sleep(250);
                            acc1.Modify((ref Account a) =>
                            {
                                a.Balance = a.Balance + 200M;
                                var list = a.Transfers;
                                Shield.SideEffect(() => list.Add(
                                    new Transfer() { OtherId = acc2.Read.Id, AmountReceived = 200M }));
                            });
                        });
                    }, TaskCreationOptions.LongRunning);
            });
            Console.WriteLine("\nCompleted 20 transactions in {0} total attempts.", transactionCount);
            Console.WriteLine("Account 1 balance: {0}", acc1.Read.Balance);
            foreach(var t in acc1.Read.Transfers)
            {
                Console.WriteLine("  {0:####,00}", t.AmountReceived);
            }
            Console.WriteLine("\nAccount 2 balance: {0}", acc2.Read.Balance);
            foreach(var t in acc2.Read.Transfers)
            {
                Console.WriteLine("  {0:####,00}", t.AmountReceived);
            }
        }

        private static void DictionaryTest()
        {
            ShieldedDict<int, Shielded<int>> dict = new ShieldedDict<int, Shielded<int>>();
            var randomizr = new Random();
            foreach (var _ in Enumerable.Repeat(0, 10))
            {
                var transactionCounter = 0;
                var time = mtTest("dictionary", 1000, i =>
                {
                    var rnd = randomizr.Next(100);
                    if (i % 2 == 0)
                        // adder task - 500 of these
                        return Task.Factory.StartNew(() =>
                        {
                            Shield.InTransaction(() =>
                            {
                                Interlocked.Increment(ref transactionCounter);
                                var v = dict [rnd];
                                int? num = v != null ? (int?)v.Read : null;
                                Thread.Sleep(10);
                                if (v == null)
                                    dict [rnd] = new Shielded<int>(1);
                                else if (v.Read == -1)
                                        dict [rnd] = null;
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
                                var v = dict [rnd];
                                int? num = v != null ? (int?)v.Read : null;
                                Thread.Sleep(10);
                                if (v == null)
                                    dict [rnd] = new Shielded<int>(-1);
                                else if (v.Read == 1)
                                        dict [rnd] = null;
                                    else
                                        v.Modify((ref int a) => a = num.Value - 1);
                            }
                            );
                        },
                        TaskCreationOptions.LongRunning
                        );
                });
                var correct = Enumerable.Range(0, 100).Sum(n => dict [n] == null ? 0 : dict [n].Read) == 0;
                Console.WriteLine(" {0} ms with {1} iterations and is {2}.",
                    time, transactionCounter, correct ? "correct" : "incorrect");
            }
        }

        /// <summary>
        /// Creates a BetShop, and tries to buy a large number of random tickets. Afterwards it
        /// checks that the rule limiting same ticket winnings is not violated.
        /// </summary>
        public static void BetShopTest()
        {
            int numEvents = 10;
            var betShop = new BetShop(numEvents);
            var randomizr = new Random();
            int reportEvery = 1000;
            Shielded<int> nextReport = new Shielded<int>(reportEvery);

            Shield.Conditional(() => betShop.TicketCount >= nextReport, () =>
            {
                nextReport.Modify((ref int n) => n += reportEvery);
                Shield.SideEffect(() =>
                {
                    int count;
                    var correct = betShop.VerifyTickets(out count);
                    Console.Write(" {0}{1}{0}", correct ? "+" : "-", count);
                });
                return true;
            });

            var time = mtTest("bet shop w/ " + numEvents, 10000, i =>
            {
                decimal payIn = (randomizr.Next(10) + 1m) * 1;
                int event1Id = randomizr.Next(numEvents) + 1;
                int event2Id = randomizr.Next(numEvents) + 1;
                int event3Id = randomizr.Next(numEvents) + 1;
                int offer1Ind = randomizr.Next(3);
                int offer2Ind = randomizr.Next(3);
                int offer3Ind = randomizr.Next(3);
                return Task.Factory.StartNew(() =>
                {
                    var offer1 = betShop.Events[event1Id].Read.BetOffers[offer1Ind];
                    var offer2 = betShop.Events[event2Id].Read.BetOffers[offer2Ind];
                    var offer3 = betShop.Events[event3Id].Read.BetOffers[offer3Ind];
                    betShop.BuyTicket(payIn, offer1, offer2, offer3);
                });
            });
            int total;
            var totalCorrect = betShop.VerifyTickets(out total);
            Console.WriteLine(" {0} ms with {1} tickets paid in and is {2}.",
                time, total, totalCorrect ? "correct" : "incorrect");
        }

        class TreeItem
        {
            public Guid Id;
        }

        public static void TreeTest()
        {
            int numTasks = 30000;
            int reportEvery = 1000;
            bool doTree = true;

            ShieldedTree<TreeItem, Guid> tree = new ShieldedTree<TreeItem, Guid>(ti => ti.Id);
            int transactionCount = 0;
            Shielded<int> lastReport = new Shielded<int>(0);
            Shielded<int> countComplete = new Shielded<int>(0);
            Shielded<DateTime> lastTime = new Shielded<DateTime>(DateTime.UtcNow);

            Shield.Conditional(() => countComplete.Read >= lastReport + reportEvery, () =>
            {
                DateTime newNow = DateTime.UtcNow;
                double speed = (countComplete - lastReport) * 1000 / newNow.Subtract(lastTime).TotalMilliseconds;
                lastTime.Assign(DateTime.UtcNow);
                lastReport.Modify((ref int n) => n += reportEvery);
                int count = countComplete.Read;
                Shield.SideEffect(() =>
                {
                    Console.Write("\n{0} at {1} item/s", count, speed);
                });
                return true;
            });

            if (doTree)
            {
                var treeTime = mtTest("tree", numTasks, i =>
                {
                    return Task.Factory.StartNew(() =>
                    {
                        var item = new TreeItem() { Id = Guid.NewGuid() };
                        Shield.InTransaction(() =>
                        {
                            Interlocked.Increment(ref transactionCount);
                            tree.Insert(item);
                        });
                        Shield.InTransaction(
                            () => countComplete.Modify((ref int c) => c++));
                    });
                });
                Guid? previous = null;
                bool correct = true;
                Shield.InTransaction(() =>
                {
                    foreach (var item in tree)
                    {
                        if (previous != null && previous.Value.CompareTo(item.Id) > 0)
                        {
                            correct = false;
                            break;
                        }
                        previous = item.Id;
                    }
                });
                Console.WriteLine("\n -- {0} ms with {1} iterations and is {2}.",
                    treeTime, transactionCount, correct ? "correct" : "incorrect");
                return;
            }

            ShieldedDict<Guid, TreeItem> dict = new ShieldedDict<Guid, TreeItem>();
            transactionCount = 0;
            Shield.InTransaction(() =>
            {
                countComplete.Assign(0);
                lastReport.Assign(0);
            });

            var time = mtTest("dictionary", numTasks, i =>
            {
                return Task.Factory.StartNew(() =>
                {
                    var item = new TreeItem() { Id = Guid.NewGuid() };
                    Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref transactionCount);
                        dict[item.Id] = item;
                    });
                    Shield.InTransaction(
                        () => countComplete.Modify((ref int c) => c++));
                });
            });
            Console.WriteLine("\n -- {0} ms with {1} iterations. Not sorted.",
                time, transactionCount);
        }

		public static void Main(string[] args)
        {
            //TimeTests();

            //OneTransaction();

            //ControlledRace();

            //DictionaryTest();

            //BetShopTest();

            TreeTest();
        }
	}
}

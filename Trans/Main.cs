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

        public static void mtTest(string name, int taskCount, Func<int, Task> task,
            Action<long> verify)
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
            verify(time);
        }

        public static void TimeTests()
        {
            var randomizr = new Random();
            int transactionCounter;

            foreach (var i in Enumerable.Repeat(0, 5))
            {
                var x = new int[100];
                transactionCounter = 0;
                mtTest("dirty write", 1000, _ =>
                {
                    var rnd = randomizr.Next(100);
                    return Task.Factory.StartNew(() =>
                    {
                        Interlocked.Increment(ref transactionCounter);
                        int v = x [rnd];
                        Thread.Sleep(10);
                        x [rnd] = v + 1;
                    },
                    TaskCreationOptions.LongRunning
                    );
                },
                time =>
                {
                    var correct = x.Sum() == 1000;
                    Console.WriteLine(" {0} ms with {1} iterations and is {2}.",
                        time, transactionCounter, correct ? "correct" : "incorrect");
                }
                );
            }

            foreach (var i in Enumerable.Repeat(0, 5))
            {
                var x = new int[100];
                transactionCounter = 0;
                var l = Enumerable.Repeat(0, 16).Select (_ => new object()).ToArray();
                mtTest("16 lock write", 1000, _ =>
                {
                    var rnd = randomizr.Next(100);
                    return Task.Factory.StartNew(() =>
                    {
                        lock (l[rnd % 16])
                        {
                            Interlocked.Increment(ref transactionCounter);
                            int v = x [rnd];
                            Thread.Sleep(10);
                            x [rnd] = v + 1;
                        }
                    },
                    TaskCreationOptions.LongRunning
                    );
                },
                time =>
                {
                    var correct = x.Sum() == 1000;
                    Console.WriteLine(" {0} ms with {1} iterations and is {2}.",
                        time, transactionCounter, correct ? "correct" : "incorrect");
                }
                );
            }

            foreach (var i in Enumerable.Repeat(0, 5))
            {
                var shx = Enumerable.Repeat(0, 100).Select(n => new Shielded<int>(n)).ToArray();
                transactionCounter = 0;
                mtTest("shielded2 write", 1000, _ =>
                {
                    var rnd = randomizr.Next(100);
                    return Task.Factory.StartNew(() =>
                    {
                        Shield.InTransaction(() =>
                        {
                            Interlocked.Increment(ref transactionCounter);
                            int v = shx[rnd].Read;
                            Thread.Sleep(10);
                            shx[rnd].Modify((ref int a) => a = v + 1);
                        });
                    },
                    TaskCreationOptions.LongRunning
                    );
                },
                time =>
                {
                    var correct = shx.Sum(s => s.Read) == 1000;
                    Console.WriteLine(" {0} ms with {1} iterations and is {2}.",
                        time, transactionCounter, correct ? "correct" : "incorrect");
                }
                );
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
            var acc1 = new Shielded<Account>();
            Shield.InTransaction(() =>
                acc1.Modify((ref Account a) =>
                {
                    a.Id = 1;
                    a.Balance = 1000M;
                    a.Transfers = new List<Transfer>();
                }));

            var acc2 = new Shielded<Account>();
            Shield.InTransaction(() =>
                acc2.Modify((ref Account a) =>
                {
                    a.Id = 2;
                    a.Balance = 1000M;
                    a.Transfers = new List<Transfer>();
                }));

            mtTest("controlled race", 20, n =>
            {
                if (n % 2 == 0)
                    return Task.Factory.StartNew(() =>
                    {
                        Shield.InTransaction(() =>
                        {
                            acc1.Modify((ref Account a) =>
                            {
                                a.Balance = a.Balance - 100M;
                                var list = a.Transfers;
                                Shield.SideEffect(() => list.Add(
                                    new Transfer() { OtherId = acc2.Read.Id, AmountReceived = -100M }));
                            });
                            Thread.Sleep(250);
                            acc2.Modify((ref Account a) =>
                            {
                                a.Balance = a.Balance + 100M;
                                var list = a.Transfers;
                                Shield.SideEffect(() => list.Add(
                                    new Transfer() { OtherId = acc1.Read.Id, AmountReceived = 100M }));
                            });
                            Shield.SideEffect(() => Console.WriteLine("Transferred 100.00 .. acc1 -> acc2"),
                                () => Console.WriteLine("Task 1 rollback!"));
                        });
                    }, TaskCreationOptions.LongRunning);
                else
                    return Task.Factory.StartNew(() =>
                    {
                        Shield.InTransaction(() =>
                        {
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
                            Shield.SideEffect(() => Console.WriteLine("Transferred 200.00 .. acc1 <- acc2"),
                                () => Console.WriteLine("Task 2 rollback!"));
                        });
                    }, TaskCreationOptions.LongRunning);
            },
            time =>
            {
                Console.WriteLine("\nAccount 1 balance: {0}", acc1.Read.Balance);
                foreach(var t in acc1.Read.Transfers)
                {
                    Console.WriteLine("  {0:####,00}", t.AmountReceived);
                }
                Console.WriteLine("\nAccount 2 balance: {0}", acc2.Read.Balance);
                foreach(var t in acc2.Read.Transfers)
                {
                    Console.WriteLine("  {0:####,00}", t.AmountReceived);
                }
            });
        }

		public static void Main(string[] args)
        {
            TimeTests();

            //OneTransaction();

            //ControlledRace();
        }
	}
}

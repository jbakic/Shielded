using System;
using System.Threading;

namespace Shielded
{
    class StampLocker
    {
        int _lockers;

        public void WaitUntil(Func<bool> test)
        {
            SpinWait sw = new SpinWait();
            int count = 0;
            do
            {
                sw.SpinOnce();
                if (test())
                    return;
            } while (!sw.NextSpinWillYield || ++count < 4);
            int? effect = null;
            try
            {
                try {} finally
                {
                    effect = Interlocked.Increment(ref _lockers);
                }
                lock (this)
                {
                    while (!test())
                        Monitor.Wait(this);
                }
            }
            finally
            {
                if (effect.HasValue)
                    Interlocked.Decrement(ref _lockers);
            }
        }

        public void Release()
        {
            Thread.MemoryBarrier();
            if (_lockers > 0)
                lock (this)
                    Monitor.PulseAll(this);
        }
    }
}


using System;
using System.Threading;

namespace Shielded
{
    class StampLocker
    {
        SpinWait _sw;
        int _lockers;

        public void WaitUntil(Func<bool> test)
        {
            do
            {
                if (test())
                    return;
                _sw.SpinOnce();
            } while (!_sw.NextSpinWillYield);

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
            if (_lockers > 0)
                lock (this)
                    Monitor.PulseAll(this);
        }
    }
}


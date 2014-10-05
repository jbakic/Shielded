using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;

namespace Shielded
{
    internal class ReadTicket
    {
        public long Stamp;
    }

    internal class WriteTicket : ReadTicket
    {
        public IEnumerable<IShielded> Changes;
    }

    /// <summary>
    /// This class handles everything about assigning version stamps to readers and
    /// writers, and takes care of determining what is the minimum used stamp, so that
    /// old unreachable versions can be released to the GC.
    /// </summary>
    internal static class VersionList
    {
        private class VersionEntry : WriteTicket
        {
            public int ReaderCount;
            public VersionEntry Later;
        }

        private static VersionEntry _current;
        private static VersionEntry _oldestRead;

        static VersionList()
        {
            // base version, has 0 stamp, and no changes
            _oldestRead = _current = new VersionEntry() { Changes = new List<IShielded>() };
        }

        /// <summary>
        /// Reader should keep a reference to his ticket, and release it with a call
        /// to <see cref="ReleaseReaderTicket"/> when done.
        /// </summary>
        public static void GetReaderTicket(out ReadTicket ticket)
        {
            try {} finally
            {
                while (true)
                {
                    var curr = _current;
                    Interlocked.Increment(ref curr.ReaderCount);
                    // if the curr changed before we incremented, it could have theoretically
                    // been trimmed away. so, if the _current changed, drop the old, and take the new.
                    if (curr == _current)
                    {
                        ticket = curr;
                        break;
                    }
                    else
                        Interlocked.Decrement(ref curr.ReaderCount);
                }
            }
        }

        /// <summary>
        /// For commute running block, because it knows that the ticket taken by
        /// the main transaction is all the protection it needs.
        /// </summary>
        public static long GetUntrackedReadStamp()
        {
            return _current.Stamp;
        }

        /// <summary>
        /// After no reading will be done for the given reader ticket, release it with this method.
        /// </summary>
        public static void ReleaseReaderTicket(ReadTicket ticket)
        {
            var node = (VersionEntry)ticket;
            Interlocked.Decrement(ref node.ReaderCount);
            TrimCopies();
        }
        
        private static int _trimFlag = 0;
        private static int _trimClock = 0;

        private static void TrimCopies()
        {
            // trimming won't start every time..
            if ((Interlocked.Increment(ref _trimClock) & 0xF) != 0)
                return;

            bool tookFlag = false;
            try
            {
                try { }
                finally
                {
                    tookFlag = Interlocked.CompareExchange(ref _trimFlag, 1, 0) == 0;
                }
                if (!tookFlag) return;

                var old = _oldestRead;
                ISet<IShielded> toTrim = null;
                while (old != _current && old.ReaderCount == 0 && old.Changes != null)
                {
                    if (toTrim == null)
#if USE_STD_HASHSET
                        toTrim = new HashSet<IShielded>();
#else
                        toTrim = new SimpleHashSet();
#endif
                    toTrim.UnionWith(old.Changes);
                    old = old.Later;
                }
                if (toTrim == null)
                    return;

                _oldestRead = old;
                var version = old.Stamp;
                foreach (var sh in toTrim)
                    sh.TrimCopies(version);
            }
            finally
            {
                if (tookFlag)
                    Interlocked.Exchange(ref _trimFlag, 0);
            }
        }

        /// <summary>
        /// Called after a thread checked and can commit. This will assign it a
        /// unique stamp to use for writing. This method will enter the new stamp
        /// into the stamp.Version. It must be done here to guarantee that no
        /// GetReaderTicket will return your stamp number before it has been
        /// written into your stamp lock! After completing your write, place your
        /// changes into the ticket, and this class will trim the old versions at
        /// the correct time.
        /// </summary>
        public static WriteTicket NewVersion(WriteStamp stamp)
        {
            var newNode = new VersionEntry();
            var current = _current;
            do
            {
                while (current.Later != null)
                    current = current.Later;
                newNode.Stamp = current.Stamp + 1;
                stamp.Version = newNode.Stamp;
            } while (Interlocked.CompareExchange(ref current.Later, newNode, null) != null);
            MoveCurrent();
            return newNode;
        }

        private static void MoveCurrent()
        {
            var current = _current;
            while (current.Later != null)
                current = current.Later;
            _current = current;
        }
    }
}


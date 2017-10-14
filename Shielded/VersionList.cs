using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;

namespace Shielded
{
    /// <summary>
    /// Issued by the <see cref="VersionList"/> static class, tells holders
    /// which version of data they should read. The VersionList will
    /// keep track of issued tickets for the purpose of safe trimming of
    /// old versions.
    /// </summary>
    internal abstract class ReadTicket
    {
        /// <summary>
        /// The version number of the ticket.
        /// </summary>
        public long Stamp { get; protected set; }
    }

    /// <summary>
    /// Issued by the <see cref="VersionList"/> static class to writers, telling
    /// them which version number to use when writing, and allwing them to
    /// report changes they make back to the VersionList.
    /// </summary>
    internal abstract class WriteTicket : ReadTicket
    {
        /// <summary>
        /// After writers complete a write, they must place into this field
        /// an enumerable of the fields they changed. Needed for trimming old
        /// versions. It is crucial to set it to something != null, otherwise
        /// trimming will never get past this version!
        /// </summary>
        public volatile IEnumerable<IShielded> Changes;
    }

    /// <summary>
    /// Taken for the duration of a commit check.
    /// </summary>
    internal abstract class CheckTicket
    {
        public SimpleHashSet Enlisted;
        public SimpleHashSet CommEnlisted;

        protected CheckTicket(SimpleHashSet enlisted, SimpleHashSet commEnlisted)
        {
            Enlisted = enlisted;
            CommEnlisted = commEnlisted;
        }

        public bool Done { get; private set; }

        public void Release()
        {
            Done = true;
            Enlisted = null;
            CommEnlisted = null;
            VersionList.OnCheckTicketReleased();
        }
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
            public VersionEntry Next;

            public void SetStamp(long val)
            {
                Stamp = val;
            }
        }

        private class CheckEntry : CheckTicket
        {
            public CheckEntry Next;

            public CheckEntry(SimpleHashSet enlisted, SimpleHashSet commEnlisted)
                : base(enlisted, commEnlisted)
            { }

            public void Wait()
            {
                SpinWait.SpinUntil(() => Done);
            }
        }

        private static volatile VersionEntry _current;
        private static VersionEntry _oldestRead;

        private static CheckEntry _checkListHead;

        static VersionList()
        {
            // base version, has 0 stamp, and no changes
            _oldestRead = _current = new VersionEntry();
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
                    // trimming sets this to int.MinValue, so unless 2 billion threads are doing this
                    // simultaneously, we're safe.
                    if (Interlocked.Increment(ref curr.ReaderCount) > 0)
                    {
                        ticket = curr;
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// For commute running block, because it knows that the ticket taken by
        /// the main transaction is all the protection it needs.
        /// </summary>
        public static ReadTicket GetUntrackedReadStamp()
        {
            return _current;
        }

        /// <summary>
        /// After no reading will be done for the given reader ticket, release it with this method.
        /// </summary>
        public static void ReleaseReaderTicket(ref ReadTicket ticket)
        {
            var node = (VersionEntry)ticket;
            ticket = null;
            Interlocked.Decrement(ref node.ReaderCount);
        }
        
        private static int _trimFlag = 0;

        public static void TrimCopies()
        {
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
                SimpleHashSet toTrim = null;
                while (old != _current && old.Next.Changes != null &&
                    Interlocked.CompareExchange(ref old.ReaderCount, int.MinValue, 0) == 0)
                {
                    // NB any transaction that holds a WriteTicker with Changes == null also
                    // has a ReadTicket with a smaller stamp. but we don't depend on that here.

                    old = old.Next;

                    if (toTrim == null)
                        toTrim = new SimpleHashSet();
                    toTrim.UnionWith(old.Changes);
                }
                if (toTrim == null)
                    return;

                old.Changes = null;
                _oldestRead = old;
                var version = old.Stamp;
                toTrim.TrimCopies(version);
            }
            finally
            {
                if (tookFlag)
                    Interlocked.Exchange(ref _trimFlag, 0);
            }
        }

        public static void EnterCheck(SimpleHashSet enlisted, SimpleHashSet commEnlisted,
            out CheckTicket ticket)
        {
            var newNode = new CheckEntry(enlisted, commEnlisted);
            ticket = newNode;

            var first = _checkListHead;
            CheckEntry alreadyChecked = null, lastNotDone = newNode;
            do
            {
                var current = newNode.Next = first;
                while (current != null && current != alreadyChecked)
                {
                    if (!current.Done)
                    {
                        if (IsPotentialConflict(newNode, current))
                            current.Wait();
                        else
                            lastNotDone = current;
                    }
                    current = current.Next;
                }
                if (current == null)
                    lastNotDone.Next = null;
                // the only times _checkListHead changes is when some new item is added to the head,
                // and when it gets set to null in OnCheckTicketReleased. thus, in case we repeat this loop,
                // either we will encounter this alreadyChecked item again and stop checking there, or all
                // the elements we see in the repetition must be new to us.
                alreadyChecked = first;
            } while ((first = Interlocked.CompareExchange(ref _checkListHead, newNode, alreadyChecked)) != alreadyChecked);
        }

        private static bool IsPotentialConflict(CheckEntry newEntry, CheckEntry oldEntry)
        {
            var oldEnlisted = oldEntry.Enlisted;
            var oldCommEnlisted = oldEntry.CommEnlisted;
            if (oldEnlisted == null)
                return false;
            return
                newEntry.Enlisted.Overlaps(oldEnlisted) ||
                oldCommEnlisted != null && newEntry.Enlisted.Overlaps(oldCommEnlisted) ||
                (newEntry.CommEnlisted != null &&
                    (newEntry.CommEnlisted.Overlaps(oldEnlisted) ||
                        (oldCommEnlisted != null && newEntry.CommEnlisted.Overlaps(oldCommEnlisted))));
        }

        public static void OnCheckTicketReleased()
        {
            var oldHead = _checkListHead;
            var current = oldHead;
            while (current != null)
            {
                if (!current.Done)
                    return;
                current = current.Next;
            }
            Interlocked.CompareExchange(ref _checkListHead, null, oldHead);
        }

        public static void NewVersion(WriteStamp stamp, out WriteTicket ticket)
        {
            var newNode = new VersionEntry();
            ticket = newNode;
            var current = _current;
            do
            {
                while (current.Next != null)
                    current = current.Next;
                var newStamp = current.Stamp + 1;
                newNode.SetStamp(newStamp);
                stamp.Version = newStamp;
            } while (Interlocked.CompareExchange(ref current.Next, newNode, null) != null);
            MoveCurrent();
        }

        private static void MoveCurrent()
        {
            var current = _current;
            while (current.Next != null)
                current = current.Next;
            _current = current;
        }
    }
}


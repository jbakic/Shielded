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
    /// State of a <see cref="WriteTicket"/>.
    /// </summary>
    internal enum VersionState
    {
        Checking,
        Commit,
        Rollback
    }

    /// <summary>
    /// Issued by the <see cref="VersionList"/> static class to writers, telling
    /// them which version number to use when writing, and allwing them to
    /// report changes they make back to the VersionList.
    /// </summary>
    internal abstract class WriteTicket : ReadTicket
    {
        public VersionState State { get; protected set; }

        public abstract void Commit();
        public abstract void Rollback();

        /// <summary>
        /// After writers complete a write, they must place into this field
        /// an enumerable of the fields they changed. Needed for trimming old
        /// versions. It is crucial to set it to something != null, otherwise
        /// trimming will never get past this version!
        /// </summary>
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

            public readonly StampLocker Locker = new StampLocker();
            // only != null during Committing state
            public SimpleHashSet Enlisted;
            public SimpleHashSet CommEnlisted;

            public override void Commit()
            {
                State = VersionState.Commit;
                Enlisted = null;
                CommEnlisted = null;
                Locker.Release();
                VersionList.MoveCurrent();
            }

            public override void Rollback()
            {
                State = VersionState.Rollback;
                Enlisted = null;
                CommEnlisted = null;
                Locker.Release();
                VersionList.MoveCurrent();
            }

            public void SetStamp(long val)
            {
                Stamp = val;
            }
        }

        private static volatile VersionEntry _current;
        private static VersionEntry _oldestRead;

        static VersionList()
        {
            _oldestRead = _current = new VersionEntry() { Changes = Enumerable.Empty<IShielded>() };
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
        public static void ReleaseReaderTicket(ReadTicket ticket)
        {
            var node = (VersionEntry)ticket;
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
#if USE_STD_HASHSET
                ISet<IShielded> toTrim = null;
#else
                SimpleHashSet toTrim = null;
#endif
                while (old != _current && Interlocked.CompareExchange(ref old.ReaderCount, int.MinValue, 0) == 0)
                {
                    // we do not want to move "old" to an element which still has not finished writing, since
                    // we must maintain the invariant that says _oldestRead.Changes have already been trimmed.
                    if (old.Later.Changes == null)
                        break;
                    // likewise, thanks to that same invariant, we first move forward, then take Changes...
                    old = old.Later;

                    if (toTrim == null)
#if USE_STD_HASHSET
                        toTrim = new HashSet<IShielded>();
#else
                        toTrim = new SimpleHashSet();
#endif
                    if (old.State != VersionState.Rollback)
                        toTrim.UnionWith(old.Changes);
                }
                if (toTrim == null)
                    return;

                old.Changes = null;
                _oldestRead = old;
                var version = old.Stamp;
#if USE_STD_HASHSET
                foreach (var sh in toTrim)
                    sh.TrimCopies(version);
#else
                toTrim.TrimCopies(version);
#endif
            }
            finally
            {
                if (tookFlag)
                    Interlocked.Exchange(ref _trimFlag, 0);
            }
        }

        public static void NewVersion(SimpleHashSet enlisted, SimpleHashSet commEnlisted, out WriteTicket ticket)
        {
            var newNode = new VersionEntry { Enlisted = enlisted, CommEnlisted = commEnlisted };
            ticket = newNode;
            var current = _current;
            do
            {
                while (current.Later != null)
                {
                    var later = current.Later;
                    if (IsConflict(newNode, later) && later.State == VersionState.Checking)
                        later.Locker.WaitUntil(() => later.State != VersionState.Checking);
                    current = later;
                }
                var newStamp = current.Stamp + 1;
                newNode.SetStamp(newStamp);
            } while (Interlocked.CompareExchange(ref current.Later, newNode, null) != null);
        }

        private static bool IsConflict(VersionEntry newEntry, VersionEntry oldEntry)
        {
            var oldEnlisted = oldEntry.Enlisted;
            var oldCommEnlisted = oldEntry.CommEnlisted;
            if (oldEntry.State != VersionState.Checking || oldEnlisted == null)
                return false;
            return
                newEntry.Enlisted.Overlaps(oldEnlisted) ||
                oldCommEnlisted != null && newEntry.Enlisted.Overlaps(oldCommEnlisted) ||
                (newEntry.CommEnlisted != null &&
                    (newEntry.CommEnlisted.Overlaps(oldEnlisted) ||
                        (oldCommEnlisted != null && newEntry.CommEnlisted.Overlaps(oldCommEnlisted))));
        }

        private static void MoveCurrent()
        {
            while (true)
            {
                var current = _current;
                if (current.Later == null || current.Later.State == VersionState.Checking)
                    break;
                while (current.Later != null && current.Later.State != VersionState.Checking)
                    current = current.Later;
                _current = current;
            }
        }
    }
}


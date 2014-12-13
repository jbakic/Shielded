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

            public void SetStamp(long val)
            {
                Stamp = val;
            }
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
        private static int _trimClock = 0;

        public static void TrimCopies()
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
#if USE_STD_HASHSET
                ISet<IShielded> toTrim = null;
#else
                SimpleHashSet toTrim = null;
#endif
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
        /// the correct time. It is critical that this be done, which is why this
        /// method returns it as an out param - to make sure no exception can
        /// cause us to lose the ref to the ticket, if it is already in the chain.
        /// </summary>
        public static void NewVersion(WriteStamp stamp, out WriteTicket ticket)
        {
            var newNode = new VersionEntry();
            ticket = newNode;
            var current = _current;
            do
            {
                while (current.Later != null)
                    current = current.Later;
                var newStamp = current.Stamp + 1;
                newNode.SetStamp(newStamp);
                stamp.Version = newStamp;
            } while (Interlocked.CompareExchange(ref current.Later, newNode, null) != null);
            MoveCurrent();
        }

        private static void MoveCurrent()
        {
            while (true)
            {
                var current = _current;
                if (current.Later == null)
                    break;
                while (current.Later != null)
                    current = current.Later;
                _current = current;
            }
        }
    }
}


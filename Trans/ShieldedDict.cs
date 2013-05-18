using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Trans
{
    /// <summary>
    /// Protects only regarding adds or removes. Updates are unprotected unless
    /// you choose to have shielded items.
    /// </summary>
    public class ShieldedDict<TKey, TItem> : IShielded where TItem : class
    {
        private class ItemKeeper
        {
            public long Version;
            public TItem Value;
            public ItemKeeper Older;
        }

        private class LocalDict
        {
            public long Version;
            public Dictionary<TKey, TItem> Items;
            public HashSet<TKey> Reads;
        }

        private readonly ConcurrentDictionary<TKey, ItemKeeper> _dict;
        private readonly ConcurrentDictionary<TKey, long> _writeStamps
            = new ConcurrentDictionary<TKey, long>();
        private ThreadLocal<LocalDict> _localDict = new ThreadLocal<LocalDict>(
            () => new LocalDict()
            {
                Reads = new HashSet<TKey>(),
                Items = new Dictionary<TKey, TItem>()
            });

        public ShieldedDict(Func<TItem, TKey> keySelector, IEnumerable<TItem> items)
        {
            _dict = new ConcurrentDictionary<TKey, ItemKeeper>(
                items.Select(i =>
                    new KeyValuePair<TKey, ItemKeeper>(
                        keySelector(i),
                        new ItemKeeper()
                        {
                            Version = 0,
                            Value = i
                        })));
        }

        public ShieldedDict()
        {
            _dict = new ConcurrentDictionary<TKey, ItemKeeper>();
        }

        private ItemKeeper CurrentTransactionOldValue(TKey key)
        {
            var stamp = Shield.CurrentTransactionStartStamp;
            SpinWait.SpinUntil(() =>
            {
                var w = _writeStamps[key];
                return w == 0 || w > stamp;
            });
            Shield.Enlist(this);
            PrepareLocal();
            _localDict.Value.Reads.Add(key);

            var point = _dict[key];
            while (point != null && point.Version > stamp)
                point = point.Older;
            return point;
        }

        private bool IsLocalPrepared()
        {
            return _localDict.IsValueCreated && _localDict.Value.Reads != null &&
                _localDict.Value.Version == Shield.CurrentTransactionStartStamp;
        }

        private void PrepareLocal()
        {
            // the second part creates the value. (and the test then passes.)
            if (!IsLocalPrepared())
            {
                _localDict.Value.Version = Shield.CurrentTransactionStartStamp;
                if (_localDict.Value.Reads != null)
                    _localDict.Value.Reads.Clear();
                else
                    _localDict.Value.Reads = new HashSet<TKey>();

                if (_localDict.Value.Items != null)
                    _localDict.Value.Items.Clear();
            }
        }

        private void PrepareForWriting(TKey key)
        {
            PrepareLocal();
            if (_localDict.Value.Items == null)
                _localDict.Value.Items = new Dictionary<TKey, TItem>();

            if (!_localDict.Value.Items.ContainsKey(key))
                _localDict.Value.Items[key] = CurrentTransactionOldValue(key).Value;
        }

        /// <summary>
        /// Never throws, just returns null if it does not have anything.
        /// </summary>
        public TItem this [TKey key]
        {
            get
            {
                if (!Shield.IsInTransaction || !IsLocalPrepared() ||
                    _localDict.Value.Items == null || !_localDict.Value.Items.ContainsKey(key))
                {
                    var v = Shield.IsInTransaction ? CurrentTransactionOldValue(key) :
                        _dict.ContainsKey(key) ? _dict[key]
                        : null;
                    return v == null ? null : v.Value;
                }
                else if (_dict.ContainsKey(key) && _dict[key].Version > Shield.CurrentTransactionStartStamp)
                    throw new TransException("Writable read collision.");
                return _localDict.Value.Items[key];
            }
            set
            {
                PrepareForWriting(key);
                _localDict.Value.Items[key] = value;
            }
        }

        bool IShielded.HasChanges
        {
            get
            {
                return IsLocalPrepared() && _localDict.Value.Items != null && _localDict.Value.Items.Any();
            }
        }

        bool IShielded.CanCommit(bool strict, long writeStamp)
        {
            if (!strict && !((IShielded)this).HasChanges)
                return true;
            // locals were prepared when we enlisted.
            else if (_localDict.Value.Reads.Any(key => _writeStamps.ContainsKey(key) && _writeStamps[key] != 0))
                return false;
            else if (_localDict.Value.Reads.All(key =>
                    !_dict.ContainsKey(key) || _dict[key].Version < Shield.CurrentTransactionStartStamp))
            {
                // touch only the ones we plan to change
                foreach (var key in _localDict.Value.Items.Keys)
                    _writeStamps.TryAdd(key, writeStamp); // we know this succeeds.
                return true;
            }
            return false;
        }
        
        bool IShielded.Commit(long writeStamp)
        {
            if (((IShielded)this).HasChanges)
            {
                foreach (var kvp in _localDict.Value.Items)
                {
                    var newCurrent = new ItemKeeper()
                    {
                        Value = kvp.Value,
                        Version = writeStamp,
                        Older = _dict.ContainsKey(kvp.Key) ? _dict[kvp.Key] : null
                    };
                    // nobody is actually changing it now.
                    _dict.AddOrUpdate(kvp.Key, newCurrent,
                                      (key, old) => newCurrent);
                    long ourStamp;
                    bool rem = _writeStamps.TryRemove(kvp.Key, out ourStamp);
                    if (!rem || ourStamp != writeStamp)
                        throw new ApplicationException("Commit from unexpected transaction");
                }
                _localDict.Value.Items = null;
                _localDict.Value.Reads = null;
                return true;
            }
            if (IsLocalPrepared())
                _localDict.Value.Reads = null;
            return false;
        }

        void IShielded.Rollback(long? writeStamp)
        {
            _localDict.Value.Reads = null;
            if (_localDict.Value.Items != null)
            {
                if (writeStamp.HasValue)
                {
                    long ws;
                    foreach (var kvp in _localDict.Value.Items)
                        if (_writeStamps [kvp.Key] == writeStamp.Value)
                            _writeStamps.TryRemove(kvp.Key, out ws);
                        else
                            throw new ApplicationException("Inconsistently locked ShieldedDict discovered on rollback.");
                }
                _localDict.Value.Items = null;
            }
        }

        void IShielded.TrimCopies(long smallestOpenTransactionId)
        {
            // NB the "smallest transaction" and others can freely read while
            // we're doing this.
            foreach (var kvp in _dict.ToArray())
            {
                var point = kvp.Value;
                ItemKeeper pointPrevious = null;
                while (point != null && point.Version > smallestOpenTransactionId)
                {
                    pointPrevious = point;
                    point = point.Older;
                }
                if (point != null)
                {
                    // point is the last accessible - his Older is not needed.
                    point.Older = null;
                    if (point.Value == null)
                    {
                        if (pointPrevious != null)
                            pointPrevious.Older = null;
                        else
                            // if this returns false, some other transaction inserted something while
                            // we were triming - to avoid complicating, just leave the point where it is, it
                            // gets trimmed eventually.
                            ((ICollection<KeyValuePair<TKey, ItemKeeper>>)_dict).Remove(kvp);
                    }
                }
                // if point were null above, data was lost, CurrentTransactionValue() might throw for some!
            }
        }
    }
}

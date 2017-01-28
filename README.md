Shielded
========

Available on [NuGet](https://www.nuget.org/packages/Shielded).

Shielded is a full-featured implementation of Software Transactional Memory
in .NET. It provides a system (the Shield static class) for running in-memory
transactions, and data structures which are aware of transactions. It can also
generate transaction-aware proxy subclasses based on a POCO class type. The
implementation is strict, with strong guarantees on safety. It is mostly
lock-free, using only one major lock which is held during the pre-commit
check.

Here is a small example:

```csharp
var n = new Shielded<int>();
int a = n;
Shield.InTransaction(() =>
    n.Value = n + 5);
```

Shielded fields are thread-safe. You can read them out of transaction, but
changes must be done inside. While inside, the library guarantees a consistent
view of all shielded fields.

Another example, the STM version of "Hello world!" - parallel addition in an
array. Here, in a dictionary:

```csharp
var dict = new ShieldedDict<int, int>();
ParallelEnumerable.Range(0, 100000)
    .ForAll(i => Shield.InTransaction(() =>
        dict[i % 100] = dict.ContainsKey(i % 100) ? dict[i % 100] + 1 : 1));
```

Shielded works with value types, and the language automatically does the needed
cloning. For ref types, it only makes the reference itself transactional.
The class should then be immutable, or, if you have a class you want to make
transactional:

```csharp
public class TestClass {
    public virtual Guid Id { get; set; }
    public virtual string Name { get; set; }
}
```

Then you create instances like this:

```csharp
using Shielded.ProxyGen;
...
var t = Factory.NewShielded<TestClass>();
```

The Factory creates a proxy sub-class, using CodeDom, which will have transactional
overrides for all virtual properties of the base class that are public or protected.
Due to CodeDom limitations, the getter and setter must have the same accessibility!
The proxy objects are thread-safe (or, at least their virtual properties are), and
can only be changed inside transactions. Usage is simple:

```csharp
var id = t.Id;
Shield.InTransaction(() =>
    t.Name = "Test object");
```

It is safe to execute any number of concurrent transactions that are reading from
or writing into the same shielded fields - each transaction will complete correctly.
This is accomplished by:
* ensuring that in one transaction you read a consistent state of
all shielded fields
* buffering writes into storage which is local for each thread

Your changes are commited and made visible to other threads only if all
the shielded fields you read or wrote into have not changed since you
started. If any have new changes, your transaction is retried from the
beginning, but this time reading the new data. Though it may seem so,
this cannot create an infinite loop since for any conflict to occur at
least one transaction must successfully commit. Overall, the system must
make progress.

This quality would place Shielded in the lock-free class of non-blocking
concurrency mechanisms, according to academic classification. However,
this is not accurate since the commit check gets done under a lock. Hence
the word "mostly" in the short description.

Features
--------

* **MVCC**: Each transaction reads a consistent snapshot of the state without
the need for locking, since updates just create new versions.
    * Old versions are dropped soon after no one is capable of reading them
    any more.
* **Read-only transactions** always complete without any repetitions and
without entering the global lock!
* **Strictness**: If a write is made anywhere, the system will insist that
all touched locations, read or written, still contain the same version
of data that they had when the transaction opened. This means it does not
suffer from the Write Skew issue.
* **Transactional collections**: Included in the library are ShieldedDict<>
(dictionary), ShieldedSeq<> (singly linked list) and ShieldedTree<> (a
red-black tree implementation).
    * It is possible to use this library with immutable collections from
    [System.Collections.Immutable](https://msdn.microsoft.com/en-us/library/mt452182%28v=vs.110%29.aspx).
* **Transaction-local storage**: ShieldedLocal<> allows storing anything
in the transaction context, visible only from within that transaction.
* To perform **side-effects** (IO, and most other operations which are not
shielded) you use the SideEffect method of the Shield class, which takes
optional onCommit and onRollback lambdas, or the **SyncSideEffect** method which
allows you to execute code during a commit, while the changed fields are still
locked.
* **Conditional transactions**: Method Shield.Conditional allows you
to define something similar to a database AFTER trigger. It receives a test, and
an action to perform, both lambdas. It runs the test, makes a note of
all shielded objects that the test had accessed, and later re-executes
the test when any of those objects is committed into. If test passes, the
action is called.
    * Implemented transactionally, so can be called from transactions, and
    can be triggered by the transaction that created it.
    * Returns an IDisposable for deactivating the subscription, also
    transactionally. It may even deactivate itself, e.g. to guarantee one-time execution.
* **Pre-commit checks**: Shield.PreCommit is very similar to Shield.Conditional,
but executes the test within a transaction that changes one of the fields it is
interested in, just before that transaction will commit.
    * Can be used to ensure certain invariants are held, or to implement
    thread prioritization by allowing only some threads which access a field
    to commit into it.
* **Custom commit operations**: You can integrate your own code into the commit process,
to execute while the shielded fields, that are being written, are held locked.
    * Already mentioned Shield.SyncSideEffect does this on the level of one transaction.
    * Using Shield.WhenCommitting, you subscribe globally for any commit, or based on
    the type of field being written. These subscriptions should never throw!
    * Shield.RunToCommit runs a transaction just up to commit, and allows you to
    commit/rollback later, or from another thread. This is useful for asynchronous
    programming.
* **Commutables**: operations which can be performed without conflict, because
they can be reordered in time and have the same net effect, i.e. they are
commutable (name borrowed from Clojure). Incrementing an int is an
example - if you don’t care what the int’s value is, you can increment it
without conflict by simply incrementing whatever value you encounter there
at commit time. Using commutes, when appropriate, reduces conflicts and
improves concurrency. Incrementing an int, conflict-free:

    ```csharp
    n.Commute((ref int a) => a++);
    ```

    * Commutes are not performed under any lock, but rather in a special
    commute subtransaction, which reads the latest data, and tries to
    commit with the same stamp as your main transaction. If only the commutes
    fail, then only the commutes get retried.
    * If, in the example above, your main transaction has already (or perhaps
    will later) read the n field or written to it (non-commutatively), the
    commute “degenerates” - it gets executed in place, in your transaction,
    and you can see it’s effect. This means consistency - if you read it, it
    will stay as read when you commit. But, it is now a potential conflict.
    * Shield has various commutable operations defined in it. Appending to a
    sequence is commutable - if you do not touch the seq, it never conflicts.
    Collection Count fields are comuted over, to avoid unnecessary conflicts.

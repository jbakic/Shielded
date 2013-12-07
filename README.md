Shielded
========

Shielded is a full-featured implementation of Software Transactional Memory
in .NET. It provides a system (the Shield static class) for running in-memory
transactions, and data structures which are aware of transactions. It can also
generate transaction-aware proxy subclasses based on a POCO class type.

The implementation is strict, with strong guarantees on safety. It is mostly
obstruction-free, using only one major lock which is held during
the pre-commit check. It is slower than non-transactional code, of
course - a trivial transaction is roughly around 2-5 times slower than
optimally written code. But, for anything non-trivial, the impact of STM
on performance becomes smaller, while the code is much simpler,
less error-prone, and achieves a high degree of concurrency.

Here is a small example:

    Shielded<int> n = new Shielded<int>();
    Shield.InTransaction(() => n.Modify((ref int a) => a += 5));

The first line creates a shielded container for an Int32, 0 will be the
initial value as it is the int’s default. The second line starts a
transaction inside which it modifies the int to be larger by 5.

If you have a class you want to make transactional:

    public class TestClass {
        public virtual Guid Id { get; set; }
        public virtual string Name { get; set; }
    }

Then you create instances like this:

    using Shielded.ProxyGen;
    ...
    var t = Factory.NewShielded<TestClass>();

Such objects are thread-safe (or, at least their virtual properties are), but can
only be changed inside transactions. Usage is simple:

    var id = t.Id;
    Shield.InTransaction(() => t.Name = "Test object");

It is perfectly safe to execute transactions along with any number of parallel
threads that are reading from or writing into the same Shielded object - this
transaction will complete correctly. It accomplishes this by:
* Ensuring that during one transaction you always read a consistent state of
all shielded fields.
* Buffering writes into storage which is local for each thread.

Your changes are commited and made visible to other threads only if all
the shielded fields you read or wrote into have not changed since you
started. If any have new changes, your transaction is retried from the
beginning, but this time reading the new data. Though it may seem so,
this cannot create an infinite loop since for any conflict to occur at
least one transaction must successfully commit. Overall, the system must
make progress.

Features
--------

* **MVCC**: Each transaction reads a consistent snapshot of the state without
the need for locking, since updates just create new versions.
    * Old versions are dropped soon after noone is capable of reading them
    any more.
* **Read-only transactions** always complete without any repetitions and
without entering the global lock!
* **Strictness**: If a write is made anywhere, the system will insist that
all touched locations, read or written, still contain the same version
of data that they had when the transaction opened. This means it does not
suffer from the Write Skew issue.
* To perform **side-effects** (IO, and all other operations which are not
shielded, should not be repeated if we retry, and which should only take
effect if you commit) you use the SideEffect method of the Shield class,
which takes optional onCommit and onRollback lambdas.
* **Conditional transactions**: The Shield’s method Conditional enables you
to define something similar to a database trigger. It receives a test, and
an action to perform, both lambdas. It runs the test, makes a note of
all shielded objects that the test had accessed, and later re-executes
the test when any of those objects is committed into. If test passes, the
action is called.
    * Implemented transactionally, so can be called from transactions, and
    can be triggered by the transaction that created it. If the action
    returns false, the subscription is removed in this transaction, which
    can guarantee one-time execution if needed.
* **Commutables**: operations which can be performed without conflict, because
they can be reordered in time and have the same net effect, i.e. they are
commutable (name borrowed from Clojure). Incrementing an int is an
example - if you don’t care what the int’s value is, you can increment it
without conflict by simply incrementing whatever value you encounter there
at commit time. Using commutes, when appropriate, reduces conflicts and
improves concurrency. Incrementing an int, conflict-free:

        n.Commute((ref int a) => a++);

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
    Assigning a value to a Shielded<> by using Assign (which means, without
    reading the old value) is also commutable.
* **Transactional collections**: Included in the library are ShieldedDict<>
(dictionary), ShieldedSeq<> (singly linked list) and ShieldedTree<> (a
red-black tree implementation).

using ExternRefForProxyTest;
using Shielded;
using System;
using System.Collections.Immutable;

namespace ShieldedTests
{
    public interface IIdentifiable<T>
    {
        T Id { get; set; }
    }
}

namespace ShieldedTests.ProxyTestEntities
{
    [Shielded]
    public class Entity1 : IIdentifiable<Guid>
    {
        public virtual Guid Id { get; set; }
        public virtual string Name { get; set; }

        public virtual string @switch { get; set; }
    }

    [Shielded]
    public class Entity2 : IIdentifiable<Guid>
    {
        public virtual Guid Id { get; set; }
        public virtual string Name { get; set; }
    }

    [Shielded]
    public class EntityWithExternalProperty : IIdentifiable<Guid>
    {
        public virtual Guid Id { get; set; }
        public virtual AnExternalClass External { get; set; }
        public virtual ImmutableArray<int> ImmutableArray { get; set; }
    }
}

namespace ShieldedTests.ProxyTestEntities2
{

    [Shielded]
    public class Entity3 : IIdentifiable<Guid>
    {
        public virtual Guid Id { get; set; }
        public virtual string Name { get; set; }
    }

    [Shielded]
    public class Entity4 : IIdentifiable<Guid>
    {
        public virtual Guid Id { get; set; }
        public virtual string Name { get; set; }
    }
}


using System;

namespace ShieldedTests
{
    public interface IIdentifiable<T>
    {
        T Id { get; set; }
    }
}

namespace ShieldedTests.ProxyTestEntities
{
    public class Entity1 : IIdentifiable<Guid>
    {
        public virtual Guid Id { get; set; }
        public virtual string Name { get; set; }

        public virtual string @switch { get; set; }
    }

    public class Entity2 : IIdentifiable<Guid>
    {
        public virtual Guid Id { get; set; }
        public virtual string Name { get; set; }
    }
}

namespace ShieldedTests.ProxyTestEntities2
{

    public class Entity3 : IIdentifiable<Guid>
    {
        public virtual Guid Id { get; set; }
        public virtual string Name { get; set; }
    }

    public class Entity4 : IIdentifiable<Guid>
    {
        public virtual Guid Id { get; set; }
        public virtual string Name { get; set; }
    }
}


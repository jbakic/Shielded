using System.Collections.Generic;

namespace CodeGenerator.Shielded
{

    /// <summary>
    /// Properties can not use CamelCase, due to scriban limitations
    /// </summary>
    public class PropertyInfo
    {
        public string Name { get; set; }

        public string Type { get; set; }

        public string Modifier { get; set; } = "public override ";

        public string Gettermodifier { get; set; }

        public string Settermodifier { get; set; }

        public string Declare { get { return Modifier + Type + " " + Name; } }

        public string Structdeclare { get { return "public " + Type + " " + Name; } }
    }

    /// <summary>
    /// Properties can not use CamelCase, due to scriban limitations
    /// </summary>
    public class PocoInfo
    {
        public string Space { get; set; }
        public string Name { get; set; }
        public string Structname { get; set; }
        public string Super { get; set; }
        public bool Iscommutable { get; set; }
        public bool Ischangednotify { get; set; }
        public List<PropertyInfo> Props { get; set; } = new List<PropertyInfo>();
        public List<string> Usingspaces { get; set; } = new List<string>();
    }
}

using CodeGenerator.Utils;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Scriban;

namespace CodeGenerator.Shielded
{
    [Generator]
    public class ShieldedPocoGenerator : ISourceGenerator
    {
        public void Initialize(GeneratorInitializationContext context)
        {
            //System.Diagnostics.Debugger.Launch();
            ResLoader.LoadDll();
            context.RegisterForSyntaxNotifications(() => new ShieldedPocoFilter());
        }

        public void Execute(GeneratorExecutionContext context)
        {
            if (context.SyntaxReceiver is ShieldedPocoFilter receiver)
            {
                var str = ResLoader.LoadTemplate("ShieldedPoco.liquid");
                Template agentTemplate = Template.Parse(str);
                foreach (var agent in receiver.AgentList)
                {
                    string fullName = agent.GetFullName();
                    var info = new PocoInfo();
                    info.Super = agent.Identifier.Text;
                    info.Name = "__shielded" + info.Super;
                    info.Structname = "__struct" + info.Super;
                    info.Space = Tools.GetNameSpace(fullName);
                    //处理Using
                    CompilationUnitSyntax root = agent.SyntaxTree.GetCompilationUnitRoot();
                    foreach (UsingDirectiveSyntax element in root.Usings)
                    {
                        info.Usingspaces.Add(element.Name.ToString());
                    }
                    info.Usingspaces.Add(Tools.GetNameSpace(fullName));

                    if (agent.BaseList != null)
                    {
                        foreach (var parent in agent.BaseList.Types)
                        {
                            var pname = parent.ToString();
                            if (pname.Equals("ICommutable"))
                                info.Iscommutable = true;
                            else if (pname.Equals("IChangedNotify"))
                                info.Ischangednotify = true;
                        }
                    }

                    foreach (var member in agent.Members)
                    {
                        if (member is PropertyDeclarationSyntax property)
                        {
                            //only handle public virtual props
                            bool isVirtual = false;
                            bool isPublic = false;
                            foreach (var m in property.Modifiers)
                            {
                                if (m.Text.Equals("virtual"))
                                    isVirtual = true;
                                if (m.Text.Equals("public"))
                                    isPublic = true;
                            }
                            if (isVirtual && isPublic)
                            {
                                var finfo = new PropertyInfo
                                {
                                    Name = property.Identifier.Text,
                                    Type = property.Type.ToString()
                                };
                                info.Props.Add(finfo);
                            }
                        }
                    }
                    var source = agentTemplate.Render(info);
                    context.AddSource($"{info.Name}.g.cs", source);
                }
            }
        }

    }
}
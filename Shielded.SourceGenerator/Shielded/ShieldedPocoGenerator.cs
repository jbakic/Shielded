using CodeGenerator.Utils;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Scriban;
using System;
using System.Linq;

namespace CodeGenerator.Shielded
{
    [Generator]
    public class ShieldedPocoGenerator : ISourceGenerator
    {
        public void Initialize(GeneratorInitializationContext context)
        {
            //open comment for debug
            //System.Diagnostics.Debugger.Launch();
            ResLoader.LoadDll();
            context.RegisterForSyntaxNotifications(() => new ShieldedPocoFilter());
        }

        /// <summary>
        /// check poco class with [Shielded] attribute is iegal 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="syntax"></param>
        /// <returns></returns>
        private bool IegalCheck(GeneratorExecutionContext context, ClassDeclarationSyntax syntax)
        {
            var semanticModel = context.Compilation.GetSemanticModel(syntax.SyntaxTree);
            var symbol = semanticModel.GetDeclaredSymbol(syntax);
            if (symbol.IsSealed)
            {
                context.LogError($"Unable to make proxies for sealed class: {symbol}");
                return false;
            }
            if (!symbol.DeclaredAccessibility.ToString().ToLower().Equals("public"))
            {
                context.LogError($"Unable to make proxies for non public class: {symbol}");
                return false;
            }
          
            bool hasNoArgumentConstructor = false;
            foreach (var con in symbol.Constructors)
            {
                if (con.Parameters.Count() == 0)
                {
                    hasNoArgumentConstructor = true;
                }
            }
            if (!hasNoArgumentConstructor)
            {
                context.LogError($"Unable to make proxies for class without no-argument Constructor : {symbol.Name}");
                return false;
            }

            return true;
        }

        public void Execute(GeneratorExecutionContext context)
        {
            if (context.SyntaxContextReceiver is ShieldedPocoFilter receiver)
            {
                var str = ResLoader.LoadTemplate("ShieldedPoco.liquid");
                Template agentTemplate = Template.Parse(str);
                foreach (var agent in receiver.ShieldedList)
                {
                    if (!IegalCheck(context, agent))
                    {
                        return;
                    }
                    string fullName = agent.GetFullName();
                    var info = new PocoInfo();
                    info.Super = agent.Identifier.Text;
                    info.Name = "__shielded" + info.Super;
                    info.Structname = "__struct" + info.Super;
                    info.Space = Tools.GetNameSpace(fullName);

                    //collect usings
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

                    var semanticModel = context.Compilation.GetSemanticModel(agent.SyntaxTree);
                    var props = agent.GetAllProperties(semanticModel);
                    foreach (var p in props)
                    {
                        string getterModifer = p.GetMethod.DeclaredAccessibility.ToString().ToLower();
                        if (getterModifer.Equals("public"))
                            getterModifer = "";
                        string setterModifer = p.SetMethod.DeclaredAccessibility.ToString().ToLower();
                        if (setterModifer.Equals("public"))
                            setterModifer = "";

                        string pname = p.ToString();
                        pname = pname.Substring(pname.LastIndexOf('.')+1);
                        //only handle public virtual props
                        if (p.IsPublic() && p.IsVirtual)
                        {
                            var finfo = new PropertyInfo
                            {
                                Name = pname,
                                Type = p.Type.ToString(),
                                Gettermodifier = getterModifer,
                                Settermodifier = setterModifer
                            };
                            info.Props.Add(finfo);
                        }
                    }

                    var source = agentTemplate.Render(info);
                    context.AddSource($"{info.Name}.g.cs", source);
                }
            }
        }

    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using CodeGenerator.Utils;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace CodeGenerator.Shielded
{
    public class ShieldedPocoFilter : ISyntaxReceiver
    {
        public List<ClassDeclarationSyntax> AgentList { get; private set; } = new List<ClassDeclarationSyntax>();

        public void OnVisitSyntaxNode(SyntaxNode syntaxNode)
        {
            if (syntaxNode is ClassDeclarationSyntax syntax)
            {
                if (IsShieldPoco(syntax))
                {
                    AgentList.Add(syntax);
                }
            }
        }

        public bool IsShieldPoco(ClassDeclarationSyntax source)
        {
            foreach (var a in source.AttributeLists)
            {
                var attStr = a.ToString().RemoveWhitespace();
                if (attStr.Contains("[Shielded]") || attStr.Contains("[ShieldedAttribute]"))
                {
                    return true;
                }
            }
            return false;
        }
    }
}

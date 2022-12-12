using System;
using System.Collections.Generic;
using System.Linq;
using CodeGenerator.Utils;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace CodeGenerator.Shielded
{
    public class ShieldedPocoFilter : ISyntaxContextReceiver
    {
        public List<ClassDeclarationSyntax> ShieldedList { get; private set; } = new List<ClassDeclarationSyntax>();

        const string ShieldedAttribute = "Shielded.ShieldedAttribute";
        public void OnVisitSyntaxNode(GeneratorSyntaxContext context)
        {
            if (context.Node is ClassDeclarationSyntax syntax)
            {
                if (syntax.HasAttribute(context.SemanticModel, ShieldedAttribute))
                {
                    ShieldedList.Add(syntax);
                }
            }
        }

    }
}

using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace CodeGenerator.Utils
{
    public static class SyntaxExt
    {
        public const string NESTED_CLASS_DELIMITER = "+";
        public const string NAMESPACE_CLASS_DELIMITER = ".";

        public static string GetFullName(this ClassDeclarationSyntax source)
        {
            Contract.Requires(null != source);

            var items = new List<string>();
            var parent = source.Parent;
            while (parent.IsKind(SyntaxKind.ClassDeclaration))
            {
                var parentClass = parent as ClassDeclarationSyntax;
                Contract.Assert(null != parentClass);
                items.Add(parentClass.Identifier.Text);

                parent = parent.Parent;
            }

            var nameSpace = parent as NamespaceDeclarationSyntax;
            Contract.Assert(null != nameSpace);
            var sb = new StringBuilder().Append(nameSpace.Name).Append(NAMESPACE_CLASS_DELIMITER);
            items.Reverse();
            items.ForEach(i => { sb.Append(i).Append(NESTED_CLASS_DELIMITER); });
            sb.Append(source.Identifier.Text);

            var result = sb.ToString();
            return result;
        }

        public static string GetNameSpace(this ClassDeclarationSyntax source)
        {
            Contract.Requires(null != source);

            var items = new List<string>();
            var parent = source.Parent;
            while (parent.IsKind(SyntaxKind.ClassDeclaration))
            {
                var parentClass = parent as ClassDeclarationSyntax;
                Contract.Assert(null != parentClass);
                items.Add(parentClass.Identifier.Text);

                parent = parent.Parent;
            }

            var nameSpace = parent as NamespaceDeclarationSyntax;
            Contract.Assert(null != nameSpace);
            var sb = new StringBuilder().Append(nameSpace.Name).Append(NAMESPACE_CLASS_DELIMITER);
            items.Reverse();
            items.ForEach(i => { sb.Append(i).Append(NESTED_CLASS_DELIMITER); });
            sb.Append(source.Identifier.Text);

            var result = sb.ToString();
            return result;
        }

        /// <summary>
        /// Find self or ancestors if has [ShieldAttribute]
        /// </summary>
        /// <param name="source"></param>
        /// <param name="semanticModel"></param>
        /// <param name="attributeName"></param>
        /// <returns></returns>
        public static bool HasAttribute(this ClassDeclarationSyntax source, SemanticModel semanticModel, string attributeName)
        {
            INamedTypeSymbol typeSymbol = semanticModel.GetDeclaredSymbol(source);
            do
            {
                foreach (var att in typeSymbol.GetAttributes())
                {
                    if (att.AttributeClass.ToString().Equals(attributeName))
                    {
                        return true;
                    }
                }
                typeSymbol = typeSymbol.BaseType;
            }
            while (typeSymbol != null);
            return false;
        }

        /// <summary>
        /// Get all properties include ancestor's
        /// </summary>
        /// <param name="source"></param>
        /// <param name="semanticModel"></param>
        /// <returns></returns>
        public static List<IPropertySymbol> GetAllProperties(this ClassDeclarationSyntax source, SemanticModel semanticModel)
        {
            List<IPropertySymbol> properties = new List<IPropertySymbol>();
            INamedTypeSymbol typeSymbol = semanticModel.GetDeclaredSymbol(source);
            do
            {
                var result = typeSymbol.GetMembers().Where(s => s.Kind == SymbolKind.Property).ToList();
                foreach (var p in result)
                {
                    if (p is IPropertySymbol ps)
                        properties.Add(ps);
                }
                typeSymbol = typeSymbol.BaseType;
            }
            while (typeSymbol != null);
            return properties;
        }

        public static bool IsPublic(this ISymbol symbol)
        {
            return symbol.DeclaredAccessibility.ToString().ToLower().Equals("public");
        }

        public static string GetModifier(this ISymbol symbol)
        {
            return symbol.DeclaredAccessibility.ToString().ToLower();
        }

    }
}

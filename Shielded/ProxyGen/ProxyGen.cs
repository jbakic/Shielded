using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CSharp;
using System.CodeDom.Compiler;
using System.CodeDom;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.ComponentModel;
using System.Collections.Concurrent;
using Shielded;

namespace Shielded.ProxyGen
{
    static class ProxyGen
    {
        static ConcurrentDictionary<Type, Type> proxies = new ConcurrentDictionary<Type, Type>();
        const string ShieldedField = "_data";
        const string ShieldedValueProperty = "Value";
        const string CommuteMethod = "Commute";
        const string OnChangedMethod = "OnChanged";
        const string ShieldedDll = "Shielded.dll";

        /// <summary>
        /// Prepare the specified types.
        /// </summary>
        public static void Prepare(Type[] types)
        {
            if (!types.Any())
                return;
            if (types.Any(NothingToDo.With))
                throw new InvalidOperationException(
                    "Unable to make proxies for types: " +
                    string.Join(", ", types.Where(NothingToDo.With)));

            var unpreparedTypes = types
                .Where(t => !proxies.ContainsKey(t))
                .ToArray();
            var compiledAssembly = MakeAssembly(cu => {
                cu.ReferencedAssemblies.Add(ShieldedDll);
                foreach (var loc in unpreparedTypes.SelectMany(GetReferences).Distinct())
                    cu.ReferencedAssemblies.Add(loc);

                foreach (var t in unpreparedTypes)
                    PrepareType(t, cu);
            });
            foreach (var t in unpreparedTypes)
                proxies.TryAdd(t, compiledAssembly.GetType(
                    t.Namespace + "." + GetNameForDerivedClass(t)));
        }

        private static IEnumerable<string> GetReferences(Type t)
        {
            return t.GetInterfaces()
                .Select(i => i.Assembly.Location)
                .Concat(new[] { t.BaseType.Assembly.Location, t.Assembly.Location })
                .Distinct();
        }

        public static bool IsProxy(Type t)
        {
            return t.BaseType != null && proxies.ContainsKey(t.BaseType);
        }

        public static Type GetFor(Type t)
        {
            if (IsProxy(t))
                return t;
            return proxies.GetOrAdd(t, CreateFor);
        }

        private static Type CreateFor(Type t)
        {
            if (NothingToDo.With(t))
                throw new InvalidOperationException(
                    "Unable to create proxy type - base type must be public and have virtual properties.");

            var compiledAssembly = MakeAssembly(cu => {
                foreach (var assembly in GetReferences(t))
                    cu.ReferencedAssemblies.Add(assembly);
                cu.ReferencedAssemblies.Add(ShieldedDll);
                PrepareType(t, cu);
            });
            return compiledAssembly.GetTypes()[0];
        }

        static void PrepareType(Type t, CodeCompileUnit cu)
        {
            var ns = CreateNamespace(t);
            CreateType(t, ns);
            cu.Namespaces.Add(ns);
        }

        private static Assembly MakeAssembly(Action<CodeCompileUnit> contentGenerator)
        {
            var provider = new CSharpCodeProvider();
            CompilerParameters cp = new CompilerParameters();
            cp.GenerateInMemory = true;
            CodeCompileUnit cu = new CodeCompileUnit();
            contentGenerator(cu);

#if DEBUG
            StringWriter sw = new StringWriter();
            provider.GenerateCodeFromCompileUnit(cu,sw , new CodeGeneratorOptions() { BracingStyle="C" });
            var s = sw.GetStringBuilder().ToString();
            Trace.WriteLine(s);
#endif

            CompilerResults cr = provider.CompileAssemblyFromDom(cp,cu);
            if (cr.Errors.Count > 0)
                ThrowErrors(cr.Errors);
            return cr.CompiledAssembly;
        }

        private static void ThrowErrors(CompilerErrorCollection compilerErrorCollection)
        {
            StringBuilder sb = new StringBuilder();
            foreach (CompilerError  e in compilerErrorCollection)
                sb.AppendLine(e.ErrorText);
            throw new ProxyGenerationException("Compiler errors:\n" + sb.ToString());
        }

        private static CodeNamespace CreateNamespace(Type t)
        {
            return new CodeNamespace(t.Namespace);
        }

        private static void CreateType(Type t, CodeNamespace nsp)
        {
            var decl = new CodeTypeDeclaration();
            decl.Name = GetNameForDerivedClass(t);
            decl.TypeAttributes = TypeAttributes.NotPublic;
            decl.Attributes = MemberAttributes.Private;
            decl.BaseTypes.Add(t);

            var theStruct = new CodeTypeDeclaration();
            theStruct.Name = GetNameForSupportingStruct(t);
            theStruct.TypeAttributes = TypeAttributes.NestedPrivate;
            theStruct.Attributes = MemberAttributes.Private;
            theStruct.IsStruct = true;

            var theShieldedType = new CodeTypeReference("Shielded.Shielded",
                new CodeTypeReference(theStruct.Name));
            var theShieldedField = new CodeMemberField(theShieldedType, ShieldedField) {
                Attributes = MemberAttributes.Private
            };
            decl.Members.Add(theShieldedField);

            var constructor = new CodeConstructor() {
                Attributes = MemberAttributes.Public,
            };
            constructor.Statements.Add(new CodeAssignStatement(
                new CodeFieldReferenceExpression(
                    new CodeThisReferenceExpression(),
                    ShieldedField),
                new CodeObjectCreateExpression(
                    theShieldedType,
                    new CodeThisReferenceExpression())));
            decl.Members.Add(constructor);

            var onChanged = GetOnChanged(t);
            bool hasCommute = HasCommute(t);
            foreach (PropertyInfo pi in
                t.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                .Where(IsInteresting))
            {
                theStruct.Members.Add(CreateStructField(pi));
                decl.Members.Add(CreatePropertyOverride(theStruct.Name, pi, onChanged));
            }

            if (hasCommute)
            {
                decl.Members.Add(new CodeMemberMethod() {
                    Attributes = MemberAttributes.Override | MemberAttributes.Public,
                    Name = CommuteMethod,
                    Parameters = {
                        new CodeParameterDeclarationExpression(typeof(Action), "a")
                    },
                    Statements = {
                        new CodeMethodInvokeExpression(
                            new CodeFieldReferenceExpression(
                                new CodeThisReferenceExpression(), ShieldedField),
                            CommuteMethod,
                            new CodeArgumentReferenceExpression("a"))
                    }
                });
            }

            decl.Members.Add(theStruct);
            nsp.Types.Add(decl);
        }

        internal static bool HasCommute(Type t)
        {
            var commMethod = t.GetMethod(CommuteMethod);
            if (commMethod == null || !commMethod.IsVirtual)
                return false;
            var commParameters = commMethod.GetParameters();
            return commParameters.Length == 1 && commParameters[0].ParameterType == typeof(Action);
        }

        internal static CodeMethodReferenceExpression GetOnChanged(Type t)
        {
            var onChanged = t.GetMethods(BindingFlags.NonPublic | BindingFlags.Instance)
                .FirstOrDefault(m => m.Name == OnChangedMethod);
            if (onChanged == null)
                return null;
            var parameters = onChanged.GetParameters();
            return parameters.Length == 1 && parameters[0].ParameterType == typeof(string)
                ? new CodeMethodReferenceExpression(new CodeThisReferenceExpression(), OnChangedMethod)
                : null;
        }

        internal static bool IsInteresting(PropertyInfo pi)
        {
            if (!pi.CanRead || !pi.CanWrite)
                return false;
            var access = pi.GetAccessors(true);
            // must have both accessors, both virtual, and both either public or protected, no mixing.
            if (access == null || access.Length != 2 || !access[0].IsVirtual)
                return false;
            if (access[0].IsPublic && access[1].IsFamily || access[0].IsFamily && access[1].IsPublic)
                throw new InvalidOperationException("Virtual property accessors must both be public, or both protected.");
            return access[0].IsPublic && access[1].IsPublic || access[0].IsFamily && access[1].IsFamily;
        }

        private static CodeTypeMember CreateStructField(PropertyInfo pi)
        {
            CodeMemberField field = new CodeMemberField();
            field.Name = pi.Name;
            field.Attributes = MemberAttributes.Public;
            field.Type = new CodeTypeReference(pi.PropertyType);
            return field;
        }

        private static CodeMemberProperty CreatePropertyOverride(string structType, PropertyInfo pi,
            CodeMethodReferenceExpression changeMethod)
        {
            CodeMemberProperty mp = new CodeMemberProperty();
            mp.Name = pi.Name;
            mp.Attributes = MemberAttributes.Override |
                (pi.GetAccessors(true)[0].IsPublic ? MemberAttributes.Public : MemberAttributes.Family);
            mp.Type = new CodeTypeReference(pi.PropertyType);
            mp.HasGet = mp.HasSet = true;
            
            mp.GetStatements.Add(new CodeMethodReturnStatement(new CodeFieldReferenceExpression() {
                TargetObject = new CodePropertyReferenceExpression() {
                    TargetObject = new CodeFieldReferenceExpression() {
                        TargetObject = new CodeThisReferenceExpression(),
                        FieldName = ShieldedField,
                    },
                    PropertyName = ShieldedValueProperty,
                },
                FieldName = pi.Name,
            }));

            // call base setter first, so they can see the old value (using getter) and new (using value)
            mp.SetStatements.Add(new CodeAssignStatement(
                new CodePropertyReferenceExpression() {
                    TargetObject = new CodeBaseReferenceExpression(),
                    PropertyName = pi.Name,
                }, new CodePropertySetValueReferenceExpression()));
            // using Modify has a huge advantage - if a class is big, we don't want to be passing
            // copies of the underlying struct around.
            mp.SetStatements.Add(new CodeSnippetStatement(
                string.Format("{0}.Modify((ref {1} a) => a.@{2} = value);",
                    ShieldedField, structType, pi.Name)));
            
            if (changeMethod != null)
                mp.SetStatements.Add(new CodeMethodInvokeExpression(
                    changeMethod, new CodePrimitiveExpression(pi.Name)));

            return mp;
        }

       

        private static string GetNameForDerivedClass(Type t)
        {
            return string.Concat("__shielded", t.Name);
        }

        private static string GetNameForSupportingStruct(Type t)
        {
            return string.Concat("__struct", t.Name);
        }
    }
}

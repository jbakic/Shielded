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
        private const string ShieldedFieldName = "_data";
        private const string ShieldedValueProperty = "Value";

        /// <summary>
        /// Prepare the specified types. They should all be in the same namespace.
        /// </summary>
        public static void Prepare(Type[] types)
        {
            if (!types.Any())
                return;

            var unpreparedTypes = types
                .Where(t => !proxies.ContainsKey(t))
                .ToArray();
            var compiledAssembly = MakeAssembly(cu => {
                cu.ReferencedAssemblies.Add("Shielded.dll");
                foreach (var loc in unpreparedTypes.Select(t => t.Assembly.Location).Distinct())
                    cu.ReferencedAssemblies.Add(loc);

                foreach (var t in unpreparedTypes)
                {
                    var ns = CreateNamespace(t);
                    CreateType(t, ns);
                    cu.Namespaces.Add(ns);
                }
            });
            foreach (var t in unpreparedTypes)
                proxies.TryAdd(t, compiledAssembly.GetType(
                    t.Namespace + "." + GetNameForDerivedClass(t)));
        }

        public static Type GetFor(Type t)
        {
            return proxies.GetOrAdd(t, CreateFor);
        }

        private static Type CreateFor(Type t)
        {
            var compiledAssembly = MakeAssembly(cu => {
                cu.ReferencedAssemblies.Add(t.Assembly.Location);
                cu.ReferencedAssemblies.Add("Shielded.dll");

                var ns = CreateNamespace(t);
                CreateType(t, ns);
                cu.Namespaces.Add(ns);
            });
            return compiledAssembly.GetTypes()[0];
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
            {
                ThrowErrors(cr.Errors);
            }
            return cr.CompiledAssembly;
        }

        private static void ThrowErrors(CompilerErrorCollection compilerErrorCollection)
        {
            StringBuilder sb = new StringBuilder();
            foreach( CompilerError  e in compilerErrorCollection)
            {
                sb.AppendLine(e.ErrorText);
            }
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
            var theShieldedField = new CodeMemberField(theShieldedType, "_data") {
                Attributes = MemberAttributes.Private
            };
            theShieldedField.InitExpression = new CodeObjectCreateExpression(theShieldedType);
            decl.Members.Add(theShieldedField);

            foreach (PropertyInfo pi in t.GetProperties().Where(IsInteresting))
            {
                theStruct.Members.Add(CreateStructField(pi));
                decl.Members.Add(CreatePropertyOverride(theStruct.Name, pi));
            }

            MethodInfo commMethod;
            if ((commMethod = t.GetMethod("Commute")) != null &&
                commMethod.IsVirtual)
            {
                var commOverride = new CodeMemberMethod() {
                    Attributes = MemberAttributes.Override | MemberAttributes.Public,
                    Name = "Commute",
                };
                commOverride.Parameters.Add(new CodeParameterDeclarationExpression(
                    new CodeTypeReference(typeof(Action)),
                    "a"));
                commOverride.ReturnType = new CodeTypeReference(typeof(void));
                commOverride.Statements.Add(new CodeMethodInvokeExpression(
                    new CodeFieldReferenceExpression(
                        new CodeThisReferenceExpression(),
                        "_data"),
                    "Commute",
                    new CodeArgumentReferenceExpression("a")));
                decl.Members.Add(commOverride);
            }

            decl.Members.Add(theStruct);
            nsp.Types.Add(decl);
        }

        internal static bool IsInteresting(PropertyInfo pi)
        {
            MethodInfo getter, setter;
            return pi.CanRead && (getter = pi.GetGetMethod()) != null && getter.IsVirtual &&
                pi.CanWrite && (setter = pi.GetSetMethod()) != null && setter.IsVirtual;
        }

        private static CodeTypeMember CreateStructField(PropertyInfo pi)
        {
            CodeMemberField field = new CodeMemberField();
            field.Name = pi.Name;
            field.Attributes = MemberAttributes.Public;
            field.Type = new CodeTypeReference(pi.PropertyType);
            return field;
        }

        private static CodeMemberProperty CreatePropertyOverride(string structType, PropertyInfo pi)
        {
            CodeMemberProperty mp = new CodeMemberProperty();
            mp.Name = pi.Name;
            mp.Attributes = MemberAttributes.Override | MemberAttributes.Public;
            mp.Type = new CodeTypeReference(pi.PropertyType);
            mp.HasGet = mp.HasSet = true;
            
            mp.GetStatements.Add(new CodeMethodReturnStatement(new CodeFieldReferenceExpression() {
                TargetObject = new CodePropertyReferenceExpression() {
                    TargetObject = new CodeFieldReferenceExpression() {
                        TargetObject = new CodeThisReferenceExpression(),
                        FieldName = ShieldedFieldName,
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
                string.Format("{0}.Modify((ref {1} a) => a.{2} = value);",
                    ShieldedFieldName, structType, pi.Name)));

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

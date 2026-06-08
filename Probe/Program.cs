using System.Reflection;

var asm = Assembly.LoadFrom(@"C:\Users\renev\.nuget\packages\graphql\8.0.2\lib\net6.0\GraphQL.dll");

void Dump(Type t)
{
    Console.WriteLine($"=== {t.FullName} (base: {t.BaseType?.FullName}) ===");
    foreach (var ctor in t.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
    {
        Console.WriteLine("  ctor(" + string.Join(", ", ctor.GetParameters().Select(p => p.ParameterType.Name + " " + p.Name)) + ")");
    }
    foreach (var prop in t.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly))
    {
        Console.WriteLine($"  prop  {prop.PropertyType.Name} {prop.Name} {{ {(prop.CanRead ? "get;" : "")}{(prop.CanWrite ? " set;" : "")} }}");
    }
    foreach (var fld in t.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly))
    {
        Console.WriteLine($"  field {fld.FieldType.Name} {fld.Name}");
    }
}

string[] types = { "GraphQL.Execution.RootExecutionNode", "GraphQL.Execution.ObjectExecutionNode", "GraphQL.Execution.ArrayExecutionNode", "GraphQL.Execution.ValueExecutionNode", "GraphQL.Execution.ExecutionNode", "GraphQL.Execution.ExecutionNode`1" };
foreach (var tn in types)
{
    var t = asm.GetType(tn);
    if (t == null) { Console.WriteLine($"-- {tn} not found"); continue; }
    Dump(t);
}

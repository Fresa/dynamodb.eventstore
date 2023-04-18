using System.Text;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace DynamoDB.EventStore.SystemTests.Telemetry;

internal sealed class XUnitLogger<T> : ILogger<T>
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly string _categoryName;
    private readonly LoggerExternalScopeProvider _scopeProvider;

    public static ILogger<T> CreateLogger(ITestOutputHelper testOutputHelper) => new XUnitLogger<T>(
        testOutputHelper,
        new LoggerExternalScopeProvider(),
        typeof(T).FullName ?? throw new InvalidOperationException($"{typeof(T)} doesn't have a fullname"));

    private XUnitLogger(ITestOutputHelper testOutputHelper, LoggerExternalScopeProvider scopeProvider, string categoryName)
    {
        _testOutputHelper = testOutputHelper;
        _scopeProvider = scopeProvider;
        _categoryName = categoryName;
    }

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

    public IDisposable BeginScope<TState>(TState state) => _scopeProvider.Push(state);

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        var sb = new StringBuilder();
        sb
            .Append($"[{DateTime.Now} {GetLogLevelString(logLevel)} {_categoryName}] ")
            .Append(formatter(state, exception));

        if (exception != null)
        {
            sb.AppendLine()
                .Append(exception);
        }

        // Append scopes
        _scopeProvider.ForEachScope((scope, builder) =>
        {
            builder.AppendLine();
            builder.Append($" => {scope}");
        }, sb);

        _testOutputHelper.WriteLine(sb.ToString());
    }

    private static string GetLogLevelString(LogLevel logLevel)
    {
        return logLevel switch
        {
            LogLevel.Trace => "TRACE",
            LogLevel.Debug => "DEBUG",
            LogLevel.Information => "INFO",
            LogLevel.Warning => "WARN",
            LogLevel.Error => "ERROR",
            LogLevel.Critical => "FATAL",
            LogLevel.None => "",
            _ => throw new ArgumentOutOfRangeException(nameof(logLevel))
        };
    }
}
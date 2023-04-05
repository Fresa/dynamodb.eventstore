using System.Diagnostics;
using Xunit.Abstractions;

namespace DynamoDB.EventStore.IntegrationTests.Amazon;

internal sealed class TestOutputHelperTraceListener : TraceListener
{
    private readonly ITestOutputHelper _testOutputHelper;

    private readonly Guid _name = Guid.NewGuid();
    private static readonly AsyncLocal<Guid> CurrentExecutionIdentifier = new();

    public TestOutputHelperTraceListener(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
        CurrentExecutionIdentifier.Value = _name;
        Name = _name.ToString();
    }

    public override bool IsThreadSafe => true;

    public override void Fail(string? message)
    {
        if (message == null)
            return;

        Write(TraceEventType.Critical, message);
    }

    public override void Fail(string? message, string? detailMessage)
    {
        if (message == null &&
            detailMessage == null)
            return;

        Write(TraceEventType.Critical, "{0}", new
        {
            Message = message,
            Details = detailMessage
        });
    }

    public override void TraceData(
        TraceEventCache? eventCache,
        string source,
        TraceEventType eventType,
        int id,
        object? data)
    {
        if (!ShouldTrace(eventCache, source, eventType, id, "", data1: data))
            return;

        if (data == null)
            return;

        Write(source, eventType, "{0}", data);
    }

    public override void TraceData(
        TraceEventCache? eventCache,
        string source,
        TraceEventType eventType,
        int id,
        params object?[]? data)
    {
        if (!ShouldTrace(eventCache, source, eventType, id, "", data: data))
            return;

        if (data == null || !data.Any())
            return;

        var format = Enumerable.Range(0, data.Length - 1)
            .Aggregate("", (currentTemplate, index) => $"{currentTemplate} {{{index}}}")
            .Trim();
        Write(source: source, eventType, format, data);
    }

    public override void TraceEvent(
        TraceEventCache? eventCache,
        string source,
        TraceEventType eventType,
        int id)
    {
    }

    public override void TraceEvent(
        TraceEventCache? eventCache,
        string source,
        TraceEventType eventType,
        int id,
        string? message)
    {
        if (!ShouldTrace(eventCache, source, eventType, id, message))
            return;

        if (message == null)
            return;

        TraceEvent(eventCache, source, eventType, id, message, args: null);
    }

    public override void TraceEvent(
        TraceEventCache? eventCache,
        string source,
        TraceEventType eventType,
        int id,
        string? format,
        params object?[]? args)
    {
        if (!ShouldTrace(eventCache, source, eventType, id, format, args))
            return;

        if (format == null)
            return;

        Write(source: source, level: eventType, exception: null, format, args ?? Array.Empty<object>());
    }

    public override void Write(object? data) =>
        Write(TraceEventType.Verbose, "{0}", data);

    public override void Write(string? message)
    {
        if (message == null)
            return;

        Write(TraceEventType.Verbose, message);
    }

    public override void Write(object? data, string? category) =>
        Write(TraceEventType.Verbose, "{0}", data);

    public override void Write(string? message, string? category)
    {
        if (message == null)
            return;
        Write(TraceEventType.Verbose, null, message);
    }

    public override void WriteLine(string? message) =>
        Write(message);

    public override void WriteLine(object? data) =>
        Write(data);

    public override void WriteLine(string? message, string? category) =>
        Write(message, category);

    public override void WriteLine(object? data, string? category) =>
        Write(data, category);

    private void Write(
        string source,
        TraceEventType eventType,
        string messageTemplate,
        params object?[] args) =>
        Write(source: source, eventType, exception: null, messageTemplate, args);

    private void Write(
        TraceEventType level,
        string messageTemplate,
        params object?[] args) =>
        Write(level, null, messageTemplate, args);

    private void Write(
        TraceEventType level,
        Exception? exception,
        string messageTemplate,
        params object?[] args) =>
        Write(source: null, level, exception, messageTemplate, args);

    private void Write(
        string? source,
        TraceEventType level,
        Exception? exception,
        string messageTemplate,
        params object?[] args)
    {
        if (CurrentExecutionIdentifier.Value != _name)
            return;

        _testOutputHelper.WriteLine($"[{Enum.GetName(level)}] {source} | {messageTemplate}", args);
        if (exception != null)
        {
            _testOutputHelper.WriteLine(exception.ToString());
        }
    }

    private bool ShouldTrace(
        TraceEventCache? cache,
        string source,
        TraceEventType eventType,
        int id,
        string? formatOrMessage,
        object?[]? args = null,
        object? data1 = null,
        object?[]? data = null)
    {
        var filter = Filter;
        return filter == null || filter.ShouldTrace(cache, source, eventType, id, formatOrMessage, args, data1, data);
    }
}
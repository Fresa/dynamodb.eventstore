namespace DynamoDB.EventStore;

public abstract class Aggregate
{
    protected Aggregate(string id)
    {
        Id = id;
    }

    /// <summary>
    /// The id of the aggregate. Is used as the partition key in DynamoDB.
    /// </summary>
    protected string Id { get; }
    internal string IdInternal => Id;

    /// <summary>
    /// The version of the aggregate. Get's monotonically increased when persisting a new commit.
    /// </summary>
    protected int Version => VersionInternal;
    internal int VersionInternal { get; set; }

    /// <summary>
    /// Read capacity units used to read the events since last snapshot. Get's set when calling <see cref="EventStore.LoadAsync"/>.
    /// </summary>
    protected double ReadCapacityUnitsSinceLastSnapshot => ReadCapacityUnitsSinceLastSnapshotInternal;
    internal double ReadCapacityUnitsSinceLastSnapshotInternal { get; set; }

    /// <summary>
    /// Get's called during <see cref="EventStore.SaveAsync"/> if <see cref="ShouldCreateSnapshot"/> is true.
    /// </summary>
    /// <param name="cancellationToken">Cancellation</param>
    /// <returns>An awaitable task</returns>
    protected abstract Task<MemoryStream> CreateSnapShotAsync(CancellationToken cancellationToken);
    internal Task<MemoryStream> CreateSnapShotInternalAsync(CancellationToken cancellationToken = default) => CreateSnapShotAsync(cancellationToken);

    /// <summary>
    /// Instructs the event store to create a snapshot when calling <see cref="EventStore.SaveAsync"/>.
    /// </summary>
    protected virtual bool ShouldCreateSnapshot => ReadCapacityUnitsSinceLastSnapshot > 3;
    internal bool ShouldCreateSnapshotInternal => ShouldCreateSnapshot;

    /// <summary>
    /// Tells the event store if a snapshot should be read when calling <see cref="EventStore.LoadAsync"/>.
    /// Defaults to true.
    /// </summary>
    protected virtual bool ShouldReadSnapshots => true;
    internal bool ShouldReadSnapshotsInternal => ShouldReadSnapshots;

    /// <summary>
    /// Get's called when a snapshot is found while loading the event stream if <see cref="ShouldReadSnapshots"/> is true.
    /// </summary>
    /// <param name="snapshot">The snapshot loaded from the database</param>
    /// <param name="cancellationToken">Cancellation</param>
    /// <returns>An awaitable task</returns>
    protected abstract Task LoadSnapshotAsync(MemoryStream snapshot, CancellationToken cancellationToken);
    internal Task LoadSnapshotInternalAsync(MemoryStream snapshot, CancellationToken cancellationToken = default) => LoadSnapshotAsync(snapshot, cancellationToken);

    /// <summary>
    /// Get's called for each commit of events returned by the event store when calling <see cref="EventStore.LoadAsync"/>.
    /// </summary>
    /// <param name="events">The committed events in the order they where committed</param>
    /// <param name="cancellationToken">Cancellation</param>
    /// <returns>An awaitable task</returns>
    protected abstract Task LoadEventsAsync(IEnumerable<MemoryStream> events, CancellationToken cancellationToken);
    internal Task LoadEventsInternalAsync(IEnumerable<MemoryStream> events, CancellationToken cancellationToken = default) => LoadEventsAsync(events, cancellationToken);

    /// <summary>
    /// Events that get's committed when calling <see cref="EventStore.SaveAsync"/>. This list is emptied after the call.
    /// </summary>
    protected List<MemoryStream> UncommittedEvents { get; } = new();
    internal List<MemoryStream> UncommittedEventsInternal => UncommittedEvents;
}
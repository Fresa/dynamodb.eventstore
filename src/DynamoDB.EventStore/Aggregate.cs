namespace DynamoDB.EventStore;

public abstract class Aggregate
{
    protected Aggregate(string id)
    {
        Id = id;
    }

    protected string Id { get; }
    internal string IdInternal => Id;

    protected int Version => VersionInternal;
    internal int VersionInternal { get; set; }

    protected double ReadCapacityUnitsSinceLastSnapshot => ReadCapacityUnitsSinceLastSnapshotInternal;
    internal double ReadCapacityUnitsSinceLastSnapshotInternal { get; set; }

    protected abstract Task<MemoryStream> CreateSnapShotAsync(CancellationToken cancellationToken);
    internal Task<MemoryStream> CreateSnapShotInternalAsync(CancellationToken cancellationToken = default) => CreateSnapShotAsync(cancellationToken);

    protected virtual bool ShouldCreateSnapshot => ReadCapacityUnitsSinceLastSnapshot > 3;
    internal bool ShouldCreateSnapshotInternal => ShouldCreateSnapshot;

    protected virtual bool ShouldReadSnapshots => true;
    internal bool ShouldReadSnapshotsInternal => ShouldReadSnapshots;

    protected abstract Task LoadSnapshotAsync(MemoryStream snapshot, CancellationToken cancellationToken);
    internal Task LoadSnapshotInternalAsync(MemoryStream snapshot, CancellationToken cancellationToken = default) => LoadSnapshotAsync(snapshot, cancellationToken);

    protected abstract Task LoadEventsAsync(IEnumerable<MemoryStream> events, CancellationToken cancellationToken);
    internal Task LoadEventsInternalAsync(IEnumerable<MemoryStream> events, CancellationToken cancellationToken = default) => LoadEventsAsync(events, cancellationToken);

    protected List<MemoryStream> UncommittedEvents { get; } = new();
    internal List<MemoryStream> UncommittedEventsInternal => UncommittedEvents;
}
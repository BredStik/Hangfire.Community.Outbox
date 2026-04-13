using Hangfire;
using Hangfire.Common;
using Hangfire.Community.Outbox.Entities;
using Hangfire.Community.Outbox.Extensions;
using Hangfire.States;
using Microsoft.EntityFrameworkCore;

namespace Hangfire.Community.Outbox.Tests;

public sealed class TestDbContext : DbContext
{
    public DbSet<OutboxJob> OutboxJobs => Set<OutboxJob>();

    public TestDbContext(DbContextOptions<TestDbContext> options) : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.MapOutboxJobs();
        base.OnModelCreating(modelBuilder);
    }
}

public sealed class SamplePayload
{
    public string Name { get; set; } = string.Empty;
    public int Count { get; set; }
}

public sealed class TestJobHandler
{
    public Task ProcessAsync(int value, SamplePayload payload, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task ProcessAsync(string value)
    {
        return Task.CompletedTask;
    }

    public void Process(int value)
    {
    }
}

public sealed class RecordingBackgroundJobClient : IBackgroundJobClient
{
    private int _nextId = 1;

    public List<(Job Job, IState State)> CreatedJobs { get; } = [];

    public Func<Job, IState, string>? OnCreate { get; set; }

    public string Create(Job job, IState state)
    {
        CreatedJobs.Add((job, state));
        return OnCreate?.Invoke(job, state) ?? (_nextId++).ToString();
    }

    public bool ChangeState(string jobId, IState state, string expectedState)
    {
        throw new NotSupportedException();
    }
}

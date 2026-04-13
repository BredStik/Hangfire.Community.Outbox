using Hangfire.Common;
using Hangfire.Community.Outbox.Entities;
using Hangfire.Community.Outbox.Extensions;
using Hangfire.Community.Outbox.Services;
using Hangfire.States;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hangfire.Community.Outbox.Tests;

public class OutboxProcessorTests
{
    [Fact]
    public async Task Process_creates_expected_states_and_updates_rows()
    {
        var services = CreateServices();
        var client = new RecordingBackgroundJobClient();
        services.AddSingleton<IBackgroundJobClient>(client);

        using var provider = services.BuildServiceProvider();
        await SeedAsync(provider, db =>
        {
            var immediate = OutboxJob.Create<TestJobHandler>(x => x.Process(1), "default");
            immediate.CreatedOn = DateTime.UtcNow.AddMinutes(-3);

            var scheduled = OutboxJob.Create<TestJobHandler>(x => x.ProcessAsync(2, new SamplePayload { Name = "scheduled", Count = 1 }, CancellationToken.None), DateTimeOffset.UtcNow.AddMinutes(5), "emails");
            scheduled.CreatedOn = DateTime.UtcNow.AddMinutes(-2);

            var delayed = OutboxJob.Create<TestJobHandler>(x => x.ProcessAsync(3, new SamplePayload { Name = "delayed", Count = 2 }, CancellationToken.None), TimeSpan.FromMinutes(1), "reports");
            delayed.CreatedOn = DateTime.UtcNow.AddMinutes(-1);

            db.Add(immediate);
            db.Add(scheduled);
            db.Add(delayed);
        });

        var processor = new OutboxProcessor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<OutboxProcessor>.Instance, new HangfireOutboxOptions());

        await processor.Process(CancellationToken.None);

        Assert.Collection(
            client.CreatedJobs,
            job => Assert.IsType<EnqueuedState>(job.State),
            job => Assert.IsType<ScheduledState>(job.State),
            job => Assert.IsType<ScheduledState>(job.State));

        await AssertProcessedStateAsync(provider, jobs =>
        {
            Assert.All(jobs, job =>
            {
                Assert.True(job.Processed);
                Assert.NotNull(job.HangfireJobId);
                Assert.Null(job.Exception);
            });
            Assert.Equal(new[] { "default", "emails", "reports" }, jobs.Select(x => x.Queue).ToArray());
        });
    }

    [Fact]
    public async Task Process_respects_filtering_ordering_and_batch_size()
    {
        var services = CreateServices();
        var client = new RecordingBackgroundJobClient();
        services.AddSingleton<IBackgroundJobClient>(client);

        using var provider = services.BuildServiceProvider();
        await SeedAsync(provider, db =>
        {
            db.Add(CreateJob(1, DateTime.UtcNow.AddMinutes(-4)));
            db.Add(CreateJob(2, DateTime.UtcNow.AddMinutes(-3)));
            db.Add(CreateJob(3, DateTime.UtcNow.AddMinutes(-2)));
            db.Add(CreateJob(4, DateTime.UtcNow.AddMinutes(-5), processed: true));
            db.Add(CreateJob(5, DateTime.UtcNow.AddMinutes(-6), exception: "boom"));
        });

        var processor = new OutboxProcessor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<OutboxProcessor>.Instance,
            new HangfireOutboxOptions { OutboxProcessorBatchSize = 2 });

        await processor.Process(CancellationToken.None);

        Assert.Equal(2, client.CreatedJobs.Count);
        Assert.Equal(new[] { 1, 2 }, client.CreatedJobs.Select(x => (int)x.Job.Args[0]).ToArray());

        await AssertProcessedStateAsync(provider, jobs =>
        {
            var states = jobs.OrderBy(x => ((int)x.GetArguments().First())).ToArray();
            Assert.True(states[0].Processed);
            Assert.True(states[1].Processed);
            Assert.False(states[2].Processed);
            Assert.True(states[3].Processed);
            Assert.Equal("boom", states[4].Exception);
        });
    }

    [Fact]
    public async Task Process_records_exception_and_continues_with_remaining_jobs()
    {
        var services = CreateServices();
        var client = new RecordingBackgroundJobClient();
        services.AddSingleton<IBackgroundJobClient>(client);

        using var provider = services.BuildServiceProvider();
        await SeedAsync(provider, db =>
        {
            db.Add(CreateJob(1, DateTime.UtcNow.AddMinutes(-3)));

            var invalid = OutboxJob.Create<TestJobHandler>(x => x.Process(2));
            invalid.JobType = "Missing.Type, Missing.Assembly";
            invalid.CreatedOn = DateTime.UtcNow.AddMinutes(-2);
            db.Add(invalid);

            db.Add(CreateJob(3, DateTime.UtcNow.AddMinutes(-1)));
        });

        var processor = new OutboxProcessor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<OutboxProcessor>.Instance, new HangfireOutboxOptions());

        await processor.Process(CancellationToken.None);

        Assert.Equal(2, client.CreatedJobs.Count);

        await AssertProcessedStateAsync(provider, jobs =>
        {
            var ordered = jobs.OrderBy(x => (int)x.GetArguments().FirstOrDefault()!).ToArray();
            Assert.True(ordered[0].Processed);
            Assert.False(ordered[1].Processed);
            Assert.Contains("Could not resolve serialized job type", ordered[1].Exception);
            Assert.True(ordered[2].Processed);
        });
    }

    private static ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        var databaseName = Guid.NewGuid().ToString();

        services.AddDbContext<TestDbContext>(options => options.UseInMemoryDatabase(databaseName));
        services.AddScoped<IDbContextAccessor>(sp => new DbContextAccessor(() => sp.GetRequiredService<TestDbContext>()));

        return services;
    }

    private static async Task SeedAsync(IServiceProvider provider, Action<TestDbContext> seed)
    {
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        seed(db);
        await db.SaveChangesAsync();
    }

    private static async Task AssertProcessedStateAsync(IServiceProvider provider, Action<OutboxJob[]> assert)
    {
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var jobs = await db.Set<OutboxJob>().OrderBy(x => x.CreatedOn).ToArrayAsync();
        assert(jobs);
    }

    private static OutboxJob CreateJob(int value, DateTime createdOn, bool processed = false, string? exception = null)
    {
        var job = OutboxJob.Create<TestJobHandler>(x => x.Process(value));
        job.CreatedOn = createdOn;
        job.Processed = processed;
        job.Exception = exception;
        return job;
    }
}

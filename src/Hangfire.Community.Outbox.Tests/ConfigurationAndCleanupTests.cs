using Hangfire.Community.Outbox.BackgroundJobs;
using Hangfire.Community.Outbox.Entities;
using Hangfire.Community.Outbox.Extensions;
using Hangfire.Community.Outbox.HostedServices;
using Hangfire.Community.Outbox.Services;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hangfire.Community.Outbox.Tests;

public class ConfigurationAndCleanupTests
{
    [Fact]
    public void AddHangfireOutbox_registers_core_services_and_hosted_service_by_default()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(options => options.UseInMemoryDatabase(Guid.NewGuid().ToString()));

        services.AddHangfireOutbox<TestDbContext>(options =>
        {
            options.WithSchema("custom");
            options.WithTableName("OutboxItems");
            options.WithOutboxProcessorFrequency(TimeSpan.FromSeconds(9));
        });

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<HangfireOutboxOptions>());
        Assert.NotNull(provider.GetRequiredService<IDbContextAccessor>());
        Assert.NotNull(provider.GetRequiredService<IOutboxProcessor>());
        Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IHostedService) && descriptor.ImplementationType == typeof(OutboxJobsProcessorBackgroundService));

        var options = provider.GetRequiredService<HangfireOutboxOptions>();
        Assert.Equal("custom", options.OutboxJobSchema);
        Assert.Equal("OutboxItems", options.OutboxJobTableName);
        Assert.Equal(TimeSpan.FromSeconds(9), options.OutboxProcessorFrequency);
    }

    [Fact]
    public void AddHangfireOutbox_skips_hosted_service_when_hangfire_recurring_processor_is_selected()
    {
        var services = new ServiceCollection();
        services.AddDbContext<TestDbContext>(options => options.UseInMemoryDatabase(Guid.NewGuid().ToString()));

        services.AddHangfireOutbox<TestDbContext>(options =>
        {
            options.OutboxProcessor = HangfireOutboxOptions.OutboxProcessorType.HangfireRecurringJob;
        });

        Assert.DoesNotContain(services, descriptor => descriptor.ServiceType == typeof(IHostedService) && descriptor.ImplementationType == typeof(OutboxJobsProcessorBackgroundService));
    }

    [Fact]
    public async Task CleanOldJobs_deletes_only_processed_jobs_matching_retention_rules()
    {
        using var provider = BuildCleanupProvider(new HangfireOutboxOptions
        {
            CleanupOlderThan = TimeSpan.FromDays(30),
            CleanupJobsWithExceptions = false
        });

        await SeedAsync(provider, db =>
        {
            db.Add(new OutboxJobProxyBuilder().ProcessedOldSuccessful());
            db.Add(new OutboxJobProxyBuilder().ProcessedOldFailed());
            db.Add(new OutboxJobProxyBuilder().ProcessedRecentSuccessful());
            db.Add(new OutboxJobProxyBuilder().UnprocessedOldSuccessful());
        });

        var job = new OutboxJobsCleanupJob(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<OutboxJobsCleanupJob>.Instance, provider.GetRequiredService<HangfireOutboxOptions>());
        await job.CleanOldJobs(CancellationToken.None);

        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var remaining = await db.Set<OutboxJob>().OrderBy(x => x.CreatedOn).ToArrayAsync();

        Assert.Equal(3, remaining.Length);
        Assert.DoesNotContain(remaining, x => x.Processed && x.Exception == null && x.CreatedOn <= DateTime.UtcNow.AddDays(-30));
        Assert.Contains(remaining, x => x.Exception == "boom");
        Assert.Contains(remaining, x => !x.Processed);
    }

    [Fact]
    public async Task CleanOldJobs_can_delete_processed_jobs_with_exceptions_when_enabled()
    {
        using var provider = BuildCleanupProvider(new HangfireOutboxOptions
        {
            CleanupOlderThan = TimeSpan.FromDays(30),
            CleanupJobsWithExceptions = true
        });

        await SeedAsync(provider, db =>
        {
            db.Add(new OutboxJobProxyBuilder().ProcessedOldSuccessful());
            db.Add(new OutboxJobProxyBuilder().ProcessedOldFailed());
            db.Add(new OutboxJobProxyBuilder().ProcessedRecentSuccessful());
        });

        var job = new OutboxJobsCleanupJob(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<OutboxJobsCleanupJob>.Instance, provider.GetRequiredService<HangfireOutboxOptions>());
        await job.CleanOldJobs(CancellationToken.None);

        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var remaining = await db.Set<OutboxJob>().ToArrayAsync();

        Assert.Single(remaining);
        Assert.True(remaining[0].Processed);
        Assert.True(remaining[0].CreatedOn > DateTime.UtcNow.AddDays(-30));
    }

    private static ServiceProvider BuildCleanupProvider(HangfireOutboxOptions options)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        var services = new ServiceCollection();
        services.AddSingleton(connection);
        services.AddDbContext<TestDbContext>((sp, db) => db.UseSqlite(sp.GetRequiredService<SqliteConnection>()));
        services.AddScoped<IDbContextAccessor>(sp => new DbContextAccessor(() => sp.GetRequiredService<TestDbContext>()));
        services.AddSingleton(options);

        var provider = services.BuildServiceProvider();

        using var scope = provider.CreateScope();
        scope.ServiceProvider.GetRequiredService<TestDbContext>().Database.EnsureCreated();

        return provider;
    }

    private static async Task SeedAsync(IServiceProvider provider, Action<TestDbContext> seed)
    {
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        seed(db);
        await db.SaveChangesAsync();
    }

    private sealed class OutboxJobProxyBuilder
    {
        public OutboxJob ProcessedOldSuccessful()
        {
            return Build(processed: true, createdOn: DateTime.UtcNow.AddDays(-31));
        }

        public OutboxJob ProcessedOldFailed()
        {
            return Build(processed: true, createdOn: DateTime.UtcNow.AddDays(-31), exception: "boom");
        }

        public OutboxJob ProcessedRecentSuccessful()
        {
            return Build(processed: true, createdOn: DateTime.UtcNow.AddDays(-2));
        }

        public OutboxJob UnprocessedOldSuccessful()
        {
            return Build(processed: false, createdOn: DateTime.UtcNow.AddDays(-31));
        }

        private static OutboxJob Build(bool processed, DateTime createdOn, string? exception = null)
        {
            var job = OutboxJob.Create<TestJobHandler>(x => x.Process(123));
            job.Processed = processed;
            job.CreatedOn = createdOn;
            job.Exception = exception;
            return job;
        }
    }
}

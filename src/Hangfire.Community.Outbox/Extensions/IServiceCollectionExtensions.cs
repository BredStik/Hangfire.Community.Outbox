using Hangfire.Community.Outbox.HostedServices;
using Hangfire.Community.Outbox.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Hangfire.Community.Outbox.Extensions;

public static class IServiceCollectionExtensions
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="serviceCollection"></param>
    /// <param name="options"></param>
    /// <typeparam name="TDbContext"></typeparam>
    public static void AddHangfireOutbox<TDbContext>(this IServiceCollection serviceCollection, Action<HangfireOutboxOptions> options = null) where TDbContext : DbContext
    {
        //OPTIONS
        var outboxOptions = new HangfireOutboxOptions();
        options?.Invoke(outboxOptions);
        outboxOptions.UpdateStaticOptions();

        //SERVICE REGISTRATIONS
        serviceCollection.AddSingleton(outboxOptions);
        serviceCollection.AddSingleton<IJobDeduplicator, JobDeduplicator>();
        serviceCollection.AddScoped<IDbContextAccessor>(sp =>
            new DbContextAccessor(sp.GetRequiredService<TDbContext>));
        serviceCollection.AddSingleton<IOutboxProcessor, OutboxProcessor>();

        if (outboxOptions.OutboxProcessor == HangfireOutboxOptions.OutboxProcessorType.HostedService)
        {
            serviceCollection.AddHostedService<OutboxJobsProcessorBackgroundService>();
        }
    }
}

public class HangfireOutboxOptions
{
    /// <summary>
    /// Defaults to "HangFireOutbox"
    /// </summary>
    public string OutboxJobSchema { get; set; } = "HangFireOutbox";
    
    /// <summary>
    /// Sets the database schema in which the outbox table will be added and used
    /// </summary>
    /// <param name="schema"></param>
    /// <returns></returns>
    public HangfireOutboxOptions WithSchema(string schema)
    {
        OutboxJobSchema = schema;
        
        return this;
    }
    
    /// <summary>
    /// Defaults to "OutboxMessages"
    /// </summary>
    public string OutboxJobTableName { get; set; } = "OutboxJobs";
    
    /// <summary>
    /// Sets the table name that will be added and used
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public HangfireOutboxOptions WithTableName(string tableName)
    {
        OutboxJobTableName = tableName;
        
        return this;
    }
    
    /// <summary>
    /// Defaults to 'true'
    /// </summary>
    public bool CleanupJobEnabled { get; set; } = true;
    
    /// <summary>
    /// Enables a job that removes old items from the outbox table
    /// </summary>
    /// <returns></returns>
    public HangfireOutboxOptions EnableCleanupJob()
    {
        CleanupJobEnabled = true;

        return this;
    }
    
    /// <summary>
    /// Disables a job that removes old items from the outbox table
    /// </summary>
    /// <returns></returns>
    public HangfireOutboxOptions DisableCleanupJob()
    {
        CleanupJobEnabled = false;

        return this;
    }
    
    /// <summary>
    /// Defaults to 30 days
    /// </summary>
    public TimeSpan CleanupOlderThan { get; set; } = TimeSpan.FromDays(30);
    
    /// <summary>
    /// Specifies how old items have to be to get cleaned up by the cleanup job
    /// </summary>
    /// <param name="duration"></param>
    /// <returns></returns>
    public HangfireOutboxOptions CleanItemsOlderThan(TimeSpan duration)
    {
        CleanupOlderThan = duration;

        return this;
    }
    
    /// <summary>
    /// Defaults to 'false'
    /// </summary>
    public bool CleanupJobsWithExceptions { get; set; } = false;

    /// <summary>
    /// Cleanup job will remove items from the outbox table even if they have not succeeded
    /// </summary>
    /// <returns></returns>
    public HangfireOutboxOptions CleanItemsWithExceptions()
    {
        CleanupJobsWithExceptions = true;

        return this;
    }
    
    /// <summary>
    /// Defaults to '0 0 * * *'
    /// </summary>
    public string CleanupJobCron { get; set; } = "0 0 * * *";

    /// <summary>
    /// Modifies the cleanup job schedule
    /// </summary>
    /// <param name="cron"></param>
    /// <returns></returns>
    public HangfireOutboxOptions WithCleanUpJobSchedule(string cron)
    {
        CleanupJobCron = cron;

        return this;
    }

    /// <summary>
    /// Defaults to 5 seconds
    /// </summary>
    public TimeSpan OutboxProcessorFrequency { get; set; } = TimeSpan.FromSeconds(5);

    
    /// <summary>
    /// Modifies the outbox processor frequency
    /// </summary>
    /// <param name="frequency"></param>
    /// <returns></returns>
    public HangfireOutboxOptions WithOutboxProcessorFrequency(TimeSpan frequency)
    {
        OutboxProcessorFrequency = frequency;

        return this;
    }

    /// <summary>
    /// Sets the outbox processor type.  Can be either HostedService or HangfireRecurringJob.  WARNING: when using HostedService, if you have multiple instances of your host, this might cause unexpected behavior.
    /// </summary>
    public OutboxProcessorType OutboxProcessor { get; set; } = OutboxProcessorType.HostedService;

    
    /// <summary>
    /// Sets the outbox processor type to HostedService.  WARNING: if you have multiple instances of your host, this might cause unexpected behavior.
    /// </summary>
    /// <returns></returns>
    public HangfireOutboxOptions WithBackgroundServiceOutboxProcessor()
    {
        OutboxProcessor = OutboxProcessorType.HostedService;

        return this;
    }

    public int OutboxProcessorBatchSize { get; set; } = 1000;

    internal void UpdateStaticOptions()
    {
        HangfireOutboxStaticOptions.Options = this;
    }

    public enum OutboxProcessorType
    {
        HostedService,
        HangfireRecurringJob
    }
}

internal static class HangfireOutboxStaticOptions
{
    internal static HangfireOutboxOptions Options { get; set; } = new HangfireOutboxOptions();
}
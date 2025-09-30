using Hangfire.Community.Outbox.Entities;
using Hangfire.Community.Outbox.Extensions;
using Hangfire.Community.Outbox.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Hangfire.Community.Outbox.BackgroundJobs;

public class OutboxJobsCleanupJob
{
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly ILogger<OutboxJobsCleanupJob> _logger;
    private readonly HangfireOutboxOptions _outboxOptions;

    public OutboxJobsCleanupJob(IServiceScopeFactory serviceScopeFactory, ILogger<OutboxJobsCleanupJob> logger, HangfireOutboxOptions outboxOptions)
    {
        _serviceScopeFactory = serviceScopeFactory;
        _logger = logger;
        _outboxOptions = outboxOptions;
    }

    [DisableConcurrentExecution(timeoutInSeconds: 600)]
    public async Task CleanOldJobs(CancellationToken ct)
    {
        _logger.LogDebug("Cleaning old outbox jobs");
        
        using var scope = _serviceScopeFactory.CreateScope();
        var dbContextAccessor = scope.ServiceProvider.GetRequiredService<IDbContextAccessor>();
        await using var dbContext = dbContextAccessor.GetDbContext();

        var threshold = DateTime.UtcNow.Subtract(_outboxOptions.CleanupOlderThan);
        
        var query = dbContext.Set<OutboxJob>().Where(x => x.Processed && x.CreatedOn <= threshold);

        if (!_outboxOptions.CleanupJobsWithExceptions)
        {
            query = query.Where(x => x.Exception == null);
        }

        var toDelete = await query.Select(x => OutboxJob.BuildProxy(x.Id)).ToArrayAsync(ct);
        var deletedCount = toDelete.Length;

        foreach (var job in toDelete)
        {
            dbContext.Entry(job).State = EntityState.Deleted;
        }

        await dbContext.SaveChangesAsync(ct);

        _logger.LogDebug("Deleted {deletedCount} outbox jobs older than {threshold}", deletedCount, threshold);
    }
}
using Hangfire.Common;
using Hangfire.Community.Outbox.Entities;
using Hangfire.Community.Outbox.Extensions;
using Hangfire.States;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Hangfire.Community.Outbox.Services;

public class OutboxProcessor: IOutboxProcessor
{
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly ILogger<OutboxProcessor> _logger;
    private readonly HangfireOutboxOptions _options;

    public OutboxProcessor(IServiceScopeFactory serviceScopeFactory, ILogger<OutboxProcessor> logger, HangfireOutboxOptions options)
    {
        _serviceScopeFactory = serviceScopeFactory;
        _logger = logger;
        _options = options;
    }
    
    public async Task Process(CancellationToken ct)
    {
        using var scope = _serviceScopeFactory.CreateScope();
        var dbContextAccessor = scope.ServiceProvider.GetRequiredService<IDbContextAccessor>();
        await using var dbContext = dbContextAccessor.GetDbContext();
        
        var backgroundJobClient = scope.ServiceProvider.GetRequiredService<IBackgroundJobClient>();
        
        var toProcess = await dbContext.Set<OutboxJob>()
            .Where(x => !x.Processed && x.Exception == null)
            .OrderBy(x => x.CreatedOn)
            .Take(_options.OutboxProcessorBatchSize)
            .ToArrayAsync(ct);

        if (toProcess.Length == 0)
        {
            return;
        }
        
        _logger.LogDebug("Processing {nbJobs} outbox jobs", toProcess.Length);
        
        foreach (var outboxMessage in toProcess)
        {
            if (ct.IsCancellationRequested)
            {
                _logger.LogDebug("Cancellation requested");
                return;
            }
            
            try
            {
                _logger.LogDebug("Processing outbox job {id}", outboxMessage.Id);
                
                var jobType = outboxMessage.GetJobType();
                var job = new Job(jobType, outboxMessage.GetMethod(), outboxMessage.GetArguments().ToArray(), outboxMessage.Queue);

                string jobId = null;
                
                if (outboxMessage.EnqueueAt.HasValue)
                {
                    //schedule for specified date
                    jobId = backgroundJobClient.Create(job, new ScheduledState(outboxMessage.EnqueueAt.Value.DateTime));
                }
                else if (outboxMessage.Delay.HasValue)
                {
                    //schedule for specified delay
                    jobId = backgroundJobClient.Create(job, new ScheduledState(outboxMessage.Delay.Value));
                }
                else
                {
                    //enqueue now
                    jobId = backgroundJobClient.Create(job, new EnqueuedState(outboxMessage.Queue));
                }

                outboxMessage.Processed = true;
                outboxMessage.HangfireJobId = jobId;
                
                _logger.LogDebug("Successfully processed outbox job {id}", outboxMessage.Id);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Unable to process outbox job {id}", outboxMessage.Id);
                outboxMessage.Exception = e.ToString();
            }
        }

        _logger.LogDebug("Persisting outbox job changes");
        await dbContext.SaveChangesAsync(ct);
    }
}

public interface IOutboxProcessor
{
    Task Process(CancellationToken ct);
}
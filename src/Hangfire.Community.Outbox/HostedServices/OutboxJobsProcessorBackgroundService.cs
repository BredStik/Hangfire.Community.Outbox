using Hangfire.Community.Outbox.Extensions;
using Hangfire.Community.Outbox.Services;
using Hangfire.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Hangfire.Community.Outbox.HostedServices;

public class OutboxJobsProcessorBackgroundService: BackgroundService
{
    private readonly IOutboxProcessor _outboxProcessor;
    private readonly JobStorage _jobStorage;
    private readonly HangfireOutboxOptions _outboxOptions;
    private readonly ILogger<OutboxJobsProcessorBackgroundService> _logger;

    public OutboxJobsProcessorBackgroundService(IOutboxProcessor outboxProcessor, JobStorage jobStorage, HangfireOutboxOptions outboxOptions, ILogger<OutboxJobsProcessorBackgroundService> logger)
    {
        _outboxProcessor = outboxProcessor;
        _jobStorage = jobStorage;
        _outboxOptions = outboxOptions;
        _logger = logger;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        IDisposable distributedLock = null;
        var distributedLockSupported = _jobStorage.HasFeature(JobStorageFeatures.Transaction.AcquireDistributedLock);

        _logger.LogDebug("DistributedLock supported on Hangfire job storage: {enabled}", distributedLockSupported);

        while (!stoppingToken.IsCancellationRequested)
        {
            if (distributedLockSupported)
            {
                try
                {
                    _logger.LogDebug("Trying to obtain distributed lock");
                    distributedLock = _jobStorage.GetConnection().AcquireDistributedLock(nameof(OutboxJobsProcessorBackgroundService), TimeSpan.FromSeconds(10));
                    _logger.LogDebug("Obtained distributed lock");
                }
                catch (Exception exc)
                {
                    //could not obtain lock
                    _logger.LogWarning(exc, "Could not obtain distributed lock");
                }

                if (distributedLock == null && distributedLockSupported)
                {
                    _logger.LogDebug("Waiting before trying to update a distributed lock again");
                    await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                    continue;
                }
            }
            

            await ProcessOutboxMessages(stoppingToken);

            distributedLock!.Dispose();

            await Task.Delay(_outboxOptions.OutboxProcessorFrequency, stoppingToken);            
        }
    }

    private async Task ProcessOutboxMessages(CancellationToken stoppingToken)
    {
        await _outboxProcessor.Process(stoppingToken);
    }
}
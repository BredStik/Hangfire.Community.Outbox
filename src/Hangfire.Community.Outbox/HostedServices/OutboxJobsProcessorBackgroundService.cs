using Hangfire.Community.Outbox.Extensions;
using Hangfire.Community.Outbox.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Hangfire.Community.Outbox.HostedServices;

public class OutboxJobsProcessorBackgroundService : BackgroundService
{
    private readonly IOutboxProcessor _outboxProcessor;
    private readonly HangfireOutboxOptions _outboxOptions;
    private readonly ILogger<OutboxJobsProcessorBackgroundService> _logger;

    // Backoff state
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromMinutes(2);
    private TimeSpan _currentBackoff;

    public OutboxJobsProcessorBackgroundService(
        IOutboxProcessor outboxProcessor,
        HangfireOutboxOptions outboxOptions,
        ILogger<OutboxJobsProcessorBackgroundService> logger)
    {
        _outboxProcessor = outboxProcessor;
        _outboxOptions = outboxOptions;
        _logger = logger;
        _currentBackoff = outboxOptions.OutboxProcessorFrequency;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox processor started");

        using var timer = new PeriodicTimer(_outboxOptions.OutboxProcessorFrequency);

        // Run once immediately on startup, then on each tick
        do
        {
            await ProcessOutboxMessages(stoppingToken);
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));

        _logger.LogInformation("Outbox processor stopped");
    }

    private async Task ProcessOutboxMessages(CancellationToken stoppingToken)
    {
        try
        {
            await _outboxProcessor.Process(stoppingToken);

            // Reset backoff on success
            _currentBackoff = _outboxOptions.OutboxProcessorFrequency;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown — don't log as error, just exit
            _logger.LogDebug("Outbox processing cancelled during shutdown");
        }
        catch (Exception ex)
        {
            // Escalate backoff on failure: 2x each time up to MaxBackoff
            _currentBackoff = TimeSpan.FromMilliseconds(
                Math.Min(_currentBackoff.TotalMilliseconds * 2, MaxBackoff.TotalMilliseconds));

            _logger.LogError(ex,
                "Outbox processing failed. Backing off for {Backoff}s before next attempt",
                _currentBackoff.TotalSeconds);

            // Wait out the backoff before returning (so the PeriodicTimer
            // tick we already consumed doesn't immediately re-trigger)
            try
            {
                await Task.Delay(_currentBackoff, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Shutdown during backoff — fine
            }
        }
    }
}
namespace Hangfire.Community.Outbox.Services
{
    public class JobDeduplicator : IJobDeduplicator
    {
        private readonly Queue<long> _processedJobsQueue;
        private readonly Dictionary<long, OutboxJobInfo> _processedJobsDictionary;
        private readonly int _maxSize;
        private readonly object _lock = new object();

        public JobDeduplicator(int maxSize = 2048)
        {
            if(maxSize < 1)
            {
                throw new ArgumentException("Has to be greater than 0", nameof(maxSize));
            }

            _processedJobsQueue = new Queue<long>(maxSize);
            _processedJobsDictionary = new Dictionary<long, OutboxJobInfo>(maxSize);
            _maxSize = maxSize;
        }

        public bool TryGetProcessed(long jobId, out OutboxJobInfo outboxJobInfo)
        {
            lock (_lock)
            {
                return _processedJobsDictionary.TryGetValue(jobId, out outboxJobInfo);
            }
        }
        public void MarkAsProcessed(OutboxJobInfo outboxJobInfo)
        {
            lock (_lock)
            {
                //reached max size, dequeue oldest item to make space for newer item
                if (_processedJobsQueue.Count == _maxSize)
                {
                    var jobIdToRemove = _processedJobsQueue.Dequeue();
                    _processedJobsDictionary.Remove(jobIdToRemove);
                }

                _processedJobsQueue.Enqueue(outboxJobInfo.RowId);
                _processedJobsDictionary.Add(outboxJobInfo.RowId, outboxJobInfo);
            }
        }
    }

    public interface IJobDeduplicator
    {
        bool TryGetProcessed(long jobId, out OutboxJobInfo outboxJobInfo);
        void MarkAsProcessed(OutboxJobInfo outboxJobInfo);
    }

    public record OutboxJobInfo(long RowId, string HangfireJobId);
}

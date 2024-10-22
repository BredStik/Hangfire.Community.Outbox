using Hangfire.Community.Outbox.Services;

namespace Hangfire.Community.Outbox.Tests.Services
{
    public class JobDeduplicatorTests
    {
        [Fact]
        public void Given_a_fixed_size_deduplicator_when_enqueuing_more_items_than_capacity_should_remove_oldest_item ()
        {
            var sut = new JobDeduplicator(2);

            var job1 = new OutboxJobInfo(1, "1");
            var job2 = new OutboxJobInfo(2, "2");
            var job3 = new OutboxJobInfo(3, "3");

            sut.MarkAsProcessed(job1);
            sut.MarkAsProcessed(job2);

            Assert.True(sut.TryGetProcessed(job1.RowId, out var retrievedJob1));
            Assert.Same(job1, retrievedJob1);
            Assert.True(sut.TryGetProcessed(job2.RowId, out var retrievedJob2));
            Assert.Same(job2, retrievedJob2); 

            sut.MarkAsProcessed(job3);

            Assert.True(sut.TryGetProcessed(job3.RowId, out var retrievedJob3));
            Assert.Same(job3, retrievedJob3);
            Assert.False(sut.TryGetProcessed(1, out _));
        }
    }
}

using System.Reflection;
using System.Text.Json;
using Hangfire.Community.Outbox.Entities;

namespace Hangfire.Community.Outbox.Tests;

public class OutboxJobTests
{
    [Fact]
    public void Create_round_trips_arguments_and_resolves_method()
    {
        var payload = new SamplePayload { Name = "alpha", Count = 7 };

        var job = OutboxJob.Create<TestJobHandler>(
            x => x.ProcessAsync(42, payload, CancellationToken.None),
            "critical");

        var arguments = job.GetArguments().ToArray();
        var method = job.GetMethod();

        Assert.Equal("critical", job.Queue);
        Assert.Equal(3, arguments.Length);
        Assert.Equal(42, Assert.IsType<int>(arguments[0]));

        var restoredPayload = Assert.IsType<SamplePayload>(arguments[1]);
        Assert.Equal("alpha", restoredPayload.Name);
        Assert.Equal(7, restoredPayload.Count);

        var restoredToken = Assert.IsType<CancellationToken>(arguments[2]);
        Assert.Equal(CancellationToken.None, restoredToken);

        Assert.Equal(nameof(TestJobHandler.ProcessAsync), method.Name);
        Assert.Equal(typeof(int), method.GetParameters()[0].ParameterType);
        Assert.Equal(typeof(SamplePayload), method.GetParameters()[1].ParameterType);
        Assert.Equal(typeof(CancellationToken), method.GetParameters()[2].ParameterType);
    }

    [Fact]
    public void GetArguments_throws_when_argument_type_cannot_be_resolved()
    {
        var job = OutboxJob.Create<TestJobHandler>(x => x.Process(1));
        SetPrivateProperty(job, "ArgumentTypesJson", JsonSerializer.Serialize(new[] { "Missing.Type, Missing.Assembly" }));
        SetPrivateProperty(job, "ArgumentValuesJson", "[null]");

        var exception = Assert.Throws<InvalidOperationException>(() => job.GetArguments().ToArray());

        Assert.Contains("Could not resolve serialized argument types", exception.Message);
        Assert.Contains("Missing.Type, Missing.Assembly", exception.Message);
    }

    [Fact]
    public void GetJobType_throws_when_job_type_cannot_be_resolved()
    {
        var job = OutboxJob.Create<TestJobHandler>(x => x.Process(1));
        job.JobType = "Missing.Type, Missing.Assembly";

        var exception = Assert.Throws<InvalidOperationException>(job.GetJobType);

        Assert.Equal("Could not resolve serialized job type 'Missing.Type, Missing.Assembly'", exception.Message);
    }

    private static void SetPrivateProperty(object target, string propertyName, object? value)
    {
        var property = target.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(property);
        property!.SetValue(target, value);
    }
}

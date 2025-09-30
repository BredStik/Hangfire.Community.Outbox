using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Hangfire.Common;
using Hangfire.Community.Outbox.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Hangfire.Community.Outbox.Entities;

[EntityTypeConfiguration(typeof(OutboxJobEntityTypeConfiguration))]
public class OutboxJob
{
    private OutboxJob()
    { }
    
    private OutboxJob(long id)
    {
        Id = id;
    }

    /// <summary>
    /// Builds an OutboxJob instance with only the Id populated.
    /// Allows to delete using EF without loading all properties.
    /// </summary>
    /// <param name="id"></param>
    /// <returns></returns>
    internal static OutboxJob BuildProxy(long id)
    {
        return new OutboxJob(id);
    }

    private static string SerializeArgs(IReadOnlyList<object> args)
    {
        return JsonSerializer.Serialize(args.Select(a => a?.GetType() == typeof(CancellationToken) ? null : a));
    }
    
    private static OutboxJob Build(Job job, string queue)
    {
        return new OutboxJob()
        {
            JobType = job.Type.GetFriendlyName(),
            ArgumentTypesJson =
                JsonSerializer.Serialize(job.Args.Select(a => a.GetType().GetFriendlyName())),
            ArgumentValuesJson = SerializeArgs(job.Args),
            MethodName = job.Method.Name,
            CreatedOn = DateTime.UtcNow,
            Queue = queue
        };
    }
    
    private static OutboxJob Build(Job job, DateTimeOffset enqueueAt, string queue)
    {
        return new OutboxJob()
        {
            JobType = job.Type.GetFriendlyName(),
            ArgumentTypesJson =
                JsonSerializer.Serialize(job.Args.Select(a => a.GetType().GetFriendlyName())),
            ArgumentValuesJson = SerializeArgs(job.Args),
            MethodName = job.Method.Name,
            CreatedOn = DateTime.UtcNow,
            EnqueueAt = enqueueAt,
            Queue = queue
        };
    }
    
    private static OutboxJob Build(Job job, TimeSpan delay, string queue)
    {
        return new OutboxJob()
        {
            JobType = job.Type.GetFriendlyName(),
            ArgumentTypesJson =
                JsonSerializer.Serialize(job.Args.Select(a => a.GetType().GetFriendlyName())),
            ArgumentValuesJson = SerializeArgs(job.Args),
            MethodName = job.Method.Name,
            CreatedOn = DateTime.UtcNow,
            Delay = delay,
            Queue = queue
        };
    }
    
    public static OutboxJob Create<T>(Expression<Func<T, Task>> expression, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), queue);
    }
    
    public static OutboxJob Create(Expression<Func<Task>> expression, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), queue);
    }
    
    public static OutboxJob Create<T>(Expression<Action<T>> expression, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), queue);
    }
    
    public static OutboxJob Create(Expression<Action> expression, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), queue);
    }
    
    public static OutboxJob Create<T>(Expression<Func<T, Task>> expression, DateTimeOffset enqueueAt, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), enqueueAt, queue);
    }
    
    public static OutboxJob Create(Expression<Func<Task>> expression, DateTimeOffset enqueueAt, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), enqueueAt, queue);
    }
    
    public static OutboxJob Create<T>(Expression<Action<T>> expression, DateTimeOffset enqueueAt, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), enqueueAt, queue);
    }
    
    public static OutboxJob Create(Expression<Action> expression, DateTimeOffset enqueueAt, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), enqueueAt, queue);
    }
    
    public static OutboxJob Create<T>(Expression<Func<T, Task>> expression, TimeSpan delay, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), delay, queue);
    }
    
    public static OutboxJob Create(Expression<Func<Task>> expression, TimeSpan delay, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), delay, queue);
    }
    
    public static OutboxJob Create<T>(Expression<Action<T>> expression, TimeSpan delay, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), delay, queue);
    }
    
    public static OutboxJob Create(Expression<Action> expression, TimeSpan delay, string queue = null)
    {
        return Build(Job.FromExpression(expression, queue), delay, queue);
    }
    
    public long Id { get; set; }
    public string JobType { get; set; }
    public string MethodName { get; set; }
    private string ArgumentTypesJson { get; set; }
    private string ArgumentValuesJson { get; set; }
    public string? Queue { get; set; }
    public string? HangfireJobId { get; set; }
    
    public DateTimeOffset? EnqueueAt { get; set; }
    public TimeSpan? Delay { get; set; }
    
    public IEnumerable<object> GetArguments()
    {
        var argumentTypeStrings = JsonSerializer.Deserialize<string[]>(ArgumentTypesJson);
        var types = argumentTypeStrings.Select(t => Type.GetType(t)).ToArray();

        if (types.Any(t => t == null))
        {
            var notFound = types.Select((type, index) => new { type, index }).Where(x => x.type == null).Select(x => x.index);
            var unresolvedTypes = string.Join("\\r\\n", notFound.Select(i => argumentTypeStrings.ElementAt(i)));
            
            throw new InvalidOperationException($"Could not resolve serialized argument types:\\r\\n{unresolvedTypes}");
        }
        
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(ArgumentValuesJson));
        var element = JsonElement.ParseValue(ref reader);
        var elements = element.EnumerateArray();
        
        for (int i = 0; i < elements.Count(); i++)
        {
            if (types[i] == typeof(CancellationToken))
            {
                yield return CancellationToken.None;
                continue;
            }
            
            yield return elements.ElementAt(i).Deserialize(types[i]);
        }
    }

    public MethodInfo GetMethod()
    {
        var argTypes = GetArguments().Select(x => x.GetType()).ToArray();
        return GetJobType().GetMethod(MethodName, argTypes);
    }

    public Type GetJobType()
    {
        var jobType = Type.GetType(JobType);

        if (jobType == null)
        {
            throw new InvalidOperationException($"Could not resolve serialized job type '{JobType}'");
        }

        return jobType;
    }
    
    public DateTime CreatedOn { get; set; }
    public bool Processed { get; set; }
    public string? Exception { get; set; }
    
    
    public class OutboxJobEntityTypeConfiguration : IEntityTypeConfiguration<OutboxJob>
    {
        public void Configure(EntityTypeBuilder<OutboxJob> builder)
        {
            builder.ToTable(HangfireOutboxStaticOptions.Options.OutboxJobTableName, HangfireOutboxStaticOptions.Options.OutboxJobSchema);

            builder.Property(x => x.JobType).IsRequired();
            builder.Property(x => x.Exception).IsRequired(false);
            builder.Property(x => x.ArgumentTypesJson).IsRequired();
            builder.Property(x => x.ArgumentValuesJson).IsRequired();
            builder.Property(x => x.Queue).IsRequired(false);
            builder.Property(x => x.MethodName).IsRequired();
            builder.Property(x => x.EnqueueAt).IsRequired(false);
            builder.Property(x => x.Delay).IsRequired(false);

            builder.HasIndex(x => x.Processed).HasDatabaseName("IDX_Processed");
            builder.HasIndex(x => x.CreatedOn).HasDatabaseName("IDX_CreatedOn");
        }
    }
}
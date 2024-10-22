# Hangfire.Community.Outbox
Outbox pattern implementation for Hangfire using EntityFramework Core

1. [Motivation](#1-motivation)
2. [Requirements](#2-requirements)
3. [Installation](#3-installation)
4. [Setup](#4-setup)
5. [Usage](#5-usage)

## 1. Motivation
Basically, the need to enqueue a Hangfire job as part of an EntityFramework unit of work...

IMPORTANT: If your DbContext's connection is the same as Hangfire's (ie: both use the same SQL Server db), you can probably enqueue a job as part of the same ambient transaction as your application's code and you probably don't need this project.

Sometimes, as part of a business process, we need to persist data to multiple stores as part of a transaction but oftentimes a distributed transaction is impossible.  The outbox pattern allows saving the information as part of a single ACID database transaction and process further messages asynchronously garanteeing an at least once delivery of messages to other systems (ie: service bus, emails, other databases, stc.).  

However, if your Hangfire's store is not RDMBS or your store's database is not the same as your application's database, you would need a distributed transaction which is not currently supported.  This is where this project comes in handy as it allows saving the jobs as outbox messages in your application's DbContext!

A classic example of the outbox pattern usage is sending an email as part of a business process.  Let's say we want to send an email to a customer as part of an order processing method, we would normally save some changes to our database and use Hangfire to schedule an email to the customer.  Most of the time, this works fine.  However, if the transaction that persists the application's state fails and/or rolls back, Hangfire would still send the email as its enqueuing was not persisted as part of the same transaction.  The project allows enqueuing job as outbox messages ensuring they will only be enqueued if the application's transaction succeeds.

![Diagram](https://raw.githubusercontent.com/BredStik/Hangfire.Community.Outbox/2ed5b4df772f15531d7d8197e8bc078a73630114/diagram.png)

For a great explanation of what the outbox pattern is and what it tries to solve, please read these excellent blog posts from Derek Comartin and Milan Jovanović:

[Outbox Pattern: Reliably Save State & Publish Events](https://codeopinion.com/outbox-pattern-reliably-save-state-publish-events/)

[Outbox Pattern For Reliable Microservices Messaging](https://www.milanjovanovic.tech/blog/outbox-pattern-for-reliable-microservices-messaging)

## 2. Requirements
1. Hangfire 1.8 or above
2. .net 6 project with EntityFramework Core 6
3. .net 7 project with EntityFramework Core 7
4. .net 8 project with EntityFramework Core 8

## 3. Installation
```
dotnet add package Hangfire.Community.Outbox
```

## 4. Setup
### Program.cs
In your program setup, register Hangfire Outbox by passing your application DbContext as a generic argument:

```
builder.Services.AddHangfireOutbox<AppDbContext>();
```

Then add this line:

```
app.UseHangfireOutbox();
```
### DbContext.OnModelCreating

If you haven't already, override the OnModelCreating function of your DbContext and add the following call:
```
modelBuilder.MapOutboxJobs();
```

ie:

```
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
  modelBuilder.MapOutboxJobs();
  base.OnModelCreating(modelBuilder);
}
```

This will map an entity that will persist jobs to be started at a later time as part of the EntityFramework unit of work.

### EntityFramework migration
Create a migration to create the outbox table in your database:
```
dotnet ef migrations add AddOutboxTable
```
Update your database to apply the pending migration(s):
```
dotnet ef database update
```

You're now setup and ready to use the outbox pattern to queue your Hangfire jobs!

## 5. Usage
Whenever you want to queue or schedule a job as part of an EntityFramework unit of work, just use one of the following approach using the same syntax as you would when queuing directly with Hangfire:
### Using extension methods on the DbContext instance
```
dbContext.EnqueueOutbox(() => Console.WriteLine("Hello from hangfire!"));
```

### Using extension methods on the IBackgroundJobClient instance
```
backgroundJobClient.EnqueueOutbox(() => Console.WriteLine("Hello from hangfire!"), dbContext);
```

Make sure to save changes on your DbContext to persist the outbox job:
```
await dbContext.SaveChangesAsync();
```

The Hangfire job will only be enqueued or scheduled when the changes are persisted to the database or an enclosing transaction completes.

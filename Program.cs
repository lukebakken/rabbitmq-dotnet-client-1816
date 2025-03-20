using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

const string queueName = "rabbitmq-dotnet-client-1816";

string Now() => DateTime.Now.ToString("s");

ConnectionFactory cf = new();

var tcs = new TaskCompletionSource();
using var cts = new CancellationTokenSource();
using var ctsr = cts.Token.Register(() => tcs.SetCanceled());

Console.CancelKeyPress += Console_CancelKeyPress;

void Console_CancelKeyPress(object? sender, ConsoleCancelEventArgs e)
{
    e.Cancel = true;
    cts.Cancel();
}

var messageCollection = new BlockingCollection<string>(1);

async Task Publisher()
{
    ushort idx = 0;
    await using IConnection conn = await cf.CreateConnectionAsync(cancellationToken: cts.Token);
    var opts = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
    await using IChannel ch = await conn.CreateChannelAsync(opts, cancellationToken: cts.Token);
    await ch.QueueDeclareAsync(queue: queueName, exclusive: false);

    try
    {
        while (true)
        {
            string msgStr = $"message: {idx}";
            idx++;
            await Console.Out.WriteLineAsync($"{Now()} [INFO] publishing msg: {msgStr}");
            var msgBytes = Encoding.ASCII.GetBytes(msgStr);
            cts.Token.ThrowIfCancellationRequested();
            await ch.BasicPublishAsync(exchange: string.Empty, routingKey: queueName, body: msgBytes, cts.Token);
            await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
        }
    }
    catch (OperationCanceledException)
    {
    }
}

async Task Consumer()
{
    try
    {
        await using IConnection conn = await cf.CreateConnectionAsync(cancellationToken: cts.Token);
        await using IChannel ch = await conn.CreateChannelAsync(cancellationToken: cts.Token);
        await ch.BasicQosAsync(prefetchCount: 1, prefetchSize: 0, global: false);
        await ch.QueueDeclareAsync(queue: queueName, exclusive: false);

        var consumer = new AsyncEventingBasicConsumer(channel: ch);
        consumer.ReceivedAsync += Consumer_ReceivedAsync;

        await ch.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);
        await tcs.Task;
    }
    catch (OperationCanceledException)
    {
    }
}

async Task Consumer_ReceivedAsync(object sender, BasicDeliverEventArgs e)
{
    var consumer = (AsyncEventingBasicConsumer)sender;
    IChannel ch = consumer.Channel;
    try
    {
        string msg = Encoding.ASCII.GetString(e.Body.ToArray());
        await Console.Out.WriteLineAsync($"{Now()} [INFO] received msg: {msg}");

        if (messageCollection.TryAdd(msg))
        {
            await ch.BasicAckAsync(e.DeliveryTag, multiple: false);
        }
        else
        {
            await Console.Out.WriteLineAsync($"{Now()} [WARNING] nacked msg: {msg}, deliveryTag: {e.DeliveryTag}");
            await ch.BasicNackAsync(e.DeliveryTag, requeue: true, multiple: false);
        }
    }
    catch (Exception ex)
    {
        await Console.Error.WriteLineAsync($"{Now()} [ERROR] error while consuming message: {ex}");
    }
}

async Task Worker()
{
    TimeSpan messageTimeout = TimeSpan.FromSeconds(1);

    while (true)
    {
        using var timeoutCts = new CancellationTokenSource(messageTimeout);
        using var lts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, timeoutCts.Token);
        try
        {
            string msg = messageCollection.Take(lts.Token);
            await Console.Out.WriteLineAsync($"{Now()} [INFO] worker read msg from channel: {msg}");
        }
        catch (OperationCanceledException)
        {
            if (timeoutCts.IsCancellationRequested)
            {
                await Console.Out.WriteLineAsync($"{Now()} [INFO] worker timed out waiting for a message!");
            }
            else if (cts.Token.IsCancellationRequested)
            {
                await Console.Out.WriteLineAsync($"{Now()} [INFO] worker is exiting!");
                return;
            }
            else
            {
                await Console.Error.WriteLineAsync($"{Now()} [ERROR] unexpected cancellation in worker!");
            }
        }
        catch (Exception ex)
        {
            await Console.Error.WriteLineAsync($"{Now()} [ERROR] unexpected exception in worker: {ex}");
        }
    }
}

var publisherTask = Task.Run(Publisher);
var consumerTask = Task.Run(Consumer);
var workerTask = Task.Run(Worker);

await Console.Out.WriteLineAsync($"{Now()} [INFO] CTRL-C to exit!");

try
{
    await tcs.Task;
}
catch (OperationCanceledException)
{
}
catch (Exception ex)
{
    await Console.Error.WriteLineAsync($"{Now()} [ERROR] unexpected exception: {ex}");
}

await Console.Out.WriteLineAsync($"{Now()} [INFO] exiting!");
await Task.WhenAll(publisherTask, consumerTask, workerTask).WaitAsync(TimeSpan.FromSeconds(5));

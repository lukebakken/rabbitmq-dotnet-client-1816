using RabbitMQ.Client;
using System.Text;

const string queueName = "rabbitmq-dotnet-client-1816";

var msg = Encoding.ASCII.GetBytes("Hello World!");

ConnectionFactory cf = new();

var cts = new CancellationTokenSource();

async Task Publisher()
{
    await using IConnection conn = await cf.CreateConnectionAsync(cancellationToken: cts.Token);
    var opts = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
    await using IChannel ch = await conn.CreateChannelAsync(opts, cancellationToken: cts.Token);

    while (true)
    {
        cts.Token.ThrowIfCancellationRequested();
        await ch.BasicPublishAsync(exchange: string.Empty, routingKey: queueName, body: msg, cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(5));
    }
}

async Task Consumer()
{
    await using IConnection conn = await cf.CreateConnectionAsync(cancellationToken: cts.Token);
    await using IChannel ch = await conn.CreateChannelAsync(cancellationToken: cts.Token);
}

async Task Worker()
{
}

var publisherTask = Task.Run(Publisher);
var consumerTask = Task.Run(Consumer);
var workerTask = Task.Run(Worker);

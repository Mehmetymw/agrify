using System.Net.Sockets;
using System.Text;
using EventBus.Base.Events;
using Newtonsoft.Json;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace EventBus.RabbitMQ;


public class EventBusRabbitMQ : BaseEventBus
{
    RabbitMQPersistenceConnection rabbitMQPersistenceConnection;
    private IConnectionFactory connectionFactory;
    private IModel consumerChannel;
    public EventBusRabbitMQ(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
    {
        if (config.Connection != null)
        {
            var connJson = JsonConvert.SerializeObject(config.Connection, new JsonSerializerSettings()
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore
            });
            connectionFactory = JsonConvert.DeserializeObject<IConnectionFactory>(connJson);
        }
        else
        {
            connectionFactory = new ConnectionFactory();
        }

        rabbitMQPersistenceConnection = new RabbitMQPersistenceConnection(connectionFactory, config.ConnectionRetryCount);

        consumerChannel = CreateConsumerChannel();

        SubsManager.OnEventRemoved += SubsManager_OnEventRemoved;
    }

    private void SubsManager_OnEventRemoved(object? sender, string eventName)
    {
        eventName = ProcessEventName(eventName);

        if (!rabbitMQPersistenceConnection.IsConnected)
        {
            rabbitMQPersistenceConnection.TryConnect();
        }

        consumerChannel.QueueUnbind(queue: eventName,
            exchange: EventBusConfig.DefaultTopicName,
            routingKey: eventName);

        if (SubsManager.IsEmpty)
        {
            consumerChannel.Close();
        }
    }
    public override Task PublishAsync(IntegrationEvent @event)
    {
        if (!rabbitMQPersistenceConnection.IsConnected)
        {
            rabbitMQPersistenceConnection.TryConnect();
        }

        var policy = Policy.Handle<BrokerUnreachableException>()
            .Or<SocketException>()
            .WaitAndRetry(EventBusConfig.ConnectionRetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
            {
                // log
            });

        var eventName = @event.GetType().Name;
        eventName = ProcessEventName(eventName);

        consumerChannel.ExchangeDeclare(exchange: EventBusConfig.DefaultTopicName, type: "direct"); // Ensure exchange exists while publishing

        var message = JsonConvert.SerializeObject(@event);
        var body = Encoding.UTF8.GetBytes(message);

        policy.Execute(() =>
        {
            var properties = consumerChannel.CreateBasicProperties();
            properties.DeliveryMode = 2; // persistent

            //consumerChannel.QueueDeclare(queue: GetSubName(eventName), // Ensure queue exists while publishing
            //                     durable: true,
            //                     exclusive: false,
            //                     autoDelete: false,
            //                     arguments: null);

            //consumerChannel.QueueBind(queue: GetSubName(eventName),
            //                  exchange: EventBusConfig.DefaultTopicName,
            //                  routingKey: eventName);

            consumerChannel.BasicPublish(
                exchange: EventBusConfig.DefaultTopicName,
                routingKey: eventName,
                mandatory: true,
                basicProperties: properties,
                body: body);
        });
        return Task.CompletedTask;
    }
    public override Task SubscribeAsync<T, TH>()
    {
        var eventName = nameof(T);
        eventName = ProcessEventName(eventName);

        if (!SubsManager.HasSubscriptionsForEvent(eventName))
        {
            if (!rabbitMQPersistenceConnection.IsConnected)
            {
                rabbitMQPersistenceConnection.TryConnect();
            }

            consumerChannel.QueueDeclare(queue: GetSubName(eventName),
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            consumerChannel.QueueBind(queue: GetSubName(eventName),
                              exchange: EventBusConfig.DefaultTopicName,
                              routingKey: eventName);
        }

        SubsManager.AddSubscription<T, TH>();
        StartBasicConsume(eventName);
        return Task.CompletedTask;
    }
    private IModel CreateConsumerChannel()
    {
        if (!rabbitMQPersistenceConnection.IsConnected)
        {
            rabbitMQPersistenceConnection.TryConnect();
        }

        var channel = rabbitMQPersistenceConnection.CreateModel();

        channel.ExchangeDeclare(exchange: EventBusConfig.DefaultTopicName,
                                type: "direct");

        return channel;
    }
    private void StartBasicConsume(string eventName)
    {
        if (consumerChannel != null)
        {
            var consumer = new EventingBasicConsumer(consumerChannel);

            consumer.Received += Consumer_Received;

            consumerChannel.BasicConsume(
                queue: GetSubName(eventName),
                autoAck: false,
                consumer: consumer);
        }
    }
    private async void Consumer_Received(object sender, BasicDeliverEventArgs eventArgs)
    {
        var eventName = eventArgs.RoutingKey;
        eventName = ProcessEventName(eventName);
        var message = Encoding.UTF8.GetString(eventArgs.Body.Span);

        try
        {
            await ProcessEvent(eventName, message);
        }
        catch (Exception ex)
        {
            // logging
        }

        consumerChannel.BasicAck(eventArgs.DeliveryTag, multiple: false);
    }
    public override Task UnSubscribeAsync<T, TH>()
    {
        SubsManager.RemoveSubscription<T, TH>();
        return Task.CompletedTask;
    }
}

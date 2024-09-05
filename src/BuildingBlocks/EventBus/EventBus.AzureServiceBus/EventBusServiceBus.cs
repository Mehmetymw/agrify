
using System.Text;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EventBus.AzureServiceBus;

public class EventBusServiceBus : BaseEventBus
{
    ILogger logger;
    ITopicClient topicClient;
    readonly ManagementClient managementClient;
    public EventBusServiceBus(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
    {
        logger = serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>;
        managementClient = new ManagementClient(config.EventBusConnectionString);
        topicClient = CreateTopicClient();
    }

    private ITopicClient CreateTopicClient()
    {
        if (topicClient is null || topicClient.IsClosedOrClosing)
            topicClient = new TopicClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, RetryPolicy.Default);

        if (!managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult())
            managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult();

        return topicClient;
    }

    public override async Task PublishAsync(IntegrationEvent @event)
    {
        var eventName = ProcessEventName(@event.GetType().Name);

        var eventStr = JsonConvert.SerializeObject(eventName);
        var bodyArray = Encoding.UTF8.GetBytes(eventStr);

        var message = new Message()
        {
            MessageId = Guid.NewGuid().ToString(),
            Body = bodyArray,
            Label = eventName,
        };

        await topicClient.SendAsync(message);
    }


    private async Task<ISubscriptionClient> CreateSubscriptionClientIfNotExists(string eventName)
    {
        var subClient = CreateSubscriptionClient(eventName);

        var exists = await managementClient.SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName));

        if (!exists)
        {
            await managementClient.CreateSubscriptionAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName));
            await RemoveRule(subClient);
        }

        await CreateRuleIfNotExists(ProcessEventName(eventName), subClient);
        return subClient;
    }

    private async Task CreateRuleIfNotExists(string eventName, ISubscriptionClient subscriptionClient)
    {
        bool ruleExits;

        try
        {
            var rule = await managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName), eventName);
            ruleExits = rule != null;
        }
        catch (MessagingEntityNotFoundException)
        {
            ruleExits = false;
        }

        if (!ruleExits)
        {
            await subscriptionClient.AddRuleAsync(new RuleDescription
            {
                Filter = new CorrelationFilter { Label = eventName },
                Name = eventName
            });
        }
    }
    private async Task RemoveRule(SubscriptionClient subscriptionClient)
    {
        try
        {
            await subscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName);
        }
        catch
        {
            logger.LogWarning("The message entity {DefaultRuleName}, couldn't found.", RuleDescription.DefaultRuleName);
        }
    }

    private SubscriptionClient CreateSubscriptionClient(string eventName)
    {
        return new SubscriptionClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, GetSubName(eventName));
    }

    public override async Task SubscribeAsync<T, TH>()
    {
        var eventName = ProcessEventName(nameof(T));

        if (!SubsManager.HasSubscriptionsForEvent(eventName))
        {
            var subClient = await CreateSubscriptionClientIfNotExists(eventName);
            RegisterSubscriptionClientMessageHandler(subClient);
        }

        logger.LogInformation("Subscribing to {EventName} with {EventHandler}", eventName, nameof(T));
        SubsManager.AddSubscription<T, TH>();

    }

    private void RegisterSubscriptionClientMessageHandler(ISubscriptionClient subscriptionClient)
    {
        subscriptionClient.RegisterMessageHandler(
            async (message, token) =>
            {
                var eventName = $"{message.Label}";
                var messageData = Encoding.UTF8.GetString(message.Body);

                if (await ProcessEvent(ProcessEventName(eventName), messageData))
                {
                    await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
                }
            },
            new MessageHandlerOptions(ExceptionReceivedHandler) { MaxConcurrentCalls = 10, AutoComplete = false });
    }

    private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
    {
        var ex = exceptionReceivedEventArgs.Exception;
        var context = exceptionReceivedEventArgs.ExceptionReceivedContext;

        logger.LogError(ex, "ERROR handling message: {ExceptionMessage} - Context: {@ExceptionContext}", ex.Message, context);

        return Task.CompletedTask;
    }


    public override async Task UnSubscribeAsync<T, TH>()
    {
        var eventName = nameof(T);

        try
        {
            var subscriptionClient = CreateSubscriptionClient(eventName);

            await subscriptionClient.RemoveRuleAsync(eventName);
        }
        catch (MessagingEntityNotFoundException)
        {
            logger.LogWarning("The messaging entity {eventName} Could not be found.", eventName);
        }

        logger.LogInformation("Unsubscribing from event {EventName}", eventName);

        SubsManager.RemoveSubscription<T, TH>();
    }

    public override void Dispose()
    {
        base.Dispose();

        topicClient.CloseAsync().GetAwaiter().GetResult();
        managementClient.CloseAsync().GetAwaiter().GetResult();
        topicClient = null;
    }

}

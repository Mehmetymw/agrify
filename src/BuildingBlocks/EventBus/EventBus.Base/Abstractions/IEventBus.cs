namespace EventBus.Base.Events.Abstractions;

public interface IEventBus
{
    Task PublishAsync(IntegrationEvent @event);
    Task SubscribeAsync<T,TH>() where T: IntegrationEvent where TH:IIntegrationEventHandler<T>;
    Task UnSubscribeAsync<T,TH>() where T: IntegrationEvent where TH:IIntegrationEventHandler<T>;
}
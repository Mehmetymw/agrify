namespace EventBus.Base.Events.Abstractions;

public interface IIntegrationEventHandler<in TIntegrationEvent>:IIntegrationEventHandler 
    where TIntegrationEvent : IntegrationEvent
{
    Task Handle(TIntegrationEvent @event);
}

public interface IIntegrationEventHandler
{
    Task Handle(IntegrationEvent @event);
}
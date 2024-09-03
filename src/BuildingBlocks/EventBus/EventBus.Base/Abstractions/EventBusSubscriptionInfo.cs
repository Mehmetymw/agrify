namespace EventBus.Base.Events.Abstractions;

public class EventBusSubscriptionInfo
{
    public Type HandlerType { get; }
    public EventBusSubscriptionInfo(Type handlerType)
    {
        HandlerType = handlerType;
    }

    public static EventBusSubscriptionInfo Typed(Type handlerType)
    {
        return new EventBusSubscriptionInfo(handlerType);
    }
}
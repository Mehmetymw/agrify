using System.Text.Json.Serialization;

namespace EventBus.Base.Events;

public class IntegrationEvent
{
    [JsonConstructor]
    public IntegrationEvent(Guid id,DateTime createdDate)
    {
        Id = id;
        CreatedDate = createdDate;
    }
    public IntegrationEvent()
    {
        Id = Guid.NewGuid();
        CreatedDate = DateTime.UtcNow;
    }

    [JsonInclude]
    public Guid Id { get; private set; }
    [JsonInclude]
    public DateTime CreatedDate { get; private set; }
    
}

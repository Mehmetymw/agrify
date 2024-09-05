using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventBus.AzureServiceBus;
using EventBus.Base.Events;
using EventBus.Base.Events.Abstractions;
using EventBus.RabbitMQ;

namespace EventBus.Factory
{
    public class EventBusFactory
    {
         public static IEventBus Create(EventBusConfig config, IServiceProvider serviceProvider)
        {
            return config.EventBusType switch
            {
                EventBusType.AzureServiceBus => new EventBusServiceBus(config, serviceProvider),
                _ => new EventBusRabbitMQ(config, serviceProvider),
            };
        }
    }
}
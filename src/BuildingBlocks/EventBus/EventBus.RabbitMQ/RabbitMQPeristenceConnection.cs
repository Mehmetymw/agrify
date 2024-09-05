using System.Net.Sockets;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace EventBus.RabbitMQ;
public class RabbitMQPersistenceConnection : IDisposable
{

    IConnection connection;
    public bool IsConnected => connection != null && connection.IsOpen;
    private readonly IConnectionFactory connectionFactory;
    private readonly int retryCount;
    private static object lock_object => new();
    private bool isDisposed;

    public RabbitMQPersistenceConnection(IConnectionFactory connectionFactory, int retryCount = 5)
    {
        this.connectionFactory = connectionFactory;
        this.retryCount = retryCount;
    }

    public IModel CreateModel()
    {
        return connection.CreateModel();
    }

    public bool TryConnect()
    {
        lock (lock_object)
        {
            var policy = Policy.Handle<SocketException>()
                .Or<BrokerUnreachableException>()
                .WaitAndRetry(retryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {

                }
            );

            policy.Execute(() =>
            {
                connection = connectionFactory.CreateConnection();
            });

            if (IsConnected)
            {
                connection.ConnectionShutdown += Connection_ConnectionShutdown;
                connection.CallbackException += Connection_CallbackException;
                connection.ConnectionBlocked += Connection_ConnectionBlocked;

                return true;
            }

            return false;
        }
    }

    private void Connection_ConnectionBlocked(object? sender, ConnectionBlockedEventArgs e)
    {
        if (isDisposed) return;
        TryConnect();
    }

    private void Connection_CallbackException(object? sender, CallbackExceptionEventArgs e)
    {
        if (isDisposed) return;
        TryConnect();
    }

    private void Connection_ConnectionShutdown(object? sender, ShutdownEventArgs e)
    {
        if (isDisposed) return;

        TryConnect();
    }

    public void Dispose()
    {
        connection.Dispose();
    }
}
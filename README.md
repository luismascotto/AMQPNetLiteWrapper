# AMQPNetLiteWrapper

A lightweight C# wrapper library that provides simplified connection management and message sending for [AMQPNetLite](https://github.com/Azure/amqpnetlite), implementing singleton-based connection pooling to optimize AMQP 1.0 messaging operations.

## Features

- Singleton pattern for connection management
- Connection pooling with automatic reconnection
- Simplified message sending interface
- Thread-safe operations
- Built-in debug logging support

## Requirements

- .NET 9.0 or later
- AMQPNetLite 2.5.4

## Installation

### As a project reference

Clone this repository and add a project reference to `AMQPNetLiteWrapper.csproj`:

```xml
<ItemGroup>
  <ProjectReference Include="../AMQPNetLiteWrapper/AMQPNetLiteWrapper.csproj" />
</ItemGroup>
```

## Usage

### Basic Message Sending

```csharp
using AMQPNetLiteWrapper;

// Get the singleton instance
var helper = AmqpHelper.Instance;

// Send a message
string connectionString = "amqp://guest:guest@localhost:5672";
string queueName = "myqueue";
string message = "Hello, AMQP!";

bool success = helper.Send(connectionString, queueName, message, "myapp");

if (success)
{
    Console.WriteLine("Message sent successfully");
}
```

### Managing Connections

```csharp
// Get a connection (creates if needed, reuses if available)
var connection = AmqpHelper.Instance.GetSenderConnection(connectionString, "myapp");

// Close all connections when shutting down
AmqpHelper.Instance.CloseAllSenderConnections();
```

### Advanced: Direct Session and Sender Usage

```csharp
using Amqp;
using AMQPNetLiteWrapper;

var connection = AmqpHelper.Instance.GetSenderConnection(connectionString, "myapp");
Session? session = null;
SenderLink? sender = null;

try
{
    session = new Session(connection);
    sender = new SenderLink(session, "link-name", queueName);
    sender.Send(new Message("Custom message"), TimeSpan.FromMilliseconds(3000));
}
finally
{
    session?.Close();
    sender?.Close();
}
```

## Building

### Build the library

```bash
dotnet restore
dotnet build
```

### Run the demo web application

The `WebApplication1` project demonstrates usage with a minimal API endpoint:

```bash
cd WebApplication1
dotnet run
```

Then access `/weatherforecast` to see the wrapper in action (requires an AMQP broker URL in `appsettings.json`).

## Project Structure

- `AMQPNetLiteWrapper/` - Core library with `AmqpHelper` singleton
- `WebApplication1/` - Demo ASP.NET Core minimal API application

## License

MIT License - see [LICENSE](LICENSE) for details.

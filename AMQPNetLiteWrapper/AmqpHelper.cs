namespace AMQPNetLiteWrapper;

using System.Globalization;
using Amqp;

public class AmqpHelper
{
    private static AmqpHelper? instance;
    private readonly Dictionary<string, Connection> senderConnections;

    //private readonly ConcurrentDictionary<string, Connection> _receiverConnections;
    private AmqpHelper() => senderConnections = [];//_receiverConnections = new ConcurrentDictionary<string, Connection>();

    public static AmqpHelper Instance => instance ??= new AmqpHelper();

    public Connection GetSenderConnection(string connectionString, string origem = "") => GetConnection(senderConnections, connectionString, origem);

    private Connection GetConnection(
        Dictionary<string, Connection> connDict,
        string connectionString,
        string origem = "")
    {
        if (connDict == null)
        {
            throw new ArgumentException($"{nameof(connDict)} is null or empty.", nameof(connDict));
        }

        if (origem == null)
        {
            throw new ArgumentNullException(nameof(origem), $"{nameof(origem)} is null.");
        }

        if (string.IsNullOrEmpty(connectionString))
        {
            throw new ArgumentException($"{nameof(connectionString)} mandatory.", nameof(connectionString));
        }
#if DEBUG
        ConsoleDbg.WriteLine($"[{origem}] Trying to get connection... outside lock");
#endif
        lock (senderConnections)
        {
#if DEBUG
            ConsoleDbg.WriteLine($"[{origem}] Trying to get connection... inside lock");
#endif

            if (senderConnections.TryGetValue(connectionString, out var connection))
            {
                if (connection.ConnectionState <= ConnectionState.Opened)
                {
#if DEBUG
                    ConsoleDbg.WriteLine(
                        $"[{origem}] Opened connection ({connection.ConnectionState})",
                        ConsoleColor.Yellow);
#endif
                    return connection;
                }
#if DEBUG
                ConsoleDbg.WriteLine(
                    $"[{origem}] Connection not ok ({connection.ConnectionState}) Removing and finalizing",
                    ConsoleColor.Magenta);
#endif
                if (senderConnections.Remove(connectionString, out var connToClose))
                {
                    try
                    {
                        connToClose.Close();
                    }
                    catch
                    {
                    }
                }
            }

#if DEBUG
            ConsoleDbg.WriteLine($"[{origem}] Abrindo nova conexão...", ConsoleColor.Green);
#endif

            var conn = new ConnectionFactory().CreateAsync(new Address(connectionString)).Result;
            senderConnections.TryAdd(connectionString, conn);

            return conn;
        }
    }

    public bool Send(string connectionString, string queueName, string message, string origem = "")
    {
        var connection = GetSenderConnection(connectionString, origem);
        if (connection == null)
        {
#if DEBUG
            ConsoleDbg.WriteLine($"[{origem}] Erro ao obter conexão AMQP", ConsoleColor.Red);
#endif

            return false;
        }

        Session? session = null;
        SenderLink? sender = null;
        try
        {
            session = new(connection);
            sender = new SenderLink(session, "SIMPLE", queueName);
            sender.Send(new Message(message), TimeSpan.FromMilliseconds(3000));
#if DEBUG
            ConsoleDbg.WriteLine($"[{origem}] Mensagem enviada com sucesso", ConsoleColor.Green);
#endif
#if DEBUG
#endif
            return true;
        }
        catch (Exception ex)
        {
#if DEBUG
            ConsoleDbg.WriteLine($"[{origem}] Erro ao enviar mensagem AMQP {ex}", ConsoleColor.Red);
#endif
            return false;
        }
        finally
        {
            session?.Close();
            sender?.Close();
        }
    }

    public void CloseAllSenderConnections(bool useAsync = false)
    {
        lock (senderConnections)
        {
            ConsoleDbg.WriteLine($"CloseAllSenderConnections...", ConsoleColor.Yellow);
            while (senderConnections.Count > 0)
            {
                if (senderConnections.Remove(senderConnections.Keys.First(), out var conn))
                {
                    try
                    {
                        if (useAsync)
                        {
                            conn.CloseAsync().Wait();
                        }
                        else
                        {
                            conn.Close();
                        }
                        ConsoleDbg.WriteLine($" Closed", ConsoleColor.Green);
                    }
                    catch (Exception ex)
                    {
                        ConsoleDbg.WriteLine($"Erro ao fechar conexão AMQP: {ex.Message}", ConsoleColor.Red);
                    }
                }
            }
        }
    }

    public void CloseAllConnections() => CloseAllSenderConnections();//CloseAllReceiverConnections();
}

public static class ConsoleDbg
{
    //private static readonly IFormatProvider FormatProvider = new CultureInfo("en-US");
    public static void WriteLine(string message, ConsoleColor color = ConsoleColor.White)
    {
        lock (Console.Out)
        {
            if (color != ConsoleColor.White)
            {
                Console.ForegroundColor = color;
            }
            Console.Write(DateTime.Now.ToString("dd/MM HH:mm:ss.ffff - "));
            Console.WriteLine(message);
            if (color != ConsoleColor.White)
            {
                Console.ResetColor();
            }
        }
    }
}

using Amqp;

namespace AMQPNetLiteWrapper;

public class AmqpHelper
{
    private static AmqpHelper? _instance;
    private readonly Dictionary<string, Connection> _senderConnections;
    //private readonly ConcurrentDictionary<string, Connection> _receiverConnections;
    private AmqpHelper()
    {
        _senderConnections = [];
        //_receiverConnections = new ConcurrentDictionary<string, Connection>();
    }

    public static AmqpHelper Instance => _instance ??= new AmqpHelper();

    public Connection GetSenderConnection(string connectionString, string origem = "")
    {
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new ArgumentException($"{nameof(connectionString)} obrigatório.", nameof(connectionString));
        }
        //ConsoleDbg.WriteLine($"[{origem}] Tentando obter conexão... lock");
        lock (_senderConnections)
        {
            //ConsoleDbg.WriteLine($"[{origem}] Tentando obter conexão... unlock");

            if (_senderConnections.TryGetValue(connectionString, out var connection))
            {
                if (connection.ConnectionState <= ConnectionState.Opened)
                {
                    ConsoleDbg.WriteLine($"[{origem}] Conexão já aberta ({connection.ConnectionState})", ConsoleColor.Yellow);
                    return connection;
                }
                ConsoleDbg.WriteLine($"[{origem}] Conexão barreada ({connection.ConnectionState}) Removendo e tentando finalizar", ConsoleColor.Magenta);
                if (_senderConnections.Remove(connectionString, out var connToClose))
                {
                    try
                    {
                        connToClose.Close();
                    }
                    catch { }
                }
            }

            ConsoleDbg.WriteLine($"[{origem}] Abrindo nova conexão...", ConsoleColor.Green);

            var conn = new ConnectionFactory().CreateAsync(new Address(connectionString)).Result;
            _senderConnections.TryAdd(connectionString, conn);

            return conn;
        }
    }

    public bool Send(string connectionString, string queueName, string message, string origem = "")
    {
        var connection = GetSenderConnection(connectionString, origem);
        if (connection == null)
        {
            ConsoleDbg.WriteLine($"[{origem}] Erro ao obter conexão AMQP", ConsoleColor.Red);
            return false;
        }

        Session? session = null;
        SenderLink? sender = null;
        try
        {
            session = new(connection);
            sender = new SenderLink(session, "SIMPLE", queueName);
            sender.Send(new Message(message), TimeSpan.FromMilliseconds(3000));
            ConsoleDbg.WriteLine($"[{origem}] Mensagem enviada com sucesso", ConsoleColor.Green);
            return true;
        }
        catch (Exception ex)
        {
            ConsoleDbg.WriteLine($"[{origem}] Erro ao enviar mensagem AMQP {ex}", ConsoleColor.Red);
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
        lock (_senderConnections)
        {
            ConsoleDbg.WriteLine($"CloseAllSenderConnections...", ConsoleColor.Yellow);
            while (_senderConnections.Count > 0)
            {
                if (_senderConnections.Remove(_senderConnections.Keys.First(), out var conn))
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

    public void CloseAllConnections()
    {
        CloseAllSenderConnections();
        //CloseAllReceiverConnections();
    }
}
public static class ConsoleDbg
{
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

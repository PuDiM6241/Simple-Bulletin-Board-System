using System.Collections;
using System.Globalization;
using Google.Protobuf;
using Messaging; // pacotes protobuf das mensagens (gerados a partir do messaging.proto)
using NetMQ;
using NetMQ.Sockets;
using Envelope = Messaging.envelope;     // o protoc nao capitaliza nomes de mensagem: o .proto declara 'envelope'
using ServerInfo = Messaging.serverInfo;
using Package = Messaging.package;

namespace MessagingClient;

// Porta do configGeneralData.py (somente o que o client usa)
public static class ConfigGeneralData
{
    // ------------------
    //     network
    // ------------------

    static string Env(string name, string defaultValue) =>
        Environment.GetEnvironmentVariable(name) ?? defaultValue;

    //Tava dando erro usando local host porque eu crio mais de um container, isso resolveu servers com problemas
    public static readonly string BrokerReqAddress = Env("BROKER_REQ_ADDRESS", "tcp://broker:5555");
    public static readonly string BrokerRepAddress = Env("BROKER_REP_ADDRESS", "tcp://broker:5556");
    public static readonly string ProxySubAddress = Env("PROXY_SUB_ADDRESS", "tcp://proxy:5557");
    public static readonly string ProxyPubAddress = Env("PROXY_PUB_ADDRESS", "tcp://proxy:5558");
    public static readonly string ReferenceAddress = Env("REFERENCE_ADDRESS", "tcp://reference:5559");


    // ---------------------
    // Time synchronization
    // ---------------------


    //Sincronisacao usa variavel de offset.
    static double physicalClockOffset = 0.0;

    //Usar isso para todos os tempos
    // (segundos desde a epoch Unix, como time.time() do Python)
    public static double Now()
    {
        return (DateTime.UtcNow - DateTime.UnixEpoch).TotalSeconds + physicalClockOffset;
    }

    public static void AdjustTime(double correctTime)
    {
        physicalClockOffset += correctTime - Now();
    }

    // ------------------------------
    //   Sistema de Log no console
    // ------------------------------

    // equivalente ao strftime("%H:%M:%S[.%f]") sobre localtime(segundos)
    public static string FormatTime(double seconds, bool microseconds = false)
    {
        var local = DateTime.UnixEpoch.AddTicks((long)(seconds * TimeSpan.TicksPerSecond)).ToLocalTime();
        return local.ToString(microseconds ? "HH:mm:ss.ffffff" : "HH:mm:ss", CultureInfo.InvariantCulture);
    }

    public static void LogEvent(string name, string text)
    {
        var timestamp = FormatTime(Now(), microseconds: true);
        Console.WriteLine($"[{name}] \n[{timestamp}] \n{text}");
    }

    static readonly Dictionary<string, string> nomesCampos = new()
    {
        ["type"] = "tipo",
        ["sender"] = "remetente",
        ["timestamp"] = "timestamp",
        ["logicalClock"] = "relógioLogico",
        ["status"] = "status",
        ["error"] = "erro",
        ["username"] = "usuario",
        ["channel"] = "canal",
        ["text"] = "texto",
        ["channels"] = "canais",
        ["rank"] = "classificacao",
        ["servers"] = "servidores",
        ["coordinator"] = "coordenador",
        ["referenceTime"] = "tempoReferencia",
        ["packages"] = "pacotes",
        ["topic"] = "topico"
    };

    // equivalente ao repr() do Python, so para o que aparece no log
    static string Repr(object? value)
    {
        switch (value)
        {
            case string s:
                if (s.Contains('\'') && !s.Contains('"'))
                    return "\"" + s + "\"";
                return "'" + s.Replace("\\", "\\\\").Replace("'", "\\'").Replace("\n", "\\n") + "'";
            case double d:
                var text = d.ToString("R", CultureInfo.InvariantCulture);
                return text.IndexOfAny(new[] { '.', 'E', 'N', 'I' }) >= 0 ? text : text + ".0";
            case IEnumerable items:
                return "[" + string.Join(", ", items.Cast<object?>().Select(Repr)) + "]";
            default:
                return Convert.ToString(value, CultureInfo.InvariantCulture) ?? "";
        }
    }

    // ListFields() do Python so devolve campos com valor diferente do padrao (proto3)
    static bool IsDefault(object? value) => value switch
    {
        null => true,
        string s => s.Length == 0,
        long l => l == 0,
        double d => d == 0.0,
        int i => i == 0,
        _ => false
    };

    public static void LogMessage(string name, string direction, Envelope envelope, string textExtra = "", string? extra = null)
    {
        if (extra != null)
        {
            textExtra = extra;
        }
        var log = new List<string>();

        foreach (var field in Envelope.Descriptor.Fields.InFieldNumberOrder())
        {
            object? value = field.Accessor.GetValue(envelope);

            if (field.IsRepeated)
            {
                if (((IList)value!).Count == 0) continue;
            }
            else if (IsDefault(value))
            {
                continue;
            }

            if (field.Name == "timestamp" || field.Name == "referenceTime")
            {
                value = FormatTime((double)value!, microseconds: true);
            }
            else if (field.Name == "channels")
            {
                value = ((IEnumerable<string>)value!).ToList();
            }
            else if (field.Name == "servers")
            {
                value = ((IEnumerable<ServerInfo>)value!)
                    .Select(server => $"nome:{server.Name} | classificacao:{server.Rank}").ToList();
            }
            else if (field.Name == "packages")
            {
                value = ((IEnumerable<Package>)value!)
                    .Select(package => $"id:{package.Id} | tipo:{package.Kind} | dados:{package.Data} | timestamp:{package.Timestamp.ToString("R", CultureInfo.InvariantCulture)}").ToList();
            }

            var nomeCampo = nomesCampos.GetValueOrDefault(field.Name, field.Name);
            log.Add($"{nomeCampo}={Repr(value)}");
        }

        var timestamp = FormatTime(Now(), microseconds: true);
        var envelopeLog = string.Join("\n", log);
        Console.WriteLine(
            ($"[{name}] \n" +
             $"[{timestamp}] \n" +
             $"{direction} {envelope.Type} \n" +
             $"{envelopeLog} \n" +
             $"{textExtra}").TrimEnd());
    }


    // ------------------
    //   mensage trader
    // ------------------
    // me arrependo de nao ter feito em c#, sinto falta de classes decentes.

    public static Envelope MakeEnvelope(
        HeartbeatClock clock, string sender, string msgType,
        double? timestamp = null,
        string? status = null,
        string? topic = null,
        string? error = null,
        string? username = null,
        string? channel = null,
        string? text = null,
        IEnumerable<string>? channels = null,
        long? rank = null,
        IEnumerable<ServerInfo>? servers = null,
        string? coordinator = null,
        double? referenceTime = null,
        IEnumerable<Package>? packages = null
    )
    {
        var envelope = new Envelope
        {
            Type = msgType,
            Sender = sender,
            Timestamp = timestamp ?? Now(),
            LogicalClock = clock.Tick()
        };

        // proto3 nao aceita null em string: so preenche o que foi informado
        if (status != null) envelope.Status = status;
        if (topic != null) envelope.Topic = topic;
        if (error != null) envelope.Error = error;
        if (username != null) envelope.Username = username;
        if (channel != null) envelope.Channel = channel;
        if (text != null) envelope.Text = text;
        if (rank != null) envelope.Rank = rank.Value;
        if (coordinator != null) envelope.Coordinator = coordinator;
        if (referenceTime != null) envelope.ReferenceTime = referenceTime.Value;

        if (channels != null)
        {
            envelope.Channels.AddRange(channels);
        }

        if (servers != null)
        {
            envelope.Servers.AddRange(servers);
        }

        if (packages != null)
        {
            envelope.Packages.AddRange(packages);
        }

        return envelope;
    }


    public static Envelope ParseEnvelope(byte[] data)
    {
        return Envelope.Parser.ParseFrom(data);
    }


    public static void SendEnvelope(NetMQSocket socket, string name, Envelope envelope)
    {
        var data = envelope.ToByteArray();
        socket.SendFrame(data);
        LogMessage(name, "||ENVIADO||\n", envelope);
    }


    // Em C# nao existe "context" explicito: o NetMQ usa um contexto global interno.
    public static Envelope? Request(string address, Envelope envelope, string name, HeartbeatClock clock, int timeoutMs = 5000)
    {
        using var socket = new RequestSocket();
        socket.Options.Linger = TimeSpan.Zero;
        socket.Connect(address);
        SendEnvelope(socket, name, envelope);
        if (!socket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(timeoutMs), out var data))  //sistema de timeout, se nao receber resposta em 5s, retorna None
        {
            return null;
        }
        var reply = ParseEnvelope(data);
        clock.Shync(reply.LogicalClock);
        LogMessage(name, "||RECEBIDO||\n", reply);
        return reply;
    }
}


public class HeartbeatClock
{
    public long Counter = 0;

    //Auto incremento
    public long Tick()
    {
        Counter += 1;
        return Counter;
    }

    //Implementacao do algoritmo de barkley
    public void Shync(long receivedClock)
    {
        Counter = Math.Max(Counter, receivedClock);
    }
}

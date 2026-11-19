using System.Text;
using Messaging;
using NetMQ;
using NetMQ.Sockets;
using Envelope = Messaging.envelope; // o protoc nao capitaliza nomes de mensagem: o .proto declara 'envelope'
using static MessagingClient.ConfigGeneralData;

namespace MessagingClient;

public static class Client
{
    // ------------------
    //     Conf
    // ------------------

    static readonly string botName = Environment.GetEnvironmentVariable("BOT_NAME") ?? System.Net.Dns.GetHostName();
    const int minChannels = 5;
    const int maxSubscriptions = 3;
    const int messagesPerTimeout = 10; // quantas mensagems enviar por vez, antes de esperar um tempo para enviar mais
    static readonly string[] phrases = { "DAORA DEMAIS", "foi numa tarde de domingo", "que alguem", "a", "ou", "palavras nao bastam", "Ultimo album da laufey", "Mo fome", "Leite quente", "acho que adele e ruim,", "amo adele" };
    static readonly HeartbeatClock clock = new HeartbeatClock();
    static readonly SubscriberSocket subSocket = new SubscriberSocket();
    static readonly HashSet<string> subscribedChannels = new();

    static Client()
    {
        subSocket.Connect(ProxyPubAddress);
        subSocket.Subscribe("");
    }

    static Envelope ReqServ(string msgType, string? channel = null, string? text = null)
    {
        while (true)
        {
            var envelope = MakeEnvelope(
                clock: clock,
                sender: botName,
                msgType: msgType,
                username: botName,
                channel: channel,
                text: text
            );
            var answer = Request(
                address: BrokerReqAddress,
                envelope: envelope,
                name: botName,
                clock: clock
            );
            if (answer != null)
            {
                return answer;
            }
            ConfigGeneralData.LogEvent(botName, $"Sem resposta({msgType}): tentando novamente..");
        }
    }


    // ------------------
    //     subscriptions
    // ------------------

    static void Subscribe(IEnumerable<string> channels)
    {
        var options = new HashSet<string>();
        if (subscribedChannels.Count < maxSubscriptions)
        {
            options = new HashSet<string>(channels.Except(subscribedChannels));
        }
        if (options.Count > 0)
        {
            var choice = options.ElementAt(Random.Shared.Next(options.Count));
            subscribedChannels.Add(choice);
            ConfigGeneralData.LogEvent(botName, $"Inscrito no canal {choice}");
        }
    }

    static void ReadPubs(byte[] data)
    {
        var envelope = ParseEnvelope(data);
        if (subscribedChannels.Contains(envelope.Topic))
        {
            clock.Shync(envelope.LogicalClock); //sincroniza o clock heartbeat do bot com o clock da mensagem recebida
            ConfigGeneralData.LogMessage(
                botName,
                "RECEBIDO",
                envelope,
                extra: $"Recebido em={FormatTime(Now())}"
            );
        }
    }

    static void ListenWait(double seconds = 3)
    {
        var end = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < end)
        {
            var ms = Math.Max(1, (int)((end - DateTime.UtcNow).TotalMilliseconds));
            if (subSocket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(ms), out var data))
            {
                ReadPubs(data);
            }
        }
    }


    // ------------------
    //     Main loop
    // ------------------

    public static void Main()
    {
        Console.OutputEncoding = Encoding.UTF8;

        //login infinito
        while (ReqServ("login").Status != "success")
        {
            ListenWait(1);
        }


        var channels = ReqServ("listChannels").Channels.ToList();
        //se nao tem canais o sufuciente, criar um canal
        if (channels.Count < minChannels)
        {
            ReqServ("createChannel", channel: $"\"channel {Random.Shared.NextInt64(long.MaxValue)}\""); //tentativa de criar um canal aleatorio
            channels = ReqServ("listChannels").Channels.ToList();
        }
        Subscribe(channels); //tenta se inscrever em um canal aleatorio


        while (true)
        {
            channels = ReqServ("listChannels").Channels.ToList(); //atualiza a lista de canais
            if (channels.Count == 0)
            {
                ListenWait(1);
                continue;
            }

            Subscribe(channels);  //tenta se inscrever em um canal aleatorio
            var target = channels[Random.Shared.Next(channels.Count)];
            for (int i = 0; i < messagesPerTimeout; i++)
            {
                var text = string.Join(" ", Enumerable.Range(0, 5).Select(_ => phrases[Random.Shared.Next(phrases.Length)]));
                ReqServ("publish", channel: $"\"{target}\"", text: text); //publica uma mensagem aleatoria no canal aleatorio
                ListenWait(1); // tenta escutar mensagens por 1 segundo, antes de publicar a proxima mensagem
            }
        }
    }
}

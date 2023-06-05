using RabbitMQ.Client;
using System.Linq;
using System.Runtime.InteropServices.JavaScript;
using System.Text;

namespace RabbitMQ.Publisher
{
    public enum LogNames
    {
        Critical = 1,
        Error = 2,
        Warning = 3,
        Info = 4
    }
    public class Program
    {
        static void Main(string[] args)
        {
            //RabbitMQ bağlanmak için Factory sınıfından instance alıyoruz URİ üzerinden bağlanmak için rabbitmq cloud
            var factory = new ConnectionFactory();
            // api.cloudamqp.com rabbitmq cloud verdiği url i factorye veriyoruz.
            factory.Uri = new Uri("amqps://uliyuxac:GTQlxYysEK14TQz-j8F-B7NQ6_V4XHp7@rattlesnake.rmq.cloudamqp.com/uliyuxac");


            try
            {
                // bağlantıyı açıyoruz using connection kullanırsak Main scopeları bitince connection kapanır. best practise budur
                using var connection = factory.CreateConnection();
                //bağlantı üzerinden bir kanal oluşturuyoruz rabbitmq ya erişmek için.
                var channel = connection.CreateModel();


                //Fanout Exchange oluşturuyoruz
                /**
                 * 1. Parametre "logs-direct" exchange adı
                 * 2. Parametre durable true ise uygulama restrat olsa da exchange kaybolmaz false olursa kaybolur 
                 * 3. Parametre type:ExchangeType.Direct exchange tipi Direct seçiyoruz
                 * **/
                channel.ExchangeDeclare("logs-topic", durable: true, type: ExchangeType.Topic);


                Random rnd = new Random();
                // kuyruğa 50 tane mesaj gönderiyoruz.
                Enumerable.Range(1, 50).ToList().ForEach(x =>
                {           
                    LogNames log1 = (LogNames)rnd.Next(1, 5);
                    LogNames log2 = (LogNames)rnd.Next(1, 5);
                    LogNames log3 = (LogNames)rnd.Next(1, 5);

                    //Route keyi "." ile ayırıyoruz.
                    var routeKey = $"{log1}.{log2}.{log3}";
                    //Kuyruğa gönderilen mesaj mesajlar byte dizisi olarak gönderilir. Bu yüzden dosya vs. de gönderilebilir.
                    string message = $"log-type: {log1}-{log2}-{log3}";

                    // Byte olarak aldık.
                    var messageBody = Encoding.UTF8.GetBytes(message);
                    // exchange kullanıyoruz üstte oluşturduğumuz exchange adını veriyoruz
                    // routekeyi veriyoruz
                    channel.BasicPublish("logs-topic", routeKey, null, messageBody);
                    Console.WriteLine(message + " mesajı kuyruğa gönderildi");
                });
                Console.ReadLine();
            }
            catch (RabbitMQ.Client.Exceptions.BrokerUnreachableException e)
            {
                Thread.Sleep(5000);
                // apply retry logic
            }
        }
    }
}
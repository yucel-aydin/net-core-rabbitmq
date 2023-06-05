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
                channel.ExchangeDeclare("logs-direct", durable: true, type: ExchangeType.Direct);

                /**
                 *Tanımlı enumların her biri için bir kuyruk oluşturuyoruz.
                 */
                Enum.GetNames(typeof(LogNames)).ToList().ForEach(x=>
                {
                    var routeKey = $"route-{x}";
                    var queueName = $"direct-queue-{x}";
                    channel.QueueDeclare(queueName, true, false, false);
                    channel.QueueBind(queueName, "logs-direct", routeKey,null);
                });

                // kuyruğa 50 tane mesaj gönderiyoruz.
                Enumerable.Range(1, 50).ToList().ForEach(x =>
                {
                    //random log type alıyoruz
                    LogNames log = (LogNames)new Random().Next(1, 5);
                    //Kuyruğa gönderilen mesaj mesajlar byte dizisi olarak gönderilir. Bu yüzden dosya vs. de gönderilebilir.
                    string message = $"log-type: {log}";
                    // Byte olarak aldık.
                    var messageBody = Encoding.UTF8.GetBytes(message);

                    var routeKey = $"route-{log}";

                    // exchange kullanıyoruz üstte oluşturduğumuz exchange adını veriyoruz
                    // routekeyi veriyoruz
                    channel.BasicPublish("logs-direct", routeKey, null, messageBody);
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
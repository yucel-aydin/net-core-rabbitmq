using RabbitMQ.Client;
using System.Linq;
using System.Runtime.InteropServices.JavaScript;
using System.Text;

namespace RabbitMQ.Publisher
{
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
                 * 1. Parametre "header-exchange" exchange adı
                 * 2. Parametre durable true ise uygulama restrat olsa da exchange kaybolmaz false olursa kaybolur 
                 * 3. Parametre type:ExchangeType.Headers exchange tipi Headers seçiyoruz
                 * **/
                channel.ExchangeDeclare("header-exchange", durable: true, type: ExchangeType.Headers);

                //key-value tipinde parametrelerimizi belirliyoruz
                Dictionary<string,object> headerParams = new Dictionary<string, object>();
                headerParams.Add("format", "pdf");
                headerParams.Add("shape", "a4");

                //kanalda properties oluşturup headerına yazdığımız parametreleri veriyoruz
                var properties= channel.CreateBasicProperties();
                properties.Headers=headerParams;
                properties.Persistent = true; // true set edilirse mesajlar da kalıcı hale gelir.


                //Mesajımınızı publish ediyoruz.
                channel.BasicPublish("header-exchange", string.Empty, properties,Encoding.UTF8.GetBytes("Header mesajım"));

                Console.WriteLine("Mesaj Gönderildi");

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
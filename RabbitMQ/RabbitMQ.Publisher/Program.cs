using RabbitMQ.Client;
using System.Linq;
using System.Text;

namespace RabbitMQ.Publisher
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //RabbitMQ bağlanmak için Factory sınıfından instance alıyoruz
            var factory = new ConnectionFactory();
            // api.cloudamqp.com rabbitmq cloud verdiği url i factorye veriyoruz.
            factory.Uri = new Uri("amqps://uliyuxac:GTQlxYysEK14TQz-j8F-B7NQ6_V4XHp7@rattlesnake.rmq.cloudamqp.com/uliyuxac");

            // bağlantıyı açıyoruz
            using var connection = factory.CreateConnection();
            
            //bağlantı üzerinden bir kanal oluşturuyoruz rabbitmq ya erişmek için.
            var channel=connection.CreateModel();

            // kuyruk oluşturuyoruz
            // "hello-queue" kuyruk adı
            //durable
            ////*false olursa oluşan kuyruklar memoryde tutular rabbitmq restart olursa kuyruklar kaybolur
            ////*true olursa fiziksel olarak kaydedilir ve kuyruklar kaybedilmez.
            //exclusive
            //// * false olursa farklı bir processten ulaşabilmek için.
            //// * true olursa bu kuyhruğa sadece burdaki oluşturulan kanal üzerinden bağlanabiliriz
            // autoDelete
            /// *false subscriber kalmasada kuyruk kalır
            /// *true bağlı son subscriber bağlantısıda kesilince kuyruk silinir
            channel.QueueDeclare("hello-queue",true,false,false);

            // kuyruğa 50 tane mesaj gönderiyoruz.
            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {
                //Kuyruğa gönderilen mesaj mesajlar byte dizisi olarak gönderilir. Bu yüzden dosya vs. de gönderilebilir.
                string message = $"Message {x}";
                // Byte olarak aldık.
                var messageBody = Encoding.UTF8.GetBytes(message);

                // exchange kullanmıyoruz
                channel.BasicPublish(string.Empty, "hello-queue", null, messageBody);


                Console.WriteLine(message + " mesajı kuyruğa gönderildi");

            });

            
      
            Console.ReadLine();





        }
    }
}
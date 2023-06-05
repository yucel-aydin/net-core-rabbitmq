using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbitMQ.Subscriber
{
    public class Program
    {
        static void Main(string[] args)
        {
            //RabbitMQ bağlanmak için Factory sınıfından instance alıyoruz  URİ üzerinden bağlanmak için rabbitmq cloud
            var factory = new ConnectionFactory();
            // api.cloudamqp.com rabbitmq cloud verdiği url i factorye veriyoruz.
            factory.Uri = new Uri("amqps://uliyuxac:GTQlxYysEK14TQz-j8F-B7NQ6_V4XHp7@rattlesnake.rmq.cloudamqp.com/uliyuxac");


            // bağlantıyı açıyoruz using connection kullanırsak Main scopeları bitince connection kapanır. best practise budur
            using var connection = factory.CreateConnection();

            //bağlantı üzerinden bir kanal oluşturuyoruz rabbitmq ya erişmek için.
            var channel = connection.CreateModel();




            //bir kerede subscribera kaç mesaj gelceğini belirtiyoruz.
            //1. parametre, mesajın boyutudur. 0 olursa herhangi boyut olabilir
            //2. parametre,  her bir subscriber a 1 er 1 er mesaj gelsin
            //3. parametre, false olursa her bir subscribe e 2. parametredeki adet kadar mesaj iletilir
            // true olursa kaç subscribe varsa toplamda 2. parametredeki sayı kadar mesaj iletilir. 
            // Örneği  channel.BasicQos(0, 6, false); dersek her subscribera tek seferde 6 mesaj iletilir.
            // True olursa subscriberlar toplamda gönderilecek mesaj sayısı 6 olur yani 2 subscriber varsa 3 birine 3 diğerine 6 subscriber varsa herbirine 1 er tane mesaj gider.
            channel.BasicQos(0, 1, false);

            //subscriber(comsumer) oluşturuyoruz ve oluşturulan kanalı verdik
            var consumer = new EventingBasicConsumer(channel);


            //bağlanacağı kuyruğu belirtiyoruz
            var queueName = "direct-queue-Critical";


            //kanal üzerinden yukarda random oluşturduğumuz kuyruğu dinliyoruz
            //autoAck
            /// *true olursa kuyruktan mesaj alındıktan sonra rabbitmq ilgili mesajı siler
            /// *false olursa  rabbitmq ya mesajı silme, işleme göre mesaj silineceğini biz bildireceğiz demiş oluruz.

            Console.WriteLine("Loglar dinleniyor...");
            //"hello-queue" kuyruğuna bir mesaj geldiğinde bu event çalışır
            consumer.Received += (object? sender, BasicDeliverEventArgs e) =>
            {
                // Publisherda byte dizisine çevirip yolladığımız mesajı burada tekrar string olarak alıyhoruz
                var messages = Encoding.UTF8.GetString(e.Body.ToArray());
                Thread.Sleep(1500);
                Console.WriteLine(messages);

                //Gelen mesajları txt ye yazıyoruz
                File.AppendAllText("log-critical.txt", messages + "\n");

                // mesajı aldık işledik,mesajın tagini Rabbitmq ya yolluyoruz ve kuyruktan bu mesaj siliniyor.
                // mesajı işlerken hata oldu o zaman bu mesajı göndermeyiz.
                channel.BasicAck(e.DeliveryTag, false);
            };
            channel.BasicConsume(queueName, false, consumer);

            Console.ReadLine();

        }

    }
}
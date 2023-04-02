using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbitMQ.Subscriber
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
            var channel = connection.CreateModel();

            // publisher tarafında bu kuyruğu zaten oluşturduğumuz için bu satırda ilgili kuyruğa bağlanırız
            // eğer publisherde oluşmazsa burda oluşur
            //publisher tarafında oluştuğuna eminsek kullanmaya gerek olmaz.
            // iki taraftada parametreler aynı olmalı
            //            channel.QueueDeclare("hello-queue", true, false, false);


            //bir kerede subscribera kaç mesaj gelceğini belirtiyoruz.
            //1. parametre, mesajın boyutudur. 0 olursa herhangi boyut olabilir
            //2. parametre,  her bir subscriber a 1 er 1 er mesaj gelsin
            //3. parametre, false olursa her bir subscribe e 2. parametredeki adet kadar mesaj iletilir
            // true olursa kaç subscribe varsa toplanda 2. parametredeki sayı kadar mesaj iletilir.
            channel.BasicQos(0, 1, false);

            //subscriber(comsumer) oluşturuyoruz ve oluşturulan kanalı verdik
            var consumer = new EventingBasicConsumer(channel);

            //kanal üzerinden "hello-queue" isimli kuyruğu dinliyoruz
            //autoAck
            /// *true olursa kuyruktan mesaj alındıktan sonra rabbitmq ilgili mesajı siler
            /// *false olursa  rabbitmq ya mesajı silme, işleme göre mesaj silineceğini biz bildireceğiz demiş oluruz.
            channel.BasicConsume("hello-queue", false, consumer);

            //"hello-queue" kuyruğuna bir mesaj geldiğinde bu event çalışır
            consumer.Received += (object? sender, BasicDeliverEventArgs e) =>
            {
                // Publisherda byte dizisine çevirip yolladığımız mesajı burada tekrar string olarak alıyhoruz
                var messages = Encoding.UTF8.GetString(e.Body.ToArray());
                Thread.Sleep(1500);
                Console.WriteLine(messages);

                // mesajı aldık işledik,mesajın tagini Rabbitmq ya yolluyoruz ve kuyruktan bu mesaj siliniyor.
                // mesajı işlerken hata oldu o zaman bu mesajı göndermeyiz.
                channel.BasicAck(e.DeliveryTag, false);
            };

            Console.ReadLine();

        }

    }
}
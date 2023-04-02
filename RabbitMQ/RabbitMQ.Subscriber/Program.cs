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


            //subscriber(comsumer) oluşturuyoruz ve oluşturulan kanalı verdik
            var consumer = new EventingBasicConsumer(channel);

            //kanal üzerinden "hello-queue" isimli kuyruğu dinliyoruz
            //autoAck
            /// *true olursa kuyruktan mesaj alındıktan sonra rabbitmq ilgili mesajı siler
            /// *false olursa  rabbitmq ya mesajı silme, işleme göre mesaj silineceğini biz bildireceğiz demiş oluruz.
            channel.BasicConsume("hello-queue", true, consumer);

            //"hello-queue" kuyruğuna bir mesaj geldiğinde bu event çalışır
            consumer.Received += (object? sender, BasicDeliverEventArgs e) =>
            {
                // Publisherda byte dizisine çevirip yolladığımız mesajı burada tekrar string olarak alıyhoruz
                var messages = Encoding.UTF8.GetString(e.Body.ToArray());

                Console.WriteLine(messages);

            };

            Console.ReadLine();

        }

    }
}
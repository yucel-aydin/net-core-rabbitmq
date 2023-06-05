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

            /**
             *burada random bir kuyruk adı oluşturuyoruz. 
             * Consumerdan birden fazla instanse ayağa kaldırırsak hepsi aynı kuyruğa bağlanmasınlar diye böyle yaptık
             * Subscriber(sonsumerlar ) down olsada kuyruk silinmesin istersek kuyruk adını random değil sabit vermeliyiz.
             */
            var randomQueue = "log-database-save-queue"; //channel.QueueDeclare().QueueName;

            /**
             *1. Parametre durable : true olursa fiziksel olarak sabit diskte kaydedilir
             *2. Parametre exclusive : false olursa başka kanallardan bu kuyruğa bağlanabilir
             *3. Parametre autoDelete : false olursa subscriberlar down olsada kuyruk silinmez.
             */
            channel.QueueDeclare(randomQueue, true, false, false);
            /**
             * var olan producer da oluşturduğumuz exchangemize bind ediyoruz
             * Burada kuyruk declare etmiyoruz çünkü subscriber kapansa dahi kuyruk kalır ama bind edersek subscriber exchange mesaj yolladığı sürece kuyruk var olur. 
             * 1. parametre kuyruk adı
             * 2. parametre exchange adı
             * 3. paramter route boş
             * 
             */
            channel.QueueBind(randomQueue, "logs-fanout", "", null);


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

            //kanal üzerinden yukarda random oluşturduğumuz kuyruğu dinliyoruz
            //autoAck
            /// *true olursa kuyruktan mesaj alındıktan sonra rabbitmq ilgili mesajı siler
            /// *false olursa  rabbitmq ya mesajı silme, işleme göre mesaj silineceğini biz bildireceğiz demiş oluruz.
            channel.BasicConsume(randomQueue, false, consumer);
            Console.WriteLine("Loglar dinleniyor...");
            //"hello-queue" kuyruğuna bir mesaj geldiğinde bu event çalışır
            consumer.Received += (object? sender, BasicDeliverEventArgs e) =>
            {
                // Publisherda byte dizisine çevirip yolladığımız mesajı burada tekrar string olarak alıyhoruz
                var messages = Encoding.UTF8.GetString(e.Body.ToArray());
                //Thread.Sleep(1500);
                Console.WriteLine(messages);

                // mesajı aldık işledik,mesajın tagini Rabbitmq ya yolluyoruz ve kuyruktan bu mesaj siliniyor.
                // mesajı işlerken hata oldu o zaman bu mesajı göndermeyiz.
                channel.BasicAck(e.DeliveryTag, false);
            };

            Console.ReadLine();

        }

    }
}
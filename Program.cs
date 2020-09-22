using Akka.Actor;
using AkkaKafka.Classes;
using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace AkkaKafka
{
    class Program
    {
        public static ActorSystem MyActorSystem;
        private static List<KeyValuePair<string,string>> kafkaConfig = new List<KeyValuePair<string, string>>
        {
            new KeyValuePair<string, string>("bootstrapserver","localhost:9092"),
            //new KeyValuePair<string, string>("bootstrapserver","localhost"),
            new KeyValuePair<string, string>("topic","AkkaKafkaTopic")
        };
        static void Main(string[] args)
        {
            //CreateKafkaMessages();
            MyActorSystem = ActorSystem.Create("MyFirstActorSystem");

            IActorRef router = MyActorSystem.ActorOf(Props.Create<RouterActor>(kafkaConfig),"router");
            for (int i = 0; i < 100; i++)
            {
                router.Tell(new StartCalculateMessage(i));
            }
            Console.WriteLine("Press R for send one more time");
            if(Console.ReadKey().Key==ConsoleKey.R)
            {
                for (int i = 0; i < 100; i++)
                {
                    router.Tell(new StartCalculateMessage(i));
                }
            }
            Console.ReadLine();
        }

        public static void CreateKafkaMessages()
        {
            var conf = new ProducerConfig { BootstrapServers = "localhost:9092"};
            using (var p = new ProducerBuilder<Null, string>(conf).Build())
            {
                for (int i = 0; i < 10; ++i)
                {
                    var message = new StartCalculateMessage(i);
                    var msgJson = JsonConvert.SerializeObject(message,new JsonSerializerSettings{
                        TypeNameHandling = TypeNameHandling.All
                        });
                   var deliveryReportTask = p.ProduceAsync("AkkaKafkaTopic", new Message<Null, string> { Value = msgJson });
                        deliveryReportTask.ContinueWith(task => {
                            Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                            });
                }
                Console.WriteLine("Messages Created");
            }
        }
    }
}

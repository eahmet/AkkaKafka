using Akka.Actor;
using Akka.Actor.Internal;
using Akka.Routing;
using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace AkkaKafka
{
    public class DummyActor : UntypedActor
    {
        private readonly IActorRef _workerActor = null;
        private string BootstrapServers = string.Empty;
        private string Topic = string.Empty;
        private IActorContext _context;
        public DummyActor(List<KeyValuePair<string,string>> kafkaConfig)
        {
            BootstrapServers = kafkaConfig.First(kvp => kvp.Key == "bootstrapserver").Value;
            Topic = kafkaConfig.First(kvp => kvp.Key == "topic").Value;
            _context = Context;
            _workerActor = Context.ActorOf(Props.Create<WorkerActor>(kafkaConfig).WithRouter(new RoundRobinPool(5)),"workerActor");

            Context.System.Scheduler.Advanced.ScheduleRepeatedly(TimeSpan.FromSeconds(1),TimeSpan.FromSeconds(2),ConsumeMessagesAndSendToWorkers);
        }
        protected override void OnReceive(object message)
        {
            throw new NotImplementedException();
        }
        protected override void PreStart()
        {
            Console.WriteLine($"- [PreStart DummyActor] {Self.Path.Uid.ToString()} is starting.");
            base.PreStart();
        }
        private void ConsumeMessagesAndSendToWorkers()
        {
            var conf = new ConsumerConfig
                {
                    GroupId = this.Topic,
                    BootstrapServers = this.BootstrapServers,
                    EnableAutoCommit = true,
                    StatisticsIntervalMs = 5000,
                    SessionTimeoutMs = 6000,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnablePartitionEof = true
                };
                using var consumer = new ConsumerBuilder<string, string>(conf)
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build();
                var partitionList = new List<TopicPartition>()
                {
                    new TopicPartition(this.Topic, 0)
                };

                consumer.Assign(partitionList);
                try
                {
                while (true)
                {
                    try
                    {
                        CancellationTokenSource cancellationToken = new CancellationTokenSource();
                        Console.CancelKeyPress += (_, e) =>
                        {
                            e.Cancel = true;
                            cancellationToken.Cancel();
                        };
                        var consumeResult = consumer.Consume(cancellationToken.Token);
                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                            break;
                        }
                        else
                        {
                            var message = JsonConvert.DeserializeObject(consumeResult.Message.Value, new JsonSerializerSettings
                            {
                                TypeNameHandling = TypeNameHandling.All
                            });
                            var provider = ((ActorSystemImpl)_context.System).Provider;
                            var newActorRef = provider.ResolveActorRef(consumeResult.Message.Key);
                            _workerActor.Tell(message,newActorRef);
                            try
                            {
                                consumer.Commit(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                Console.WriteLine($"Commit error: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    //consumer.Close();
                }
                finally
                {
                    consumer.Unassign();
                    consumer.Close();
                    consumer.Dispose();
                }
        }
    }
}

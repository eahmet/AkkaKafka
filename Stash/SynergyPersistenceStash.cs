using Akka.Actor;
using Akka.Actor.Internal;
using Akka.Serialization;
using AkkaKafka.Interface;
using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AkkaKafka.Stash
{
    public class SynergyPersistenceStash : ISynergyPersistenceStash
    {
        private readonly ActorCell _actorCell;
        private IActorContext _context;
        private string _bootstrapServers = string.Empty;
        private string _topic = string.Empty;
        static readonly object _lockobject = new object();
        private ProducerConfig _producerConf;
        private ConsumerConfig _consumerConf;
        private static IProducer<string, string> _producerBuilder;
        private static IConsumer<string, string> _consumerBuilder;
        private static bool _kafkaBroken = false;

        public SynergyPersistenceStash(IActorContext context, List<KeyValuePair<string, string>> kafkaConfig)
        {
            _actorCell = (ActorCell)context;
            _context = context;
            _bootstrapServers = kafkaConfig.First(kvp => kvp.Key == "bootstrapserver").Value;
            _topic = kafkaConfig.First(kvp => kvp.Key == "topic").Value;
            _producerConf = new ProducerConfig { BootstrapServers = this._bootstrapServers,MessageTimeoutMs = 10000 };
            _producerBuilder = new ProducerBuilder<string, string>(_producerConf)
                .SetErrorHandler(ProducerBuilderErrorHandler)
                .Build();
           
            _consumerConf = new ConsumerConfig
            {
                GroupId = this._topic,
                BootstrapServers = this._bootstrapServers,
                EnableAutoCommit = true
            };
            _consumerBuilder = new ConsumerBuilder<string, string>(_consumerConf)
                .SetErrorHandler(ConsumerBuilderErrorHandler)
                    .Build();

            var partitionList = new List<TopicPartition>()
                {
                    new TopicPartition(this._topic, 0)
                };

            _consumerBuilder.Assign(partitionList);
        }
        
        public bool IsPersistenceQueueAlive()
        {
            return _kafkaBroken;
        }
        
        public async Task ClearStash()
        {
            var adminClientBuilder = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrapServers });
            var adminClient = adminClientBuilder.Build();
            await adminClient.DeleteTopicsAsync(new List<string> { _topic }, null);
        }

        public long GetStashedCount()
        {
            var conf = new ConsumerConfig
            {
                GroupId = this._topic,
                BootstrapServers = this._bootstrapServers,
                EnableAutoCommit = true,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };
            using var consumer = new ConsumerBuilder<string, string>(conf).Build();
            consumer.Subscribe(this._topic);

            var i = 0;
            while (true)
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };
                var result = consumer.Consume(cts.Token);
                if (result.IsPartitionEOF)
                {
                    break;
                }

                i += 1;
            }
            consumer.Close();
            return i;
        }

        public long GetStashedCount(string instance)
        {
            //TODO Get Topic Message Count
            return 0;
        }

        public IEnumerable<object> GetStashedMessages()
        {
            return null;
        }

        public object GetStashedMessage()
        {
            var conf = new ConsumerConfig
            {
                GroupId = this._topic,
                BootstrapServers = this._bootstrapServers,
                EnableAutoCommit = true,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };
            using var consumer = new ConsumerBuilder<string, string>(conf)
                .Build();
            consumer.Subscribe(this._topic);

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            var result = consumer.Consume(cts.Token);
            consumer.Commit();
            consumer.Close();
            return JsonConvert.DeserializeObject(result.Message.Value, new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All
            });
        }

        public async Task Stash(object message, IActorRef sender)
        {
           if(_kafkaBroken) return;
            var msgJson = JsonConvert.SerializeObject(message, new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All
            });
            var result = await _producerBuilder.ProduceAsync(this._topic, new Message<string, string>() { Key = Serialization.SerializedActorPath(sender), Value = msgJson });
        }

        public async Task Stash()
        {
            var currMsg = _actorCell.CurrentMessage;
            var sender = _actorCell.Sender;

            await Stash(currMsg, sender);
        }

        public void Unstash()
        {
            if(_kafkaBroken) return;
                try
                {
                    CancellationTokenSource cancellationToken = new CancellationTokenSource();
                    Console.CancelKeyPress += (_, e) =>
                    {
                        e.Cancel = true;
                        cancellationToken.Cancel();
                    };
                    var consumeResult = _consumerBuilder.Consume(cancellationToken.Token);
                    if (consumeResult.IsPartitionEOF)
                    {
                        Console.WriteLine($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                    }
                    else
                    {
                        var message = JsonConvert.DeserializeObject(consumeResult.Message.Value, new JsonSerializerSettings
                        {
                            TypeNameHandling = TypeNameHandling.All
                        });
                        var provider = ((ActorSystemImpl)_context.System).Provider;
                        var newActorRef = provider.ResolveActorRef(consumeResult.Message.Key);

                        Envelope envelope = new Envelope(message, newActorRef);
                        Dispatch(envelope);
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
        }

        #region [Private Methods]
        private void Dispatch(Envelope envelope)
        {
            _actorCell.Dispatcher.Dispatch(_actorCell, envelope);

        }
        private void ConsumerBuilderErrorHandler(IConsumer<string, string> consumer, Error error)
        {
            if (error.IsError)
            {
                _kafkaBroken = true;
            }
            else
                _kafkaBroken = false;
        }

        private void ProducerBuilderErrorHandler(IProducer<string, string> producer, Error error)
        {
            if (error.IsError)
            {
                _kafkaBroken = true;
            }
            else
                _kafkaBroken = false;
        }
        #endregion
    }
}

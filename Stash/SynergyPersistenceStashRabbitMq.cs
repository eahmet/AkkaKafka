using Akka.Actor;
using Akka.Actor.Internal;
using Akka.Serialization;
using AkkaKafka.Classes;
using AkkaKafka.Interface;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AkkaKafka.Stash
{
    public class SynergyPersistenceStashRabbitMq : ISynergyPersistenceStash
    {
        private readonly ActorCell _actorCell;
        private IActorContext _context;
        private string BootstrapServers = string.Empty;
        private string Topic = string.Empty;
        static readonly object _lockobject = new object();
        private ConnectionFactory _factory;
        private IConnection _connection;
        private IModel _model;

        public SynergyPersistenceStashRabbitMq(IActorContext context, List<KeyValuePair<string, string>> config)
        {
            _actorCell = (ActorCell)context;
            _context = context;
            BootstrapServers = config.First(kvp => kvp.Key == "bootstrapserver").Value;
            Topic = config.First(kvp => kvp.Key == "topic").Value;
            _factory = new ConnectionFactory()
            {
                HostName = this.BootstrapServers
            };
            _connection = _factory.CreateConnection();
            _model = _connection.CreateModel();
            _model.ExchangeDeclare(this.Topic,ExchangeType.Direct);
            _model.QueueDeclare(queue: this.Topic,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);
            _model.QueueBind(this.Topic, this.Topic, "directexchange_key");

        }
        public Task ClearStash()
        {
            throw new NotImplementedException();
        }

        public long GetStashedCount()
        {
            throw new NotImplementedException();
        }

        public long GetStashedCount(string instance)
        {
            throw new NotImplementedException();
        }

        public object GetStashedMessage()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<object> GetStashedMessages()
        {
            throw new NotImplementedException();
        }

        public void Prepend(IEnumerable<Envelope> envelopes)
        {
            throw new NotImplementedException();
        }

        public async Task Stash(object message, IActorRef sender)
        {
            /*var factory = new ConnectionFactory() { HostName = this.BootstrapServers };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: this.Topic,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var msgJson = JsonConvert.SerializeObject(message, new JsonSerializerSettings
                {
                    TypeNameHandling = TypeNameHandling.All
                });
                var bodyObject = new StashObject(Serialization.SerializedActorPath(sender), msgJson);

                var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(bodyObject, new JsonSerializerSettings
                {
                    TypeNameHandling = TypeNameHandling.All
                }));
                
                channel.BasicPublish(exchange: "",
                                     routingKey: this.Topic,
                                     basicProperties: null,
                                     body: body);

            }*/

            var msgJson = JsonConvert.SerializeObject(message, new JsonSerializerSettings
                {
                    TypeNameHandling = TypeNameHandling.All
                });
                var bodyObject = new StashObject(Serialization.SerializedActorPath(sender), msgJson);

                var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(bodyObject, new JsonSerializerSettings
                {
                    TypeNameHandling = TypeNameHandling.All
                }));
            _model.BasicPublish(this.Topic, "directexchange_key", null, body);
            
        }

        public async Task Stash()
        {
            var currMsg = _actorCell.CurrentMessage;
            var sender = _actorCell.Sender;

            await Stash(currMsg, sender);
        }

        public void Unstash()
        {
            /*var factory = new ConnectionFactory() { HostName = this.BootstrapServers };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: this.Topic,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);


                var a = channel.BasicGet(queue: this.Topic, autoAck: true);
                if (a != null)
                {
                    var body = a.Body.ToArray();

                    var stashObject = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(body), new JsonSerializerSettings
                    {
                        TypeNameHandling = TypeNameHandling.All
                    });
                    StashObject asd = (StashObject)stashObject;
                    var provider = ((ActorSystemImpl)_context.System).Provider;
                    var newActorRef = provider.ResolveActorRef(asd.ActorRef);
                    var message = JsonConvert.DeserializeObject(asd.Message, new JsonSerializerSettings
                    {
                        TypeNameHandling = TypeNameHandling.All
                    });
                    Envelope envelope = new Envelope(message, newActorRef);
                    Dispatch(envelope);
                }
            }*/
            //_model.BasicQos(1,0,false);
            
            var messageBody = _model.BasicGet(queue: this.Topic, autoAck: true);
            bool isMessageCorrupt = false;
            if (messageBody != null)
                {
                    var stashObject = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(messageBody.Body.ToArray()), new JsonSerializerSettings
                    {
                        TypeNameHandling = TypeNameHandling.All,
                        Error = delegate(object sender, ErrorEventArgs args)
                        {
                            args.ErrorContext.Handled = true;
                            isMessageCorrupt = true;
                        }
                        
                    });
                    if(isMessageCorrupt) return;

                    StashObject asd = (StashObject)stashObject;
                    var provider = ((ActorSystemImpl)_context.System).Provider;
                    var newActorRef = provider.ResolveActorRef(asd.ActorRef);
                    var message = JsonConvert.DeserializeObject(asd.Message, new JsonSerializerSettings
                    {
                        TypeNameHandling = TypeNameHandling.All
                    });
                    Envelope envelope = new Envelope(message, newActorRef);
                    Dispatch(envelope);
                }
        }
        private void Dispatch(Envelope envelope)
        {
            _actorCell.Dispatcher.Dispatch(_actorCell, envelope);

        }
       
        public bool IsPersistenceQueueAlive()
        {
            throw new NotImplementedException();
        }
    }
}

namespace AkkaKafka.Classes
{
    public class StashObject
    {
        public string ActorRef {get;}
        public string Message {get;}
        public StashObject(string actorRef, string message)
        {
            ActorRef=actorRef;
            Message = message;
        }
    }
}

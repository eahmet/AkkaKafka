using System;
using System.Collections.Generic;
using System.Text;

namespace AkkaKafka.Classes
{
    public class StartCalculateMessage
    {
        public int Id {get;}
        public StartCalculateMessage(int id)
        {
            Id=id;
        }
    }
}

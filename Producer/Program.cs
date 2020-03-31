using System;
using Confluent.Kafka;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var producer = new Producer();
            
            while(true)
            {

                Console.WriteLine("Digite a mensagem");
                var msg = Console.ReadLine();;
                if(msg == "exit")
                    break;
                producer.SendMessage(msg);

            }
            
            
        }

    }

    public class Producer
    {

        public string SendMessage(string msg)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var sendResult = producer
                    .ProduceAsync("test", new Message<Null, string> { Value = msg})
                    .GetAwaiter()
                    .GetResult();

                    return $"Mensagem {sendResult.Value} de {sendResult.TopicPartitionOffset}";
                }
                catch (ProduceException<Null, string> e)
                {

                    Console.WriteLine($"Erro ao enviar mensagem {e.Error.Reason}");
                }

                return default;

            }
        }

    }
}

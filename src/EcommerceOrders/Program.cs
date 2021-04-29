using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace EcommerceOrders
{
    class Program
    {
        static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, collection) =>
                {
                    collection.AddHostedService<KafkaProducerHostedService>();
                    collection.AddHostedService<KafkaConsumerHostedService>();
                });
    }

    public class KafkaProducerHostedService : IHostedService
    {
        private readonly ILogger<KafkaProducerHostedService> _logger;
        private readonly IProducer<Null, string> _producer;
        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            _logger = logger;
            var config = new ProducerConfig()
            {
                BootstrapServers = "127.0.0.1:9092"
            };
            _producer = new ProducerBuilder<Null, string>(config)
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .Build();
        }
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                var connectionString = "Server=127.0.0.1;Database=dbuserinformations;Uid=root;Pwd=985206;";
                using (MySqlConnection con = new MySqlConnection(connectionString))
                {
                    con.Open();
                    var command = new MySqlCommand("SELECT * FROM dt_user_information LIMIT 5", con);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        var dataOfUser = new UserData
                        {
                            ID = reader["ID"].ToString(),
                            Name = reader["Name"].ToString(),
                            Address = reader["Address"].ToString(),
                            Phone = reader["Phone"].ToString()
                        };

                        var dataOfUserConvertedToJsonToSendToTopic = JsonConvert.SerializeObject(dataOfUser);
                        var message = await _producer.ProduceAsync("demo", new Message<Null, string>() { Value = dataOfUserConvertedToJsonToSendToTopic });
                        _logger.LogInformation($"Message sent: {message.Message.Value} | Offset: {message.Offset}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Exceção: {ex.GetType().FullName} | Mensagem: {ex.Message}");
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }

    public class KafkaConsumerHostedService : IHostedService
    {
        private readonly ILogger<KafkaConsumerHostedService> _logger;
        private readonly IConsumer<Null, string> _consumer;
        public KafkaConsumerHostedService(ILogger<KafkaConsumerHostedService> logger)
        {
            _logger = logger;

            var config = new ConsumerConfig
            {
                BootstrapServers = "127.0.0.1:9092",
                GroupId = nameof(KafkaConsumerHostedService),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            _consumer = new ConsumerBuilder<Null, string>(config).Build();
        }
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _consumer.Subscribe("demo");
            try
            {
                try
                {
                    while (true)
                    {
                        var messageReceived = _consumer.Consume(cancellationToken);
                        var userInformation = JsonConvert.DeserializeObject<UserData>(messageReceived.Message.Value);
                        _logger.LogInformation($"Message received: {userInformation.Name} | Offset: {messageReceived.Offset}");
                        _logger.LogInformation($"Saving in the database");

                        var connectionString = "Server=127.0.0.1;Database=dbuserinformations;Uid=root;Pwd=985206;";
                        using (MySqlConnection con = new MySqlConnection(connectionString))
                        {
                            con.Open();
                            var command = new MySqlCommand($"INSERT INTO dt_user_information(ID, Name, Phone, Address) VALUES ('{userInformation.ID}', '{userInformation.Name}', '{userInformation.Phone}', '{userInformation.Address}')", con);
                            command.ExecuteNonQuery();
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    StopAsync(cancellationToken);
                    _logger.LogWarning("Execution cancelled by the user");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Exceção: {ex.GetType().FullName} | Mensagem: {ex.Message}");
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _consumer?.Dispose();
            return Task.CompletedTask;
        }
    }
}

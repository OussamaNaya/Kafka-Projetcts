const { Kafka } = require("kafkajs");

// Kafka config
const kafka = new Kafka({
  clientId: "logger-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "log-group" });

async function runConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: "logs", fromBeginning: true });

  console.log("👂 Consumer listening...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log("📩 Message reçu :", message.value.toString());
    },
  });
}

runConsumer();
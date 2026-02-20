const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "notification-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "notification-group" });

async function runConsumer() {
  await consumer.connect();
  console.log("👂 Consumer is listining ...")

  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      
      console.log(`📧 Notification: Order ${order.orderId} for ${order.user}`);
    },
  });
}

runConsumer();
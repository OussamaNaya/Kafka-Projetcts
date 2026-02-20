const express = require("express");
const { Kafka } = require("kafkajs");

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
async function runProducer() { 
    await producer.connect(); 
    console.log("✅ Producer is runing ...")
}

runProducer();

app.post("/order", async (req, res) => {
  const { orderId, user } = req.body;

  await producer.send({
    topic: "orders",
    messages: [
        { 
            value: JSON.stringify({ orderId, user, event: "ORDER_CREATED" }) 
        }
    ],
  });

  res.json({ status: "Order created and sent to Kafka", orderId, user });
});


app.listen(3000, () => {
  console.log("🚀 Order API running on http://localhost:3000");
});
const express = require("express");
const { Kafka } = require("kafkajs");

const app = express();
app.use(express.json());

// Kafka config
const kafka = new Kafka({
  clientId: "logger-app",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function runProducer() {
  await producer.connect();
  console.log("✅ Producer connected");
}

runProducer();

// API endpoint
app.post("/log", async (req, res) => {
  const { message } = req.body;

  await producer.send({
    topic: "logs",
    messages: [{ value: message }],
  });

  res.json({ status: "Message sent to Kafka", message });
});

app.listen(3000, () => {
  console.log("🚀 Producer API running on http://localhost:3000");
});
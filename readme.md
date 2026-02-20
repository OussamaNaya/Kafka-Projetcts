# Kafka Notifications Project (Express.js)

Mini-projet pour simuler un service de notification avec Node.js et Express.

## 🌿 Branches

| Branche | Contenu |
|--------|---------|
| `kafka-notifications` | Code complet Producer + Consumer |
| `main` | README et documentation |

---

## 🏗️ Architecture

```
Client (Postman)
       ↓
Order API (Producer)
       ↓
Kafka (topic: orders)
       ↓
Notification Service (Consumer)
```

---

## 🧑‍🍳 Producer (Order API)

**`producer.js`**

```js
app.post("/order", async (req, res) => {
  const { orderId, user } = req.body;
  await producer.send({
    topic: "orders",
    messages: [{ value: JSON.stringify({ orderId, user, event: "ORDER_CREATED" }) }],
  });
  res.json({ status: "Order created", orderId, user });
});
```

- Reçoit les données de l'utilisateur
- Envoie l'événement au topic Kafka `orders`

---

## 👂 Consumer (Notification Service)

**`consumer.js`**

```js
await consumer.subscribe({ topic: "orders", fromBeginning: true });

await consumer.run({
  eachMessage: async ({ message }) => {
    const order = JSON.parse(message.value.toString());
    console.log(`📧 Notification: Order ${order.orderId} for ${order.user}`);
  },
});
```

- Lit les messages du topic `orders`
- Affiche une notification simulée
- `groupId` : `notification-group` pour le partage de charge

---

## 🧪 Test

**Requête POST :**

```http
POST http://localhost:4000/order
Content-Type: application/json

{
  "orderId": 123,
  "user": "Alice"
}
```

**Console du consumer :**

```
📧 Notification: Order 123 for Alice
```
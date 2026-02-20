# Kafka Logger Project (Express.js)

Ce projet est une introduction simple à Apache Kafka avec Node.js (Express).  
Il montre comment :
- produire des messages (Producer)
- consommer des messages (Consumer)
- comprendre le workflow Kafka

Le code complet est disponible dans la branche : **kafka-logger**

---

## 🧠 Qu'est-ce que Kafka ?

Apache Kafka est une plateforme de streaming distribuée qui permet :
- d'envoyer des messages entre applications
- de traiter des événements en temps réel
- de découpler les services (microservices)

Kafka fonctionne avec :
- **Producer** → envoie des messages
- **Broker** → stocke les messages
- **Topic** → catégorie de messages
- **Consumer** → lit les messages
- **Consumer Group** → groupe de consumers

---

## 🏗️ Architecture du projet

```
Client (Postman)
       ↓
Express API (Producer)
       ↓
Kafka (topic: logs)
       ↓
Consumer (console.log)
```

---

## 🔄 Workflow (comment ça marche)

1. Le client envoie une requête POST `/log`
2. Express agit comme Producer Kafka
3. Le message est envoyé au topic `logs`
4. Kafka stocke le message
5. Le Consumer lit le message
6. Le message est affiché dans la console

---

## 📦 Composants Kafka utilisés

| Composant       | Valeur          |
|----------------|-----------------|
| Topic           | `logs`          |
| Producer        | Express API     |
| Consumer        | service Node.js |
| Consumer Group  | `log-group`     |
| Broker          | Kafka (Docker)  |

---

## 🧑‍🍳 Producer (`producer.js`)

Le Producer est responsable d'envoyer des messages vers Kafka.

### Étapes principales du code :

```js
const producer = kafka.producer();
await producer.connect();
```

➡️ Crée un producer et se connecte au broker Kafka.

```js
app.post("/log", async (req, res) => {
  const { message } = req.body;
  await producer.send({
    topic: "logs",
    messages: [{ value: message }],
  });
  res.json({ status: "Message sent", message });
});
```

**Explication ligne par ligne :**

| Code | Rôle |
|------|------|
| `app.post("/log")` | crée une API REST |
| `const { message } = req.body` | récupère le message envoyé par le client |
| `producer.send({...})` | envoie le message au topic Kafka `logs` |
| `messages: [{ value: message }]` | Kafka enregistre le message |
| `res.json(...)` | renvoie une réponse au client |

➡️ Express joue ici le rôle de **Producer Kafka**.

---

## 👂 Consumer (`consumer.js`)

Le Consumer écoute Kafka et lit les messages.

### Étapes principales du code :

```js
const consumer = kafka.consumer({ groupId: "log-group" });
await consumer.connect();
await consumer.subscribe({ topic: "logs", fromBeginning: true });
```

➡️ Le consumer rejoint le Consumer Group `log-group`  
➡️ Il s'abonne au topic `logs`

```js
await consumer.run({
  eachMessage: async ({ message }) => {
    console.log("Message reçu :", message.value.toString());
  },
});
```

**Explication :**

| Code | Rôle |
|------|------|
| `consumer.run()` | démarre l'écoute |
| `eachMessage` | est appelée à chaque nouveau message |
| `message.value.toString()` | convertit le message en texte |
| `console.log(...)` | affiche le message |

➡️ Le consumer est un **service indépendant** du producer.

---

## 🔑 Consumer Group

Tous les consumers ayant le même `groupId` appartiennent au même groupe.

Dans ce projet :
- `groupId` = `log-group`
- Si on lance **plusieurs consumers** :
  - chaque message est traité par **un seul consumer** du groupe
  - cela permet le **partage de charge** (scalabilité)
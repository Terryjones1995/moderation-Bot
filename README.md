# Discord Moderation Bot (Node.js)

This is a Node.js Discord moderation bot that greets new members, enforces a **7-day account age rule**, logs actions, and uses **OpenAI** to automatically delete inappropriate content (racism, sports betting, etc.).

---

## 🚀 Features

- ✅ Greets new members via **DM**.
- ✅ Blocks users with accounts under 7 days old from sending messages.
- ✅ Logs all actions (joins, deletes, etc.) in a dedicated `#bot-logs` channel.
- ✅ Auto-creates a `Moderation` category and `bot-logs` channel on startup if they don’t exist.
- ✅ Uses **OpenAI** to detect harmful messages and deletes them.

---

## 📦 Requirements

- [Node.js](https://nodejs.org/) (v18 or later recommended)
- A **Discord bot token**
- An **OpenAI API key**

---

## 🔧 Setup

1. **Clone this repo:**
   ```bash
   git clone https://github.com/yourusername/discord-moderation-bot.git
   cd discord-moderation-bot
   ```

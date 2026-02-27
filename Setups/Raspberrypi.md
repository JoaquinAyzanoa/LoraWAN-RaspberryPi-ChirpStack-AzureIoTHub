# Raspberry Pi Setup — ChirpStack via Docker

---

## 📋 Step 1: Update the System

Make sure your Raspberry Pi OS or Ubuntu is up-to-date:

```bash
sudo apt update && sudo apt full-upgrade -y
```

---

## 🐳 Step 2: Install Docker & Docker Compose

```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
newgrp docker
```

---

## 📦 Step 3: Get ChirpStack Docker Compose Setup

ChirpStack provides an official Docker Compose configuration that runs all necessary services (Network Server, Application Server, Gateway Bridge, MQTT, PostgreSQL, Redis).

```bash
git clone https://github.com/chirpstack/chirpstack-docker.git
cd chirpstack-docker
```

---

## ⚙️ Step 4: Adjust Region Settings (if needed)

By default the configuration uses **EU868**. If you are in another region (e.g., US915), update the following:

- **`/configuration/chirpstack/chirpstack.toml`** → enable your region
- **`/docker-compose.yml`** → make sure the correct bridge configuration is used

This tells ChirpStack which frequency plan to use.

---

## ▶️ Step 5: Start ChirpStack

```bash
docker compose up -d
```

---

## 🧪 Step 6: Verify ChirpStack Is Running

Open a browser and navigate to:

```
http://<RASPBERRY_PI_IP>:8080
```

You should see the ChirpStack login page. Default credentials:

| Field    | Value   |
|----------|---------|
| Username | `admin` |
| Password | `admin` |

---

## 🔍 Step 7: Verify UDP Traffic Is Reaching the Pi

Even if the gateway appears to be sending packets and ChirpStack looks "online", UDP traffic may not actually reach the Pi due to firewall or network issues.

### Check with `tcpdump`

Run the following on the Raspberry Pi:

```bash
sudo apt install tcpdump
sudo tcpdump -AUq port 1700
```

If packets are arriving from the gateway, you will see output in the terminal. If nothing appears, check your firewall rules and network configuration.
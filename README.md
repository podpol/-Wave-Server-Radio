<p align="center">
  <img width="120" alt="wave-logo" src="https://github.com/user-attachments/assets/0e6657b3-31a5-4995-bec5-0fd1f25982c2">
</p>

<h1 align="center">Wave 🌊</h1>
<p align="center"><i>Find your wave. Be on the same wave.</i></p>
<p align="center">Structured P2P voice radio for teams, communities, and friends.<br>No accounts. No tracking. No chaos on the airwaves.</p>

<p align="center">
  <img alt="Node.js" src="https://img.shields.io/badge/Node.js-18%2B-339933?logo=node.js&logoColor=white">
  <img alt="Chrome Extension" src="https://img.shields.io/badge/Chrome-Extension-4285F4?logo=googlechrome&logoColor=white">
  <img alt="WebRTC" src="https://img.shields.io/badge/Audio-P2P%20WebRTC-orange">
  <img alt="License" src="https://img.shields.io/badge/License-MIT%20%2F%20AGPL--3.0-blue">
</p>

---

## 🌊 What is Wave?

Imagine the internet as an ocean, and people as ships. Sometimes ships meet to talk, and then they sail away, perhaps never to meet again.

Wave is a lightweight browser extension that turns any tab into a structured voice "radio station." No chaos, no talking over each other, no background noise. Just like on a real ship: there's a **Captain** (admin), **Navigators** (speaking rights), and **Travelers** (listeners).

**The Golden Rule:** the server ("Lighthouse") never hears your voice. Audio flows directly between browsers over an encrypted P2P channel (WebRTC). The server only helps ships find each other — and, as of v2.0, a growing network of Lighthouses helps ships find each other **even across servers**, without needing to know any specific address in advance.

---

## ⚓ Features

| | |
|---|---|
| **Clear roles** | Captain (1), Navigators (up to 10), Travelers (up to 30) |
| **Voice modes** | Push-to-Talk, VOX (voice activation), Toggle, Manual |
| **Ship's Logbook (chat)** | Text chat with emoji, word triggers, auto-hiding of unwanted content |
| **Airwave discipline** | Vote-to-kick, 30-minute blacklist, Captain rights transfer |
| **Absolute privacy** | No registration, no database of audio, no voice logs |
| **Customization** | 11 themes, custom fonts, 5 interface languages |
| **Network flexibility** | Global Open Ocean (internet) or isolated Local Harbor (LAN, no internet needed) |
| **🆕 Federation** | Channels are discoverable network-wide, not tied to one server |
| **🆕 Relay Helper Mode** | Anyone can help the network from inside the extension — no server setup needed |

---

## 🗺️ How it works

Wave has two layers that are easy to confuse, so here they are, kept apart on purpose:

### Layer 1 — Voice & files: always pure P2P, always has been

```
Browser A (Navigator) ────── P2P Audio (WebRTC, DTLS-SRTP) ──────► Browser B (Traveler)
        │                                                                │
        └──────────────────── Signaling only (Socket.io) ────────────────┘
                                        │
                              🗼 Lighthouse (Node.js server)
                    (Only introduces browsers to each other. Never touches audio.)
```

Your voice and shared files never pass through any server, ever — not for reliability, not for moderation, not for anything. The server's only job here is the initial handshake (exchanging WebRTC connection details), same as it's always been.

### Layer 2 — Federation: a network of Lighthouses, not just one

```
        Lighthouse A  ⇄  Lighthouse B  ⇄  Lighthouse C   ← servers gossip
             │                 │                │           channel metadata
        clients          clients           clients          and peer lists
```

Every Wave channel lives ("is hosted") on one Lighthouse. But **every** Lighthouse in the network knows about **every** channel on **every other** Lighthouse — so a client connected to Lighthouse A can find and join a channel that actually lives on Lighthouse C, without ever having heard of it before. Under the hood, the client gets transparently redirected to the right server.

New Lighthouses don't need to know the whole network in advance either — pointing a new server at just **one** already-running Lighthouse (a "bootstrap" address) is enough. Servers gossip their peer lists to each other, and the mesh grows on its own.

> **Being honest about limits:** a browser extension cannot accept incoming network connections — that's a browser sandboxing rule, not a Wave limitation, and no amount of clever code changes it. So there is no "click a button and your laptop becomes a server" magic. What the extension *can* do is described below as **Relay Helper Mode**. A real, always-on Lighthouse still needs someone to run `server.js` on an actual machine (a $5 VPS is plenty).

---

## 🚀 Quick start (just want to talk to people?)

1. Install the **Wave** extension from the Chrome Web Store *(or load it unpacked from `/extension` — see below)*.
2. Click the Wave icon → **⛵ Set Sail** to create a channel, or **📡 Hail a Ship** to join one with an 8-digit code.
3. That's it — no account, no sign-up.

You're automatically connected to one of the public Lighthouses. If you want your own private server (for a community, a company, or just to keep your own data on your own hardware), read on.

<details>
<summary><b>Loading the extension unpacked (for development / before it's on the Store)</b></summary>

1. Clone this repo.
2. Open `chrome://extensions`.
3. Enable **Developer mode** (top right).
4. Click **Load unpacked** and select the `extension/` (or wherever `sidepanel.html` lives) folder.
5. Pin the Wave icon to your toolbar.

</details>

---

## 🖥️ Running your own Lighthouse server

Anyone can run a Lighthouse — for a private community, for redundancy, or just to help the wider network. The more independent Lighthouses exist, the less any single server (including ours) matters to keep the whole thing running.

### Requirements

- **Node.js 18+**
- A machine with a public IP (a cheap VPS is enough — a Raspberry Pi behind a home router will **not** work for this, see the NAT note below)
- Two open/forwarded ports: **3000** (clients) and **3001** (federation, server-to-server)

### Windows

```
1. Download/clone this repo
2. Run  install.bat   — installs dependencies, generates a .env with unique
                         secrets, asks for a server name and (optionally)
                         a bootstrap address of an existing network
3. Run  start.bat      — starts the server
```

Both scripts print their own errors instead of just closing, and never silently vanish — if a window "flashes" and disappears, something is stopping the `.bat` file itself from running (antivirus quarantine, execution policy, or Group Policy on a managed Windows Server) rather than a bug inside the script.

### Linux / macOS

```bash
git clone https://github.com/<you>/wave-server.git
cd wave-server
npm install
cp .env.example .env    # then edit .env — see reference below
node server.js
```

Run it under `pm2`, `systemd`, or Docker for a real deployment so it survives reboots and crashes.

---

## ⚙️ Configuration reference (`.env`)

| Variable | What it does |
|---|---|
| `PORT` | Port clients connect to (default `3000`) |
| `NODE_ENV` | Set to `production` for real deployments — this also **refuses to start** with default/insecure tokens, on purpose |
| `API_KEY` | Protects `/api/*` admin endpoints (stats, moderation) |
| `BOT_TOKENS` | Comma-separated tokens that let bots authenticate |
| `ALLOWED_ORIGINS` | Extra CORS origins, if you need them (usually leave empty) |
| `FEDERATION_ID` | This server's unique ID in the network |
| `FEDERATION_URL` | **Your real public address**, e.g. `ws://1.2.3.4:3001` — this is what other servers use to reach you |
| `FEDERATION_NAME` | Human-readable name shown to other nodes |
| `FEDERATION_SECRET` | Shared secret for servers you explicitly federate with (peers must use the same one) |
| `FEDERATION_PEERS` | Comma-separated list of servers to always stay connected to |
| `FEDERATION_BOOTSTRAP` | **One** known address of an already-running network — enough to join and auto-discover the rest via gossip |
| `OFFICIAL_SERVER_IDS` | IDs of servers you fully trust (usually just your own) — see the trust model below |
| `FEDERATION_MAX_PEERS` | Cap on how large your mesh grows (default `40`) |
| `TURN_SECRET` / `TURN_URLS` | Optional TURN server for voice calls stuck behind strict/symmetric NAT — without this, calls fall back to STUN only, which doesn't cover every network |

---

## 🔐 Security model

**What the server can see:** who's in which channel, channel names, and connection metadata. **What it can never see:** your voice, or the contents of shared files — those never touch it.

**Federation trust tiers** — not every server in the network is trusted equally:

- **Official** — servers you list in `OFFICIAL_SERVER_IDS`. Typically just your own infrastructure.
- **Volunteer** — any other server that connects with the correct `FEDERATION_SECRET`. Before a volunteer server's claims are trusted, the network verifies it's reachable at the address it claims (a callback connection, similar in spirit to how ACME domain validation works) — and even then, a volunteer server can never hijack an existing channel name or delete a channel it doesn't own.

**Hardening baked into the server:**
- Timing-safe secret comparison (no timing side-channel on the federation secret)
- Per-IP connection limits and message-size caps on the federation port, enforced *before* any authentication happens
- Rate limiting on both client and server-to-server traffic
- `helmet` security headers, strict CORS (no more blanket `origin: null`)
- Refuses to boot in `NODE_ENV=production` with default/placeholder tokens instead of silently running insecurely

**On NAT and "becoming a node":** we want to be upfront that a home computer behind a router (the overwhelming majority of users) cannot accept inbound connections, and neither can a browser extension, regardless of IP type. A real Lighthouse needs a real server. What every user *can* do, from right inside the extension, is:

### 🕸️ Relay Helper Mode

Found under **Settings → Network** in the extension. It doesn't require a public IP or open ports — it only ever makes *outbound* connections. While it's on, your extension:
- keeps a warm local cache of the channel list, so a brief server hiccup doesn't leave you staring at "no connection"
- helps the network absorb load without needing a dedicated server

It's not a replacement for running your own Lighthouse — it's the honest, actually-possible version of "help the network" for people who don't want to run a server.

---

## 🧭 Interface compass

| Icon / Button | Action |
|---|---|
| ⛵ Set Sail | Create a new Wave. You become its Captain. |
| 📡 Hail a Ship | Join an existing Wave with an 8-digit code. |
| 🎤 Broadcast | Turn on your mic (Navigators and Captains only). |
| ✋ Hand | Raise your hand to request the floor. |
| ⚓ Drop Anchor | *(Captain only)* Close the Wave — everyone's sent to shore. |
| 🔒/🔓 Wave Mode | *(Captain only)* Toggle free boarding vs. approval-required. |
| 🕸️ Network tab | See federation status, toggle Relay Helper Mode |

---

## 🩺 Troubleshooting

- **Calls won't connect for some people** — likely symmetric NAT on their network; configure `TURN_URLS`/`TURN_SECRET` (see above).
- **A channel from another server won't load** — the home Lighthouse for that channel may be offline; federation redirects only work while the origin server is reachable.
- **Windows install script closes instantly** — see the note in the *Running your own Lighthouse server* section above; it means the `.bat` itself isn't running, not that the server code failed.

---

## 🤝 Support & Community

Found a bug in the compass? Want to suggest a new theme?

- Open an **Issue** on GitHub.
- Constructive suggestions to improve the voyage are always welcome.

## 📱 Android app (coming soon)

Wave isn't just for desktop — a native Android app bringing the same P2P voice experience to mobile is in development. Stay tuned.

---

<p align="center">
  <sub>Made with ❤️ for those who value silence and order on the airwaves.</sub><br>
  <sub>Server: AGPL-3.0 · Extension: MIT — use freely, but remember good manners at sea.</sub>
</p>

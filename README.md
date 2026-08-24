# Service Bus Studio — Azure Service Bus Explorer for Windows & macOS

![Service Bus Studio — a fast, native Azure Service Bus explorer built in Rust](assets/banner.jpg)

[![Latest release](https://img.shields.io/github/v/release/hm-harshit/Service-Bus-Studio)](../../releases/latest)
[![Downloads](https://img.shields.io/github/downloads/hm-harshit/Service-Bus-Studio/total)](../../releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20macOS-lightgrey)
![Built with Rust](https://img.shields.io/badge/built%20with-Rust-orange)

**Service Bus Studio** is a fast, free, open-source **Azure Service Bus explorer** —
a native desktop GUI to **browse queues, topics and subscriptions, peek and send messages,
and manage dead-letter queues**. A modern alternative to the classic
[Service Bus Explorer](https://github.com/paolosalvatori/servicebusexplorer),
written in Rust: one small self-contained binary, instant startup, no .NET runtime,
no installer.

## Why Service Bus Studio?

- ⚡ **Fast & native** — a single ~10 MB executable, starts instantly, low memory
- 🔍 **Non-destructive peek** — browse messages over AMQP without touching delivery counts
- ☠️ **Dead-letter workflows** — inspect, resubmit, and purge DLQ messages in two clicks
- 🛠️ **Full entity management** — create, configure, and delete queues, topics & subscriptions
- 🎨 **Modern UI** — light & dark themes, keyboard shortcuts, live message counts
- 🔓 **Open source (MIT)** — no telemetry, no account, your connection strings stay on your machine

## Features

**Messaging**
- Peek messages (queues, subscriptions, and their dead-letter queues) with sequence-number paging
- Message inspector: system properties, custom application properties, JSON pretty-printed body, copy to clipboard
- Send messages with subject, content type, message/correlation/session IDs, TTL, custom properties, and batch copies
- Load any peeked message into the send form to edit & resend
- Receive & delete, purge all, resubmit dead-lettered messages back to the entity

**Entity management**
- Namespace tree with live active / dead-letter / scheduled counts per entity
- Create & delete queues, topics, subscriptions
- Edit entity settings: lock duration, TTL, max delivery count, auto-delete-on-idle, forwarding, status (Active/Disabled), and more
- Full read-only property view (size, timestamps, counts)

**Workflow**
- Light & dark themes, saved connection history, entity filter, F5 refresh
- Timestamped operation log panel
- Friendly errors that explain missing SAS claims (Send/Listen/Manage)

## Install

**Windows (x64):** download `ServiceBusStudio-*-windows-x64.zip` from the
[latest release](../../releases/latest), unzip anywhere, run `ServiceBusStudio.exe`.
No installer, no runtime, no admin rights.

**macOS (Apple Silicon):** download `ServiceBusStudio-*-macos-arm64.tar.gz` from the
[latest release](../../releases/latest), extract, drag `ServiceBusStudio.app` to
Applications. The app is unsigned — right-click → Open on first launch
(or `xattr -cr ServiceBusStudio.app`).

**Build from source** (Windows, macOS, or Linux with [Rust](https://rustup.rs)):

```sh
git clone https://github.com/hm-harshit/Service-Bus-Studio.git
cd Service-Bus-Studio
cargo build --release
```

## Getting started

1. Launch the app — the Connect dialog opens automatically.
2. Paste a Service Bus **namespace connection string**
   (`Endpoint=sb://…;SharedAccessKeyName=…;SharedAccessKey=…`) from the Azure portal
   (*Service Bus → Shared access policies*).
3. Hit **Connect** — the namespace tree loads with all queues, topics, and subscriptions.

Use a **Manage**-level key (e.g. `RootManageSharedAccessKey`) for full functionality.
Listen/Send-only or entity-scoped (`EntityPath=…`) keys work too, with reduced scope —
the app tells you exactly what the key can do.

## Roadmap

- **1.1** — Session browsing, scheduled-message list & cancel
- **1.2** — Subscription rules & filters CRUD
- **1.3** — Message import/export (JSON), bulk resubmit with edit
- **1.4** — Entra ID (Azure AD) sign-in, named saved namespaces
- Not planned: Event Hubs / Relay / Notification Hubs (separate products)

## Architecture

- `src/main.rs` — [egui](https://github.com/emilk/egui)/eframe UI, immediate mode, single window
- `src/worker.rs` — background tokio thread owning the AMQP client ([azservicebus](https://crates.io/crates/azservicebus)); UI ↔ worker via channels
- `src/mgmt.rs` — entity CRUD over the Service Bus ATOM management REST API with SAS auth

Connection history is stored in `%APPDATA%\sbx.json` (plaintext — same caveat as the
original Service Bus Explorer).

## Keywords

Azure Service Bus explorer · Service Bus GUI · Service Bus client · queue browser ·
peek messages · dead-letter queue viewer · DLQ resubmit · topic subscription manager ·
Windows · macOS · Rust · Service Bus Explorer alternative

## License

[MIT](LICENSE) © 2026 Harshit Mahendra

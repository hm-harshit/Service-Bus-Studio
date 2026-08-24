//! Background worker: owns the AMQP client + management client, executes
//! commands from the UI thread on a tokio runtime.

use crate::mgmt::{Entity, MgmtClient};
use azservicebus::core::BasicRetryPolicy;
use azservicebus::{
    ServiceBusClient, ServiceBusClientOptions, ServiceBusMessage, ServiceBusReceiveMode,
    ServiceBusReceiver, ServiceBusReceiverOptions, ServiceBusSenderOptions, SubQueue,
};
use fe2o3_amqp_types::primitives::SimpleValue;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Target {
    Queue(String),
    Subscription { topic: String, name: String },
}

impl Target {
    pub fn key(&self) -> String {
        match self {
            Target::Queue(q) => q.clone(),
            Target::Subscription { topic, name } => format!("{topic}/{name}"),
        }
    }
    /// Where a resubmitted message gets sent.
    pub fn send_destination(&self) -> &str {
        match self {
            Target::Queue(q) => q,
            Target::Subscription { topic, .. } => topic,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct OutMessage {
    pub body: String,
    pub subject: String,
    pub content_type: String,
    pub message_id: String,
    pub correlation_id: String,
    pub session_id: String,
    pub ttl_secs: String,
    pub props: Vec<(String, String)>,
    pub count: u32,
}

#[derive(Clone, Debug, Default)]
pub struct MsgView {
    pub seq: i64,
    pub id: String,
    pub subject: String,
    pub content_type: String,
    pub session: String,
    pub correlation: String,
    pub enqueued: String,
    pub expires: String,
    pub delivery_count: u32,
    pub size: usize,
    pub body: String,
    pub props: Vec<(String, String)>,
    pub dl_reason: String,
    pub dl_description: String,
}

pub enum Cmd {
    Connect(String),
    Refresh,
    Peek { target: Target, dlq: bool, max: u32 },
    Send { destination: String, msg: OutMessage },
    Receive { target: Target, dlq: bool, max: u32 },
    Purge { target: Target, dlq: bool },
    Resubmit { target: Target, msgs: Vec<MsgView> },
    CreateQueue(String),
    CreateTopic(String),
    CreateSubscription { topic: String, name: String },
    DeleteEntity(String), // path: queue | topic | topic/Subscriptions/sub
    UpdateEntity { path: String, tag: String, fields: Vec<(String, String)> },
}

pub enum Evt {
    Connected(String),
    Entities { queues: Vec<Entity>, topics: Vec<Entity>, subs: HashMap<String, Vec<Entity>> },
    Messages { key: String, dlq: bool, msgs: Vec<MsgView> },
    Status(String),
    Error(String),
    Busy(bool),
}

fn fmt_simple_value(v: &SimpleValue) -> String {
    match v {
        SimpleValue::String(s) => s.to_string(),
        SimpleValue::Bool(b) => b.to_string(),
        SimpleValue::Int(i) => i.to_string(),
        SimpleValue::Long(i) => i.to_string(),
        SimpleValue::Double(d) => d.to_string(),
        SimpleValue::Float(f) => f.to_string(),
        other => format!("{other:?}"),
    }
}

macro_rules! msg_view {
    ($m:expr) => {{
        let body = match $m.body() {
            Ok(b) => String::from_utf8_lossy(b).into_owned(),
            Err(_) => "<non-data AMQP body>".to_string(),
        };
        MsgView {
            seq: $m.sequence_number(),
            id: $m.message_id().map(|c| c.into_owned()).unwrap_or_default(),
            subject: $m.subject().unwrap_or_default().to_string(),
            content_type: $m.content_type().unwrap_or_default().to_string(),
            session: $m.session_id().unwrap_or_default().to_string(),
            correlation: $m.correlation_id().map(|c| c.into_owned()).unwrap_or_default(),
            enqueued: $m.enqueued_time().to_string(),
            expires: $m.expires_at().to_string(),
            delivery_count: $m.delivery_count().unwrap_or(0),
            size: body.len(),
            body,
            props: $m
                .application_properties()
                .map(|ap| {
                    ap.0.iter()
                        .map(|(k, v)| (k.clone(), fmt_simple_value(v)))
                        .collect()
                })
                .unwrap_or_default(),
            dl_reason: $m.dead_letter_reason().unwrap_or_default().to_string(),
            dl_description: $m.dead_letter_error_description().unwrap_or_default().to_string(),
        }
    }};
}

struct Worker {
    mgmt: Option<MgmtClient>,
    amqp: Option<ServiceBusClient<BasicRetryPolicy>>,
    tx: Sender<Evt>,
    repaint: egui::Context,
}

impl Worker {
    fn emit(&self, evt: Evt) {
        let _ = self.tx.send(evt);
        self.repaint.request_repaint();
    }

    async fn receiver_for(
        &mut self,
        target: &Target,
        dlq: bool,
        mode: ServiceBusReceiveMode,
    ) -> anyhow::Result<ServiceBusReceiver> {
        let client = self.amqp.as_mut().ok_or_else(|| anyhow::anyhow!("not connected"))?;
        let options = ServiceBusReceiverOptions {
            receive_mode: mode,
            sub_queue: if dlq { SubQueue::DeadLetter } else { SubQueue::None },
            ..Default::default()
        };
        Ok(match target {
            Target::Queue(q) => client.create_receiver_for_queue(q.clone(), options).await?,
            Target::Subscription { topic, name } => {
                client.create_receiver_for_subscription(topic, name, options).await?
            }
        })
    }

    async fn handle(&mut self, cmd: Cmd) -> anyhow::Result<()> {
        match cmd {
            Cmd::Connect(cs) => {
                let mgmt = MgmtClient::new(&cs)?;
                let amqp =
                    ServiceBusClient::new_from_connection_string(cs, ServiceBusClientOptions::default())
                        .await?;
                let ns = mgmt.namespace.clone();
                self.mgmt = Some(mgmt);
                self.amqp = Some(amqp);
                self.emit(Evt::Connected(ns));
                self.refresh().await?;
            }
            Cmd::Refresh => self.refresh().await?,
            Cmd::Peek { target, dlq, max } => {
                let mut rx = self
                    .receiver_for(&target, dlq, ServiceBusReceiveMode::PeekLock)
                    .await?;
                let mut msgs: Vec<MsgView> = Vec::new();
                let mut from: Option<i64> = None;
                while (msgs.len() as u32) < max {
                    let batch = rx
                        .peek_messages((max - msgs.len() as u32).min(32), from)
                        .await?;
                    if batch.is_empty() {
                        break;
                    }
                    from = Some(batch.last().unwrap().sequence_number() + 1);
                    msgs.extend(batch.iter().map(|m| msg_view!(m)));
                }
                let _ = rx.dispose().await;
                self.emit(Evt::Status(format!("Peeked {} message(s)", msgs.len())));
                self.emit(Evt::Messages { key: target.key(), dlq, msgs });
            }
            Cmd::Send { destination, msg } => {
                let client = self.amqp.as_mut().ok_or_else(|| anyhow::anyhow!("not connected"))?;
                let mut sender = client
                    .create_sender(destination.clone(), ServiceBusSenderOptions::default())
                    .await?;
                let count = msg.count.max(1);
                for i in 0..count {
                    let mut m = ServiceBusMessage::new(msg.body.clone());
                    if !msg.subject.is_empty() {
                        m.set_subject(msg.subject.clone());
                    }
                    if !msg.content_type.is_empty() {
                        m.set_content_type(msg.content_type.clone());
                    }
                    if !msg.message_id.is_empty() {
                        let id = if count > 1 { format!("{}-{i}", msg.message_id) } else { msg.message_id.clone() };
                        m.set_message_id(id).map_err(|e| anyhow::anyhow!("bad message id: {e:?}"))?;
                    }
                    if !msg.correlation_id.is_empty() {
                        m.set_correlation_id(msg.correlation_id.clone());
                    }
                    if !msg.session_id.is_empty() {
                        m.set_session_id(Some(msg.session_id.clone()))
                            .map_err(|e| anyhow::anyhow!("bad session id: {e:?}"))?;
                    }
                    if let Ok(secs) = msg.ttl_secs.trim().parse::<u64>() {
                        if secs > 0 {
                            let _ = m.set_time_to_live(Duration::from_secs(secs));
                        }
                    }
                    if !msg.props.is_empty() {
                        let ap = m
                            .application_properties_mut()
                            .get_or_insert_with(Default::default);
                        for (k, v) in &msg.props {
                            if !k.is_empty() {
                                ap.0.insert(k.clone(), SimpleValue::String(v.clone()));
                            }
                        }
                    }
                    sender.send_message(m).await?;
                }
                let _ = sender.dispose().await;
                self.emit(Evt::Status(format!("Sent {count} message(s) to {destination}")));
            }
            Cmd::Receive { target, dlq, max } => {
                let mut rx = self
                    .receiver_for(&target, dlq, ServiceBusReceiveMode::ReceiveAndDelete)
                    .await?;
                let batch = rx
                    .receive_messages_with_max_wait_time(max, Duration::from_secs(5))
                    .await?;
                let msgs: Vec<MsgView> = batch.iter().map(|m| msg_view!(m)).collect();
                let _ = rx.dispose().await;
                self.emit(Evt::Status(format!(
                    "Received & deleted {} message(s) from {}{}",
                    msgs.len(),
                    target.key(),
                    if dlq { " (DLQ)" } else { "" }
                )));
                self.emit(Evt::Messages { key: target.key(), dlq, msgs });
            }
            Cmd::Purge { target, dlq } => {
                let mut rx = self
                    .receiver_for(&target, dlq, ServiceBusReceiveMode::ReceiveAndDelete)
                    .await?;
                let mut total = 0usize;
                loop {
                    let batch = rx
                        .receive_messages_with_max_wait_time(200, Duration::from_secs(2))
                        .await?;
                    if batch.is_empty() {
                        break;
                    }
                    total += batch.len();
                    self.emit(Evt::Status(format!("Purging… {total} deleted")));
                }
                let _ = rx.dispose().await;
                self.emit(Evt::Status(format!(
                    "Purged {total} message(s) from {}{}",
                    target.key(),
                    if dlq { " (DLQ)" } else { "" }
                )));
                self.emit(Evt::Messages { key: target.key(), dlq, msgs: Vec::new() });
            }
            Cmd::Resubmit { target, msgs } => {
                let client = self.amqp.as_mut().ok_or_else(|| anyhow::anyhow!("not connected"))?;
                let dest = target.send_destination().to_string();
                let mut sender = client
                    .create_sender(dest.clone(), ServiceBusSenderOptions::default())
                    .await?;
                let n = msgs.len();
                for v in msgs {
                    let mut m = ServiceBusMessage::new(v.body.into_bytes());
                    if !v.subject.is_empty() {
                        m.set_subject(v.subject);
                    }
                    if !v.content_type.is_empty() {
                        m.set_content_type(v.content_type);
                    }
                    if !v.correlation.is_empty() {
                        m.set_correlation_id(v.correlation);
                    }
                    if !v.session.is_empty() {
                        let _ = m.set_session_id(Some(v.session));
                    }
                    if !v.props.is_empty() {
                        let ap = m
                            .application_properties_mut()
                            .get_or_insert_with(Default::default);
                        for (k, val) in v.props {
                            ap.0.insert(k, SimpleValue::String(val));
                        }
                    }
                    sender.send_message(m).await?;
                }
                let _ = sender.dispose().await;
                self.emit(Evt::Status(format!("Resubmitted {n} message(s) to {dest}")));
            }
            Cmd::CreateQueue(name) => {
                self.mgmt()?.create(&name, "QueueDescription").await?;
                self.emit(Evt::Status(format!("Created queue {name}")));
                self.refresh().await?;
            }
            Cmd::CreateTopic(name) => {
                self.mgmt()?.create(&name, "TopicDescription").await?;
                self.emit(Evt::Status(format!("Created topic {name}")));
                self.refresh().await?;
            }
            Cmd::CreateSubscription { topic, name } => {
                self.mgmt()?
                    .create(&format!("{topic}/Subscriptions/{name}"), "SubscriptionDescription")
                    .await?;
                self.emit(Evt::Status(format!("Created subscription {topic}/{name}")));
                self.refresh().await?;
            }
            Cmd::DeleteEntity(path) => {
                self.mgmt()?.delete(&path).await?;
                self.emit(Evt::Status(format!("Deleted {path}")));
                self.refresh().await?;
            }
            Cmd::UpdateEntity { path, tag, fields } => {
                self.mgmt()?.update_entity(&path, &tag, &fields).await?;
                self.emit(Evt::Status(format!("Updated {path}")));
                self.refresh().await?;
            }
        }
        Ok(())
    }

    fn mgmt(&self) -> anyhow::Result<&MgmtClient> {
        self.mgmt.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))
    }

    async fn refresh(&mut self) -> anyhow::Result<()> {
        let mgmt = self.mgmt()?.clone();
        // Entity-scoped connection string: can't list the namespace, show just that entity.
        if let Some(path) = &mgmt.entity_path {
            let ent = match mgmt.get_entity(path).await {
                Ok(e) => e,
                // e.g. send-only key: no management rights at all — still show the entity
                Err(_) => crate::mgmt::Entity { name: path.clone(), ..Default::default() },
            };
            self.emit(Evt::Status(format!(
                "Connection string is scoped to '{path}' — showing only that entity. Use a namespace-level Manage key to browse everything."
            )));
            self.emit(Evt::Entities { queues: vec![ent], topics: Vec::new(), subs: HashMap::new() });
            return Ok(());
        }
        let queues = mgmt.list_queues().await?;
        let topics = mgmt.list_topics().await?;
        let mut subs = HashMap::new();
        for t in &topics {
            subs.insert(t.name.clone(), mgmt.list_subscriptions(&t.name).await?);
        }
        self.emit(Evt::Status(format!(
            "Loaded {} queue(s), {} topic(s)",
            queues.len(),
            topics.len()
        )));
        self.emit(Evt::Entities { queues, topics, subs });
        Ok(())
    }
}

pub fn spawn(repaint: egui::Context) -> (Sender<Cmd>, Receiver<Evt>) {
    let (cmd_tx, cmd_rx) = std::sync::mpsc::channel::<Cmd>();
    let (evt_tx, evt_rx) = std::sync::mpsc::channel::<Evt>();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let mut worker = Worker { mgmt: None, amqp: None, tx: evt_tx, repaint };
        while let Ok(cmd) = cmd_rx.recv() {
            worker.emit(Evt::Busy(true));
            if let Err(e) = rt.block_on(worker.handle(cmd)) {
                let msg = format!("{e:#}");
                let friendly = if msg.contains("'Listen' claim") {
                    "This access key only has Send rights — peeking/receiving needs the 'Listen' claim. Use a key with Listen or Manage rights (e.g. RootManageSharedAccessKey).".to_string()
                } else if msg.contains("'Send' claim") {
                    "This access key cannot send — it lacks the 'Send' claim.".to_string()
                } else {
                    msg
                };
                worker.emit(Evt::Error(friendly));
            }
            worker.emit(Evt::Busy(false));
        }
    });
    (cmd_tx, evt_rx)
}

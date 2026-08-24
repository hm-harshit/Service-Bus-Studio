//! Service Bus entity management via the ATOM REST API (list/create/delete).

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

const API_VERSION: &str = "2021-05";

#[derive(Clone)]
pub struct MgmtClient {
    pub namespace: String, // e.g. myns.servicebus.windows.net
    /// Set when the connection string is scoped to a single entity (EntityPath=...).
    pub entity_path: Option<String>,
    key_name: String,
    key: String,
    http: reqwest::Client,
}

#[derive(Clone, Debug, Default)]
pub struct Entity {
    pub name: String,
    pub active: i64,
    pub dead_letter: i64,
    pub scheduled: i64,
    pub total: i64,
    pub size_bytes: i64,
    /// All description fields flattened to (name, value) for the overview panel.
    pub props: Vec<(String, String)>,
}

pub fn parse_connection_string(cs: &str) -> Result<(String, String, String, Option<String>)> {
    let mut endpoint = None;
    let mut key_name = None;
    let mut key = None;
    let mut entity_path = None;
    for part in cs.split(';') {
        let part = part.trim();
        if let Some((k, _)) = part.split_once('=') {
            // value may itself contain '=' padding; take everything after the first '='
            let v = part[part.find('=').unwrap() + 1..].trim();
            match k.trim() {
                "Endpoint" => endpoint = Some(v.to_string()),
                "SharedAccessKeyName" => key_name = Some(v.to_string()),
                "SharedAccessKey" => key = Some(v.to_string()),
                "EntityPath" => entity_path = Some(v.to_string()),
                _ => {}
            }
        }
    }
    let endpoint = endpoint.ok_or_else(|| anyhow!("connection string missing Endpoint"))?;
    let ns = endpoint
        .trim_start_matches("sb://")
        .trim_end_matches('/')
        .to_string();
    Ok((
        ns,
        key_name.ok_or_else(|| anyhow!("missing SharedAccessKeyName"))?,
        key.ok_or_else(|| anyhow!("missing SharedAccessKey"))?,
        entity_path,
    ))
}

impl MgmtClient {
    pub fn new(conn_str: &str) -> Result<Self> {
        let (namespace, key_name, key, entity_path) = parse_connection_string(conn_str)?;
        Ok(Self {
            namespace,
            entity_path,
            key_name,
            key,
            http: reqwest::Client::new(),
        })
    }

    fn sas_token(&self, uri: &str) -> String {
        let sr = urlencoding::encode(&uri.to_lowercase()).into_owned();
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let to_sign = format!("{sr}\n{expiry}");
        let mut mac = Hmac::<Sha256>::new_from_slice(self.key.as_bytes()).unwrap();
        mac.update(to_sign.as_bytes());
        let sig = B64.encode(mac.finalize().into_bytes());
        format!(
            "SharedAccessSignature sr={sr}&sig={}&se={expiry}&skn={}",
            urlencoding::encode(&sig),
            self.key_name
        )
    }

    async fn request(
        &self,
        method: reqwest::Method,
        path_and_query: &str,
        body: Option<String>,
    ) -> Result<String> {
        self.request_full(method, path_and_query, body, false).await
    }

    async fn request_full(
        &self,
        method: reqwest::Method,
        path_and_query: &str,
        body: Option<String>,
        if_match: bool, // true = update an existing entity instead of creating
    ) -> Result<String> {
        let url = format!("https://{}/{}", self.namespace, path_and_query);
        // sign the exact resource being accessed (without the query string), so
        // entity-scoped keys work for requests on their own entity
        let audience = url.split('?').next().unwrap();
        let token = self.sas_token(audience);
        let mut req = self.http.request(method, &url).header("Authorization", token);
        if if_match {
            req = req.header("If-Match", "*");
        }
        if let Some(b) = body {
            req = req
                .header("Content-Type", "application/atom+xml;type=entry;charset=utf-8")
                .body(b);
        }
        let resp = req.send().await.context("management request failed")?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            let hint = if status.as_u16() == 401 {
                "\nHint: this key may be entity-scoped or lack Manage rights. To browse the namespace use a Manage-level connection string (e.g. RootManageSharedAccessKey) WITHOUT an EntityPath."
            } else {
                ""
            };
            bail!("HTTP {status}: {}{hint}", text.chars().take(400).collect::<String>());
        }
        Ok(text)
    }

    async fn list(&self, resource: &str) -> Result<Vec<Entity>> {
        let mut out = Vec::new();
        loop {
            let page = self
                .request(
                    reqwest::Method::GET,
                    &format!(
                        "{resource}?api-version={API_VERSION}&$top=100&$skip={}",
                        out.len()
                    ),
                    None,
                )
                .await?;
            let n = parse_feed(&page, &mut out)?;
            // ponytail: hard cap at 2000 entities, add real paging UI if someone hits it
            if n < 100 || out.len() >= 2000 {
                return Ok(out);
            }
        }
    }

    pub async fn get_entity(&self, path: &str) -> Result<Entity> {
        let xml = self
            .request(
                reqwest::Method::GET,
                &format!("{path}?api-version={API_VERSION}"),
                None,
            )
            .await?;
        let mut v = Vec::new();
        parse_feed(&xml, &mut v)?;
        let mut ent = v.into_iter().next().ok_or_else(|| anyhow!("entity not found: {path}"))?;
        if ent.name.is_empty() {
            ent.name = path.to_string();
        }
        Ok(ent)
    }

    pub async fn list_queues(&self) -> Result<Vec<Entity>> {
        self.list("$Resources/Queues").await
    }
    pub async fn list_topics(&self) -> Result<Vec<Entity>> {
        self.list("$Resources/Topics").await
    }
    pub async fn list_subscriptions(&self, topic: &str) -> Result<Vec<Entity>> {
        self.list(&format!("{topic}/Subscriptions")).await
    }

    pub async fn create(&self, path: &str, description_tag: &str) -> Result<()> {
        let body = format!(
            r#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><{description_tag} xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"/></content></entry>"#
        );
        self.request(
            reqwest::Method::PUT,
            &format!("{path}?api-version={API_VERSION}"),
            Some(body),
        )
        .await?;
        Ok(())
    }

    /// Update an entity: PUT the full description with If-Match. `fields` must
    /// already be in schema order; empty values are skipped.
    pub async fn update_entity(
        &self,
        path: &str,
        description_tag: &str,
        fields: &[(String, String)],
    ) -> Result<()> {
        let mut inner = String::new();
        for (k, v) in fields {
            if !v.is_empty() {
                inner.push_str(&format!("<{k}>{}</{k}>", xml_escape(v)));
            }
        }
        let body = format!(
            r#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><{description_tag} xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect">{inner}</{description_tag}></content></entry>"#
        );
        self.request_full(
            reqwest::Method::PUT,
            &format!("{path}?api-version={API_VERSION}"),
            Some(body),
            true,
        )
        .await?;
        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        self.request(
            reqwest::Method::DELETE,
            &format!("{path}?api-version={API_VERSION}"),
            None,
        )
        .await?;
        Ok(())
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// Editable fields per entity kind, in the exact element order the ATOM schema requires.
pub fn editable_fields(description_tag: &str) -> &'static [&'static str] {
    match description_tag {
        "QueueDescription" => &[
            "LockDuration", "MaxSizeInMegabytes", "RequiresDuplicateDetection", "RequiresSession",
            "DefaultMessageTimeToLive", "DeadLetteringOnMessageExpiration",
            "DuplicateDetectionHistoryTimeWindow", "MaxDeliveryCount", "EnableBatchedOperations",
            "Status", "ForwardTo", "UserMetadata", "AutoDeleteOnIdle", "EnablePartitioning",
            "ForwardDeadLetteredMessagesTo", "EnableExpress",
        ],
        "TopicDescription" => &[
            "DefaultMessageTimeToLive", "MaxSizeInMegabytes", "RequiresDuplicateDetection",
            "DuplicateDetectionHistoryTimeWindow", "EnableBatchedOperations", "Status",
            "SupportOrdering", "AutoDeleteOnIdle", "EnablePartitioning", "EnableExpress",
        ],
        _ => &[
            "LockDuration", "RequiresSession", "DefaultMessageTimeToLive",
            "DeadLetteringOnMessageExpiration", "DeadLetteringOnFilterEvaluationExceptions",
            "MaxDeliveryCount", "EnableBatchedOperations", "Status", "ForwardTo",
            "AutoDeleteOnIdle", "ForwardDeadLetteredMessagesTo",
        ],
    }
}

/// Parse an ATOM feed of entities, append to `out`, return number of entries in this page.
fn parse_feed(xml: &str, out: &mut Vec<Entity>) -> Result<usize> {
    let doc = roxmltree::Document::parse(xml).context("bad ATOM XML")?;
    let mut n = 0;
    for entry in doc
        .descendants()
        .filter(|e| e.has_tag_name(("http://www.w3.org/2005/Atom", "entry")))
    {
        n += 1;
        let mut ent = Entity::default();
        if let Some(t) = entry
            .children()
            .find(|c| c.has_tag_name(("http://www.w3.org/2005/Atom", "title")))
        {
            ent.name = t.text().unwrap_or_default().to_string();
        }
        // flatten every leaf element of the *Description into props
        for node in entry.descendants().filter(|d| {
            d.is_element() && !d.children().any(|c| c.is_element()) && d.text().is_some()
        }) {
            let tag = node.tag_name().name();
            if matches!(tag, "id" | "title" | "updated" | "name") {
                continue;
            }
            let val = node.text().unwrap_or_default().to_string();
            match tag {
                "ActiveMessageCount" => ent.active = val.parse().unwrap_or(0),
                "DeadLetterMessageCount" => ent.dead_letter = val.parse().unwrap_or(0),
                "ScheduledMessageCount" => ent.scheduled = val.parse().unwrap_or(0),
                "MessageCount" => ent.total = val.parse().unwrap_or(0),
                "SizeInBytes" => ent.size_bytes = val.parse().unwrap_or(0),
                _ => {}
            }
            ent.props.push((tag.to_string(), val));
        }
        out.push(ent);
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_connection_string() {
        let (ns, kn, k, ep) = parse_connection_string(
            "Endpoint=sb://foo.servicebus.windows.net/;SharedAccessKeyName=Root;SharedAccessKey=abc+d=;EntityPath=my-queue",
        )
        .unwrap();
        assert_eq!(ns, "foo.servicebus.windows.net");
        assert_eq!(kn, "Root");
        assert_eq!(k, "abc+d=");
        assert_eq!(ep.as_deref(), Some("my-queue"));
    }

    #[test]
    fn parses_feed() {
        let xml = r#"<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>orders</title><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:d2p1="http://schemas.microsoft.com/netservices/2011/06/servicebus"><MaxDeliveryCount>10</MaxDeliveryCount><CountDetails><d2p1:ActiveMessageCount>5</d2p1:ActiveMessageCount><d2p1:DeadLetterMessageCount>2</d2p1:DeadLetterMessageCount></CountDetails></QueueDescription></content></entry></feed>"#;
        let mut v = Vec::new();
        assert_eq!(parse_feed(xml, &mut v).unwrap(), 1);
        assert_eq!(v[0].name, "orders");
        assert_eq!(v[0].active, 5);
        assert_eq!(v[0].dead_letter, 2);
        assert!(v[0].props.iter().any(|(k, val)| k == "MaxDeliveryCount" && val == "10"));
    }
}

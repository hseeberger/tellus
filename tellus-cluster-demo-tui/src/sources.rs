use crate::{config::Config, sse::SseParser};
use crossterm::event::KeyEvent;
use futures_util::StreamExt;
use reqwest::{Client, header::ACCEPT};
use serde::de::DeserializeOwned;
use std::{sync::Arc, time::Duration};
use tellus_cluster_demo::{ClusterView, NodeEvent, ProbeReport, VerifierEvent, VerifierStatus};
use tellus_cluster_inspect::{InspectEvent, events::Stamped};
use tokio::{sync::mpsc, time::sleep};

const HTTP_TIMEOUT: Duration = Duration::from_secs(3);
const CLUSTER_INTERVAL: Duration = Duration::from_secs(2);
const PROBE_INTERVAL: Duration = Duration::from_secs(10);
const STATUS_INTERVAL: Duration = Duration::from_secs(2);
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const LAST_EVENT_ID: &str = "last-event-id";

#[derive(Debug)]
pub enum Input {
    Key(KeyEvent),
    Tick,
    Node(usize, NodeInput),
    Verifier(VerifierInput),
}

#[derive(Debug)]
pub enum NodeInput {
    Stream(StreamInput<NodeEvent>),
    Inspect(StreamInput<InspectEvent>),
    Cluster(ClusterView),
    Probe(ProbeReport),
}

#[derive(Debug)]
pub enum VerifierInput {
    Stream(StreamInput<VerifierEvent>),
    Status(VerifierStatus),
}

#[derive(Debug)]
pub enum StreamInput<E> {
    Connected,
    Disconnected(String),
    Event(Stamped<E>),
}

pub fn spawn(config: &Config, tx: mpsc::Sender<Input>) {
    let client = Arc::new(client());
    for (index, url) in config.nodes.iter().enumerate() {
        tokio::spawn(stream(
            client.clone(),
            format!("{url}/events"),
            tx.clone(),
            move |input| Input::Node(index, NodeInput::Stream(input)),
        ));
        tokio::spawn(stream(
            client.clone(),
            format!("{url}/inspect/events"),
            tx.clone(),
            move |input| Input::Node(index, NodeInput::Inspect(input)),
        ));
        tokio::spawn(poll(
            client.clone(),
            format!("{url}/cluster"),
            CLUSTER_INTERVAL,
            tx.clone(),
            move |view| Input::Node(index, NodeInput::Cluster(view)),
        ));
        tokio::spawn(poll(
            client.clone(),
            format!("{url}/probe"),
            PROBE_INTERVAL,
            tx.clone(),
            move |report| Input::Node(index, NodeInput::Probe(report)),
        ));
    }
    tokio::spawn(stream(
        client.clone(),
        format!("{}/events", config.verifier),
        tx.clone(),
        |input| Input::Verifier(VerifierInput::Stream(input)),
    ));
    tokio::spawn(poll(
        client,
        format!("{}/status", config.verifier),
        STATUS_INTERVAL,
        tx,
        |status| Input::Verifier(VerifierInput::Status(status)),
    ));
}

async fn stream<E, W>(client: Arc<Client>, url: String, tx: mpsc::Sender<Input>, wrap: W)
where
    E: DeserializeOwned,
    W: Fn(StreamInput<E>) -> Input,
{
    let mut last_id = None::<String>;
    let mut backoff = MIN_BACKOFF;

    loop {
        let mut request = client.get(&url).header(ACCEPT, "text/event-stream");
        if let Some(id) = &last_id {
            request = request.header(LAST_EVENT_ID, id);
        }

        let reason = match request.send().await {
            Ok(response) if response.status().is_success() => {
                if tx.send(wrap(StreamInput::Connected)).await.is_err() {
                    return;
                }
                let mut parser = SseParser::default();
                let mut bytes = response.bytes_stream();
                'read: loop {
                    match bytes.next().await {
                        Some(Ok(chunk)) => {
                            for message in parser.push(&chunk) {
                                backoff = MIN_BACKOFF;
                                match serde_json::from_str(&message.data) {
                                    Ok(stamped) => {
                                        let input = wrap(StreamInput::Event(stamped));
                                        if tx.send(input).await.is_err() {
                                            return;
                                        }
                                        // Acknowledged only once delivered, else a reconnect skips
                                        // it.
                                        if let Some(id) = message.id {
                                            last_id = Some(id);
                                        }
                                    }

                                    Err(error) => break 'read format!("unreadable event: {error}"),
                                }
                            }
                        }

                        Some(Err(error)) => break error.to_string(),

                        None => break "stream ended".to_string(),
                    }
                }
            }

            Ok(response) => format!("HTTP {}", response.status()),

            Err(error) => error.to_string(),
        };

        if tx
            .send(wrap(StreamInput::Disconnected(reason)))
            .await
            .is_err()
        {
            return;
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn poll<T, W>(
    client: Arc<Client>,
    url: String,
    interval: Duration,
    tx: mpsc::Sender<Input>,
    wrap: W,
) where
    T: DeserializeOwned,
    W: Fn(T) -> Input,
{
    loop {
        if let Ok(response) = client.get(&url).send().await
            && let Ok(value) = response.json::<T>().await
            && tx.send(wrap(value)).await.is_err()
        {
            return;
        }
        sleep(interval).await;
    }
}

fn client() -> Client {
    Client::builder()
        .connect_timeout(HTTP_TIMEOUT)
        .build()
        .expect("HTTP client")
}

#![cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
use anyhow::Context;
use mq_bridge::endpoints::ibm_mq::{IbmMqConsumer, IbmMqPublisher};
use mq_bridge::models::IbmMqConfig;
use mq_bridge::traits::{MessageConsumer, MessageDisposition, MessagePublisher};
use mqi::attribute::MQIA_CURRENT_Q_DEPTH;
use mqi::connection::{Credentials, MqServer, ThreadNone, Tls};
use mqi::constants;
use mqi::get::GetWait;
use mqi::result::ResultCompErrExt;
use mqi::types::{ApplName, CipherSpec, KeyRepo, MessageFormat, QueueManagerName, QueueName};
use mqi::{mqstr, MqStr, Object, Syncpoint};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut cfg = IbmMqConfig::new("localhost(1414)", "QM1", "DEV.APP.SVRCONN");
    cfg.username = Some("app".to_string());
    cfg.password = Some("adminpass".to_string());
    cfg.queue = Some("DEV.QUEUE.1".to_string());
    cfg.tls.required = true;
    cfg.tls.cert_file = Some("tests/integration/docker-compose/ibm-mq-certs/client".to_string());
    cfg.cipher_spec = Some("ANY_TLS12".to_string());

    let publisher = IbmMqPublisher::new(&cfg).await?;
    publisher.send("hello-wrapper".into()).await?;
    publisher.send("hello-raw".into()).await?;

    let mut consumer = IbmMqConsumer::new(&cfg).await?;
    let received = consumer.receive().await?;
    println!(
        "wrapper payload len={} bytes={:?}",
        received.message.payload.len(),
        received.message.payload
    );
    (received.commit)(MessageDisposition::Ack).await?;

    let qm_name =
        QueueManagerName(MqStr::<48>::try_from(cfg.queue_manager.as_str()).context("qm name")?);
    let mq_server_string = format!("{}/TCP/{}", cfg.channel, cfg.url);
    let mq_server = MqServer::try_from(mq_server_string.as_str()).context("mqserver")?;
    let credentials = Credentials::User(
        cfg.username.as_deref().unwrap_or(""),
        cfg.password.as_deref().unwrap_or("").into(),
    );
    let key_repo = KeyRepo(MqStr::<256>::try_from(
        cfg.tls.cert_file.as_deref().context("missing key repo")?,
    )?);
    let cipher = CipherSpec(MqStr::<32>::try_from(
        cfg.cipher_spec.as_deref().context("missing cipher")?,
    )?);
    let tls = Tls::new(&key_repo, None, &cipher);
    let qm = mqi::connect::<ThreadNone>(&(
        ApplName(mqstr!("debug_ibm_mq_tls")),
        Some(tls),
        QueueManagerName(qm_name.0),
        credentials,
        None::<CipherSpec>,
        mq_server,
    ))
    .discard_warning()
    .context("connect raw mq")?;

    let queue_name = QueueName(MqStr::<48>::try_from(
        cfg.queue.as_deref().context("missing queue")?,
    )?);
    let queue = Object::open(
        qm.connection_ref(),
        &(
            queue_name,
            constants::MQOO_INPUT_AS_Q_DEF | constants::MQOO_FAIL_IF_QUIESCING,
        ),
    )
    .discard_warning()
    .context("open raw queue")?;

    if let Ok(values) = queue.inquire(&[MQIA_CURRENT_Q_DEPTH]) {
        println!("queue depth before raw get: {:?}", values);
    }

    let gmo = (
        constants::MQGMO_WAIT | constants::MQGMO_SYNCPOINT | constants::MQGMO_FAIL_IF_QUIESCING,
        GetWait::Wait(5_000),
    );
    let mut buffer = vec![0u8; 1024];
    let message: Option<(_, MessageFormat)> = queue
        .get_data_with(&gmo, &mut buffer)
        .discard_warning()
        .context("raw get")?;
    if let Some((data, _format)) = message {
        println!("raw payload len={} bytes={:?}", data.len(), data);
    } else {
        println!("raw payload missing");
    }
    Syncpoint::new(&qm)
        .commit()
        .discard_warning()
        .context("raw commit")?;
    Ok(())
}

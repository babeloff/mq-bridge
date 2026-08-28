use anyhow::{bail, Result};
use clap::Parser;
use sea_streamer_socket::{SeaConsumerOptions, SeaStreamer};
use sea_streamer_types::{
    Consumer, ConsumerMode, ConsumerOptions, Message, Producer, StreamUrl, Streamer,
};
use std::str::FromStr;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, help = "Streamer source URI")]
    input: StreamUrl,
    #[clap(long, help = "Streamer sink URI")]
    output: StreamUrl,
    #[clap(long, default_value = "end", help = "Stream from `start` or `end`")]
    offset: Offset,
}

#[derive(Debug, Clone)]
enum Offset {
    Start,
    End,
}

impl FromStr for Offset {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "start" => Ok(Self::Start),
            "end" => Ok(Self::End),
            _ => Err("unknown offset; use start or end"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        input,
        output,
        offset,
    } = Args::parse();

    if input == output && input.streamer().protocol() != Some("stdio") {
        bail!("input == output");
    }

    let source = SeaStreamer::connect(input.streamer(), Default::default()).await?;
    let mut options = SeaConsumerOptions::new(ConsumerMode::RealTime);
    options.set_kafka_consumer_options(|options| {
        options.set_auto_offset_reset(match offset {
            Offset::Start => sea_streamer_kafka::AutoOffsetReset::Earliest,
            Offset::End => sea_streamer_kafka::AutoOffsetReset::Latest,
        });
    });
    let consumer = source.create_consumer(input.stream_keys(), options).await?;

    let sink = SeaStreamer::connect(output.streamer(), Default::default()).await?;
    let producer = sink
        .create_producer(output.stream_key()?, Default::default())
        .await?;

    loop {
        let message = consumer.next().await?;
        producer.send(message.message())?;
    }
}

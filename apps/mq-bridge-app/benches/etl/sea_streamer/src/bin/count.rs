use anyhow::Result;
use clap::Parser;
use sea_streamer_file::{is_end_of_stream, FileErr, FileId, MessageSource, StreamMode};

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, help = "Sea Streamer .ss file to count")]
    file: FileId,
}

#[tokio::main]
async fn main() -> Result<()> {
    let Args { file } = Args::parse();
    let mut source = MessageSource::new(file, StreamMode::Replay).await?;
    let mut count = 0_u64;

    loop {
        let message = match source.next().await {
            Ok(message) => message,
            Err(FileErr::NotEnoughBytes) => break,
            Err(error) => return Err(error.into()),
        };
        if is_end_of_stream(&message.message) {
            break;
        }
        count += 1;
    }

    println!("{count}");
    Ok(())
}

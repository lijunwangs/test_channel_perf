
use {
    async_channel::{bounded as async_bounded, Receiver as AsyncReceiver, Sender as AsyncSender},
    crossbeam::channel,
    std::{
        sync::{atomic::{AtomicU64, Ordering}, Arc,},
        thread,
        time::{Duration, Instant},
    },
};

const NUM_MESSAGES: usize = 10_000_000;
const MSG_SIZE: usize = 1024;
static MSG: [u8; MSG_SIZE] = [42u8; MSG_SIZE];
const CHANNEL_SIZE: usize = 250_000;
const TIME_OUT_MS: u64 = 5;
fn run_crossbeam() {
    let (tx, rx) = channel::bounded::<[u8; MSG_SIZE]>(CHANNEL_SIZE);
    let start = Instant::now();

    let sender = thread::spawn(move || {
        for _ in 0..NUM_MESSAGES {
            let _ = tx.try_send(MSG); // drop if full
        }
    });

    let count = Arc::new(AtomicU64::new(0));
    let receiver = thread::spawn({
        let count = count.clone();
        move || loop {
            match rx.recv_timeout(Duration::from_millis(TIME_OUT_MS)) {
                Ok(_) => {
                    count.fetch_add(1, Ordering::Relaxed);
                }
                Err(channel::RecvTimeoutError::Timeout) => {
                    // No messages for 100ms, assume we're done
                    break;
                }
                Err(channel::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    });

    sender.join().unwrap();
    receiver.join().unwrap();

    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "crossbeam_channel: {:.2} messages/sec",
        count.load(Ordering::Relaxed) as f64 / elapsed
    );
}


async fn run_async_channel() {
    use tokio::time::{timeout, Duration};

    let (tx, rx): (AsyncSender<[u8; MSG_SIZE]>, AsyncReceiver<[u8; MSG_SIZE]>) = async_bounded(250_000);
    let start = Instant::now();

    let sender = tokio::spawn(async move {
        let mut sent = 0;
        for _ in 0..NUM_MESSAGES {
            if tx.try_send(MSG).is_ok() {
                sent += 1;
            }
        }
        sent
    });

    let receiver = tokio::spawn(async move {
        let mut received = 0;
        loop {
            match timeout(Duration::from_millis(TIME_OUT_MS), rx.recv()).await {
                Ok(Ok(_)) => {
                    received += 1;
                    if received >= NUM_MESSAGES {
                        break;
                    }
                }
                Ok(Err(_)) | Err(_) => break, // timeout or channel closed
            }
        }
        received
    });

    let sent = sender.await.unwrap();
    let received = receiver.await.unwrap();

    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "async_channel: {:.2} messages/sec (sent {}, received {})",
        received as f64 / elapsed,
        sent,
        received
    );
}


#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("Running crossbeam_channel benchmark...");
    run_crossbeam();

    println!("Running async_channel benchmark...");
    run_async_channel().await;
}

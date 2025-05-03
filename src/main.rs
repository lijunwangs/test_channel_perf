use {
    async_channel::{bounded as async_bounded, Receiver as AsyncReceiver, Sender as AsyncSender},
    crossbeam::channel,
    std::{
        sync::{atomic::AtomicU64, Arc},
        thread,
        time::Instant,
    },
};

const NUM_MESSAGES: usize = 10_000_000;
const MSG_SIZE: usize = 1024;
static MSG: [u8; MSG_SIZE] = [42u8; MSG_SIZE];
const CHANNEL_SIZE: usize = 250_000;

fn run_crossbeam() {
    let (tx, rx) = channel::bounded::<[u8; MSG_SIZE]>(CHANNEL_SIZE);
    let start = Instant::now();

    let sender = thread::spawn(move || {
        for _ in 0..NUM_MESSAGES {
            let _ = tx.try_send(MSG);
        }
    });

    let count = Arc::new(AtomicU64::new(0));
    let receiver = thread::spawn({
        let count = count.clone();
        move || loop {
            let reslut = rx.recv();
            if reslut.is_err() {
                break;
            } else {
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    });

    sender.join().unwrap();
    receiver.join().unwrap();

    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "crossbeam_channel: {:.2} messages/sec",
        count.load(std::sync::atomic::Ordering::Relaxed) as f64 / elapsed
    );
}

async fn run_async_channel() {
    let (tx, rx): (AsyncSender<[u8; MSG_SIZE]>, AsyncReceiver<[u8; MSG_SIZE]>) =
        async_bounded(CHANNEL_SIZE);
    let start = Instant::now();

    let sender = tokio::spawn(async move {
        for _ in 0..NUM_MESSAGES {
            tx.send(MSG).await.unwrap();
        }
    });

    let receiver = tokio::spawn(async move {
        for _ in 0..NUM_MESSAGES {
            let _ = rx.recv().await.unwrap();
        }
    });

    sender.await.unwrap();
    receiver.await.unwrap();

    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "async_channel: {:.2} messages/sec",
        NUM_MESSAGES as f64 / elapsed
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("Running crossbeam_channel benchmark...");
    run_crossbeam();

    println!("Running async_channel benchmark...");
    run_async_channel().await;
}

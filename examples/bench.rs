#[path = "../support.rs"]
mod support;
use support::block_on;
use ed::{Event, EventDispatcher};
use std::time::Instant;

struct Ev { n: u64 }
impl Event for Ev {}

fn bench(n_listeners: usize, iters: u64) {
    let d = EventDispatcher::new();
    for i in 0..n_listeners {
        let name = format!("l{i}");
        d.add_sync_named_listener::<Ev, _>("evt", 0, |e: &mut Ev, _| { e.n += 1; Ok(()) });
    }
    let mut e = Ev { n: 0 };
    block_on(d.dispatch_named::<Ev>("evt", &mut e)).unwrap();
    assert_eq!(e.n, n_listeners as u64);
    let start = Instant::now();
    for _ in 0..iters {
        block_on(d.dispatch_named::<Ev>("evt", &mut e)).unwrap();
    }
    let ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("ed listeners={n_listeners}: {ns:.1}ns/dispatch ({:.1}ns/listener)", ns / n_listeners.max(1) as f64);
}

fn main() {
    bench(1, 500000);
    bench(8, 100000);
    bench(64, 20000);
}

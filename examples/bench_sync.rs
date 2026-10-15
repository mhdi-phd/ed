#[path = "../support.rs"]
mod support;
use ed::{Event, EventDispatcher};
use std::time::Instant;

struct Ev { n: u64 }
impl Event for Ev {}

fn main() {
    for n_listeners in [1usize, 8, 64] {
        let d = EventDispatcher::new();
        for i in 0..n_listeners {
            let name = format!("l{i}");
            d.add_sync_named_listener::<Ev, _>("evt", 0, |e: &mut Ev, _| { e.n += 1; Ok(()) });
        }
        let mut e = Ev { n: 0 };
        d.dispatch_sync_named::<Ev>("evt", &mut e).unwrap();
        assert_eq!(e.n, n_listeners as u64);
        let iters = 500_000u64;
        let start = Instant::now();
        for _ in 0..iters {
            d.dispatch_sync_named::<Ev>("evt", &mut e).unwrap();
        }
        let ns = start.elapsed().as_nanos() as f64 / iters as f64;
        println!("ed sync listeners={n_listeners}: {ns:.1}ns/dispatch");
    }
}

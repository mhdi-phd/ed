#[path = "../support.rs"]
mod support;
use support::block_on;

use ed::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct Ev {
    n: i64,
}
impl Event for Ev {}

#[test]
fn attack_direct_recursion_bomb() {
    static COUNT: AtomicUsize = AtomicUsize::new(0);
    COUNT.store(0, Ordering::SeqCst);
    let d = EventDispatcher::new().with_max_depth(5);
    let d2 = d.clone();
    let d3 = d.clone();
    d.add_sync_listener::<Ev, _>(0, move |e: &mut Ev, _| {
        COUNT.fetch_add(1, Ordering::SeqCst);
        let _ = block_on(d2.dispatch(e));
        let _ = block_on(d3.dispatch_named("other", e));
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    let c = COUNT.load(Ordering::SeqCst);
    assert!(c <= 6, "recursion not bounded: {c}");
    let mut e2 = Ev::default();
    block_on(d.dispatch(&mut e2)).unwrap();
}

#[test]
fn attack_nested_ctx_recursion_bomb() {
    static COUNT: AtomicUsize = AtomicUsize::new(0);
    COUNT.store(0, Ordering::SeqCst);
    let d = EventDispatcher::new().with_max_depth(4);
    d.add_sync_listener::<Ev, _>(0, |e: &mut Ev, ctx| {
        COUNT.fetch_add(1, Ordering::SeqCst);
        let _ = block_on(ctx.dispatch(e));
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert!(COUNT.load(Ordering::SeqCst) <= 4);
}

#[test]
fn attack_mutate_during_dispatch() {
    let d = EventDispatcher::new();
    d.add_sync_listener::<Ev, _>(10, |e: &mut Ev, ctx| {
        e.n += 1;
        let d2 = EventDispatcher::new();
        let _ = &d2;
        let _ = ctx;
        Ok(())
    });
    for i in 0..100 {
        let name = format!("dyn{i}");
        let d2 = d.clone();
        d.add_sync_listener::<Ev, _>(0, move |_, _| {
            let n2 = format!("late{i}");
            d2.add_sync_listener::<Ev, _>(0, |_, _| Ok(()));
            let _ = &name;
            Ok(())
        });
    }
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert!(d.listener_count::<Ev>() >= 100);
}

#[test]
fn attack_concurrent_dispatch_stress() {
    let d = Arc::new(EventDispatcher::new());
    for i in 0..32 {
        d.add_sync_listener::<Ev, _>((i % 5) as i32, |e: &mut Ev, _| {
            e.n += 1;
            Ok(())
        });
    }
    let mut handles = vec![];
    for _ in 0..8 {
        let d2 = d.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..2000 {
                let mut e = Ev::default();
                block_on(d2.dispatch(&mut e)).unwrap();
                assert_eq!(e.n, 32);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn attack_priority_extremes() {
    let d = EventDispatcher::new();
    d.add_sync_listener::<Ev, _>(i32::MIN, |e: &mut Ev, _| {
        e.n = e.n * 10 + 1;
        Ok(())
    });
    d.add_sync_listener::<Ev, _>(i32::MAX, |e: &mut Ev, _| {
        e.n = e.n * 10 + 2;
        Ok(())
    });
    d.add_sync_listener::<Ev, _>(0, |e: &mut Ev, _| {
        e.n = e.n * 10 + 3;
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.n, 231);
}

#[test]
fn attack_thousand_listeners_order() {
    let d = EventDispatcher::new();
    for i in 0..1000 {
        d.add_sync_listener::<Ev, _>(1000 - i as i32, |e: &mut Ev, _| {
            e.n += 1;
            Ok(())
        });
    }
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.n, 1000);
    assert_eq!(d.listener_count::<Ev>(), 1000);
}

#[test]
fn attack_remove_during_dispatch_no_panic() {
    let d = EventDispatcher::new();
    let d2 = d.clone();
    let victim = d.add_sync_listener::<Ev, _>(0, |e: &mut Ev, _| {
        e.n += 1;
        Ok(())
    });
    d.add_sync_listener::<Ev, _>(10, move |_, _| {
        d2.remove_listener(victim);
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    let mut e2 = Ev::default();
    block_on(d.dispatch(&mut e2)).unwrap();
    assert_eq!(e2.n, 0);
}

#[test]
fn attack_frozen_shared_across_threads() {
    let d = EventDispatcher::new();
    for i in 0..16 {
        d.add_sync_listener::<Ev, _>(0, |e: &mut Ev, _| {
            e.n += 1;
            Ok(())
        });
    }
    let f = Arc::new(d.freeze());
    let mut handles = vec![];
    for _ in 0..8 {
        let f2 = f.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..1000 {
                let mut e = Ev::default();
                block_on(f2.dispatch(&mut e)).unwrap();
                assert_eq!(e.n, 16);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn attack_many_distinct_events() {
    let d = EventDispatcher::new();
    for i in 0..500 {
        let ev = format!("event{i}");
        d.add_sync_named_listener::<Ev, _>(&ev, 0, |e: &mut Ev, _| {
            e.n += 1;
            Ok(())
        });
    }
    for i in 0..500 {
        let ev = format!("event{i}");
        let mut e = Ev::default();
        block_on(d.dispatch_named::<Ev>(&ev, &mut e)).unwrap();
        assert_eq!(e.n, 1);
    }
}

#[test]
fn attack_cache_ptr_reuse_dispatches_correct_bucket() {
    let d = EventDispatcher::new();
    for (tag, val) in [("A", 1i64), ("B", 2i64)] {
        let name = format!("evt_{tag}");
        d.add_sync_named_listener::<Ev, _>(&name, 0, move |e: &mut Ev, _| {
            e.n = val;
            Ok(())
        });
        let mut e = Ev::default();
        let s = format!("evt_{tag}");
        block_on(d.dispatch_named(&s, &mut e)).unwrap();
        assert_eq!(e.n, val);
        drop(s);
    }
    for i in 0..2000 {
        for (tag, expect) in [("A", 1i64), ("B", 2i64)] {
            let mut e = Ev::default();
            let s = format!("evt_{tag}");
            assert_eq!(s.len(), 5);
            block_on(d.dispatch_named(&s, &mut e)).unwrap();
            assert_eq!(e.n, expect, "wrong bucket at iter {i} tag {tag}");
            drop(s);
        }
    }
}

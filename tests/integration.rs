#[path = "../support.rs"]
mod support;
use support::block_on;

use ed::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct Ev {
    log: Vec<&'static str>,
    n: i32,
}
impl Event for Ev {}

#[derive(Default)]
struct Other {
    n: i32,
}
impl Event for Other {}

fn new_disp() -> EventDispatcher {
    EventDispatcher::new()
}

#[test]
fn priority_order_and_fifo() {
    let d = new_disp();
    d.add_sync_listener::<Ev, _>(-10, |e, _| {
        e.log.push("low");
        Ok(())
    });
    d.add_sync_listener::<Ev, _>(10, |e, _| {
        e.log.push("high");
        Ok(())
    });
    d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("mid1");
        Ok(())
    });
    d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("mid2");
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log, vec!["high", "mid1", "mid2", "low"]);
}

#[test]
fn listener_ids_are_unique() {
    let d = new_disp();
    let a = d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("a");
        Ok(())
    });
    let b = d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("b");
        Ok(())
    });
    assert_ne!(a, b);
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log.len(), 2);
}

#[test]
fn stop_propagation() {
    let d = new_disp();
    d.add_sync_listener::<Ev, _>(10, |e, ctx| {
        e.log.push("first");
        ctx.stop_propagation();
        Ok(())
    });
    d.add_sync_listener::<Ev, _>(0, |_, _| {
        panic!("must not run");
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log, vec!["first"]);
}

#[test]
fn listener_error_aborts_chain() {
    let d = new_disp();
    d.add_sync_listener::<Ev, _>(10, |_, _| {
        Err("boom".into())
    });
    d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("after");
        Ok(())
    });
    let mut e = Ev::default();
    let r = block_on(d.dispatch(&mut e));
    assert!(matches!(r, Err(DispatchError::ListenerError(_))));
    assert!(e.log.is_empty());
}

#[test]
fn nested_dispatch_via_ctx() {
    let d = new_disp();
    d.add_sync_named_listener::<Ev, _>("outer", 0, |e, ctx| {
        e.log.push("outer");
        block_on(ctx.dispatch_named("inner", e)).unwrap();
        e.log.push("outer-after");
        Ok(())
    });
    d.add_sync_named_listener::<Ev, _>("inner", 0, |e, _| {
        e.log.push("inner");
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch_named::<Ev>("outer", &mut e)).unwrap();
    assert_eq!(e.log, vec!["outer", "inner", "outer-after"]);
}

#[test]
fn depth_limit_direct_bypass_blocked() {
    static ENTRIES: AtomicUsize = AtomicUsize::new(0);
    ENTRIES.store(0, Ordering::SeqCst);
    let d = new_disp().with_max_depth(3);
    let d2 = d.clone();
    d.add_sync_listener::<Ev, _>(0, move |e: &mut Ev, _| {
        ENTRIES.fetch_add(1, Ordering::SeqCst);
        let _ = block_on(d2.dispatch(e));
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert!(ENTRIES.load(Ordering::SeqCst) <= 3);
    let mut e2 = Ev::default();
    block_on(d.dispatch(&mut e2)).unwrap();
}

#[test]
fn depth_limit_ctx_path_errors() {
    static DEEPEST_ERR: AtomicUsize = AtomicUsize::new(0);
    DEEPEST_ERR.store(0, Ordering::SeqCst);
    let d = new_disp().with_max_depth(2);
    d.add_sync_listener::<Ev, _>(0, |e, ctx| {
        let depth = ctx.depth();
        match block_on(ctx.dispatch(e)) {
            Err(DispatchError::MaxDepthExceeded(_)) => {
                DEEPEST_ERR.store(depth, Ordering::SeqCst);
            }
            Ok(()) => {}
            Err(other) => panic!("unexpected: {other:?}"),
        }
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(DEEPEST_ERR.load(Ordering::SeqCst), 2);
}

#[test]
fn ctx_depth_reported() {
    let d = new_disp();
    d.add_sync_listener::<Ev, _>(0, |_, ctx| {
        assert_eq!(ctx.depth(), 1);
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
}

#[test]
fn add_remove() {
    let d = new_disp();
    let a = d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("x");
        Ok(())
    });
    let b = d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("x");
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log.len(), 2);
    assert!(d.remove_listener(a));
    assert!(!d.remove_listener(a));
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log.len(), 1);
    assert!(d.remove_listener(b));
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert!(e.log.is_empty());
}

#[test]
fn mixed_types_under_one_name() {
    let d = new_disp();
    d.add_sync_named_listener::<Ev, _>("evt", 0, |e: &mut Ev, _| {
        e.n += 1;
        Ok(())
    });
    d.add_sync_named_listener::<Other, _>("evt", 0, |o: &mut Other, _| {
        o.n += 10;
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch_named::<Ev>("evt", &mut e)).unwrap();
    assert_eq!(e.n, 1);
    let mut o = Other::default();
    block_on(d.dispatch_named::<Other>("evt", &mut o)).unwrap();
    assert_eq!(o.n, 10);
}

#[test]
fn async_and_sync_mixed() {
    let d = new_disp();
    d.add_sync_listener::<Ev, _>(10, |e, _| {
        e.log.push("sync");
        Ok(())
    });
    d.add_async_listener::<Ev, _>(0, |e: &mut Ev, _| {
        Box::pin(async move {
            e.log.push("async");
            Ok(())
        })
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log, vec!["sync", "async"]);
}

#[test]
fn freeze_is_independent() {
    let d = new_disp();
    d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("a");
        Ok(())
    });
    let f = d.freeze();
    d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("b");
        Ok(())
    });
    let mut e = Ev::default();
    block_on(f.dispatch(&mut e)).unwrap();
    assert_eq!(e.log, vec!["a"]);
    let mut e2 = Ev::default();
    block_on(d.dispatch(&mut e2)).unwrap();
    assert_eq!(e2.log, vec!["a", "b"]);
}

#[test]
fn frozen_nested_dispatch() {
    let d = new_disp();
    d.add_sync_named_listener::<Ev, _>("outer", 0, |e, ctx| {
        e.log.push("outer");
        block_on(ctx.dispatch_named("inner", e)).unwrap();
        Ok(())
    });
    d.add_sync_named_listener::<Ev, _>("inner", 0, |e, _| {
        e.log.push("inner");
        Ok(())
    });
    let f = d.freeze();
    drop(d);
    let mut e = Ev::default();
    block_on(f.dispatch_named::<Ev>("outer", &mut e)).unwrap();
    assert_eq!(e.log, vec!["outer", "inner"]);
}

#[test]
fn frozen_depth_limit() {
    static N: AtomicUsize = AtomicUsize::new(0);
    N.store(0, Ordering::SeqCst);
    let d = new_disp().with_max_depth(2);
    d.add_sync_listener::<Ev, _>(0, |_, ctx| {
        N.fetch_add(1, Ordering::SeqCst);
        let _ = block_on(ctx.dispatch(&mut Ev::default()));
        Ok(())
    });
    let f = d.freeze();
    let mut e = Ev::default();
    block_on(f.dispatch(&mut e)).unwrap();
    assert!(N.load(Ordering::SeqCst) <= 2);
}

#[test]
fn empty_dispatch_ok() {
    let d = new_disp();
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    block_on(d.dispatch_named::<Ev>("nope", &mut e)).unwrap();
}

#[test]
fn subscriber_api() {
    struct S;
    impl EventSubscriber for S {
        fn register(&self, d: &EventDispatcher) -> Vec<ListenerId> {
            vec![d.add_sync_listener::<Ev, _>(0, |e, _| {
                e.log.push("s1");
                Ok(())
            })]
        }
    }
    let d = new_disp();
    let ids = d.add_subscriber(&S);
    assert_eq!(ids.len(), 1);
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log, vec!["s1"]);
    assert_eq!(d.remove_listeners(&ids), 1);
    assert_eq!(d.remove_listeners(&ids), 0);
    let mut e2 = Ev::default();
    block_on(d.dispatch(&mut e2)).unwrap();
    assert!(e2.log.is_empty());
}

#[test]
fn subscriber_with_state() {
    struct Greeter {
        hits: Arc<AtomicUsize>,
    }
    impl EventSubscriber for Greeter {
        fn register(&self, d: &EventDispatcher) -> Vec<ListenerId> {
            let hits = self.hits.clone();
            vec![d.add_sync_listener::<Ev, _>(0, move |e, _| {
                hits.fetch_add(1, Ordering::SeqCst);
                e.log.push("greeted");
                Ok(())
            })]
        }
    }
    let d = new_disp();
    let g = Greeter { hits: Arc::new(AtomicUsize::new(0)) };
    let hits = g.hits.clone();
    d.add_subscriber(&g);
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log, vec!["greeted"]);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn nested_dispatch_sync_via_ctx() {
    let d = new_disp();
    d.add_sync_named_listener::<Ev, _>("inner", 0, |e, _| {
        e.log.push("inner");
        Ok(())
    });
    d.add_sync_named_listener::<Ev, _>("outer", 0, |e, ctx| {
        e.log.push("outer");
        ctx.dispatch_sync_named("inner", e).unwrap();
        e.log.push("outer-after");
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch_named::<Ev>("outer", &mut e)).unwrap();
    assert_eq!(e.log, vec!["outer", "inner", "outer-after"]);
}

#[test]
fn ctx_dispatch_sync_depth_limit() {
    let d = new_disp().with_max_depth(2);
    d.add_sync_listener::<Ev, _>(0, |e, ctx| {
        let r = ctx.dispatch_sync(e);
        assert!(matches!(r, Ok(()) | Err(DispatchError::MaxDepthExceeded(_))));
        if ctx.depth() == 2 {
            assert!(matches!(r, Err(DispatchError::MaxDepthExceeded(_))));
        }
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
}

#[test]
fn ctx_dispatch_sync_rejects_async_listeners() {
    let d = new_disp();
    d.add_async_named_listener::<Ev, _>("async-ev", 0, |_: &mut Ev, _| {
        Box::pin(async move { Ok(()) })
    });
    d.add_sync_listener::<Ev, _>(0, |e, ctx| {
        let r = ctx.dispatch_sync_named("async-ev", e);
        assert!(matches!(r, Err(DispatchError::SyncDispatchUnavailable)));
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
}

#[test]
fn frozen_dispatch_sync_works_and_rejects_async() {
    let d = new_disp();
    d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("s");
        Ok(())
    });
    let f = d.freeze();
    let mut e = Ev::default();
    f.dispatch_sync(&mut e).unwrap();
    assert_eq!(e.log, vec!["s"]);
    let d2 = new_disp();
    d2.add_async_listener::<Ev, _>(0, |_: &mut Ev, _| {
        Box::pin(async move { Ok(()) })
    });
    let f2 = d2.freeze();
    let mut e2 = Ev::default();
    assert!(matches!(f2.dispatch_sync(&mut e2), Err(DispatchError::SyncDispatchUnavailable)));
}

#[test]
fn remove_listener_needs_only_id() {
    let d = new_disp();
    let a = d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("a");
        Ok(())
    });
    let b = d.add_sync_named_listener::<Ev, _>("named", 0, |e, _| {
        e.log.push("b");
        Ok(())
    });
    assert!(d.remove_listener(a));
    assert!(!d.remove_listener(a));
    let mut e0 = Ev::default();
    block_on(d.dispatch(&mut e0)).unwrap();
    assert!(e0.log.is_empty());
    let mut e = Ev::default();
    block_on(d.dispatch_named::<Ev>("named", &mut e)).unwrap();
    assert_eq!(e.log, vec!["b"]);
    assert!(d.remove_listener(b));
    let mut e2 = Ev::default();
    block_on(d.dispatch_named::<Ev>("named", &mut e2)).unwrap();
    assert!(e2.log.is_empty());
}

#[test]
fn concurrent_add_and_dispatch() {
    let d = Arc::new(new_disp());
    let hits = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    for t in 0..8 {
        let d2 = d.clone();
        let hits2 = hits.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..50 {
                let hits3 = hits2.clone();
                d2.add_sync_listener::<Ev, _>(0, move |_, _| {
                    hits3.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                });
                let mut e = Ev::default();
                block_on(d2.dispatch(&mut e)).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    hits.store(0, Ordering::SeqCst);
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 400);
}

#[test]
fn max_depth_zero_still_dispatches_top_level() {
    let d = new_disp().with_max_depth(0);
    d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("a");
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log, vec!["a"]);
}

#[test]
fn max_depth_zero_blocks_nesting() {
    let d = new_disp().with_max_depth(0);
    d.add_sync_listener::<Ev, _>(0, |e, ctx| {
        e.log.push("a");
        let mut inner = Ev::default();
        let r = block_on(ctx.dispatch_named("recur", &mut inner));
        assert!(matches!(r, Err(DispatchError::MaxDepthExceeded(0))));
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log, vec!["a"]);
}

#[test]
fn unicode_and_empty_names() {
    let d = new_disp();
    d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("ar");
        Ok(())
    });
    d.add_sync_listener::<Ev, _>(0, |e, _| {
        e.log.push("empty");
        Ok(())
    });
    let mut e = Ev::default();
    block_on(d.dispatch(&mut e)).unwrap();
    assert_eq!(e.log.len(), 2);
    let id = d.add_sync_listener::<Ev, _>(0, |_, _| Ok(()));
    assert!(d.remove_listener(id));
}

#[test]
fn dispatch_sync_matches_async() {
    let d = new_disp();
    d.add_sync_listener::<Ev, _>(5, |e, _| {
        e.log.push("b");
        Ok(())
    });
    d.add_sync_listener::<Ev, _>(10, |e, _| {
        e.log.push("a");
        Ok(())
    });
    let mut e1 = Ev::default();
    d.dispatch_sync(&mut e1).unwrap();
    let mut e2 = Ev::default();
    block_on(d.dispatch(&mut e2)).unwrap();
    assert_eq!(e1.log, e2.log);
    assert_eq!(e1.log, vec!["a", "b"]);
}

#[test]
fn dispatch_sync_rejects_async_listeners() {
    let d = new_disp();
    d.add_async_listener::<Ev, _>(0, |_: &mut Ev, _| {
        Box::pin(async move { Ok(()) })
    });
    let mut e = Ev::default();
    assert!(matches!(d.dispatch_sync(&mut e), Err(DispatchError::SyncDispatchUnavailable)));
}

#[test]
fn dispatch_sync_nested_and_stopped() {
    let d = new_disp();
    d.add_sync_named_listener::<Ev, _>("outer", 0, |e, ctx| {
        e.log.push("outer");
        ctx.stop_propagation();
        Ok(())
    });
    d.add_sync_named_listener::<Ev, _>("outer", 0, |_, _| {
        panic!("stopped");
    });
    let mut e = Ev::default();
    d.dispatch_sync_named::<Ev>("outer", &mut e).unwrap();
    assert_eq!(e.log, vec!["outer"]);
}

#[test]
fn remove_by_id_keeps_other_type_under_shared_name() {
    #[derive(Default)]
    struct Other2 {
        n: i32,
    }
    impl Event for Other2 {
        fn default_name() -> &'static str {
            "shared"
        }
    }
    #[derive(Default)]
    struct EvShared {
        n: i32,
    }
    impl Event for EvShared {
        fn default_name() -> &'static str {
            "shared"
        }
    }
    let d = new_disp();
    let id = d.add_sync_named_listener::<EvShared, _>("shared", 0, |e: &mut EvShared, _| {
        e.n += 1;
        Ok(())
    });
    let id_other = d.add_sync_named_listener::<Other2, _>("shared", 0, |e: &mut Other2, _| {
        e.n += 10;
        Ok(())
    });
    assert!(d.remove_listener(id));
    let mut e = EvShared::default();
    block_on(d.dispatch_named::<EvShared>("shared", &mut e)).unwrap();
    assert_eq!(e.n, 0);
    let mut o = Other2::default();
    block_on(d.dispatch_named::<Other2>("shared", &mut o)).unwrap();
    assert_eq!(o.n, 10);
    assert!(d.remove_listener(id_other));
}

#[test]
fn query_api_reflects_add_and_remove() {
    let d = new_disp();
    assert!(!d.has_listeners::<Ev>());
    assert_eq!(d.listener_count::<Ev>(), 0);
    let a = d.add_sync_listener::<Ev, _>(7, |e, _| {
        e.log.push("a");
        Ok(())
    });
    let b = d.add_sync_named_listener::<Ev, _>("custom", -3, |e, _| {
        e.log.push("b");
        Ok(())
    });
    d.add_sync_listener::<Other, _>(1, |e, _| {
        e.n += 1;
        Ok(())
    });
    assert!(d.has_listeners::<Ev>());
    assert_eq!(d.listener_count::<Ev>(), 1);
    assert_eq!(d.named_listener_count::<Ev>("custom"), 1);
    assert!(d.has_named_listeners::<Ev>("custom"));
    assert_eq!(d.listener_count::<Other>(), 1);
    assert_eq!(d.listener_priority(a), Some(7));
    assert_eq!(d.listener_priority(b), Some(-3));
    let foreign = new_disp().add_sync_listener::<Ev, _>(1, |_, _| Ok(()));
    assert_eq!(d.listener_priority(foreign), None);
    assert!(d.remove_listener(a));
    assert!(!d.has_listeners::<Ev>());
    assert_eq!(d.listener_count::<Ev>(), 0);
    assert_eq!(d.listener_priority(a), None);
    assert!(!d.remove_listener(a));
    assert_eq!(d.named_listener_count::<Ev>("custom"), 1);
    assert_eq!(d.listener_priority(b), Some(-3));
}

#[test]
fn frozen_query_api_matches_live() {
    let d = new_disp();
    let a = d.add_sync_listener::<Ev, _>(5, |e, _| {
        e.log.push("a");
        Ok(())
    });
    d.add_sync_named_listener::<Ev, _>("custom", 9, |e, _| {
        e.log.push("b");
        Ok(())
    });
    let f = d.freeze();
    assert!(f.has_listeners::<Ev>());
    assert_eq!(f.listener_count::<Ev>(), 1);
    assert_eq!(f.named_listener_count::<Ev>("custom"), 1);
    assert_eq!(f.listener_priority(a), Some(5));
    let foreign = new_disp().add_sync_listener::<Ev, _>(1, |_, _| Ok(()));
    assert_eq!(f.listener_priority(foreign), None);
    assert!(!f.has_listeners::<Other>());
}

#[test]
fn indexed_removal_across_names_and_types() {
    let d = new_disp();
    let mut ids = Vec::new();
    for name in ["n1", "n2", "n3"] {
        ids.push(d.add_sync_named_listener::<Ev, _>(name, 0, |e, _| {
            e.n += 1;
            Ok(())
        }));
        ids.push(d.add_sync_named_listener::<Other, _>(name, 0, |e, _| {
            e.n += 10;
            Ok(())
        }));
    }
    for id in [ids[0], ids[3], ids[4]] {
        assert!(d.remove_listener(id));
    }
    assert_eq!(d.named_listener_count::<Ev>("n1"), 0);
    assert_eq!(d.named_listener_count::<Other>("n1"), 1);
    assert_eq!(d.named_listener_count::<Ev>("n2"), 1);
    assert_eq!(d.named_listener_count::<Other>("n2"), 0);
    assert_eq!(d.named_listener_count::<Ev>("n3"), 0);
    assert_eq!(d.named_listener_count::<Other>("n3"), 1);
    let mut e = Ev::default();
    block_on(d.dispatch_named::<Ev>("n2", &mut e)).unwrap();
    assert_eq!(e.n, 1);
    let mut o = Other::default();
    block_on(d.dispatch_named::<Other>("n3", &mut o)).unwrap();
    assert_eq!(o.n, 10);
}

#[test]
fn listeners_listing_is_in_dispatch_order() {
    let d = new_disp();
    let a = d.add_sync_listener::<Ev, _>(-10, |_, _| Ok(()));
    let b = d.add_sync_listener::<Ev, _>(10, |_, _| Ok(()));
    let c = d.add_sync_listener::<Ev, _>(10, |_, _| Ok(()));
    // Highest priority first; the tie keeps registration order (FIFO).
    let expected = vec![(b, 10), (c, 10), (a, -10)];
    assert_eq!(d.listeners::<Ev>(), expected);
    assert_eq!(d.named_listeners::<Ev>("nope"), Vec::<(ListenerId, i32)>::new());
    let f = d.freeze();
    assert_eq!(f.listeners::<Ev>(), expected);
    assert!(d.remove_listener(b));
    assert_eq!(d.listeners::<Ev>(), vec![(c, 10), (a, -10)]);
}

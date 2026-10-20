#[path = "../support.rs"]
mod support;
use support::{block_on, poll_once};

use ed::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Default)]
struct Ev {
    log: Vec<&'static str>,
}
impl Event for Ev {}

fn assert_send<F: Send>(f: F) -> F {
    f
}

#[test]
fn dispatch_futures_are_send() {
    let d = EventDispatcher::new();
    let mut e = Ev::default();
    block_on(assert_send(d.dispatch(&mut e))).unwrap();
    let mut e2 = Ev::default();
    block_on(assert_send(d.dispatch_named::<Ev>("anything", &mut e2))).unwrap();
    let f = d.freeze();
    let mut e3 = Ev::default();
    block_on(assert_send(f.dispatch(&mut e3))).unwrap();
    let mut e4 = Ev::default();
    block_on(assert_send(f.dispatch_named::<Ev>("anything", &mut e4))).unwrap();
}

struct YieldOnce {
    yielded: bool,
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[test]
fn empty_dispatch_then_add_listener_still_fires() {
    let d = EventDispatcher::new();
    let mut e = Ev::default();
    block_on(d.dispatch_named::<Ev>("late", &mut e)).unwrap();
    block_on(d.dispatch_named::<Ev>("late", &mut e)).unwrap();
    d.add_sync_named_listener::<Ev, _>("late", 0, |e: &mut Ev, _| {
        e.log.push("fired");
        Ok(())
    });
    block_on(d.dispatch_named::<Ev>("late", &mut e)).unwrap();
    assert_eq!(e.log, vec!["fired"]);
}

#[test]
fn panic_in_async_listener_restores_ambient_depth() {
    let d = EventDispatcher::new().with_max_depth(2);
    d.add_async_listener::<Ev, _>(0, |_: &mut Ev, _| {
        Box::pin(async move { panic!("boom") })
    });
    let mut e = Ev::default();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = block_on(d.dispatch(&mut e));
    }));
    assert!(r.is_err());
    let d2 = EventDispatcher::new().with_max_depth(2);
    d2.add_sync_named_listener::<Ev, _>("inner", 0, |e: &mut Ev, _| {
        e.log.push("inner");
        Ok(())
    });
    d2.add_sync_listener::<Ev, _>(0, |e: &mut Ev, ctx| {
        block_on(ctx.dispatch_named("inner", e)).unwrap();
        Ok(())
    });
    let mut e2 = Ev::default();
    block_on(d2.dispatch(&mut e2)).unwrap();
    assert_eq!(e2.log, vec!["inner"]);
}

#[test]
fn parked_dispatch_does_not_inflate_other_dispatch_depth() {
    let d = EventDispatcher::new().with_max_depth(2);
    d.add_sync_named_listener::<Ev, _>("inner", 0, |e: &mut Ev, _| {
        e.log.push("inner");
        Ok(())
    });
    d.add_async_listener::<Ev, _>(0, |e: &mut Ev, ctx| {
        Box::pin(async move {
            YieldOnce { yielded: false }.await;
            ctx.dispatch_named("inner", e).await?;
            e.log.push("outer-done");
            Ok(())
        })
    });
    let mut e = Ev::default();
    let mut fut = Box::pin(d.dispatch(&mut e));
    assert!(matches!(poll_once(fut.as_mut()), Poll::Pending));
    let d2 = EventDispatcher::new().with_max_depth(2);
    d2.add_sync_named_listener::<Ev, _>("b-inner", 0, |e: &mut Ev, _| {
        e.log.push("b-inner");
        Ok(())
    });
    d2.add_sync_listener::<Ev, _>(0, |e: &mut Ev, ctx| {
        e.log.push("b-outer");
        block_on(ctx.dispatch_named("b-inner", e)).unwrap();
        Ok(())
    });
    let mut eb = Ev::default();
    block_on(d2.dispatch(&mut eb)).unwrap();
    assert_eq!(eb.log, vec!["b-outer", "b-inner"]);
    block_on(fut).unwrap();
    assert_eq!(e.log, vec!["inner", "outer-done"]);
}

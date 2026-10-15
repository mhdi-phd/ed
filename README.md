# ed — async event dispatcher

An event dispatcher in the spirit of Symfony's EventDispatcher, written in Rust:
numeric listener IDs, priorities, propagation control, nested dispatch, and a
depth guard that cannot be bypassed.

- **Sync + async listeners** in one dispatcher. A pure-sync fast path runs
  without any `Arc` refcounting on the hot path.
- **`ListenerId`** (numeric, globally unique): `add_*` returns an ID,
  `remove_listener(id)` / `remove_named_listener(event, id)` removes by ID.
- **Priorities**: higher runs first; equal priorities run in registration
  order (FIFO).
- **Unified channel**: one name-keyed map. `Event::default_name()` (the type
  name) is the default event name, exactly like Symfony's `$event::class`.
  Different event types may share a custom name; dispatch filters by type.
- **Propagation**: `ctx.stop_propagation()`.
- **Nested dispatch**: `ctx.dispatch(..)` / `ctx.dispatch_named(..)` from any
  listener. A thread-local depth counter + RAII guard bounds nesting on
  **every** entry path — including a direct `dispatcher.dispatch()` call from
  inside a listener, which bypassed the limit in earlier designs.
- **`freeze()`** → `ImmutableEventDispatcher`: lock-free, shareable snapshot.
- **`EventSubscriber`** trait for grouped registration.
- Zero dependencies (only `futures-core`-shaped `BoxFuture` alias), zero
  `unsafe`, no locks held while listeners run.

## Performance (release, x86-64)

| listeners | per dispatch |
|----------:|-------------:|
| 1         | ~14ns        |
| 8         | ~48ns        |
| 64        | ~350ns       |

For comparison on the same machine: WordPress-PHP `apply_filters` ≈ 680ns,
Symfony-PHP `dispatch` ≈ 195ns (1 listener).

## Rules that matter

- Listener identity is the numeric `ListenerId` (globally unique).
  `remove_named_listener(event, id)` removes by ID.
- `with_max_depth(n)` bounds *nested* dispatch depth (default 128).
  A top-level `dispatch` always runs; nesting deeper than `n` returns
  `DispatchError::MaxDepthExceeded`.
- Listeners added/removed during a dispatch take effect on the next dispatch
  (snapshot semantics). Concurrent mutation is safe but not linearizable with
  an in-flight dispatch.
- Dispatch futures are `!Send` (thread-local depth guard). Drive each future
  to completion on one thread, or use `dispatch_sync` for sync listeners.

## Example

```rust
use ed::{Event, EventDispatcher};

struct UserRegistered { name: String }
impl Event for UserRegistered {}

let d = EventDispatcher::new();
d.add_sync_listener::<UserRegistered, _>("welcome-mail", 10, |e, _| {
    println!("welcome {}", e.name);
    Ok(())
});
d.add_async_listener::<UserRegistered, _>("audit", 0, |e: &mut UserRegistered, _| {
    Box::pin(async move { /* ... */ Ok(()) })
});

let mut e = UserRegistered { name: "mustafa".into() };
block_on(d.dispatch(&mut e)).unwrap();
```

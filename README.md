# ed — async event dispatcher

An event dispatcher in the spirit of Symfony's EventDispatcher, written in Rust:
numeric listener IDs, priorities, propagation control, nested dispatch, and a
depth guard that cannot be bypassed.

- **Sync + async listeners** in one dispatcher. When every listener for an
  event is sync, a fast path runs them without building a future.
- **`ListenerId`** (numeric, globally unique): `add_*` returns an ID,
  `remove_listener(id)` removes by ID.
- **Priorities**: higher runs first; equal priorities run in registration
  order (FIFO).
- **Keyed by (name, type)**: listeners live under `(event name, TypeId)`.
  `Event::default_name()` (the type name) is the default event name, exactly
  like Symfony's `$event::class`. Different event types may share a custom
  name without interfering; dispatching a name with a different event type
  than the registered one finds no listeners, by design.
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
| 1         | ~94ns        |
| 8         | ~120ns       |
| 64        | ~546ns       |

(`dispatch_sync` at 1 listener: ~85ns.)

For comparison on the same machine: WordPress-PHP `apply_filters` ≈ 680ns,
Symfony-PHP `dispatch` ≈ 195ns (1 listener).

## Rules that matter

- Listener identity is the numeric `ListenerId` (globally unique).
  `remove_listener(id)` removes by ID.
- `with_max_depth(n)` bounds the *total* dispatch depth including the root
  dispatch (default 128): `with_max_depth(2)` allows the root plus one
  nested level. `0` is clamped to `1` (root only), so `0` and `1` behave the
  same; exceeding the bound returns `DispatchError::MaxDepthExceeded`.
- Listeners added/removed during a dispatch take effect on the next dispatch
  (snapshot semantics). Concurrent mutation is safe but not linearizable with
  an in-flight dispatch.
- Dispatch futures are `Send` and panic-safe: the thread-local depth mirror
  is set for the duration of each `poll` and restored by an RAII guard, so
  tasks may migrate between threads and a panicking listener cannot leave
  the depth counter poisoned.

## Example

```rust
use ed::{Event, EventDispatcher};

struct UserRegistered { name: String }
impl Event for UserRegistered {}

let d = EventDispatcher::new();
d.add_sync_listener::<UserRegistered, _>(10, |e, _| {
    println!("welcome {}", e.name);
    Ok(())
});
d.add_async_listener::<UserRegistered, _>(0, |e: &mut UserRegistered, _| {
    Box::pin(async move { /* ... */ Ok(()) })
});

let mut e = UserRegistered { name: "mustafa".into() };
block_on(d.dispatch(&mut e)).unwrap();
```

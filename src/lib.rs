use std::any::{Any, TypeId};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::cell::Cell;
use std::collections::HashMap;
use std::cmp::Reverse;
use std::fmt;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ListenerResult = Result<(), BoxError>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub const DEFAULT_MAX_DEPTH: usize = 128;

#[derive(Debug)]
pub enum DispatchError {
    MaxDepthExceeded(usize),
    ListenerError(BoxError),
    SyncDispatchUnavailable,
}

impl fmt::Display for DispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxDepthExceeded(max) => write!(f, "max nested dispatch depth exceeded ({max})"),
            Self::ListenerError(err) => write!(f, "listener error: {err}"),
            Self::SyncDispatchUnavailable => {
                write!(f, "sync dispatch unavailable (async listeners present)")
            }
        }
    }
}

impl std::error::Error for DispatchError {}

impl From<BoxError> for DispatchError {
    fn from(err: BoxError) -> Self {
        Self::ListenerError(err)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ListenerId(u64);

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[inline]
fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

pub trait Event: Any + Send + Sync + 'static {
    fn default_name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

thread_local! {
    static AMBIENT: Cell<usize> = const { Cell::new(0) };
}

#[inline]
fn ambient_depth() -> usize {
    AMBIENT.with(|c| c.get())
}

#[inline]
fn set_ambient(depth: usize) -> usize {
    AMBIENT.with(|c| { let prev = c.get(); c.set(depth); prev })
}

#[inline]
fn depth_limit(max: usize) -> usize {
    if max == 0 { 1 } else { max }
}

#[inline]
fn entry_depth(max: usize) -> Result<usize, DispatchError> {
    let depth = ambient_depth() + 1;
    if depth > depth_limit(max) {
        Err(DispatchError::MaxDepthExceeded(max))
    } else {
        Ok(depth)
    }
}

struct AmbientGuard {
    prev: usize,
}

impl AmbientGuard {
    #[inline]
    fn set(depth: usize) -> Self {
        AmbientGuard { prev: set_ambient(depth) }
    }
}

impl Drop for AmbientGuard {
    #[inline]
    fn drop(&mut self) {
        let prev = self.prev;
        let _ = AMBIENT.try_with(|c| c.set(prev));
    }
}

struct WithAmbient<F> {
    inner: Pin<Box<F>>,
    depth: usize,
}

impl<F> WithAmbient<F> {
    #[inline]
    fn new(inner: F, depth: usize) -> Self {
        WithAmbient { inner: Box::pin(inner), depth }
    }
}

impl<F: Future> Future for WithAmbient<F> {
    type Output = F::Output;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<F::Output> {
        let _guard = AmbientGuard::set(self.depth);
        self.inner.as_mut().poll(cx)
    }
}

type SyncFn = Box<
    dyn Fn(&mut (dyn Any + Send + Sync), &mut DispatchContext) -> ListenerResult + Send + Sync,
>;
type AsyncFn = Box<
    dyn for<'a> Fn(&'a mut (dyn Any + Send + Sync), &'a mut DispatchContext) -> BoxFuture<'a, ListenerResult>
        + Send
        + Sync,
>;

enum ErasedCallable {
    Sync(SyncFn),
    Async(AsyncFn),
}

#[derive(Clone)]
struct ListenerEntry {
    id: ListenerId,
    priority: i32,
    callable: Arc<ErasedCallable>,
}

impl ListenerEntry {
    fn new_sync<E, F>(priority: i32, func: F) -> Self
    where
        E: Event,
        F: Fn(&mut E, &mut DispatchContext) -> ListenerResult + Send + Sync + 'static,
    {
        Self {
            id: ListenerId(next_id()),
            priority,
            callable: Arc::new(ErasedCallable::Sync(Box::new(move |any_event, ctx| {
                if let Some(event) = any_event.downcast_mut::<E>() {
                    func(event, ctx)
                } else {
                    Ok(())
                }
            }))),
        }
    }

    fn new_async<E, F>(priority: i32, func: F) -> Self
    where
        E: Event,
        F: for<'a> Fn(&'a mut E, &'a mut DispatchContext) -> BoxFuture<'a, ListenerResult>
            + Send
            + Sync
            + 'static,
    {
        Self {
            id: ListenerId(next_id()),
            priority,
            callable: Arc::new(ErasedCallable::Async(Box::new(move |any_event, ctx| {
                if let Some(event) = any_event.downcast_mut::<E>() {
                    func(event, ctx)
                } else {
                    Box::pin(async { Ok(()) })
                }
            }))),
        }
    }
}

#[derive(Default)]
struct EventBucket {
    raw: Vec<ListenerEntry>,
    sorted: Option<Arc<[ListenerEntry]>>,
}

impl EventBucket {
    fn add(&mut self, entry: ListenerEntry) {
        self.raw.push(entry);
        self.sorted = None;
    }

    fn remove(&mut self, id: ListenerId) -> bool {
        let before = self.raw.len();
        self.raw.retain(|e| e.id != id);
        if self.raw.len() != before {
            self.sorted = None;
            true
        } else {
            false
        }
    }

    fn get_sorted(&mut self) -> Arc<[ListenerEntry]> {
        if let Some(ref cached) = self.sorted {
            return Arc::clone(cached);
        }
        self.raw.sort_by_key(|e| Reverse(e.priority));
        let slice: Arc<[ListenerEntry]> = self.raw.clone().into();
        self.sorted = Some(Arc::clone(&slice));
        slice
    }
}

type Buckets = HashMap<Box<str>, HashMap<TypeId, EventBucket>>;

struct StateInner {
    buckets: RwLock<Buckets>,
}

struct FrozenState {
    buckets: HashMap<Box<str>, HashMap<TypeId, Arc<[ListenerEntry]>>>,
}

#[derive(Clone, Copy)]
enum Backend<'a> {
    Live(&'a StateInner),
    Frozen(&'a FrozenState),
}

fn fetch(backend: Backend<'_>, event_name: &str, type_id: TypeId) -> Option<Arc<[ListenerEntry]>> {
    match backend {
        Backend::Live(inner) => {
            enum Look {
                Hit(Arc<[ListenerEntry]>),
                Dirty,
                Miss,
            }
            let look = {
                let buckets = inner.buckets.read().unwrap_or_else(|e| e.into_inner());
                match buckets.get(event_name).and_then(|m| m.get(&type_id)) {
                    None => Look::Miss,
                    Some(b) => match &b.sorted {
                        Some(s) => Look::Hit(Arc::clone(s)),
                        None if b.raw.is_empty() => Look::Miss,
                        None => Look::Dirty,
                    },
                }
            };
            match look {
                Look::Hit(s) => Some(s),
                Look::Miss => None,
                Look::Dirty => {
                    let mut buckets = inner.buckets.write().unwrap_or_else(|e| e.into_inner());
                    buckets.get_mut(event_name).and_then(|m| m.get_mut(&type_id)).and_then(|b| {
                        if b.raw.is_empty() { None } else { Some(b.get_sorted()) }
                    })
                }
            }
        }
        Backend::Frozen(state) => state
            .buckets
            .get(event_name)
            .and_then(|m| m.get(&type_id))
            .cloned(),
    }
}

#[inline]
fn all_sync(listeners: &[ListenerEntry]) -> bool {
    listeners
        .iter()
        .all(|e| matches!(&*e.callable, ErasedCallable::Sync(_)))
}

fn run_sync_loop<E: Event>(backend: Backend<'_>, max_depth: usize, event: &mut E, listeners: &[ListenerEntry], depth: usize) -> Result<(), DispatchError> {
    let _ambient = AmbientGuard::set(depth);
    let mut ctx = DispatchContext { backend, stopped: false, depth, max_depth };
    for entry in listeners.iter() {
        if ctx.stopped {
            break;
        }
        match &*entry.callable {
            ErasedCallable::Sync(f) => f(event, &mut ctx)?,
            ErasedCallable::Async(_) => return Err(DispatchError::SyncDispatchUnavailable),
        }
    }
    Ok(())
}

async fn run_async_loop<E: Event>(backend: Backend<'_>, max_depth: usize, listeners: &[ListenerEntry], event: &mut E, depth: usize) -> Result<(), DispatchError> {
    let mut ctx = DispatchContext { backend, stopped: false, depth, max_depth };
    for entry in listeners.iter() {
        if ctx.stopped {
            break;
        }
        match &*entry.callable {
            ErasedCallable::Sync(f) => f(event, &mut ctx)?,
            ErasedCallable::Async(f) => f(event, &mut ctx).await?,
        }
    }
    Ok(())
}

fn try_fast_sync<E: Event>(backend: Backend<'_>, max_depth: usize, event_name: &str, event: &mut E, depth: usize) -> Option<Result<(), DispatchError>> {
    let listeners = fetch(backend, event_name, TypeId::of::<E>())?;
    if !all_sync(&listeners) {
        return None;
    }
    Some(run_sync_loop(backend, max_depth, event, &listeners, depth))
}

fn dispatch_sync_from<E: Event>(backend: Backend<'_>, max_depth: usize, event_name: &str, event: &mut E, depth: usize) -> Result<(), DispatchError> {
    let listeners = match fetch(backend, event_name, TypeId::of::<E>()) {
        Some(l) => l,
        None => return Ok(()),
    };
    if !all_sync(&listeners) {
        return Err(DispatchError::SyncDispatchUnavailable);
    }
    run_sync_loop(backend, max_depth, event, &listeners, depth)
}

fn dispatch_async_from<'a, E: Event>(backend: Backend<'a>, max_depth: usize, event_name: &'a str, event: &'a mut E, depth: usize) -> WithAmbient<impl Future<Output = Result<(), DispatchError>> + 'a> {
    WithAmbient::new(async move {
        let listeners = match fetch(backend, event_name, TypeId::of::<E>()) {
            Some(l) => l,
            None => return Ok(()),
        };
        if all_sync(&listeners) {
            return run_sync_loop(backend, max_depth, event, &listeners, depth);
        }
        run_async_loop(backend, max_depth, &listeners, event, depth).await
    }, depth)
}

pub struct DispatchContext<'a> {
    backend: Backend<'a>,
    stopped: bool,
    depth: usize,
    max_depth: usize,
}

impl<'a> DispatchContext<'a> {
    #[inline]
    pub fn stop_propagation(&mut self) {
        self.stopped = true;
    }

    #[inline]
    pub fn is_propagation_stopped(&self) -> bool {
        self.stopped
    }

    #[inline]
    pub fn depth(&self) -> usize {
        self.depth
    }

    pub async fn dispatch<E: Event>(&self, event: &mut E) -> Result<(), DispatchError> {
        self.dispatch_named(E::default_name(), event).await
    }

    pub async fn dispatch_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = self.depth + 1;
        if depth > depth_limit(self.max_depth) {
            return Err(DispatchError::MaxDepthExceeded(self.max_depth));
        }
        if let Some(r) = try_fast_sync(self.backend, self.max_depth, event_name, event, depth) {
            return r;
        }
        dispatch_async_from(self.backend, self.max_depth, event_name, event, depth).await
    }

    pub fn dispatch_sync<E: Event>(&self, event: &mut E) -> Result<(), DispatchError> {
        self.dispatch_sync_named(E::default_name(), event)
    }

    pub fn dispatch_sync_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = self.depth + 1;
        if depth > depth_limit(self.max_depth) {
            return Err(DispatchError::MaxDepthExceeded(self.max_depth));
        }
        dispatch_sync_from(self.backend, self.max_depth, event_name, event, depth)
    }
}

#[derive(Clone)]
pub struct EventDispatcher {
    inner: Arc<StateInner>,
    max_depth: usize,
}

impl Default for EventDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl EventDispatcher {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(StateInner { buckets: RwLock::new(HashMap::new()) }),
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }

    /// Sets the maximum nested dispatch depth. `0` means the root dispatch
    /// still runs but any nested dispatch is rejected (it is NOT unlimited).
    pub fn with_max_depth(mut self, max_depth: usize) -> Self {
        self.max_depth = max_depth;
        self
    }

    fn add_entry<E: Event>(&self, event_name: &str, entry: ListenerEntry) -> ListenerId {
        let id = entry.id;
        let mut buckets = self.inner.buckets.write().unwrap_or_else(|e| e.into_inner());
        buckets
            .entry(event_name.into())
            .or_default()
            .entry(TypeId::of::<E>())
            .or_default()
            .add(entry);
        id
    }

    pub fn add_async_listener<E, F>(&self, priority: i32, listener: F) -> ListenerId
    where
        E: Event,
        F: for<'a> Fn(&'a mut E, &'a mut DispatchContext) -> BoxFuture<'a, ListenerResult>
            + Send
            + Sync
            + 'static,
    {
        self.add_async_named_listener(E::default_name(), priority, listener)
    }

    pub fn add_async_named_listener<E, F>(&self, event_name: &str, priority: i32, listener: F) -> ListenerId
    where
        E: Event,
        F: for<'a> Fn(&'a mut E, &'a mut DispatchContext) -> BoxFuture<'a, ListenerResult>
            + Send
            + Sync
            + 'static,
    {
        let entry = ListenerEntry::new_async::<E, F>(priority, listener);
        self.add_entry::<E>(event_name, entry)
    }

    pub fn add_sync_listener<E, F>(&self, priority: i32, listener: F) -> ListenerId
    where
        E: Event,
        F: Fn(&mut E, &mut DispatchContext) -> ListenerResult + Send + Sync + 'static,
    {
        self.add_sync_named_listener(E::default_name(), priority, listener)
    }

    /// Every call creates a new listener with a fresh `ListenerId`; nothing is
    /// replaced. Callers that re-register must store the ID and call
    /// `remove_listener` first, or the bucket grows without bound.
    pub fn add_sync_named_listener<E, F>(&self, event_name: &str, priority: i32, listener: F) -> ListenerId
    where
        E: Event,
        F: Fn(&mut E, &mut DispatchContext) -> ListenerResult + Send + Sync + 'static,
    {
        let entry = ListenerEntry::new_sync::<E, F>(priority, listener);
        self.add_entry::<E>(event_name, entry)
    }

    pub fn remove_listener(&self, id: ListenerId) -> bool {
        let mut buckets = self.inner.buckets.write().unwrap_or_else(|e| e.into_inner());
        let mut located: Option<(Box<str>, TypeId)> = None;
        'search: for (name, by_type) in buckets.iter() {
            for (type_id, bucket) in by_type.iter() {
                if bucket.raw.iter().any(|e| e.id == id) {
                    located = Some((name.clone(), *type_id));
                    break 'search;
                }
            }
        }
        let Some((name, type_id)) = located else {
            return false;
        };
        let by_type = buckets.get_mut(&*name).expect("located above");
        let bucket = by_type.get_mut(&type_id).expect("located above");
        bucket.remove(id);
        if bucket.raw.is_empty() {
            by_type.remove(&type_id);
        }
        if by_type.is_empty() {
            buckets.remove(&*name);
        }
        true
    }

    pub fn has_listeners<E: Event>(&self) -> bool {
        let buckets = self.inner.buckets.read().unwrap_or_else(|e| e.into_inner());
        buckets
            .get(E::default_name())
            .and_then(|m| m.get(&TypeId::of::<E>()))
            .is_some_and(|b| !b.raw.is_empty())
    }

    pub fn has_named_listeners(&self, event_name: &str) -> bool {
        let buckets = self.inner.buckets.read().unwrap_or_else(|e| e.into_inner());
        buckets
            .get(event_name)
            .is_some_and(|m| m.values().any(|b| !b.raw.is_empty()))
    }

    pub fn listener_count<E: Event>(&self) -> usize {
        let buckets = self.inner.buckets.read().unwrap_or_else(|e| e.into_inner());
        buckets
            .get(E::default_name())
            .and_then(|m| m.get(&TypeId::of::<E>()))
            .map_or(0, |b| b.raw.len())
    }

    #[inline]
    pub async fn dispatch<E: Event>(&self, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        if let Some(r) = try_fast_sync(Backend::Live(&self.inner), self.max_depth, E::default_name(), event, depth) {
            return r;
        }
        dispatch_async_from(Backend::Live(&self.inner), self.max_depth, E::default_name(), event, depth).await
    }

    #[inline]
    pub async fn dispatch_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        if let Some(r) = try_fast_sync(Backend::Live(&self.inner), self.max_depth, event_name, event, depth) {
            return r;
        }
        dispatch_async_from(Backend::Live(&self.inner), self.max_depth, event_name, event, depth).await
    }

    pub fn dispatch_sync<E: Event>(&self, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        dispatch_sync_from(Backend::Live(&self.inner), self.max_depth, E::default_name(), event, depth)
    }

    pub fn dispatch_sync_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        dispatch_sync_from(Backend::Live(&self.inner), self.max_depth, event_name, event, depth)
    }

    pub fn freeze(&self) -> ImmutableEventDispatcher {
        let mut buckets = self.inner.buckets.write().unwrap_or_else(|e| e.into_inner());
        let mut frozen: HashMap<Box<str>, HashMap<TypeId, Arc<[ListenerEntry]>>> = HashMap::with_capacity(buckets.len());
        for (name, by_type) in buckets.iter_mut() {
            let mut types: HashMap<TypeId, Arc<[ListenerEntry]>> = HashMap::with_capacity(by_type.len());
            for (type_id, bucket) in by_type.iter_mut() {
                if !bucket.raw.is_empty() {
                    types.insert(*type_id, bucket.get_sorted());
                }
            }
            if !types.is_empty() {
                frozen.insert(name.clone(), types);
            }
        }
        ImmutableEventDispatcher {
            state: Arc::new(FrozenState { buckets: frozen }),
            max_depth: self.max_depth,
        }
    }
}

#[derive(Clone)]
pub struct ImmutableEventDispatcher {
    state: Arc<FrozenState>,
    max_depth: usize,
}

impl ImmutableEventDispatcher {
    #[inline]
    pub async fn dispatch<E: Event>(&self, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        if let Some(r) = try_fast_sync(Backend::Frozen(&self.state), self.max_depth, E::default_name(), event, depth) {
            return r;
        }
        dispatch_async_from(Backend::Frozen(&self.state), self.max_depth, E::default_name(), event, depth).await
    }

    #[inline]
    pub async fn dispatch_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        if let Some(r) = try_fast_sync(Backend::Frozen(&self.state), self.max_depth, event_name, event, depth) {
            return r;
        }
        dispatch_async_from(Backend::Frozen(&self.state), self.max_depth, event_name, event, depth).await
    }

    pub fn dispatch_sync<E: Event>(&self, event: &mut E) -> Result<(), DispatchError> {
        self.dispatch_sync_named(E::default_name(), event)
    }

    pub fn dispatch_sync_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        dispatch_sync_from(Backend::Frozen(&self.state), self.max_depth, event_name, event, depth)
    }
}

pub trait EventSubscriber: Send + Sync + 'static {
    fn register(&self, dispatcher: &EventDispatcher);
}

impl EventDispatcher {
    pub fn add_subscriber<S: EventSubscriber>(&self, subscriber: &S) {
        subscriber.register(self);
    }
}

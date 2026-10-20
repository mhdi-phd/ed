use std::any::{Any, TypeId};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::cell::{Cell, RefCell};
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

static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[inline]
fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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
    type_id: TypeId,
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
            type_id: TypeId::of::<E>(),
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
            type_id: TypeId::of::<E>(),
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

    fn remove(&mut self, id: ListenerId, type_id: TypeId) -> bool {
        let before = self.raw.len();
        self.raw.retain(|e| e.id != id || e.type_id != type_id);
        if self.raw.len() != before {
            self.sorted = None;
            true
        } else {
            false
        }
    }

    fn remove_any(&mut self, id: ListenerId) -> bool {
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

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

struct StateInner {
    generation: u64,
    version: AtomicU64,
    buckets: RwLock<HashMap<Box<str>, EventBucket>>,
}

impl StateInner {
    fn new() -> Self {
        Self {
            generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            version: AtomicU64::new(0),
            buckets: RwLock::new(HashMap::new()),
        }
    }

    #[inline]
    fn bump(&self) {
        self.version.fetch_add(1, Ordering::Release);
    }
}

#[derive(Clone)]
struct FastEntry {
    version: u64,
    listeners: Option<Arc<[ListenerEntry]>>,
    all_sync: bool,
}

type FastKey = (u64, Box<str>);

thread_local! {
    static FAST: RefCell<Vec<(FastKey, FastEntry)>> = const { RefCell::new(Vec::new()) };
}

const FAST_CAP: usize = 64;

fn fast_lookup(generation: u64, event_name: &str, version: u64) -> Option<Option<Arc<[ListenerEntry]>>> {
    FAST.with(|c| {
        let Ok(c) = c.try_borrow() else {
            return None;
        };
        for ((g, n), e) in c.iter() {
            if *g == generation && e.version == version && &**n == event_name {
                return Some(e.listeners.clone());
            }
        }
        None
    })
}

fn fast_store(generation: u64, event_name: &str, version: u64, listeners: Option<Arc<[ListenerEntry]>>) {
    let all_sync = listeners
        .as_ref()
        .map_or(true, |l| l.iter().all(|e| matches!(&*e.callable, ErasedCallable::Sync(_))));
    FAST.with(|c| {
        let Ok(mut c) = c.try_borrow_mut() else {
            return;
        };
        for ((g, n), e) in c.iter_mut() {
            if *g == generation && &**n == event_name {
                *e = FastEntry {
                    version,
                    listeners,
                    all_sync,
                };
                return;
            }
        }
        if c.len() >= FAST_CAP {
            c.remove(0);
        }
        c.push(((generation, event_name.into()), FastEntry { version, listeners, all_sync }));
    })
}

#[derive(Clone, Copy)]
enum Backend<'a> {
    Live(&'a StateInner),
    Frozen(&'a FrozenState),
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
        match self.backend {
            Backend::Live(inner) => {
                EventDispatcher::dispatch_fast(inner, self.max_depth, event_name, event, depth).await
            }
            Backend::Frozen(state) => {
                ImmutableEventDispatcher::dispatch_frozen_inner(state, self.max_depth, event_name, event, depth)
                .await
            }
        }
    }

    pub fn dispatch_sync<E: Event>(&self, event: &mut E) -> Result<(), DispatchError> {
        self.dispatch_sync_named(E::default_name(), event)
    }

    pub fn dispatch_sync_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = self.depth + 1;
        if depth > depth_limit(self.max_depth) {
            return Err(DispatchError::MaxDepthExceeded(self.max_depth));
        }
        match self.backend {
            Backend::Live(inner) => {
                EventDispatcher::dispatch_sync_inner(inner, self.max_depth, event_name, event, depth)
            }
            Backend::Frozen(state) => {
                ImmutableEventDispatcher::dispatch_frozen_sync_inner(state, self.max_depth, event_name, event, depth)
            }
        }
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
            inner: Arc::new(StateInner::new()),
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }

    /// Sets the maximum nested dispatch depth. `0` means the root dispatch
    /// still runs but any nested dispatch is rejected (it is NOT unlimited).
    pub fn with_max_depth(mut self, max_depth: usize) -> Self {
        self.max_depth = max_depth;
        self
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
        let id = entry.id;
        let mut buckets = self.inner.buckets.write().unwrap_or_else(|e| e.into_inner());
        buckets.entry(event_name.into()).or_default().add(entry);
        self.inner.bump();
        id
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
    /// `remove_named_listener` first, or the bucket grows without bound.
    pub fn add_sync_named_listener<E, F>(&self, event_name: &str, priority: i32, listener: F) -> ListenerId
    where
        E: Event,
        F: Fn(&mut E, &mut DispatchContext) -> ListenerResult + Send + Sync + 'static,
    {
        let entry = ListenerEntry::new_sync::<E, F>(priority, listener);
        let id = entry.id;
        let mut buckets = self.inner.buckets.write().unwrap_or_else(|e| e.into_inner());
        buckets.entry(event_name.into()).or_default().add(entry);
        self.inner.bump();
        id
    }

    pub fn remove_listener<E: Event>(&self, id: ListenerId) -> bool {
        self.remove_named_listener::<E>(E::default_name(), id)
    }

    pub fn remove_named_listener<E: Event>(&self, event_name: &str, id: ListenerId) -> bool {
        let mut buckets = self.inner.buckets.write().unwrap_or_else(|e| e.into_inner());
        let removed = match buckets.get_mut(event_name) {
            Some(bucket) => {
                let r = bucket.remove(id, TypeId::of::<E>());
                if bucket.raw.is_empty() {
                    buckets.remove(event_name);
                }
                r
            }
            None => false,
        };
        if removed {
            self.inner.bump();
        }
        removed
    }

    pub fn has_listeners<E: Event>(&self) -> bool {
        self.has_named_listeners(E::default_name())
    }

    pub fn has_named_listeners(&self, event_name: &str) -> bool {
        let buckets = self.inner.buckets.read().unwrap_or_else(|e| e.into_inner());
        buckets
            .get(event_name)
            .is_some_and(|b| !b.raw.is_empty())
    }

    pub fn listener_count<E: Event>(&self) -> usize {
        let buckets = self.inner.buckets.read().unwrap_or_else(|e| e.into_inner());
        buckets
            .get(E::default_name())
            .map(|b| b.raw.len())
            .unwrap_or(0)
    }

    #[inline]
    pub async fn dispatch<E: Event>(&self, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        if let Some(r) = Self::try_sync_dispatch(Backend::Live(&self.inner), self.inner.generation, self.inner.version.load(Ordering::Acquire), self.max_depth, E::default_name(), event, depth) {
            return r;
        }
        Self::dispatch_fast(&self.inner, self.max_depth, E::default_name(), event, depth).await
    }

    #[inline]
    pub async fn dispatch_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        if let Some(r) = Self::try_sync_dispatch(Backend::Live(&self.inner), self.inner.generation, self.inner.version.load(Ordering::Acquire), self.max_depth, event_name, event, depth) {
            return r;
        }
        Self::dispatch_fast(&self.inner, self.max_depth, event_name, event, depth).await
    }

    pub fn dispatch_sync<E: Event>(&self, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        Self::dispatch_sync_inner(&self.inner, self.max_depth, E::default_name(), event, depth)
    }

    pub fn dispatch_sync_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        Self::dispatch_sync_inner(&self.inner, self.max_depth, event_name, event, depth)
    }

    pub fn remove_listener_by_id(&self, id: ListenerId) -> bool {
        let removed = {
            let mut buckets = self.inner.buckets.write().unwrap_or_else(|e| e.into_inner());
            let mut found = false;
            let mut empty: Option<Box<str>> = None;
            for (name, bucket) in buckets.iter_mut() {
                if bucket.remove_any(id) {
                    found = true;
                    if bucket.raw.is_empty() {
                        empty = Some(name.clone());
                    }
                    break;
                }
            }
            if let Some(name) = empty {
                buckets.remove(&*name);
            }
            found
        };
        if removed {
            self.inner.bump();
        }
        removed
    }

    fn dispatch_sync_inner<E: Event>(inner: &StateInner, max_depth: usize, event_name: &str, event: &mut E, depth: usize) -> Result<(), DispatchError> {
        if let Some(r) = Self::try_sync_dispatch(Backend::Live(inner), inner.generation, inner.version.load(Ordering::Acquire), max_depth, event_name, event, depth) {
            return r;
        }
        let generation = inner.generation;
        let version = inner.version.load(Ordering::Acquire);
        let listeners = Self::slow_lookup(inner, event_name);
        fast_store(generation, event_name, version, listeners.clone());
        let listeners = match listeners {
            Some(l) => l,
            None => return Ok(()),
        };
        if !listeners
            .iter()
            .all(|e| matches!(&*e.callable, ErasedCallable::Sync(_)))
        {
            return Err(DispatchError::SyncDispatchUnavailable);
        }
        Self::run_sync_loop(Backend::Live(inner), max_depth, event, &listeners, depth)
    }

    #[inline]
    fn run_sync_loop<E: Event>(backend: Backend<'_>, max_depth: usize, event: &mut E, listeners: &[ListenerEntry], depth: usize) -> Result<(), DispatchError> {
        let _ambient = AmbientGuard::set(depth);
        let mut ctx = DispatchContext {
            backend,
            stopped: false,
            depth,
            max_depth,
        };
        let wanted = TypeId::of::<E>();
        for entry in listeners.iter() {
            if ctx.stopped {
                break;
            }
            if entry.type_id != wanted {
                continue;
            }
            match &*entry.callable {
                ErasedCallable::Sync(f) => {
                    if let Err(err) = f(event, &mut ctx) {
                        return Err(DispatchError::ListenerError(err));
                    }
                }
                ErasedCallable::Async(_) => return Err(DispatchError::SyncDispatchUnavailable),
            }
        }
        Ok(())
    }

    #[inline]
    fn try_sync_dispatch<E: Event>(backend: Backend<'_>, generation: u64, version: u64, max_depth: usize, event_name: &str, event: &mut E, depth: usize) -> Option<Result<(), DispatchError>> {
        let result: Result<Option<Result<(), DispatchError>>, _> = FAST.try_with(|c| {
            let cache = c.try_borrow().ok()?;
            for ((g, n), e) in cache.iter() {
                if *g == generation && e.version == version && &**n == event_name {
                    if !e.all_sync {
                        return None;
                    }
                    let listeners: &[ListenerEntry] = match &e.listeners {
                        Some(l) => l,
                        None => return Some(Ok(())),
                    };
                    return Some(Self::run_sync_loop(backend, max_depth, event, listeners, depth));
                }
            }
            None
        });
        result.unwrap_or_default()
    }

    #[inline]
    fn dispatch_fast<'a, E: Event>(inner: &'a StateInner, max_depth: usize, event_name: &'a str, event: &'a mut E, depth: usize) -> WithAmbient<impl Future<Output = Result<(), DispatchError>> + 'a> {
        WithAmbient::new(Self::dispatch_fast_inner(inner, max_depth, event_name, event, depth), depth)
    }

    async fn dispatch_fast_inner<E: Event>(inner: &StateInner, max_depth: usize, event_name: &str, event: &mut E, depth: usize) -> Result<(), DispatchError> {
        let generation = inner.generation;
        let version = inner.version.load(Ordering::Acquire);
        let listeners = match fast_lookup(generation, event_name, version) {
            Some(listeners) => listeners,
            None => {
                let listeners = Self::slow_lookup(inner, event_name);
                fast_store(generation, event_name, version, listeners.clone());
                listeners
            }
        };
        let listeners = match listeners {
            Some(l) => l,
            None => return Ok(()),
        };
        let mut ctx = DispatchContext {
            backend: Backend::Live(inner),
            stopped: false,
            depth,
            max_depth,
        };
        let wanted = TypeId::of::<E>();
        for entry in listeners.iter() {
            if ctx.stopped {
                break;
            }
            if entry.type_id != wanted {
                continue;
            }
            match &*entry.callable {
                ErasedCallable::Sync(f) => f(event, &mut ctx)?,
                ErasedCallable::Async(f) => f(event, &mut ctx).await?,
            }
        }
        Ok(())
    }

    #[cold]
    fn slow_lookup(inner: &StateInner, event_name: &str) -> Option<Arc<[ListenerEntry]>> {
        let cached = {
            let buckets = inner.buckets.read().unwrap_or_else(|e| e.into_inner());
            match buckets.get(event_name) {
                None => None,
                Some(b) if b.raw.is_empty() => None,
                Some(b) => b.sorted.clone(),
            }
        };
        match cached {
            Some(cached) => Some(cached),
            None => {
                let mut buckets = inner.buckets.write().unwrap_or_else(|e| e.into_inner());
                match buckets.get_mut(event_name) {
                    Some(b) if !b.raw.is_empty() => Some(b.get_sorted()),
                    _ => None,
                }
            }
        }
    }

    pub fn freeze(&self) -> ImmutableEventDispatcher {
        let mut buckets = self.inner.buckets.write().unwrap_or_else(|e| e.into_inner());
        let mut frozen = HashMap::with_capacity(buckets.len());
        for (name, bucket) in buckets.iter_mut() {
            if !bucket.raw.is_empty() {
                frozen.insert(name.clone(), bucket.get_sorted());
            }
        }
        ImmutableEventDispatcher {
            state: Arc::new(FrozenState {
                generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
                buckets: frozen,
            }),
            max_depth: self.max_depth,
        }
    }
}

struct FrozenState {
    generation: u64,
    buckets: HashMap<Box<str>, Arc<[ListenerEntry]>>,
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
        if let Some(r) = EventDispatcher::try_sync_dispatch(Backend::Frozen(&self.state), self.state.generation, 0, self.max_depth, E::default_name(), event, depth) {
            return r;
        }
        Self::dispatch_frozen_inner(&self.state, self.max_depth, E::default_name(), event, depth)
            .await
    }

    #[inline]
    pub async fn dispatch_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        if let Some(r) = EventDispatcher::try_sync_dispatch(Backend::Frozen(&self.state), self.state.generation, 0, self.max_depth, event_name, event, depth) {
            return r;
        }
        Self::dispatch_frozen_inner(&self.state, self.max_depth, event_name, event, depth).await
    }

    pub fn dispatch_sync<E: Event>(&self, event: &mut E) -> Result<(), DispatchError> {
        self.dispatch_sync_named(E::default_name(), event)
    }

    pub fn dispatch_sync_named<E: Event>(&self, event_name: &str, event: &mut E) -> Result<(), DispatchError> {
        let depth = entry_depth(self.max_depth)?;
        Self::dispatch_frozen_sync_inner(&self.state, self.max_depth, event_name, event, depth)
    }

    fn dispatch_frozen_sync_inner<E: Event>(state: &FrozenState, max_depth: usize, event_name: &str, event: &mut E, depth: usize) -> Result<(), DispatchError> {
        if let Some(r) = EventDispatcher::try_sync_dispatch(Backend::Frozen(state), state.generation, 0, max_depth, event_name, event, depth) {
            return r;
        }
        let generation = state.generation;
        let listeners = match fast_lookup(generation, event_name, 0) {
            Some(listeners) => listeners,
            None => {
                let listeners = state.buckets.get(event_name).cloned();
                fast_store(generation, event_name, 0, listeners.clone());
                listeners
            }
        };
        let listeners = match listeners {
            Some(l) => l,
            None => return Ok(()),
        };
        if !listeners
            .iter()
            .all(|e| matches!(&*e.callable, ErasedCallable::Sync(_)))
        {
            return Err(DispatchError::SyncDispatchUnavailable);
        }
        EventDispatcher::run_sync_loop(Backend::Frozen(state), max_depth, event, &listeners, depth)
    }

    #[inline]
    fn dispatch_frozen_inner<'a, E: Event>(state: &'a FrozenState, max_depth: usize, event_name: &'a str, event: &'a mut E, depth: usize) -> WithAmbient<impl Future<Output = Result<(), DispatchError>> + 'a> {
        WithAmbient::new(Self::dispatch_frozen_impl(state, max_depth, event_name, event, depth), depth)
    }

    async fn dispatch_frozen_impl<E: Event>(state: &FrozenState, max_depth: usize, event_name: &str, event: &mut E, depth: usize) -> Result<(), DispatchError> {
        let generation = state.generation;
        let listeners = match fast_lookup(generation, event_name, 0) {
            Some(listeners) => listeners,
            None => {
                let listeners = state.buckets.get(event_name).cloned();
                fast_store(generation, event_name, 0, listeners.clone());
                listeners
            }
        };
        let listeners = match listeners {
            Some(l) => l,
            None => return Ok(()),
        };
        let mut ctx = DispatchContext {
            backend: Backend::Frozen(state),
            stopped: false,
            depth,
            max_depth,
        };
        let wanted = TypeId::of::<E>();
        for entry in listeners.iter() {
            if ctx.stopped {
                break;
            }
            if entry.type_id != wanted {
                continue;
            }
            match &*entry.callable {
                ErasedCallable::Sync(f) => f(event, &mut ctx)?,
                ErasedCallable::Async(f) => f(event, &mut ctx).await?,
            }
        }
        Ok(())
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

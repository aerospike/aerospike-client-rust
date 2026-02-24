use std::cell::UnsafeCell;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// A cheaply cloneable wrapper around a value that can be shared across async tasks **without
/// a lock**, provided the caller upholds a partition invariant.
///
/// # Motivation
///
/// Parallel batch execution needs to distribute a `Vec<BatchOperation>` across N per-node futures
/// so that each future can write results directly into its own slice of the vec.  A standard
/// `Arc<Mutex<Vec<…>>>` would work but introduces unnecessary lock contention: the futures never
/// touch the same elements, so there is no actual data race to prevent.
///
/// `SemanticSync<T>` satisfies the compiler's `Send + Sync` requirements while giving callers
/// unsynchronized access.  The name reflects that the *semantic* guarantee (disjoint access) is
/// provided by the caller, not enforced by the type.
///
/// # Safety contract
///
/// All `SemanticSync` clones that access the inner value concurrently (via [`as_ref`] or
/// [`as_ref_mut`]) **must** access disjoint parts of `T`.  Violating this is undefined
/// behavior.
///
/// The typical lifecycle is:
///
/// 1. Create one owner: `let sem = SemanticSync::new(vec)`.
/// 2. Distribute cheap `Arc` clones to parallel futures; each future operates on a disjoint
///    index range.
/// 3. After all futures complete (or are dropped), call [`into_inner`] on the owner to reclaim
///    the value.  This is safe because no other `Arc` clone can be accessing the data at that
///    point.
///
/// [`as_ref`]: SemanticSync::as_ref
/// [`as_ref_mut`]: SemanticSync::as_ref_mut
/// [`into_inner`]: SemanticSync::into_inner
pub struct SemanticSync<T> {
    inner: Arc<UnsafeCell<Option<T>>>,
}

impl<T> Clone for SemanticSync<T> {
    /// Creates a new handle pointing to the same inner value.  This is an `Arc` clone — O(1)
    /// and allocation-free.
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// SAFETY: `T: Send` is required so the value itself may be sent across threads.  The caller
// is responsible for ensuring that concurrent access to `T` respects the disjoint-partition
// contract described on the struct.
unsafe impl<T: Send> Send for SemanticSync<T> {}
unsafe impl<T: Send + Sync> Sync for SemanticSync<T> {}

impl<T> SemanticSync<T> {
    /// Wraps `value`, making it accessible through any number of cheap clones.
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(UnsafeCell::new(Some(value))),
        }
    }

    /// Replaces the inner value.
    ///
    /// # Safety
    /// No other handle may be accessing the value (reading or writing) at the time of the call.
    pub fn set(&self, new_value: T) {
        unsafe {
            *self.inner.get() = Some(new_value);
        }
    }

    /// Returns a shared reference to the inner value.
    ///
    /// # Safety
    /// No mutable reference obtained via [`as_ref_mut`] may be alive at the same time, and the
    /// caller must not violate the disjoint-partition contract with other concurrent handles.
    ///
    /// # Panics
    /// Panics if the value has already been taken by [`into_inner`].
    ///
    /// [`as_ref_mut`]: SemanticSync::as_ref_mut
    /// [`into_inner`]: SemanticSync::into_inner
    pub fn as_ref(&self) -> &T {
        unsafe { (*self.inner.get()).as_ref().expect("value already taken") }
    }

    /// Returns a mutable reference to the inner value.
    ///
    /// # Safety
    /// The caller must have exclusive logical access to the portion of `T` it intends to
    /// modify — no other handle may be reading or writing the same region concurrently.
    ///
    /// # Panics
    /// Panics if the value has already been taken by [`into_inner`].
    ///
    /// [`into_inner`]: SemanticSync::into_inner
    pub fn as_ref_mut(&self) -> &mut T {
        unsafe { (*self.inner.get()).as_mut().expect("value already taken") }
    }

    /// Takes the inner value out, leaving `None` in its place.
    ///
    /// Returns `Some(T)` on the first call and `None` on any subsequent call.
    ///
    /// # Safety
    /// Must only be called once all other handles have been dropped (i.e., this is the sole
    /// `Arc` holder).  Calling this while another handle is still accessing the value is
    /// undefined behavior.
    pub fn into_inner(&self) -> Option<T> {
        unsafe { (*self.inner.get()).take() }
    }
}

impl<T> Deref for SemanticSync<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> DerefMut for SemanticSync<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_ref_mut()
    }
}

impl<T: fmt::Debug> fmt::Debug for SemanticSync<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            match &*self.inner.get() {
                Some(v) => write!(f, "{:?}", v),
                None => write!(f, "None"),
            }
        }
    }
}

impl<T: fmt::Display> fmt::Display for SemanticSync<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            match &*self.inner.get() {
                Some(v) => write!(f, "{}", v),
                None => write!(f, "None"),
            }
        }
    }
}

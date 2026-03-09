use std::cell::UnsafeCell;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub struct SemanticSync<T> {
    inner: Arc<UnsafeCell<Option<T>>>,
}

impl<T> Clone for SemanticSync<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl<T: Send> Send for SemanticSync<T> {}
unsafe impl<T: Send + Sync> Sync for SemanticSync<T> {}

impl<T> SemanticSync<T> {
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(UnsafeCell::new(Some(value))),
        }
    }

    pub fn set(&self, new_value: T) {
        unsafe {
            *self.inner.get() = Some(new_value);
        }
    }

    /// # Safety
    /// No mutable access while this reference is alive
    pub fn as_ref(&self) -> &T {
        unsafe { (*self.inner.get()).as_ref().expect("value already taken") }
    }

    /// # Safety
    /// Caller must ensure exclusive access
    pub fn as_ref_mut(&self) -> &mut T {
        unsafe { (*self.inner.get()).as_mut().expect("value already taken") }
    }

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

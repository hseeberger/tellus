use std::sync::{Mutex, MutexGuard, PoisonError};
#[cfg(feature = "cluster")]
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Recovers from poisoning: no guarded structure is left inconsistent by a contained panic.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(feature = "cluster")]
pub(crate) fn read<T>(rw_lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    rw_lock.read().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(feature = "cluster")]
pub(crate) fn write<T>(rw_lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    rw_lock.write().unwrap_or_else(PoisonError::into_inner)
}

//! # Storage Module
//! 
//! Este módulo proporciona las funcionalidades de almacenamiento para el motor
//! de base de datos SQLite, incluyendo el acceso a disco y la gestión de páginas.

pub mod disk;
pub mod pager;
pub mod cache;

// Re-exportar para facilitar el acceso
pub use disk::DiskManager;
pub use pager::Pager;
pub use cache::BufferPool;
//! # Utils Module
//! 
//! Este módulo contiene utilidades generales que se utilizan en toda la implementación
//! del motor de almacenamiento SQLite.

pub mod varint;
pub mod serialization;
pub mod cmp;

// Re-exportar para facilitar el acceso
pub use varint::{encode_varint, decode_varint, varint_size, MAX_VARINT_SIZE};
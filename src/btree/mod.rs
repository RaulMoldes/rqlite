//! # B-Tree Module
//! 
//! Este módulo implementa las estructuras y operaciones relacionadas con los árboles B-Tree
//! utilizados en SQLite para almacenar tablas e índices.

pub mod node;
pub mod cell;
pub mod record;
pub mod btree;

// Re-exportar para facilitar el acceso
pub use node::BTreeNode;
pub use cell::BTreeCellFactory;
pub use record::Record;
pub use btree::{BTree, TreeType};
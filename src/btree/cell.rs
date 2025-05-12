//! # B-Tree Cell Factory Module
//! 
//! Este módulo implementa una fábrica para crear celdas de B-Tree,
//! que son las unidades fundamentales de almacenamiento en un árbol B-Tree de SQLite.

use std::io;

use crate::page::{BTreeCell, TableLeafCell, TableInteriorCell, IndexLeafCell, IndexInteriorCell};
use crate::utils::serialization::SqliteValue;

/// Fábrica para crear celdas de B-Tree.
///
/// Esta estructura proporciona métodos para crear diferentes tipos de celdas
/// utilizadas en los árboles B-Tree de SQLite.
pub struct BTreeCellFactory;

impl BTreeCellFactory {
    /// Crea una nueva celda de tabla hoja.
    ///
    /// # Parámetros
    /// * `rowid` - ID de la fila (rowid).
    /// * `payload` - Datos de la celda.
    /// * `max_local_payload` - Tamaño máximo de payload que puede almacenarse localmente.
    /// * `min_local_payload` - Tamaño mínimo de payload que debe almacenarse localmente.
    /// * `page_size` - Tamaño de página en bytes.
    /// * `usable_size` - Tamaño de página utilizable (excluyendo encabezado y espacio reservado).
    ///
    /// # Errores
    /// Retorna un error si hay problemas al crear la celda.
    ///
    /// # Retorno
    /// Una tupla conteniendo:
    /// - La celda creada
    /// - Datos que no caben en la celda local y deben almacenarse en páginas de overflow (si los hay)
    pub fn create_table_leaf_cell(
        rowid: i64,
        payload: Vec<u8>,
        max_local_payload: usize,
        min_local_payload: usize,
        page_size: u32,
        usable_size: usize,
    ) -> io::Result<(BTreeCell, Option<Vec<u8>>)> {
        let payload_size = payload.len();
        
        // Determinar cuánto del payload se almacena localmente
        let local_payload_size = if payload_size <= max_local_payload {
            // Todo el payload cabe en la celda
            payload_size
        } else {
            // Calcular M (tamaño mínimo local)
            // Fórmula: min(X, usable_size - 35) / 4
            let m = min_local_payload.min((usable_size - 35) / 4);
            
            if payload_size <= m {
                // Almacenar todo localmente
                payload_size
            } else {
                // Almacenar parte localmente y parte en overflow
                m + ((payload_size - m) % (usable_size - 4))
            }
        };
        
        // Parte del payload que se almacena localmente
        let local_payload = payload[0..local_payload_size].to_vec();
        
        // Parte del payload que se almacena en overflow (si la hay)
        let overflow_payload = if local_payload_size < payload_size {
            Some(payload[local_payload_size..].to_vec())
        } else {
            None
        };
        
        // Crear la celda
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: payload_size as u64,
            row_id: rowid,
            payload: local_payload,
            overflow_page: None, // Se establecerá más tarde
        });
        
        Ok((cell, overflow_payload))
    }

    /// Crea una nueva celda de tabla interior.
    ///
    /// # Parámetros
    /// * `left_child_page` - Número de página del hijo izquierdo.
    /// * `key` - Clave (rowid) que define el límite entre los hijos izquierdo y derecho.
    ///
    /// # Retorno
    /// La celda creada.
    pub fn create_table_interior_cell(
        left_child_page: u32,
        key: i64,
    ) -> BTreeCell {
        BTreeCell::TableInterior(TableInteriorCell {
            left_child_page,
            key,
        })
    }

    /// Crea una nueva celda de índice hoja.
    ///
    /// # Parámetros
    /// * `payload` - Datos de la celda.
    /// * `max_local_payload` - Tamaño máximo de payload que puede almacenarse localmente.
    /// * `min_local_payload` - Tamaño mínimo de payload que debe almacenarse localmente.
    /// * `page_size` - Tamaño de página en bytes.
    /// * `usable_size` - Tamaño de página utilizable (excluyendo encabezado y espacio reservado).
    ///
    /// # Errores
    /// Retorna un error si hay problemas al crear la celda.
    ///
    /// # Retorno
    /// Una tupla conteniendo:
    /// - La celda creada
    /// - Datos que no caben en la celda local y deben almacenarse en páginas de overflow (si los hay)
    pub fn create_index_leaf_cell(
        payload: Vec<u8>,
        max_local_payload: usize,
        min_local_payload: usize,
        page_size: u32,
        usable_size: usize,
    ) -> io::Result<(BTreeCell, Option<Vec<u8>>)> {
        let payload_size = payload.len();
        
        // Determinar cuánto del payload se almacena localmente (similar a tabla hoja)
        let local_payload_size = if payload_size <= max_local_payload {
            payload_size
        } else {
            // Calcular M (tamaño mínimo local)
            let m = min_local_payload.min((usable_size - 35) / 4);
            
            if payload_size <= m {
                payload_size
            } else {
                m + ((payload_size - m) % (usable_size - 4))
            }
        };
        
        // Parte del payload que se almacena localmente
        let local_payload = payload[0..local_payload_size].to_vec();
        
        // Parte del payload que se almacena en overflow (si la hay)
        let overflow_payload = if local_payload_size < payload_size {
            Some(payload[local_payload_size..].to_vec())
        } else {
            None
        };
        
        // Crear la celda
        let cell = BTreeCell::IndexLeaf(IndexLeafCell {
            payload_size: payload_size as u64,
            payload: local_payload,
            overflow_page: None, // Se establecerá más tarde
        });
        
        Ok((cell, overflow_payload))
    }

    /// Crea una nueva celda de índice interior.
    ///
    /// # Parámetros
    /// * `left_child_page` - Número de página del hijo izquierdo.
    /// * `payload` - Datos de la celda.
    /// * `max_local_payload` - Tamaño máximo de payload que puede almacenarse localmente.
    /// * `min_local_payload` - Tamaño mínimo de payload que debe almacenarse localmente.
    /// * `page_size` - Tamaño de página en bytes.
    /// * `usable_size` - Tamaño de página utilizable (excluyendo encabezado y espacio reservado).
    ///
    /// # Errores
    /// Retorna un error si hay problemas al crear la celda.
    ///
    /// # Retorno
    /// Una tupla conteniendo:
    /// - La celda creada
    /// - Datos que no caben en la celda local y deben almacenarse en páginas de overflow (si los hay)
    pub fn create_index_interior_cell(
        left_child_page: u32,
        payload: Vec<u8>,
        max_local_payload: usize,
        min_local_payload: usize,
        page_size: u32,
        usable_size: usize,
    ) -> io::Result<(BTreeCell, Option<Vec<u8>>)> {
        let payload_size = payload.len();
        
        // Determinar cuánto del payload se almacena localmente (similar a tabla hoja)
        let local_payload_size = if payload_size <= max_local_payload {
            payload_size
        } else {
            // Calcular M (tamaño mínimo local)
            let m = min_local_payload.min((usable_size - 35) / 4);
            
            if payload_size <= m {
                payload_size
            } else {
                m + ((payload_size - m) % (usable_size - 4))
            }
        };
        
        // Parte del payload que se almacena localmente
        let local_payload = payload[0..local_payload_size].to_vec();
        
        // Parte del payload que se almacena en overflow (si la hay)
        let overflow_payload = if local_payload_size < payload_size {
            Some(payload[local_payload_size..].to_vec())
        } else {
            None
        };
        
        // Crear la celda
        let cell = BTreeCell::IndexInterior(IndexInteriorCell {
            left_child_page,
            payload_size: payload_size as u64,
            payload: local_payload,
            overflow_page: None, // Se establecerá más tarde
        });
        
        Ok((cell, overflow_payload))
    }

    /// Calcula el tamaño máximo de payload que puede almacenarse localmente.
    ///
    /// # Parámetros
    /// * `usable_size` - Tamaño utilizable de la página.
    /// * `max_payload_fraction` - Fracción máxima de una página que puede ocupar una celda.
    ///
    /// # Retorno
    /// Tamaño máximo en bytes.
    pub fn max_local_payload(usable_size: usize, max_payload_fraction: u8) -> usize {
        let max_fraction = (usable_size - 12) * max_payload_fraction as usize / 255;
        let absolute_max = usable_size - 35;
        max_fraction.min(absolute_max)
    }

    /// Calcula el tamaño mínimo de payload que debe almacenarse localmente.
    ///
    /// # Parámetros
    /// * `usable_size` - Tamaño utilizable de la página.
    /// * `min_payload_fraction` - Fracción mínima de una página que debe ocupar una celda.
    ///
    /// # Retorno
    /// Tamaño mínimo en bytes.
    pub fn min_local_payload(usable_size: usize, min_payload_fraction: u8) -> usize {
        // Fórmula: (usable_size - 12) * X / 255
        (usable_size - 12) * min_payload_fraction as usize / 255
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_interior_cell() {
        let cell = BTreeCellFactory::create_table_interior_cell(42, 123);
        
        match cell {
            BTreeCell::TableInterior(interior_cell) => {
                assert_eq!(interior_cell.left_child_page, 42);
                assert_eq!(interior_cell.key, 123);
            },
            _ => panic!("Tipo de celda incorrecto"),
        }
    }

    #[test]
    fn test_create_table_leaf_cell_small_payload() {
        let payload = vec![1, 2, 3, 4, 5];
        let max_local = 100;
        let min_local = 32;
        let page_size = 4096;
        let usable_size = 4000;
        
        let (cell, overflow) = BTreeCellFactory::create_table_leaf_cell(
            42, payload.clone(), max_local, min_local, page_size, usable_size
        ).unwrap();
        
        match cell {
            BTreeCell::TableLeaf(leaf_cell) => {
                assert_eq!(leaf_cell.row_id, 42);
                assert_eq!(leaf_cell.payload_size as usize, payload.len());
                assert_eq!(leaf_cell.payload, payload);
                assert_eq!(leaf_cell.overflow_page, None);
            },
            _ => panic!("Tipo de celda incorrecto"),
        }
        
        assert_eq!(overflow, None);
    }

    #[test]
    fn test_create_table_leaf_cell_large_payload() {
        let payload = vec![0; 5000]; // Payload grande que no cabe en una celda
        let max_local = 1000;
        let min_local = 32;
        let page_size = 4096;
        let usable_size = 4000;
        
        let (cell, overflow) = BTreeCellFactory::create_table_leaf_cell(
            42, payload.clone(), max_local, min_local, page_size, usable_size
        ).unwrap();
        
        match cell {
            BTreeCell::TableLeaf(leaf_cell) => {
                assert_eq!(leaf_cell.row_id, 42);
                assert_eq!(leaf_cell.payload_size as usize, payload.len());
                assert!(leaf_cell.payload.len() < payload.len()); // Solo parte del payload está en la celda
                assert_eq!(leaf_cell.overflow_page, None);
            },
            _ => panic!("Tipo de celda incorrecto"),
        }
        
        assert!(overflow.is_some());
        let overflow_data = overflow.unwrap();
        assert!(!overflow_data.is_empty());
        
        // Verificar que la longitud combinada es igual a la longitud original
        let local_length = match &cell {
            BTreeCell::TableLeaf(leaf_cell) => leaf_cell.payload.len(),
            _ => panic!("Tipo de celda incorrecto"),
        };
        
        assert_eq!(local_length + overflow_data.len(), payload.len());
    }

    #[test]
    fn test_max_min_local_payload() {
        let usable_size = 4000;
        
        // max_payload_fraction = 255 (100%)
        let max_local = BTreeCellFactory::max_local_payload(usable_size, 255);
        assert_eq!(max_local, usable_size - 35);
        
        // max_payload_fraction = 64 (25%)
        let max_local = BTreeCellFactory::max_local_payload(usable_size, 64);
        let expected = (usable_size - 12) * 64 / 255;
        assert_eq!(max_local, expected);
        
        // min_payload_fraction = 32 (12.5%)
        let min_local = BTreeCellFactory::min_local_payload(usable_size, 32);
        let expected = (usable_size - 12) * 32 / 255;
        assert_eq!(min_local, expected);
    }
}
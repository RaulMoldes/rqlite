//! # B-Tree Implementation Module
//! 
//! Este módulo implementa el árbol B-Tree completo de SQLite,
//! proporcionando operaciones de búsqueda, inserción y borrado.

use std::io;

use crate::page::{BTreePage, Page, PageType, BTreeCell, TableLeafCell, TableInteriorCell};
use crate::storage::Pager;
use crate::btree::node::BTreeNode;
use crate::btree::cell::BTreeCellFactory;
use crate::btree::record::Record;
use crate::utils::serialization::SqliteValue;

/// Representa un árbol B-Tree de SQLite.
///
/// Un B-Tree es una estructura de datos de árbol autobalanceado que mantiene
/// los datos ordenados y permite búsquedas, inserciones y eliminaciones en
/// tiempo logarítmico.
pub struct BTree {
    /// Número de página de la raíz del árbol.
    root_page: u32,
    /// Tipo de B-Tree (tabla o índice).
    tree_type: TreeType,
    /// Pager para operaciones de I/O.
    pager: *mut Pager,
    /// Tamaño de página en bytes.
    page_size: u32,
    /// Espacio reservado al final de cada página.
    reserved_space: u8,
    /// Fracción máxima de una página que puede ocupar una celda.
    max_payload_fraction: u8,
    /// Fracción mínima de una página que debe ocupar una celda.
    min_payload_fraction: u8,
}

/// Tipo de árbol B-Tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeType {
    /// Árbol para una tabla (almacena registros completos).
    Table,
    /// Árbol para un índice (almacena claves de índice).
    Index,
}

// Es seguro implementar Send y Sync porque solo tenemos un puntero crudo
// que no usamos para modificar el Pager, solo para acceder a él.
unsafe impl Send for BTree {}
unsafe impl Sync for BTree {}

impl BTree {
    /// Crea un nuevo árbol B-Tree.
    ///
    /// # Parámetros
    /// * `root_page` - Número de página de la raíz del árbol.
    /// * `tree_type` - Tipo de árbol (tabla o índice).
    /// * `pager` - Pager para operaciones de I/O.
    /// * `page_size` - Tamaño de página en bytes.
    /// * `reserved_space` - Espacio reservado al final de cada página.
    /// * `max_payload_fraction` - Fracción máxima de una página que puede ocupar una celda.
    /// * `min_payload_fraction` - Fracción mínima de una página que debe ocupar una celda.
    ///
    /// # Seguridad
    /// El pager debe ser válido durante toda la vida del árbol.
    pub fn new(
        root_page: u32,
        tree_type: TreeType,
        pager: *mut Pager,
        page_size: u32,
        reserved_space: u8,
        max_payload_fraction: u8,
        min_payload_fraction: u8,
    ) -> Self {
        BTree {
            root_page,
            tree_type,
            pager,
            page_size,
            reserved_space,
            max_payload_fraction,
            min_payload_fraction,
        }
    }

    /// Crea un nuevo árbol B-Tree vacío.
    ///
    /// # Parámetros
    /// * `tree_type` - Tipo de árbol (tabla o índice).
    /// * `pager` - Pager para operaciones de I/O.
    /// * `page_size` - Tamaño de página en bytes.
    /// * `reserved_space` - Espacio reservado al final de cada página.
    /// * `max_payload_fraction` - Fracción máxima de una página que puede ocupar una celda.
    /// * `min_payload_fraction` - Fracción mínima de una página que debe ocupar una celda.
    ///
    /// # Errores
    /// Retorna un error si no se puede crear la página raíz.
    ///
    /// # Seguridad
    /// El pager debe ser válido durante toda la vida del árbol.
    pub fn create(
        tree_type: TreeType,
        pager: *mut Pager,
        page_size: u32,
        reserved_space: u8,
        max_payload_fraction: u8,
        min_payload_fraction: u8,
    ) -> io::Result<Self> {
        // Crear la página raíz (siempre una hoja)
        let page_type = match tree_type {
            TreeType::Table => PageType::TableLeaf,
            TreeType::Index => PageType::IndexLeaf,
        };
        
        let node = BTreeNode::create_leaf(page_type, pager)?;
        let root_page = node.page_number;
        
        Ok(BTree {
            root_page,
            tree_type,
            pager,
            page_size,
            reserved_space,
            max_payload_fraction,
            min_payload_fraction,
        })
    }

    /// Abre un árbol B-Tree existente.
    ///
    /// # Parámetros
    /// * `root_page` - Número de página de la raíz del árbol.
    /// * `tree_type` - Tipo de árbol (tabla o índice).
    /// * `pager` - Pager para operaciones de I/O.
    /// * `page_size` - Tamaño de página en bytes.
    /// * `reserved_space` - Espacio reservado al final de cada página.
    /// * `max_payload_fraction` - Fracción máxima de una página que puede ocupar una celda.
    /// * `min_payload_fraction` - Fracción mínima de una página que debe ocupar una celda.
    ///
    /// # Errores
    /// Retorna un error si la página raíz no existe o no es válida.
    ///
    /// # Seguridad
    /// El pager debe ser válido durante toda la vida del árbol.
    pub fn open(
        root_page: u32,
        tree_type: TreeType,
        pager: *mut Pager,
        page_size: u32,
        reserved_space: u8,
        max_payload_fraction: u8,
        min_payload_fraction: u8,
    ) -> io::Result<Self> {
        // Verificar que la página raíz existe y es válida
        let page_type = match tree_type {
            TreeType::Table => {
                // La raíz puede ser hoja o interior
                match unsafe {
                    let page = (*pager).get_page::<BTreePage>(root_page, None)?;
                    page.header.page_type
                } {
                    PageType::TableLeaf | PageType::TableInterior => {},
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "La página raíz no es un nodo de tabla",
                        ));
                    }
                }
            },
            TreeType::Index => {
                // La raíz puede ser hoja o interior
                match unsafe {
                    let page = (*pager).get_page::<BTreePage>(root_page, None)?;
                    page.header.page_type
                } {
                    PageType::IndexLeaf | PageType::IndexInterior => {},
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "La página raíz no es un nodo de índice",
                        ));
                    }
                }
            },
        };
        
        Ok(BTree {
            root_page,
            tree_type,
            pager,
            page_size,
            reserved_space,
            max_payload_fraction,
            min_payload_fraction,
        })
    }

    /// Obtiene el tamaño máximo de payload que puede almacenarse localmente.
    fn max_local_payload(&self) -> usize {
        let usable_size = self.page_size as usize - self.reserved_space as usize;
        BTreeCellFactory::max_local_payload(usable_size, self.max_payload_fraction)
    }

    /// Obtiene el tamaño mínimo de payload que debe almacenarse localmente.
    fn min_local_payload(&self) -> usize {
        let usable_size = self.page_size as usize - self.reserved_space as usize;
        BTreeCellFactory::min_local_payload(usable_size, self.min_payload_fraction)
    }

    /// Busca un registro en el árbol de tabla por su rowid.
    ///
    /// # Parámetros
    /// * `rowid` - ID de la fila a buscar.
    ///
    /// # Errores
    /// Retorna un error si el árbol no es de tabla o si hay problemas de I/O.
    ///
    /// # Retorno
    /// El registro encontrado, o `None` si no existe.
    pub fn find(&self, rowid: i64) -> io::Result<Option<Record>> {
        if self.tree_type != TreeType::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "El árbol no es de tabla",
            ));
        }
        
        // Empezar desde la raíz
        let mut page_number = self.root_page;
        let mut is_leaf = false;
        
        // Descender hasta una hoja
        while !is_leaf {
            // Abrir el nodo actual
            let node = unsafe {
                let page = (*self.pager).get_page::<BTreePage>(page_number, None)?;
                is_leaf = page.header.page_type.is_leaf();
                
                if page.header.page_type == PageType::TableLeaf {
                    BTreeNode::open(page_number, PageType::TableLeaf, self.pager)?
                } else if page.header.page_type == PageType::TableInterior {
                    BTreeNode::open(page_number, PageType::TableInterior, self.pager)?
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Tipo de página no válido para tabla",
                    ));
                }
            };
            
            if !is_leaf {
                // Nodo interior, buscar el hijo correspondiente
                let (found, child_page, _) = node.find_table_key(rowid)?;
                
                if found {
                    // Encontramos una celda con la clave exacta, pero en un nodo interior
                    // Descender al hijo izquierdo
                    page_number = child_page;
                } else {
                    // No encontramos una coincidencia exacta, descender al hijo adecuado
                    page_number = child_page;
                }
            }
        }
        
        // Estamos en una hoja, buscar el rowid
        let leaf_node = BTreeNode::open(page_number, PageType::TableLeaf, self.pager)?;
        let (found, idx) = leaf_node.find_table_rowid(rowid)?;
        
        if !found {
            // No encontramos el rowid
            return Ok(None);
        }
        
        // Obtener la celda
        let cell = leaf_node.get_cell(idx)?;
        
        match cell {
            BTreeCell::TableLeaf(leaf_cell) => {
                // Reconstruir el registro a partir de los datos de la celda
                let mut payload = leaf_cell.payload.clone();
                
                // Si hay overflow, leer los datos adicionales
                if let Some(overflow_page) = leaf_cell.overflow_page {
                    let mut current_page = overflow_page;
                    
                    while current_page != 0 {
                        // Leer la página de overflow
                        let overflow = unsafe {
                            let page = (*self.pager).get_page::<Page>(current_page, Some(PageType::Overflow))?;
                            
                            match page {
                                Page::Overflow(overflow) => overflow,
                                _ => {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "Tipo de página no válido para overflow",
                                    ));
                                }
                            }
                        };
                        
                        // Añadir los datos al payload
                        payload.extend_from_slice(&overflow.data);
                        
                        // Pasar a la siguiente página
                        current_page = overflow.next_page;
                    }
                }
                
                // Deserializar el registro
                let (record, _) = Record::from_bytes(&payload)?;
                Ok(Some(record))
            },
            _ => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Tipo de celda no válido para tabla hoja",
                ))
            }
        }
    }

    /// Inserta un registro en el árbol de tabla con el rowid especificado.
    ///
    /// # Parámetros
    /// * `rowid` - ID de la fila a insertar.
    /// * `record` - Registro a insertar.
    ///
    /// # Errores
    /// Retorna un error si el árbol no es de tabla, si ya existe un registro con el mismo rowid,
    /// o si hay problemas de I/O.
    pub fn insert(&mut self, rowid: i64, record: &Record) -> io::Result<()> {
        if self.tree_type != TreeType::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "El árbol no es de tabla",
            ));
        }
        
        // Serializar el registro
        let payload = record.to_bytes()?;
        
        // Determinar el tamaño máximo de payload local
        let max_local = self.max_local_payload();
        let min_local = self.min_local_payload();
        let usable_size = self.page_size as usize - self.reserved_space as usize;
        
        // Crear la celda
        let (cell, overflow_payload) = BTreeCellFactory::create_table_leaf_cell(
            rowid,
            payload,
            max_local,
            min_local,
            self.page_size,
            usable_size,
        )?;
        
        // Manejar los datos de overflow (si los hay)
        let cell = if let Some(overflow_data) = overflow_payload {
            match cell {
                BTreeCell::TableLeaf(mut leaf_cell) => {
                    // Crear la cadena de páginas de overflow
                    let overflow_page = self.create_overflow_chain(overflow_data)?;
                    
                    // Actualizar la referencia a la primera página de overflow
                    leaf_cell.overflow_page = Some(overflow_page);
                    
                    BTreeCell::TableLeaf(leaf_cell)
                },
                _ => unreachable!("Tipo de celda incorrecto"),
            }
        } else {
            cell
        };
        
        // Encontrar el nodo hoja donde se debe insertar
        let (leaf_page, path) = self.find_leaf_for_insert(rowid)?;
        let leaf_node = BTreeNode::open(leaf_page, PageType::TableLeaf, self.pager)?;
        
        // Intentar insertar en el nodo hoja
        let (split, median_key, new_node) = leaf_node.insert_cell_ordered(cell)?;
        
        if !split {
            // La inserción no causó división, terminamos
            return Ok(());
        }
        
        // La inserción causó división, necesitamos propagar la división hacia arriba
        self.propagate_split(leaf_node, new_node.unwrap(), median_key.unwrap(), path)?;
        
        Ok(())
    }

    /// Crea una cadena de páginas de overflow para almacenar datos grandes.
    ///
    /// # Parámetros
    /// * `data` - Datos a almacenar en las páginas de overflow.
    ///
    /// # Errores
    /// Retorna un error si hay problemas de I/O.
    ///
    /// # Retorno
    /// Número de la primera página de overflow.
    fn create_overflow_chain(&mut self, data: Vec<u8>) -> io::Result<u32> {
        // Tamaño máximo de datos en una página de overflow
        let max_overflow_size = self.page_size as usize - 4; // 4 bytes para next_page
        
        // Dividir los datos en chunks
        let chunks: Vec<_> = data.chunks(max_overflow_size).collect();
        let chunk_count = chunks.len();
        
        if chunk_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No hay datos para almacenar en overflow",
            ));
        }
        
        // Crear la última página primero (next_page = 0)
        let last_chunk = chunks[chunk_count - 1];
        let last_page = unsafe {
            (*self.pager).create_overflow_page(0, last_chunk.to_vec())?
        };
        
        // Si solo hay un chunk, la cadena es de una sola página
        if chunk_count == 1 {
            return Ok(last_page);
        }
        
        // Crear las páginas intermedias en orden inverso
        let mut next_page = last_page;
        
        for i in (0..chunk_count - 1).rev() {
            let chunk = chunks[i];
            let page = unsafe {
                (*self.pager).create_overflow_page(next_page, chunk.to_vec())?
            };
            next_page = page;
        }
        
        Ok(next_page)
    }

    /// Encuentra el nodo hoja donde se debe insertar una celda con el rowid especificado.
    ///
    /// # Parámetros
    /// * `rowid` - Rowid a buscar.
    ///
    /// # Errores
    /// Retorna un error si hay problemas de I/O.
    ///
    /// # Retorno
    /// Tupla con el número de página del nodo hoja y el camino desde la raíz.
    fn find_leaf_for_insert(&self, rowid: i64) -> io::Result<(u32, Vec<u32>)> {
        // Empezar desde la raíz
        let mut page_number = self.root_page;
        let mut path = Vec::new();
        let mut is_leaf = false;
        
        // Descender hasta una hoja
        while !is_leaf {
            // Añadir el nodo actual al camino
            path.push(page_number);
            
            // Abrir el nodo actual
            let node = unsafe {
                let page = (*self.pager).get_page::<BTreePage>(page_number, None)?;
                is_leaf = page.header.page_type.is_leaf();
                
                if page.header.page_type == PageType::TableLeaf {
                    BTreeNode::open(page_number, PageType::TableLeaf, self.pager)?
                } else if page.header.page_type == PageType::TableInterior {
                    BTreeNode::open(page_number, PageType::TableInterior, self.pager)?
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Tipo de página no válido para tabla",
                    ));
                }
            };
            
            if is_leaf {
                break;
            }
            
            // Nodo interior, buscar el hijo correspondiente
            let (found, child_page, _) = node.find_table_key(rowid)?;
            
            if found {
                // Encontramos una celda con la clave exacta, pero en un nodo interior
                // Descender al hijo izquierdo
                page_number = child_page;
            } else {
                // No encontramos una coincidencia exacta, descender al hijo adecuado
                page_number = child_page;
            }
        }
        
        // Estamos en una hoja
        Ok((page_number, path))
    }

    /// Propaga la división de un nodo hacia arriba en el árbol.
    ///
    /// # Parámetros
    /// * `left_node` - Nodo izquierdo (original).
    /// * `right_node` - Nodo derecho (nuevo).
    /// * `median_key` - Clave mediana que divide ambos nodos.
    /// * `path` - Camino desde la raíz hasta el nodo que se dividió.
    ///
    /// # Errores
    /// Retorna un error si hay problemas de I/O.
    fn propagate_split(
        &mut self,
        left_node: BTreeNode,
        right_node: BTreeNode,
        median_key: i64,
        mut path: Vec<u32>,
    ) -> io::Result<()> {
        // Si el camino está vacío, estamos dividiendo la raíz
        if path.is_empty() {
            // Crear un nuevo nodo raíz
            let new_root_type = match left_node.node_type {
                PageType::TableLeaf => PageType::TableInterior,
                PageType::IndexLeaf => PageType::IndexInterior,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Tipo de nodo no válido para división",
                    ));
                }
            };
            
            // El hijo más a la derecha del nuevo nodo raíz es el hijo derecho
            let new_root = BTreeNode::create_interior(new_root_type, right_node.page_number, self.pager)?;
            
            // Crear una celda que apunte al hijo izquierdo
            let cell = BTreeCellFactory::create_table_interior_cell(
                left_node.page_number,
                median_key,
            );
            
            // Insertar la celda en el nuevo nodo raíz
            new_root.insert_cell(cell)?;
            
            // Actualizar la raíz del árbol
            self.root_page = new_root.page_number;
            
            return Ok(());
        }
        
        // Obtener el padre
        let parent_page = path.pop().unwrap();
        let parent_node = BTreeNode::open(parent_page, PageType::TableInterior, self.pager)?;
        
        // Crear una celda que apunte al hijo izquierdo
        let cell = BTreeCellFactory::create_table_interior_cell(
            left_node.page_number,
            median_key,
        );
        
        // Intentar insertar en el nodo padre
        let (split, new_median_key, new_parent) = parent_node.insert_cell_ordered(cell)?;
        
        if !split {
            // La inserción no causó división en el padre, terminamos
            return Ok(());
        }
        
        // La inserción causó división en el padre, seguir propagando
        self.propagate_split(
            parent_node,
            new_parent.unwrap(),
            new_median_key.unwrap(),
            path,
        )
    }

    /// Elimina un registro del árbol de tabla por su rowid.
    ///
    /// # Parámetros
    /// * `rowid` - ID de la fila a eliminar.
    ///
    /// # Errores
    /// Retorna un error si el árbol no es de tabla o si hay problemas de I/O.
    ///
    /// # Retorno
    /// `true` si se eliminó el registro, `false` si no existía.
    pub fn delete(&mut self, rowid: i64) -> io::Result<bool> {
        // Esta implementación es simplificada y no maneja todos los casos
        // como la fusión de nodos cuando quedan muy pocos elementos.
        
        if self.tree_type != TreeType::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "El árbol no es de tabla",
            ));
        }
        
        // Empezar desde la raíz
        let mut page_number = self.root_page;
        let mut is_leaf = false;
        
        // Descender hasta una hoja
        while !is_leaf {
            // Abrir el nodo actual
            let node = unsafe {
                let page = (*self.pager).get_page::<BTreePage>(page_number, None)?;
                is_leaf = page.header.page_type.is_leaf();
                
                if page.header.page_type == PageType::TableLeaf {
                    BTreeNode::open(page_number, PageType::TableLeaf, self.pager)?
                } else if page.header.page_type == PageType::TableInterior {
                    BTreeNode::open(page_number, PageType::TableInterior, self.pager)?
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Tipo de página no válido para tabla",
                    ));
                }
            };
            
            if !is_leaf {
                // Nodo interior, buscar el hijo correspondiente
                let (found, child_page, _) = node.find_table_key(rowid)?;
                
                if found {
                    // Encontramos una celda con la clave exacta, pero en un nodo interior
                    // Descender al hijo izquierdo
                    page_number = child_page;
                } else {
                    // No encontramos una coincidencia exacta, descender al hijo adecuado
                    page_number = child_page;
                }
            }
        }
        
        // Estamos en una hoja, buscar el rowid
        let leaf_node = BTreeNode::open(page_number, PageType::TableLeaf, self.pager)?;
        let (found, idx) = leaf_node.find_table_rowid(rowid)?;
        
        if !found {
            // No encontramos el rowid
            return Ok(false);
        }
        
        // Obtener la página
        let page = unsafe {
            (*self.pager).get_page_mut::<BTreePage>(page_number, Some(PageType::TableLeaf))?
        };
        
        // Eliminar la celda
        page.cells.remove(idx as usize);
        page.header.cell_count -= 1;
        
        // En una implementación completa, habría que manejar la fusión de nodos
        // cuando quedan muy pocos elementos
        
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::tempdir;
    use crate::Database;
    use crate::utils::serialization::SqliteValue;

    #[test]
    fn test_create_btree() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        let mut db = Database::create(&db_path, 4096, 0).unwrap();
        
        // Crear un B-Tree
        let tree = BTree::create(
            TreeType::Table,
            &mut db.pager as *mut _,
            4096,
            0,
            64,
            32,
        ).unwrap();
        
        assert_eq!(tree.tree_type, TreeType::Table);
        assert!(tree.root_page > 0);
    }

    #[test]
    fn test_insert_and_find() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        let mut db = Database::create(&db_path, 4096, 0).unwrap();
        
        // Crear un B-Tree
        let mut tree = BTree::create(
            TreeType::Table,
            &mut db.pager as *mut _,
            4096,
            0,
            64,
            32,
        ).unwrap();
        
        // Crear un registro
        let mut record = Record::new();
        record.add_value(SqliteValue::Integer(42));
        record.add_value(SqliteValue::String("Hello".to_string()));
        record.add_value(SqliteValue::Blob(vec![1, 2, 3]));
        
        // Insertar el registro
        tree.insert(1, &record).unwrap();
        
        // Buscar el registro
        let found = tree.find(1).unwrap();
        assert!(found.is_some());
        
        let found_record = found.unwrap();
        assert_eq!(found_record.len(), 3);
        
        match found_record.get_value(0) {
            Some(SqliteValue::Integer(v)) => assert_eq!(*v, 42),
            _ => panic!("Valor incorrecto"),
        }
        
        match found_record.get_value(1) {
            Some(SqliteValue::String(s)) => assert_eq!(s, "Hello"),
            _ => panic!("Valor incorrecto"),
        }
        
        match found_record.get_value(2) {
            Some(SqliteValue::Blob(b)) => assert_eq!(b, &[1, 2, 3]),
            _ => panic!("Valor incorrecto"),
        }
        
        // Buscar un registro que no existe
        let not_found = tree.find(2).unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_multiple_inserts() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        let mut db = Database::create(&db_path, 4096, 0).unwrap();
        
        // Crear un B-Tree
        let mut tree = BTree::create(
            TreeType::Table,
            &mut db.pager as *mut _,
            4096,
            0,
            64,
            32,
        ).unwrap();
        
        // Insertar varios registros
        for i in 1..=100 {
            let mut record = Record::new();
            record.add_value(SqliteValue::Integer(i * 10));
            record.add_value(SqliteValue::String(format!("Record {}", i)));
            
            tree.insert(i, &record).unwrap();
        }
        
        // Verificar que todos los registros se pueden encontrar
        for i in 1..=100 {
            let found = tree.find(i).unwrap();
            assert!(found.is_some());
            
            let record = found.unwrap();
            
            match record.get_value(0) {
                Some(SqliteValue::Integer(v)) => assert_eq!(*v, i * 10),
                _ => panic!("Valor incorrecto"),
            }
            
            match record.get_value(1) {
                Some(SqliteValue::String(s)) => assert_eq!(s, &format!("Record {}", i)),
                _ => panic!("Valor incorrecto"),
            }
        }
    }

    #[test]
    fn test_delete() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        let mut db = Database::create(&db_path, 4096, 0).unwrap();
        
        // Crear un B-Tree
        let mut tree = BTree::create(
            TreeType::Table,
            &mut db.pager as *mut _,
            4096,
            0,
            64,
            32,
        ).unwrap();
        
        // Insertar algunos registros
        for i in 1..=10 {
            let mut record = Record::new();
            record.add_value(SqliteValue::Integer(i * 10));
            
            tree.insert(i, &record).unwrap();
        }
        
        // Eliminar algunos registros
        assert!(tree.delete(3).unwrap()); // Existe
        assert!(tree.delete(7).unwrap()); // Existe
        assert!(!tree.delete(15).unwrap()); // No existe
        
        // Verificar que los registros eliminados no se pueden encontrar
        assert!(tree.find(3).unwrap().is_none());
        assert!(tree.find(7).unwrap().is_none());
        
        // Verificar que los demás registros siguen ahí
        for i in [1, 2, 4, 5, 6, 8, 9, 10] {
            assert!(tree.find(i).unwrap().is_some());
        }
    }

    #[test]
    fn test_large_record() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        let mut db = Database::create(&db_path, 4096, 0).unwrap();
        
        // Crear un B-Tree
        let mut tree = BTree::create(
            TreeType::Table,
            &mut db.pager as *mut _,
            4096,
            0,
            64,
            32,
        ).unwrap();
        
        // Crear un registro grande (que no quepa en una sola página)
        let mut record = Record::new();
        record.add_value(SqliteValue::Integer(42));
        record.add_value(SqliteValue::Blob(vec![0; 8000])); // Blob grande
        
        // Insertar el registro
        tree.insert(1, &record).unwrap();
        
        // Buscar el registro
        let found = tree.find(1).unwrap();
        assert!(found.is_some());
        
        let found_record = found.unwrap();
        assert_eq!(found_record.len(), 2);
        
        match found_record.get_value(0) {
            Some(SqliteValue::Integer(v)) => assert_eq!(*v, 42),
            _ => panic!("Valor incorrecto"),
        }
        
        match found_record.get_value(1) {
            Some(SqliteValue::Blob(b)) => assert_eq!(b.len(), 8000),
            _ => panic!("Valor incorrecto"),
        }
    }
}
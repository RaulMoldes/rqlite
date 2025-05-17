//! # B-Tree Node Module
//! 
//! This module defines the `BTreeNode` struct and its associated methods.
//! It provides functionality for creating, opening, and manipulating B-Tree nodes.
//!

use std::io;

use crate::page::{BTreePage, PageType, Page, BTreeCell};
use crate::storage::Pager;

/// Represents a B-Tree node.
///
/// A mode in a B-Tree can be either an interior node (which contains keys and child pointers)
/// or a leaf node (which contains actual data). See `PageType` for more details.
pub struct BTreeNode {
    /// Number of the page where the node is stored.
    pub page_number: u32,
    /// Type of the node (interior or leaf).
    pub node_type: PageType,
    /// Mutable reference to the pager for I/O operations.
    /// This could be an intelligent pointer to make it thread-safe and accessible by multiple workers, but let's keep it simple for now.
    pager: *mut Pager,
}

// It is safe to send and sync this struct across threads as long as the raw pointer to the pager is valid.
unsafe impl Send for BTreeNode {}
unsafe impl Sync for BTreeNode {}

impl BTreeNode {
    /// Creates a new B-Tree node.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página donde se almacena el nodo.
    /// * `node_type` - Tipo de nodo.
    /// * `pager` - Referencia al pager para operaciones de I/O.
    ///
    /// # Seguridad
    /// El pager debe ser válido durante toda la vida del nodo.
    pub fn new(page_number: u32, node_type: PageType, pager: *mut Pager) -> Self {
        BTreeNode {
            page_number,
            node_type,
            pager,
        }
    }

     /// Abre un nodo B-Tree existente.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página donde se almacena el nodo.
    /// * `node_type` - Tipo esperado del nodo.
    /// * `pager` - Referencia al pager para operaciones de I/O.
    ///
    /// # Errores
    /// Retorna un error si la página no existe, no se puede leer, o si el tipo no coincide.
    ///
    /// # Seguridad
    /// El pager debe ser válido durante toda la vida del nodo.
    pub fn open(page_number: u32, node_type: PageType, pager: *mut Pager) -> io::Result<Self> {
        // Verificar que la página existe y es del tipo correcto
        unsafe {
            let page = (*pager).get_page(page_number, Some(node_type))?;
            
            match page {
                Page::BTree(btree_page) => {
                    if btree_page.header.page_type != node_type {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Tipo de página incorrecto: esperado {:?}, obtenido {:?}",
                                node_type, btree_page.header.page_type),
                        ));
                    }
                },
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "La página no es de tipo BTree",
                    ));
                }
            }
        }
        
        // Crear un nuevo nodo B-Tree con la página existente
        Ok(BTreeNode {
            page_number,
            node_type,
            pager,
        })
    }
   
    /// Obtiene el número de celdas en el nodo.
    ///
    /// # Errores
    /// Retorna un error si hay problemas de I/O.
    pub fn cell_count(&self) -> io::Result<u16> {
        let page = self.get_page()?;
        Ok(page.header.cell_count)
    }

    // Obtiene la página B-Tree asociada a este nodo.
    ///
    /// # Errores
    /// Retorna un error si hay problemas de I/O.
    fn get_page(&self) -> io::Result<&BTreePage> {
        unsafe {
            match (*self.pager).get_page(self.page_number, Some(self.node_type))? {
                Page::BTree(btree_page) => Ok(btree_page),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "La página no es de tipo BTree",
                )),
            }
        }
    }

    /// Obtiene la página B-Tree asociada a este nodo para modificación.
    ///
    /// # Errores
    /// Retorna un error si hay problemas de I/O.
    fn get_page_mut(&self) -> io::Result<&mut BTreePage> {
        unsafe {
            match (*self.pager).get_page_mut(self.page_number, Some(self.node_type))? {
                Page::BTree(btree_page) => Ok(btree_page),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "La página no es de tipo BTree",
                )),
            }
        }
    }
    
    // Crea un nuevo nodo B-Tree hoja.
    ///
    /// # Parámetros
    /// * `node_type` - Tipo de nodo hoja (TableLeaf o IndexLeaf).
    /// * `pager` - Referencia al pager para operaciones de I/O.
    ///
    /// # Errores
    /// Retorna un error si no se puede crear la página o si el tipo no es una hoja.
    ///
    /// # Seguridad
    /// El pager debe ser válido durante toda la vida del nodo.
    pub fn create_leaf(node_type: PageType, pager: *mut Pager) -> io::Result<Self> {
        if !node_type.is_leaf() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("El tipo de página no es una hoja: {:?}", node_type),
            ));
        }
        
        let page_number = unsafe {
            (*pager).create_btree_page(node_type, None)?
        };
        
        Ok(BTreeNode {
            page_number,
            node_type,
            pager,
        })
    }

    /// Crea un nuevo nodo B-Tree interior.
    ///
    /// # Parámetros
    /// * `node_type` - Tipo de nodo interior (TableInterior o IndexInterior).
    /// * `right_most_page` - Número de página del hijo más a la derecha.
    /// * `pager` - Referencia al pager para operaciones de I/O.
    ///
    /// # Errores
    /// Retorna un error si no se puede crear la página o si el tipo no es interior.
    ///
    /// # Seguridad
    /// El pager debe ser válido durante toda la vida del nodo.
    pub fn create_interior(node_type: PageType, right_most_page: u32, pager: *mut Pager) -> io::Result<Self> {
        if !node_type.is_interior() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("El tipo de página no es interior: {:?}", node_type),
            ));
        }
        
        let page_number = unsafe {
            (*pager).create_btree_page(node_type, Some(right_most_page))?
        };
        
        Ok(BTreeNode {
            page_number,
            node_type,
            pager,
        })
    }

    /// Obtiene una celda del nodo.
    ///
    /// # Parámetros
    /// * `index` - Índice de la celda (comenzando desde 0).
    ///
    /// # Errores
    /// Retorna un error si el índice está fuera de rango o si hay problemas de I/O.
    ///
    /// # Retorno
    /// Referencia a la celda.
    pub fn get_cell(&self, index: u16) -> io::Result<&BTreeCell> {
        let page = self.get_page()?;
        
        if index >= page.header.cell_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Índice de celda fuera de rango: {}, máximo {}", index, page.header.cell_count - 1),
            ));
        }
        
        Ok(&page.cells[index as usize])
    }

    /// Obtiene una celda del nodo para modificación.
    ///
    /// # Parámetros
    /// * `index` - Índice de la celda (comenzando desde 0).
    ///
    /// # Errores
    /// Retorna un error si el índice está fuera de rango o si hay problemas de I/O.
    ///
    /// # Retorno
    /// Referencia mutable a la celda.
    pub fn get_cell_mut(&self, index: u16) -> io::Result<&mut BTreeCell> {
        let page = self.get_page_mut()?;
        
        if index >= page.header.cell_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Índice de celda fuera de rango: {}, máximo {}", index, page.header.cell_count - 1),
            ));
        }
        
        Ok(&mut page.cells[index as usize])
    }

    /// Inserta una celda en el nodo.
    ///
    /// # Parámetros
    /// * `cell` - Celda a insertar.
    ///
    /// # Errores
    /// Retorna un error si el tipo de celda no coincide con el tipo de nodo o si no hay espacio.
    pub fn insert_cell(&self, cell: BTreeCell) -> io::Result<u16> {
        // Verificar que el tipo de celda coincide con el tipo de nodo
        match (&self.node_type, &cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(_)) => {},
            (PageType::TableInterior, BTreeCell::TableInterior(_)) => {},
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {},
            (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {},
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Tipo de celda incompatible con el tipo de nodo: {:?}", self.node_type),
                ));
            }
        }
        
        let page = self.get_page_mut()?;
        
        // Añadir la celda a la página
        page.add_cell(cell)?;
        
        // Retornar el índice de la celda recién insertada
        Ok(page.header.cell_count - 1)
    }

    /// Obtiene el número de página del hijo más a la derecha (solo para nodos interiores).
    ///
    /// # Errores
    /// Retorna un error si el nodo no es interior o si hay problemas de I/O.
    pub fn get_right_most_child(&self) -> io::Result<u32> {
        if !self.node_type.is_interior() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "El nodo no es interior",
            ));
        }
        
        let page = self.get_page()?;
        
        match page.header.right_most_page {
            Some(page_number) => Ok(page_number),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "No hay hijo más a la derecha",
            )),
        }
    }

    /// Establece el número de página del hijo más a la derecha (solo para nodos interiores).
    ///
    /// # Parámetros
    /// * `page_number` - Número de página del hijo más a la derecha.
    ///
    /// # Errores
    /// Retorna un error si el nodo no es interior o si hay problemas de I/O.
    pub fn set_right_most_child(&self, page_number: u32) -> io::Result<()> {
        if !self.node_type.is_interior() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "El nodo no es interior",
            ));
        }
        
        let page = self.get_page_mut()?;
        
        page.header.right_most_page = Some(page_number);
        
        Ok(())
    }

    /// Obtiene el espacio libre en el nodo.
    ///
    /// # Errores
    /// Retorna un error si hay problemas de I/O.
    pub fn free_space(&self) -> io::Result<usize> {
        let page = self.get_page()?;
        Ok(page.free_space())
    }

    /// Para nodos interiores de tabla, busca la celda que contiene la clave especificada.
    ///
    /// # Parámetros
    /// * `key` - Clave a buscar (rowid).
    ///
    /// # Errores
    /// Retorna un error si el nodo no es interior de tabla o si hay problemas de I/O.
    ///
    /// # Retorno
    /// Tuple con:
    /// - `true` si se encontró una celda con la clave exacta, `false` en caso contrario
    /// - Número de página del hijo que puede contener la clave
    /// - Índice de la celda que contiene la clave o donde debería insertarse
    pub fn find_table_key(&self, key: i64) -> io::Result<(bool, u32, u16)> {
        if self.node_type != PageType::TableInterior {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "El nodo no es interior de tabla",
            ));
        }
        
        let page = self.get_page()?;
        let cell_count = page.header.cell_count;
        
        // Búsqueda binaria
        let mut left = 0;
        let mut right = cell_count.saturating_sub(1) as i32;
        
        while left <= right {
            let mid = left + (right - left) / 2;
            let mid_idx = mid as u16;
            
            let cell = &page.cells[mid as usize];
            let mid_key = match cell {
                BTreeCell::TableInterior(cell) => cell.key,
                _ => unreachable!("Tipo de celda incorrecto"),
            };
            
            if mid_key == key {
                // Encontramos una coincidencia exacta
                let left_child = match cell {
                    BTreeCell::TableInterior(cell) => cell.left_child_page,
                    _ => unreachable!("Tipo de celda incorrecto"),
                };
                
                return Ok((true, left_child, mid_idx));
            } else if mid_key > key {
                // La clave está a la izquierda
                right = mid - 1;
            } else {
                // La clave está a la derecha
                left = mid + 1;
            }
        }
        
        // No encontramos una coincidencia exacta
        
        if cell_count == 0 || right < 0 {
            // La clave es menor que todas las claves en el nodo
            // o el nodo está vacío, por lo que la clave debería estar
            // en el hijo más a la derecha
            match page.header.right_most_page {
                Some(right_most_page) => Ok((false, right_most_page, 0)),
                None => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "No hay hijo más a la derecha",
                )),
            }
        } else {
            // La clave está entre dos claves en el nodo
            let idx = right as u16;
            let cell = &page.cells[idx as usize];
            let left_child = match cell {
                BTreeCell::TableInterior(cell) => cell.left_child_page,
                _ => unreachable!("Tipo de celda incorrecto"),
            };
            
            Ok((false, left_child, idx))
        }
    }

    /// Para nodos hoja de tabla, busca la celda con el rowid especificado.
    ///
    /// # Parámetros
    /// * `rowid` - Rowid a buscar.
    ///
    /// # Errores
    /// Retorna un error si el nodo no es hoja de tabla o si hay problemas de I/O.
    ///
    /// # Retorno
    /// Tuple con:
    /// - `true` si se encontró una celda con el rowid exacto, `false` en caso contrario
    /// - Índice de la celda que contiene el rowid o donde debería insertarse
    pub fn find_table_rowid(&self, rowid: i64) -> io::Result<(bool, u16)> {
        if self.node_type != PageType::TableLeaf {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "El nodo no es hoja de tabla",
            ));
        }
        
        let page = self.get_page()?;
        let cell_count = page.header.cell_count;
        
        // Búsqueda binaria
        let mut left = 0;
        let mut right = cell_count.saturating_sub(1) as i32;
        //print!("Buscando rowid {} en el nodo hoja: ", rowid);
        //println!("Número de celdas: {}", cell_count);
        //println!("left: {}, right: {}", left, right);
        while left <= right {
            let mid = left + (right - left) / 2;
            //println!("mid: {}", mid);
            let mid_idx = mid as u16;
            
            
            let cell = &page.cells[mid as usize];
            let mid_rowid = match cell {
                BTreeCell::TableLeaf(cell) => cell.row_id,
                _ => unreachable!("Tipo de celda incorrecto"),
            };
            
            if mid_rowid == rowid {
                // Encontramos una coincidencia exacta
                return Ok((true, mid_idx));
            } else if mid_rowid > rowid {
                // El rowid está a la izquierda
                right = mid - 1;
            } else {
                // El rowid está a la derecha
                left = mid + 1;
            }
        }
        
        // No encontramos una coincidencia exacta
        // La posición de inserción es left
        Ok((false, left as u16))
    }

    /// Divide el nodo actual en dos, moviendo aproximadamente la mitad de las celdas
    /// al nuevo nodo. Este método se utiliza durante la inserción cuando un nodo está lleno.
    ///
    /// # Parámetros
    /// * `new_page_number` - Número de página para el nuevo nodo (opcional).
    ///
    /// # Errores
    /// Retorna un error si hay problemas de I/O.
    ///
    /// # Retorno
    /// - Nuevo nodo creado durante la división
    /// - Clave mediana (para nodos interiores) o máxima (para nodos hoja)
    /// - Índice de la celda mediana o máxima
    pub fn split(&self, new_page_number: Option<u32>) -> io::Result<(BTreeNode, i64, u16)> {
        let page = self.get_page_mut()?;
        let cell_count = page.header.cell_count;
        
        // Determinar el punto de división (aproximadamente la mitad)
        let split_point = cell_count / 2;
        
        // Crear un nuevo nodo
        let new_node = match self.node_type {
            PageType::TableLeaf => {
                // Para nodos hoja, simplemente crear un nuevo nodo hoja
                BTreeNode::create_leaf(self.node_type, self.pager)?
            },
            PageType::TableInterior => {
                // Para nodos interiores, necesitamos un hijo más a la derecha
                let mid_cell = match &page.cells[split_point as usize] {
                    BTreeCell::TableInterior(cell) => cell,
                    _ => unreachable!("Tipo de celda incorrecto"),
                };
                
                BTreeNode::create_interior(self.node_type, mid_cell.left_child_page, self.pager)?
            },
            PageType::IndexLeaf => {
                // Similar a TableLeaf
                BTreeNode::create_leaf(self.node_type, self.pager)?
            },
            PageType::IndexInterior => {
                // Similar a TableInterior pero con estructura diferente
                let mid_cell = match &page.cells[split_point as usize] {
                    BTreeCell::IndexInterior(cell) => cell,
                    _ => unreachable!("Tipo de celda incorrecto"),
                };
                
                BTreeNode::create_interior(self.node_type, mid_cell.left_child_page, self.pager)?
            },
            PageType::Free => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "No se puede dividir un nodo de tipo Free",
                ));
            },
            PageType::Overflow => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "No se puede dividir un nodo de tipo Overflow",
                ));
            },
        };
        
        // Mover las celdas después del punto de división al nuevo nodo
        let cells_to_move = page.cells.split_off(split_point as usize);
        let cell_indices_to_move = page.cell_indices.split_off(split_point as usize);
        
        // Actualizar el contador de celdas del nodo actual
        page.header.cell_count = split_point;
        
        // Mover las celdas al nuevo nodo
        let new_page = new_node.get_page_mut()?;
        new_page.cells = cells_to_move;
        new_page.cell_indices = cell_indices_to_move;
        new_page.header.cell_count = cell_count - split_point;
        
        // Para nodos interiores, necesitamos actualizar el hijo más a la derecha
        if self.node_type.is_interior() {
            if let Some(right_most) = page.header.right_most_page {
                new_page.header.right_most_page = Some(right_most);
                
                // La celda mediana se convierte en el nuevo punto de división
                let mid_cell = match self.node_type {
                    PageType::TableInterior => {
                        let cell = match &page.cells[split_point as usize - 1] {
                            BTreeCell::TableInterior(cell) => cell,
                            _ => unreachable!("Tipo de celda incorrecto"),
                        };
                        
                        // El hijo más a la derecha del nodo original se convierte en
                        // el hijo más a la izquierda del nuevo nodo
                        page.header.right_most_page = Some(cell.left_child_page);
                        
                        cell.key
                    },
                    PageType::IndexInterior => {
                        let cell = match &page.cells[split_point as usize - 1] {
                            BTreeCell::IndexInterior(cell) => cell,
                            _ => unreachable!("Tipo de celda incorrecto"),
                        };
                        
                        // Similar a TableInterior pero debe extraer la clave del payload
                        // En una implementación completa, se extraería la clave del payload
                        // Por simplicidad, usamos un valor ficticio
                        42 // Valor ficticio, en una implementación real se extraería del payload
                    },
                    _ => unreachable!("Tipo de nodo incorrecto"),
                };
                
                return Ok((new_node, mid_cell, split_point - 1));
            }
        }
        
        // Para nodos hoja, la clave mediana es la primera clave del nuevo nodo
        let median_key = match self.node_type {
            PageType::TableLeaf => {
                let cell = match &new_page.cells[0] {
                    BTreeCell::TableLeaf(cell) => cell,
                    _ => unreachable!("Tipo de celda incorrecto"),
                };
                
                cell.row_id
            },
            PageType::IndexLeaf => {
                // Similar a TableLeaf pero debe extraer la clave del payload
                // En una implementación completa, se extraería la clave del payload
                // Por simplicidad, usamos un valor ficticio
                42 // Valor ficticio, en una implementación real se extraería del payload
            },
            _ => unreachable!("Tipo de nodo incorrecto"),
        };
        
        Ok((new_node, median_key, 0))
    }

    /// Inserta una celda en el nodo en la posición correcta según la clave.
    ///
    /// # Parámetros
    /// * `cell` - Celda a insertar.
    ///
    /// # Errores
    /// Retorna un error si el tipo de celda no coincide con el tipo de nodo,
    /// si no hay espacio, o si hay problemas de I/O.
    ///
    /// # Retorno
    /// Tuple con:
    /// - `true` si el nodo se dividió, `false` en caso contrario
    /// - Clave mediana (si el nodo se dividió)
    /// - Nuevo nodo (si el nodo se dividió)
    pub fn insert_cell_ordered(&self, cell: BTreeCell) -> io::Result<(bool, Option<i64>, Option<BTreeNode>)> {
        // Verificar que el tipo de celda coincide con el tipo de nodo
        match (&self.node_type, &cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(_)) => {},
            (PageType::TableInterior, BTreeCell::TableInterior(_)) => {},
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {},
            (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {},
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Tipo de celda incompatible con el tipo de nodo: {:?}", self.node_type),
                ));
            }
        }
        //println!("Insertando celda: {:?}", cell);
        let page = self.get_page_mut()?;
        //println!("Página antes de la inserción: {:?}", page);
        // Verificar si hay suficiente espacio para la celda
        let cell_size = cell.size();
        //print!("Espacio libre en la página: {}", page.free_space());
        //println!("Tamaño de la celda: {}", cell_size);
        if page.free_space() < cell_size + 2 { // 2 bytes para el índice de la celda
            //println!("No hay suficiente espacio, dividiendo el nodo");
            // No hay suficiente espacio, dividir el nodo
            let (new_node, median_key, _) = self.split(None)?;
            
            // Decidir en qué nodo insertar la celda
            let insert_in_new = match (&self.node_type, &cell) {
                (PageType::TableLeaf, BTreeCell::TableLeaf(table_cell)) => {
                    table_cell.row_id >= median_key
                },
                (PageType::TableInterior, BTreeCell::TableInterior(table_cell)) => {
                    table_cell.key >= median_key
                },
                (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {
                    // En una implementación completa, compararíamos la clave en el payload
                    // Por simplicidad, asumimos que va en el nodo original
                    false
                },
                (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {
                    // Similar a IndexLeaf
                    false
                },
                _ => unreachable!("Tipo de celda incorrecto"),
            };
            
            
            if insert_in_new {
                //println!("Insertando en el nuevo nodo:");
                // Insertar en el nuevo nodo
                new_node.insert_cell_ordered(cell)?;
            } else {
                //println!("Insertando en el nodo original");
                // Insertar en el nodo original
                self.insert_cell_ordered(cell)?;
            }
            
            return Ok((true, Some(median_key), Some(new_node)));
        }
        //println!("Hay suficiente espacio, insertando en el nodo correcto");
        // Hay suficiente espacio, insertar la celda en la posición correcta
        let position = match (&self.node_type, &cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(table_cell)) => {
                //println!("Buscando posición para la celda de tabla hoja");
                if page.header.cell_count == 0 {
                    // Si el array de celdas está vacío, insertar en la posición 0
                    0
                } else {
                    let (_, pos) = self.find_table_rowid(table_cell.row_id)?;
                    pos
                }
            },
            (PageType::TableInterior, BTreeCell::TableInterior(table_cell)) => {
                let (found, _, pos) = self.find_table_key(table_cell.key)?;
                if found {
                    // Si ya existe una celda con esta clave, reemplazarla
                    self.get_page_mut()?.cells[pos as usize] = cell;
                    return Ok((false, None, None));
                }
                pos
            },
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {
                // En una implementación completa, buscaríamos la posición correcta
                // Por simplicidad, insertamos al final
                page.header.cell_count
            },
            (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {
                // Similar a IndexLeaf
                page.header.cell_count
            },
            _ => unreachable!("Tipo de celda incorrecto"),
        };
        
        // Insertar la celda en la posición correcta
        page.cells.insert(position as usize, cell);
        page.header.cell_count += 1;
        
        // Actualizar los índices de celda
        // En una implementación completa, se actualizarían correctamente
        // Por simplicidad, asumimos que se actualizan automáticamente
        
        Ok((false, None, None))
    }


}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::tempdir;
    use crate::page::{Page, BTreePage, PageType, TableLeafCell, TableInteriorCell};
    use crate::storage::Pager;

    #[test]
    fn test_create_leaf_node() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear un pager
        let mut pager = Pager::create(&db_path, 4096, 0).unwrap();
        
        // Crear un nodo hoja
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &mut pager as *mut Pager).unwrap();
        
        assert_eq!(node.node_type, PageType::TableLeaf);
        assert_eq!(node.cell_count().unwrap(), 0);
    }

    #[test]
    fn test_create_interior_node() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear un pager
        let mut pager = Pager::create(&db_path, 4096, 0).unwrap();
        
        // Crear un nodo interior
        let node = BTreeNode::create_interior(PageType::TableInterior, 1, &mut pager as *mut Pager).unwrap();
        
        assert_eq!(node.node_type, PageType::TableInterior);
        assert_eq!(node.cell_count().unwrap(), 0);
        assert_eq!(node.get_right_most_child().unwrap(), 1);
    }

    #[test]
    fn test_insert_cell() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear un pager
        let mut pager = Pager::create(&db_path, 4096, 0).unwrap();
        
        // Crear un nodo hoja
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &mut pager as *mut Pager).unwrap();
        
        // Crear una celda
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![0; 10],
            overflow_page: None,
        });
        
        // Insertar la celda
        let idx = node.insert_cell(cell).unwrap();
        assert_eq!(idx, 0);
        
        // Verificar que la celda se insertó correctamente
        assert_eq!(node.cell_count().unwrap(), 1);
        
        let cell = node.get_cell(0).unwrap();
        match cell {
            BTreeCell::TableLeaf(table_cell) => {
                assert_eq!(table_cell.row_id, 42);
                assert_eq!(table_cell.payload_size, 10);
            },
            _ => panic!("Tipo de celda incorrecto"),
        }
    }

    #[test]
    fn test_find_table_rowid() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear un pager
        let mut pager = Pager::create(&db_path, 4096, 0).unwrap();
        
        // Crear un nodo hoja
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &mut pager as *mut Pager).unwrap();
        
        // Insertar algunas celdas
        for i in [10, 20, 30, 40, 50] {
            let cell = BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 10,
                row_id: i,
                payload: vec![0; 10],
                overflow_page: None,
            });
            
            node.insert_cell(cell).unwrap();
        }
        
        // Buscar rowids existentes
        let (found, idx) = node.find_table_rowid(10).unwrap();
        assert!(found);
        assert_eq!(idx, 0);
        
        let (found, idx) = node.find_table_rowid(30).unwrap();
        assert!(found);
        assert_eq!(idx, 2);
        
        let (found, idx) = node.find_table_rowid(50).unwrap();
        assert!(found);
        assert_eq!(idx, 4);
        
        // Buscar rowids que no existen
        let (found, idx) = node.find_table_rowid(15).unwrap();
        assert!(!found);
        assert_eq!(idx, 1);
        
        let (found, idx) = node.find_table_rowid(5).unwrap();
        assert!(!found);
        assert_eq!(idx, 0);
        
        let (found, idx) = node.find_table_rowid(60).unwrap();
        assert!(!found);
        assert_eq!(idx, 5);
    }

    #[test]
    fn test_insert_cell_ordered() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear un pager
        let mut pager = Pager::create(&db_path, 4096, 0).unwrap();
        
        // Crear un nodo hoja
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &mut pager as *mut Pager).unwrap();
        
        // Insertar celdas ordenadas
        for i in [30, 10, 50, 20, 40] {
            let cell = BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 10,
                row_id: i,
                payload: vec![0; 10],
                overflow_page: None,
            });
            
            let (split, median_key, new_node) = node.insert_cell_ordered(cell).unwrap();
            assert!(!split);
            assert_eq!(median_key, None);
            assert_eq!(new_node.is_none(), true);
        }
        
        // Verificar que las celdas están ordenadas por rowid
        assert_eq!(node.cell_count().unwrap(), 5);
        
        for i in 0..5 {
            let cell = node.get_cell(i).unwrap();
            match cell {
                BTreeCell::TableLeaf(table_cell) => {
                    assert_eq!(table_cell.row_id, (i as i64 + 1) * 10);
                },
                _ => panic!("Tipo de celda incorrecto"),
            }
        }
    }

    #[test]
    fn test_split_node() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear un pager con página pequeña para forzar divisiones
        let mut pager = Pager::create(&db_path, 512, 0).unwrap();
        
        // Crear un nodo hoja
        let node = BTreeNode::create_leaf(PageType::TableLeaf, &mut pager as *mut Pager).unwrap();
        
        // Insertar suficientes celdas para llenar el nodo
        for i in 1..=20 {
            let cell = BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 20,
                row_id: i,
                payload: vec![0; 20],
                overflow_page: None,
            });
            
            let (split, median_key, new_node) = node.insert_cell_ordered(cell).unwrap();
            
            if split {
                // Verificar que la división ocurrió correctamente
                assert!(median_key.is_some());
                assert!(new_node.is_some());
                
                let median = median_key.unwrap();
                let new = new_node.unwrap();
                
                // Verificar que las celdas se dividieron aproximadamente por la mitad
                let node_count = node.cell_count().unwrap();
                let new_count = new.cell_count().unwrap();
                
                assert!(node_count > 0);
                assert!(new_count > 0);
                assert!(node_count + new_count >= 20);
                
                // Verificar que la clave mediana es correcta
                assert!(median > 0);
                
                // Verificar que las celdas están correctamente distribuidas
                let max_node = match node.get_cell(node_count - 1).unwrap() {
                    BTreeCell::TableLeaf(cell) => cell.row_id,
                    _ => panic!("Tipo de celda incorrecto"),
                };
                
                let min_new = match new.get_cell(0).unwrap() {
                    BTreeCell::TableLeaf(cell) => cell.row_id,
                    _ => panic!("Tipo de celda incorrecto"),
                };
                
                assert!(max_node < min_new);
                assert_eq!(min_new, median);
                
                break;
            }
        }
    }
}
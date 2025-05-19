//! # B-Tree Node Module
//! 
//! This module defines the `BTreeNode` struct and its associated methods.
//! It provides functionality for creating, opening, and manipulating B-Tree nodes.
//!

use std::io;
use std::rc::Rc;
use std::cell::RefCell;
use std::cell::{RefMut};
use std::hash::Hasher;
use crate::page::{BTreeCell, BTreePage, Page, PageType, TableInteriorCell};
use crate::utils::cmp::KeyValue;
use crate::storage::Pager;
use std::io::Cursor;

//use super::btree;


/// Extracts a key from an index node payload.
/// 
/// # Parameters
/// * `payload` - The raw payload bytes from which to extract the key
/// 
/// # Returns
/// A comparable key value extracted from the payload
/// 
/// # Errors
/// Returns an error if the payload cannot be parsed
pub fn extract_key_from_payload(payload: &[u8]) -> io::Result<KeyValue> {
    use crate::utils::serialization::{SqliteValue, deserialize_values};
    
    // Parse the payload as SQLite values
    let mut cursor = Cursor::new(payload);
    let (values, _) = deserialize_values(&mut cursor)?;
    
    // Use the first value as the key
    if values.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Empty payload in index node",
        ));
    }
    
    // Convert the first value to a comparable key
    match &values[0] {
        SqliteValue::Integer(i) => Ok(KeyValue::Integer(*i)),
        SqliteValue::Float(f) => Ok(KeyValue::Float(*f)),
        SqliteValue::String(s) => Ok(KeyValue::String(s.clone())),
        SqliteValue::Blob(b) => Ok(KeyValue::Blob(b.clone())),
        SqliteValue::Null => Ok(KeyValue::Null),
    }
}

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
    /// Opted to use `Rc<RefCell<Pager>>` because each node needs a mutable reference to the pager,
    /// However, there is going to be only one pager in the whole B-Tree, and there will be only one writer at a time.
    /// This allows us to share the pager across multiple nodes while still allowing for mutable access.
    /// If we wanted to have multiple writers, we would need to use a more complex synchronization mechanism, maybe using `Mutex` or `RwLock`.
    pager: Rc<RefCell<Pager>>, // Decided to switch to Rc<RefCell<Pager>> for thread safety and mutability
}



impl BTreeNode {
    /// Creates a new B-Tree node.
    ///
    /// # Parameters
    /// * `page_number` - Number of the page where the node is stored.
    /// * `node_type` - Type of node.
    /// * `pager` - Reference to the pager for I/O operations.
    ///
    /// # Before, I was going to use a raw pointer for the pager, so the pager needed to be alive for the whole life of the node, but now with Rc and RefCell is not needed..
    pub fn new(page_number: u32, node_type: PageType, shared_pager: Rc<RefCell<Pager>>) -> Self {
        BTreeNode {
            page_number,
            node_type,
            pager: Rc::clone(&shared_pager),
        }
    }

     /// Opens an existing B-Tree node.
     /// 
     /// # Parameters
     /// * `page_number` - Number of the page where the node is stored.
     /// * `node_type` - Type of node.
     /// * `pager` - Reference to the pager for I/O operations.
     /// 
     /// # Safety
     /// The caller must ensure that the pager is valid and that the page exists. (Now this is not needed, because we are using Rc<RefCell<Pager>>)
    pub fn open(page_number: u32, node_type: PageType, shared_pager: Rc<RefCell<Pager>>) -> io::Result<Self> {
        // Verify that the page exists and is of the correct type
      
          
                    
        {
            let mut pager_ref = shared_pager.borrow_mut();
            let page = pager_ref.get_page(page_number, Some(node_type))?;
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

        // Creates a new BTreeNode with the given page number and type
        Ok(BTreeNode {
            page_number,
            node_type,
            pager: Rc::clone(&shared_pager),
        })
}
   
    /// Obtains the number of cells in the node.
    /// 
    /// # Errors
    /// Returns an error if there are I/O issues.
    pub fn cell_count(&self) -> io::Result<u16> {
        let page = &self.get_page_owned()?;
        Ok(page.header.cell_count)
    }

  

    /// Obtains the ownership of the B-Tree page associated with this node.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    /// 
    /// # Returns
    /// Returns an owned B-Tree page. 
    /// I want to improve this to be able to return references but makes it more complex, because to get a page i need a mutable `pager` and to get a mutable pager i need to borrow it mutably, and this is not possible if i have a reference to the pager.
    fn get_page_owned(&self) -> io::Result<BTreePage> {
        let mut pager_ref = self.pager.borrow_mut();
        match pager_ref.get_page(self.page_number, Some(self.node_type))? {
            Page::BTree(ref btree_page) => Ok(btree_page.clone()),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "The page is not of BTree type",
            )),
        }
    }

   
    
    // Get a mutable reference to the BTreePage.
    // This is a bit tricky because we need to ensure that the pager is not borrowed mutably
    // while we are trying to get a mutable reference to the page. I am using RefMut to handle this.
    /// Obtains a mutable reference to the B-Tree page associated with this node.
    /// 
    /// # Errors
    /// Returns an error if there are I/O issues.
    fn get_page_mut(&self) -> io::Result<impl std::ops::DerefMut<Target = BTreePage> + '_> {
        let mut pager_ref = self.pager.borrow_mut();
        match pager_ref.get_page_mut(self.page_number, Some(self.node_type))? {
            Page::BTree(_btree_page) => Ok(RefMut::map(pager_ref, |p| {
                match p.get_page_mut(self.page_number, Some(self.node_type)).unwrap() {
                    Page::BTree(page) => page,
                    _ => unreachable!(),
                }
            })),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "The page is not of BTree type",
            )),
        }
    }

    
    // Creates a new Leaf btree node.
    ///
    /// # Parameters
    /// * `node_type` - Type of node (TableLeaf or IndexLeaf).
    /// * `pager` - Reference to the pager for I/O operations.
    /// 
    /// # Errors
    /// Returns an error if the page cannot be created or if the type is not leaf.
    pub fn create_leaf(node_type: PageType, pager:Rc<RefCell<Pager>> ) -> io::Result<Self> {
        if !node_type.is_leaf() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("The BTreeNode is not of type leaf: {:?}", node_type),
            ));
        }

        // I prefer to create the node first so that I do not have to borrow the pager before cloning it.
        // I think this is more elegant and less error-prone.
        let mut new_node = BTreeNode {
            page_number: 0,
            node_type,
            pager: Rc::clone(&pager),
        };
        
        let page_number =  {
            new_node.pager.borrow_mut().create_btree_page(node_type, None)?
        };
        new_node.page_number = page_number;
        Ok(new_node)
    }

    /// Creates a new interior B-Tree node.
    ///
    /// # Parameters
    /// * `node_type` - Type of interior node (TableInterior or IndexInterior).
    /// * `right_most_page` - Page number of the right-most child.
    /// * `pager` - Reference to the pager for I/O operations.
    ///
    /// # Errors
    /// Returns an error if the page cannot be created or if the type is not interior.
    pub fn create_interior(node_type: PageType, right_most_page: Option<u32> // Decided to make this optional because it aids flexibility.
        
         , pager:Rc<RefCell<Pager>>) -> io::Result<Self> {
        if !node_type.is_interior() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("The BTreeNode is not of type interior: {:?}", node_type),
            ));
        }
        
        let mut new_node = BTreeNode {
            page_number: 0,
            node_type,
            pager: Rc::clone(&pager),
        };
        
        let mut right_most_page = right_most_page;
        // If the right-most page is None, we set it to 0. As ponting to zero means pointing to nothing.
        if right_most_page.is_none() {
            right_most_page = Some(0);
        }

        let page_number =  {
            new_node.pager.borrow_mut().create_btree_page(node_type, right_most_page)?
        };
        new_node.page_number = page_number;
        Ok(new_node)
    }

    /// Gets a cell from the node.
    ///
    /// # Parameters
    /// * `index` - Index of the cell (starting from 0).
    ///
    /// # Errors
    /// Returns an error if the index is out of range or if there are I/O issues.
    ///
    /// # Returns
    /// An owned cell.
    pub fn get_cell_owned(&self, index: u16) -> io::Result<BTreeCell> {
        let page = &(self.get_page_owned()?); // As soon as I get the page, I can release the pager.
        
        if index >= page.header.cell_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Índice de celda fuera de rango: {}, máximo {}", index, page.header.cell_count - 1),
            ));
        }
        
        Ok(page.cells[index as usize].clone())
    }

    /// Gets a cell from the node for modification.
    ///
    /// # Parameters
    /// * `index` - Index of the cell (starting from 0).
    ///
    /// # Errors
    /// Returns an error if the index is out of range or if there are I/O issues.
    ///
    /// # Returns
    /// Mutable reference to the cell.
    pub fn get_cell_mut(&self, index: u16) -> io::Result<impl std::ops::DerefMut<Target = BTreeCell> + '_> {
        let pager_ref = self.pager.borrow_mut();
        let page_ref = RefMut::map(pager_ref, |pager| {
            match pager.get_page_mut(self.page_number, Some(self.node_type)).unwrap() {
                Page::BTree(page) => page,
                _ => unreachable!(),
            }
        });

        if index >= page_ref.header.cell_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Index of cell is out of range: {}, maximum {}", index, page_ref.header.cell_count - 1),
            ));
        }

        Ok(RefMut::map(page_ref, move |page| &mut page.cells[index as usize]))
    }

    /// Inserts a cell into the node.
    /// 
    /// # Parameters
    /// * `cell` - Cell to insert.
    /// 
    /// # Errors
    /// Returns an error if the cell type does not match the node type,
    /// if there is no space, or if there are I/O issues.
    /// 
    /// # Returns
    /// The index of the newly inserted cell.
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
                    format!("Type of cell is not compatible with the type of node: {:?}", self.node_type),
                ));
            }
        }
        
        let mut page = self.get_page_mut()?;
        println!("Cell: {:?} added to page: {}", cell.clone(), self.page_number);
        // Añadir la celda a la página
        page.add_cell(cell)?;
        
        // Retornar el índice de la celda recién insertada
        Ok(page.header.cell_count - 1)
    }

    /// Obtains the right-most child of the node (only for interior nodes).
    /// 
    /// # Errors
    /// Returns an error if the node is not an interior node or if there are I/O issues.
    /// 
    /// # Returns
    /// The page number of the right-most child.
    pub fn get_right_most_child(&self) -> io::Result<u32> {
        if !self.node_type.is_interior() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The node is not interior type.",
            ));
        }
        
        let page = &(self.get_page_owned()?);
        
        match page.header.right_most_page {
            Some(page_number) => Ok(page_number),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Right-most child not found",
            )),
        }
    }

    /// Stablishes the right-most child of the node (only for interior nodes).
    /// 
    /// # Parameters
    /// * `page_number` - Page number of the right-most child.
    /// 
    /// # Errors
    /// Returns an error if the node is not an interior node or if there are I/O issues.
    pub fn set_right_most_child(&self, page_number: u32) -> io::Result<()> {
        if !self.node_type.is_interior() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The node is not interior type.",
            ));
        }
        
        let mut page = self.get_page_mut()?;
        
        page.header.right_most_page = Some(page_number);
        
        Ok(())
    }

    /// Gets the free space in the node.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    pub fn free_space(&self) -> io::Result<usize> {
        let page = &(self.get_page_owned()?);
        Ok(page.free_space())
    }

    /// Searches for the appropriate position in an index node based on the key.
///
/// # Parameters
/// * `index_key` - The key to search for
///
/// # Errors
/// Returns an error if there are I/O issues or if the node is not an index node.
///
/// # Returns
/// A tuple with:
/// - `true` if an exact match was found, `false` otherwise
/// - The index where the key should be inserted
pub fn find_index_key(&self, index_key: &KeyValue) -> io::Result<(bool, u16)> {
    if !self.node_type.is_index() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "The node is not an index node",
        ));
    }
    
    let page = &(self.get_page_owned()?);
    let cell_count = page.header.cell_count;
    
    // Binary search
    let mut left = 0;
    let mut right = cell_count.saturating_sub(1) as i32;
    
    while left <= right {
        let mid = left + (right - left) / 2;
        let mid_idx = mid as u16;
        
        let cell = &page.cells[mid as usize];
        let cell_key = match cell {
            BTreeCell::IndexLeaf(leaf_cell) => {
                extract_key_from_payload(&leaf_cell.payload)?
            },
            BTreeCell::IndexInterior(interior_cell) => {
                extract_key_from_payload(&interior_cell.payload)?
            },
            _ => unreachable!("Incorrect cell type"),
        };
        
        match cell_key.partial_cmp(index_key) {
            Some(std::cmp::Ordering::Equal) => {
                return Ok((true, mid_idx));
            },
            Some(std::cmp::Ordering::Greater) => {
                right = mid - 1;
            },
            Some(std::cmp::Ordering::Less) => {
                left = mid + 1;
            },
            None => {
                // This should not happen with our implementation
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Incomparable key types",
                ));
            }
        }
    }
    
    // No exact match found
    Ok((false, left as u16))
}

    /// For table interior nodes, searches for the cell containing the specified key.
    ///
    /// # Parameters
    /// * `key` - Key to search for (rowid).
    ///
    /// # Errors
    /// Returns an error if the node is not a table interior node or if there are I/O issues.
    ///
    /// # Returns
    /// Tuple with:
    /// - `true` if a cell with the exact key was found, `false` otherwise
    /// - Page number of the child that may contain the key
    /// - Index of the cell containing the key or where it should be inserted
    pub fn find_table_key(&self, key: i64) -> io::Result<(bool, u32, u16)> {
        if self.node_type != PageType::TableInterior {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The node is not a table interior",
            ));
        }
        
        let page = &(self.get_page_owned()?); // As always I get the page and release the pager
        let cell_count = page.header.cell_count;
        
        // Binary search
        let mut left = 0;
        let mut right = cell_count.saturating_sub(1) as i32;
        
        while left <= right {
            let mid = left + (right - left) / 2;
            println!("Left: {}, Right: {}, Mid: {}", left, right, mid);
            let mid_idx = mid as u16;
            
            let cell = &page.cells[mid as usize];
            let mid_key = match cell {
                BTreeCell::TableInterior(cell) => cell.key,
                _ => unreachable!("Incorrect cell type"),
            };
            
            match mid_key.partial_cmp(&key){
                Some(std::cmp::Ordering::Equal) => {
                    // Found an exact match
                    let left_child = match cell {
                        BTreeCell::TableInterior(cell) => cell.left_child_page,
                        _ => unreachable!("Incorrect cell type"),
                    };
                    
                    return Ok((true, left_child, mid_idx));
                },
                Some(std::cmp::Ordering::Greater) => {
                    // The key is to the left
                    right = mid - 1;
                    println!("Key is to the left");
                },
                Some(std::cmp::Ordering::Less) => {
                    // The key is to the right
                    left = mid + 1;
                },
                None => {
                    // This should not happen with our implementation
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Incomparable key types",
                    ));
                }

            }
            
        }

        if let Some(right_most_page) = page.header.right_most_page  {
            // The key is greater than the largest key
            if key > right_most_page as i64 {
                println!("Key is greater than the largest key");
                return Ok((false, right_most_page, cell_count));
            }
         
        } 
        
        if cell_count == 0 || right < 0 {
            // The node is empty or the key is less than the smallest key
            println!("Key is less than the smallest key");
            match &page.cells[0] {
                BTreeCell::TableInterior(cell) => {
                    let left_child = cell.left_child_page;
                    Ok((false, left_child, 0))
                },
                _ => unreachable!("Incorrect cell type"),
            }
        } else {
            println!("Key is between two keys");

            // The key is between two keys
            let idx = right as u16;
            let cell = &page.cells[idx as usize];
            let left_child = match cell {
                BTreeCell::TableInterior(cell) => cell.left_child_page,
                _ => unreachable!("Incorrect cell type"),
            };
          
        
            Ok((false, left_child, idx))
        }
    }

    /// For table leaf nodes, searches for the cell with the specified rowid.
    ///
    /// # Parameters
    /// * `rowid` - Rowid to search for.
    ///
    /// # Errors
    /// Returns an error if the node is not a table leaf or if there are I/O issues.
    ///
    /// # Returns
    /// Tuple with:
    /// - `true` if a cell with the exact rowid was found, `false` otherwise
    /// - Index of the cell containing the rowid or where it should be inserted
    pub fn find_table_rowid(&self, rowid: i64) -> io::Result<(bool, u16)> {
        if self.node_type != PageType::TableLeaf {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The node is not a table leaf",
            ));
        }
        
        let page = &(self.get_page_owned()?);
        let cell_count = page.header.cell_count;
        
        // If the node is empty, return immediately
        if cell_count == 0 {
            return Ok((false, 0));
        }

        // Binary search
        let mut left = 0;
        let mut right = cell_count.saturating_sub(1) as i32;
       
        while left <= right {
            let mid = left + (right - left) / 2;
            println!("Left: {}, Right: {}, Mid: {}", left, right, mid);
            let mid_idx = mid as u16;
            
            
            let cell = &page.cells[mid as usize];
            let mid_rowid = match cell {
                BTreeCell::TableLeaf(cell) => cell.row_id,
                _ => unreachable!("Incorrect cell type"),
            };
            
            if mid_rowid == rowid {
                // Found an exact match
                return Ok((true, mid_idx));
            } else if mid_rowid > rowid {
                // The row key is to the left
                right = mid - 1;
            } else {
                // The row key is to the right
                left = mid + 1;
            }
        }
        
        // No exact match found
    
        // Then the rowid should be inserted at th left
        Ok((false, left as u16))
    }
/// Splits the current node into two, moving approximately half of the cells
/// to the new node. This method is used during insertion when a node is full.
///
/// # Errors
/// Returns an error if there are I/O issues.
///
/// # Returns
/// - New node created during the split
/// - Median key (for interior nodes) or rowid (for leaf nodes)
/// - Index of the median cell
pub fn split(&self) -> io::Result<(BTreeNode, i64, u16)> {
    let cell_count = {
        let page = &(self.get_page_owned()?);
        page.header.cell_count
    };
    
    // Find the splitting point (middle of the node)
    let split_point = cell_count / 2;
    
    // Create a new node of the same type
    let new_node = match self.node_type {
        PageType::TableLeaf => BTreeNode::create_leaf(self.node_type, Rc::clone(&self.pager))?,
        PageType::TableInterior => BTreeNode::create_interior(self.node_type, None, Rc::clone(&self.pager))?,
        PageType::IndexLeaf => BTreeNode::create_leaf(self.node_type, Rc::clone(&self.pager))?,
        PageType::IndexInterior => BTreeNode::create_interior(self.node_type, None, Rc::clone(&self.pager))?,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot split a non-B-Tree page",
            ));
        }
    };

    // SOLUCIÓN: Usar ámbitos separados para evitar préstamos simultáneos
    
    // Primero, obtener datos de la página original
    let (cells_to_move, indices_to_move, orig_right_most_page, median_key) = {
        let mut orig_page = self.get_page_mut()?;
        
        // Mover cells desde la página original a un vector temporal
        let cells = orig_page.cells.drain(split_point as usize..).collect::<Vec<_>>();
        let indices = orig_page.cell_indices.drain(split_point as usize..).collect::<Vec<_>>();
        
        // Actualizar contador de cells
        orig_page.header.cell_count = split_point;
        
        // Guardar referencia al right-most page si es un nodo interior
        let right_most = orig_page.header.right_most_page;
        
        // Calcular la clave mediana dependiendo del tipo de nodo
        let median = if self.node_type.is_interior() {
            if let Some(right_most) = right_most {
                // Para nodos interiores, obtén la clave de la celda que será promovida
                match self.node_type {
                    PageType::TableInterior => {
                        let (mid_left_child_page, mid_key) = match &orig_page.cells[(split_point - 1) as usize] {
                            BTreeCell::TableInterior(cell) => (cell.left_child_page, cell.key),
                            _ => unreachable!("Incorrect cell type"),
                        };
                        
                        // Actualizar el right-most child de la página original
                        orig_page.header.right_most_page = Some(mid_left_child_page);
                        (mid_key, split_point - 1)
                    },
                    PageType::IndexInterior => {
                        // Obtener datos para nodos de índice interior
                        let (mid_payload, mid_left_child_page) = match &orig_page.cells[(split_point - 1) as usize] {
                            BTreeCell::IndexInterior(cell) => (cell.payload.clone(), cell.left_child_page),
                            _ => unreachable!("Incorrect cell type"),
                        };
                        
                        // Actualizar el right-most child
                        orig_page.header.right_most_page = Some(mid_left_child_page);
                        
                        // Extraer clave del payload
                        let key_value = extract_key_from_payload(&mid_payload)?;
                        let key = match key_value {
                            KeyValue::Integer(i) => i,
                            KeyValue::Float(f) => f as i64,
                            _ => {
                                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                                std::hash::Hash::hash(&key_value, &mut hasher);
                                hasher.finish() as i64
                            }
                        };
                        (key, split_point - 1)
                    },
                    _ => unreachable!("Not an interior node"),
                }
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Interior node without right-most child pointer",
                ));
            }
        } else {
            // Para nodos hoja, la clave mediana es el rowid de la primera celda en el nuevo nodo
            if cells.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "No cells to move to new node",
                ));
            }
            
            match self.node_type {
                PageType::TableLeaf => {
                    match &cells[0] {
                        BTreeCell::TableLeaf(cell) => (cell.row_id, 0),
                        _ => unreachable!("Incorrect cell type"),
                    }
                },
                PageType::IndexLeaf => {
                    match &cells[0] {
                        BTreeCell::IndexLeaf(cell) => {
                            let key_value = extract_key_from_payload(&cell.payload)?;
                            match key_value {
                                KeyValue::Integer(i) => (i, 0),
                                KeyValue::Float(f) => (f as i64, 0),
                                _ => {
                                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                                    std::hash::Hash::hash(&key_value, &mut hasher);
                                    (hasher.finish() as i64, 0)
                                }
                            }
                        },
                        _ => unreachable!("Incorrect cell type"),
                    }
                },
                _ => unreachable!("Not a leaf node"),
            }
        };
        
        // Actualizar el content start offset
        orig_page.update_content_start_offset();
        
        (cells, indices, right_most, median)
    };
    
    // Luego, actualizar la nueva página
    {
        let mut new_page = new_node.get_page_mut()?;
        
        // Si es un nodo interior, establecer el right-most page
        if self.node_type.is_interior() {
            new_page.header.right_most_page = orig_right_most_page;
        }
        
        // Agregar las celdas y los índices a la nueva página
        for cell in cells_to_move {
            new_page.cells.push(cell);
        }
        
        for idx in indices_to_move {
            new_page.cell_indices.push(idx);
        }
        
        // Actualizar el conteo de celdas
        new_page.header.cell_count = new_page.cells.len() as u16;
        
        // Actualizar el content start offset
        new_page.update_content_start_offset();
    }
    
    Ok((new_node, median_key.0, median_key.1))
}
/// Inserts a cell into the node in the correct position based on the key.
///
/// # Parameters
/// * `cell` - Cell to insert.
///
/// # Errors
/// Returns an error if the cell type does not match the node type,
/// if there is no space, or if there are I/O issues.
///
/// # Returns
/// Tuple with:
/// - `true` if the node was split, `false` otherwise
/// - Median key (if the node was split)
/// - New node (if the node was split)
pub fn insert_cell_ordered(&self, cell: BTreeCell) -> io::Result<(bool, Option<i64>, Option<BTreeNode>)> {
    // Verify that the cell type matches the node type
    match (&self.node_type, &cell) {
        (PageType::TableLeaf, BTreeCell::TableLeaf(_)) => {},
        (PageType::TableInterior, BTreeCell::TableInterior(_)) => {},
        (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {},
        (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {},
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Cell type incompatible with node type: {:?}", self.node_type),
            ));
        }
    }
    
    // Calculate cell size
    let cell_size = cell.size();
    let cell_index_size = 2; // 2 bytes for the cell index
    println!("Cell size: {}", cell_size);
    println!("Cell index size: {}", cell_index_size);
    // Check if the node has enough space for the new cell
    let free_space = {
        let page = &(self.get_page_owned()?);
        page.free_space()
        
    };
    println!("Free space: {}", free_space);
    
    if free_space < cell_size + cell_index_size {
        // Not enough space, split the node
        println!("Not enough space, splitting the node");
        let (new_node, median_key, _) = self.split()?;
        println!("New node created with page number: {}", new_node.page_number);
        // Determine which node should receive the new cell
        let insert_in_new = match (&self.node_type, &cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(table_cell)) => {
                table_cell.row_id >= median_key
            },
            (PageType::TableInterior, BTreeCell::TableInterior(table_cell)) => {
                table_cell.key >= median_key
            },
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(index_cell)) => {
                // Extract the key from the payload
                let key_value = extract_key_from_payload(&index_cell.payload)?;
                
                // Need a comparable form of the median key
                let median_key_value = KeyValue::Integer(median_key);
                
                // Compare with the median key
                match key_value.partial_cmp(&median_key_value) {
                    Some(std::cmp::Ordering::Less) => false,
                    Some(_) => true,
                    None => {
                        // Fallback to a simple comparison
                        index_cell.payload_size as i64 >= median_key
                    }
                }
            },
            (PageType::IndexInterior, BTreeCell::IndexInterior(index_cell)) => {
                // Similar logic as IndexLeaf
                let key_value = extract_key_from_payload(&index_cell.payload)?;
                let median_key_value = KeyValue::Integer(median_key);
                
                match key_value.partial_cmp(&median_key_value) {
                    Some(std::cmp::Ordering::Less) => false,
                    Some(_) => true,
                    None => {
                        // Fallback to a simple comparison
                        index_cell.payload_size as i64 >= median_key
                    }
                }
            },
            _ => unreachable!("Incorrect cell type"),
        };
        
        // Insert the cell into the appropriate node
        if insert_in_new {
            new_node.insert_cell_ordered(cell)?;
        } else {
            self.insert_cell_ordered(cell)?;
        }
        
        return Ok((true, Some(median_key), Some(new_node)));
    }
    
    // There's enough space, find the correct position for the new cell
    let position = match (&self.node_type, &cell) {
        (PageType::TableLeaf, BTreeCell::TableLeaf(table_cell)) => {
            // Find position based on rowid for table leaf nodes
            println!("Buscando posición para la celda de tabla con rowid: {}", table_cell.row_id);
            let (found, idx) = self.find_table_rowid(table_cell.row_id)?;
            println!("Índice encontrado: {}", idx);
            if found {
                // Replace existing cell with the same rowid
                let mut page = self.get_page_mut()?;
                page.cells[idx as usize] = cell;
                return Ok((false, None, None));
            }
            
            idx
        },
        (PageType::TableInterior, BTreeCell::TableInterior(table_cell)) => {
            // Find position based on key for table interior nodes
            let page = self.get_page_owned()?;
            
            if page.header.cell_count == 0 {
                0
            } else {
                // Binary search for the correct position
                let mut left = 0;
                let mut right = page.header.cell_count as i32 - 1;
                let mut pos = 0;
                
                while left <= right {
                    let mid = left + (right - left) / 2;
                    let mid_cell = &page.cells[mid as usize];
                    
                    let mid_key = match mid_cell {
                        BTreeCell::TableInterior(interior_cell) => interior_cell.key,
                        _ => unreachable!("Incorrect cell type"),
                    };
                    
                    if mid_key == table_cell.key {
                        // Replace cell with same key
                        let mut page = self.get_page_mut()?;
                        page.cells[mid as usize] = cell;
                        return Ok((false, None, None));
                    } else if mid_key > table_cell.key {
                        right = mid - 1;
                    } else {
                        left = mid + 1;
                        pos = left as u16;
                    }
                }
                
                pos
            }
        },
        (PageType::IndexLeaf, BTreeCell::IndexLeaf(index_cell)) => {
            // For index leaf nodes, find position based on the key in the payload
            let key_value = extract_key_from_payload(&index_cell.payload)?;
            let (found, idx) = self.find_index_key(&key_value)?;
            
            if found {
                // Replace existing cell with the same key
                let mut page = self.get_page_mut()?;
                page.cells[idx as usize] = cell;
                return Ok((false, None, None));
            }
            
            idx
        },
        (PageType::IndexInterior, BTreeCell::IndexInterior(index_cell)) => {
            // Similar to IndexLeaf
            let key_value = extract_key_from_payload(&index_cell.payload)?;
            let (found, idx) = self.find_index_key(&key_value)?;
            
            if found {
                // Replace existing cell with the same key
                let mut page = self.get_page_mut()?;
                page.cells[idx as usize] = cell;
                return Ok((false, None, None));
            }
            
            idx
        },
        _ => unreachable!("Incorrect cell type"),
    };
    
    // Insert the cell at the calculated position
    let mut page = self.get_page_mut()?;
    
    // Calculate appropriate cell offset in the page
    let offset = if page.header.cell_count > 0 && position < page.header.cell_count {
        // For interior cells, try to maintain proper spacing
        let existing_offset = page.cell_indices[position as usize];
        existing_offset - cell_size as u16
    } else {
        // Calculate a new offset for the end of the page
        page.header.content_start_offset - cell_size as u16
    };
    
    // Insert the cell and its index
    if position < page.header.cell_count {
        page.cells.insert(position as usize, cell);
        page.cell_indices.insert(position as usize, offset);
    } else {
        page.cells.push(cell);
        page.cell_indices.push(offset);
    }
    
    // Update page metadata
    page.header.cell_count += 1;
    page.header.content_start_offset = page.cell_indices.iter().min().copied().unwrap_or(page.header.content_start_offset);
    
    Ok((false, None, None))
}


}


#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;
    use std::cell::RefCell;
    use std::path::Path;
    use tempfile::tempdir;
    use crate::storage::Pager;
    use crate::page::{PageType, BTreeCell, TableLeafCell, TableInteriorCell, IndexLeafCell, IndexInteriorCell};
    use crate::utils::cmp::KeyValue;

    // Helper function to create a test pager
    fn create_test_pager() -> Rc<RefCell<Pager>> {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(db_path, 4096, None, 0).unwrap();
        Rc::new(RefCell::new(pager))
    }

    #[test]
    fn test_extract_key_from_payload() {
        // Create a payload with an integer
        let mut payload = Vec::new();
        crate::utils::serialization::serialize_values(
            &[crate::utils::serialization::SqliteValue::Integer(42)],
            &mut payload
        ).unwrap();
        
        let key = extract_key_from_payload(&payload).unwrap();
        match key {
            KeyValue::Integer(i) => assert_eq!(i, 42),
            _ => panic!("Expected Integer key"),
        }
        
        // Create a payload with a string
        let mut payload = Vec::new();
        crate::utils::serialization::serialize_values(
            &[crate::utils::serialization::SqliteValue::String("test".to_string())],
            &mut payload
        ).unwrap();
        
        let key = extract_key_from_payload(&payload).unwrap();
        match key {
            KeyValue::String(s) => assert_eq!(s, "test"),
            _ => panic!("Expected String key"),
        }
        
        // Create a payload with a float
        let mut payload = Vec::new();
        crate::utils::serialization::serialize_values(
            &[crate::utils::serialization::SqliteValue::Float(std::f64::consts::PI)],
            &mut payload
        ).unwrap();
        
        let key = extract_key_from_payload(&payload).unwrap();
        match key {
            KeyValue::Float(f) => assert_eq!(f, std::f64::consts::PI),
            _ => panic!("Expected Float key"),
        }
        
        // Test error case with empty payload
        let payload = Vec::new();
        let result = extract_key_from_payload(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_leaf_node() {
        let pager = create_test_pager();
        
        // Create a table leaf node
        let table_leaf = BTreeNode::create_leaf(PageType::TableLeaf, Rc::clone(&pager)).unwrap();
        assert_eq!(table_leaf.node_type, PageType::TableLeaf);
        assert!(table_leaf.page_number > 0);
        
        // Create an index leaf node
        let index_leaf = BTreeNode::create_leaf(PageType::IndexLeaf, Rc::clone(&pager)).unwrap();
        assert_eq!(index_leaf.node_type, PageType::IndexLeaf);
        assert!(index_leaf.page_number > 0);
        
        // Test error with non-leaf page type
        let result = BTreeNode::create_leaf(PageType::TableInterior, Rc::clone(&pager));
        assert!(result.is_err());
    }

    #[test]
    fn test_create_interior_node() {
        let pager = create_test_pager();
        
        // Create a table interior node
        let table_interior = BTreeNode::create_interior(PageType::TableInterior, None, Rc::clone(&pager)).unwrap();
        assert_eq!(table_interior.node_type, PageType::TableInterior);
        assert!(table_interior.page_number > 0);
        
        // Create an index interior node
        let index_interior = BTreeNode::create_interior(PageType::IndexInterior, None, Rc::clone(&pager)).unwrap();
        assert_eq!(index_interior.node_type, PageType::IndexInterior);
        assert!(index_interior.page_number > 0);
        
        // Test error with non-interior page type
        let result = BTreeNode::create_interior(PageType::TableLeaf,None,  Rc::clone(&pager));
        assert!(result.is_err());
    }

    #[test]
    fn test_cell_operations() {
        let pager = create_test_pager();
        
        // Create a leaf node
        let node = BTreeNode::create_leaf(PageType::TableLeaf, Rc::clone(&pager)).unwrap();
        
        // Initially the node has no cells
        assert_eq!(node.cell_count().unwrap(), 0);
        
        // Create a cell
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });
        
        // Insert the cell
        let index = node.insert_cell(cell.clone()).unwrap();
        assert_eq!(index, 0);
        
        // Now the node should have one cell
        assert_eq!(node.cell_count().unwrap(), 1);
        
        // Get the cell back
        let retrieved_cell = node.get_cell_owned(0).unwrap();
        match retrieved_cell {
            BTreeCell::TableLeaf(leaf) => {
                assert_eq!(leaf.row_id, 42);
                assert_eq!(leaf.payload, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
            },
            _ => panic!("Expected TableLeaf cell"),
        }
        
        // Test getting a cell with invalid index
        let result = node.get_cell_owned(1);
        assert!(result.is_err());
        
        // Test modifying a cell
        {
            let mut cell = node.get_cell_mut(0).unwrap();
            match *cell {
                BTreeCell::TableLeaf(ref mut leaf_cell) => {
                    leaf_cell.row_id = 99;
                },
                _ => panic!("Expected TableLeaf cell"),
            }
        }
        
        // Verify modification
        let modified_cell = node.get_cell_owned(0).unwrap();
        match modified_cell {
            BTreeCell::TableLeaf(leaf) => {
                assert_eq!(leaf.row_id, 99);
            },
            _ => panic!("Expected TableLeaf cell"),
        }
    }

    #[test]
    fn test_right_most_child() {
        let pager = create_test_pager();
        
        // Create an interior node
        let node = BTreeNode::create_interior(PageType::TableInterior,None,  Rc::clone(&pager)).unwrap();
        
        // Set the right-most child
        node.set_right_most_child(123).unwrap();
        
        // Get the right-most child
        let child = node.get_right_most_child().unwrap();
        assert_eq!(child, 123);
        
        // Try to set/get right-most child on a leaf node (should fail)
        let leaf_node = BTreeNode::create_leaf(PageType::TableLeaf, Rc::clone(&pager)).unwrap();
        let result = leaf_node.set_right_most_child(456);
        assert!(result.is_err());
        
        let result = leaf_node.get_right_most_child();
        assert!(result.is_err());
    }

    #[test]
    fn test_find_table_rowid() {
        let pager = create_test_pager();
        
        // Create a leaf node
        let node = BTreeNode::create_leaf(PageType::TableLeaf, Rc::clone(&pager)).unwrap();
        
        // Insert some cells with different rowids
        let cells = [
            BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 10,
                row_id: 10,
                payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                overflow_page: None,
            }),
            BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 10,
                row_id: 20,
                payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                overflow_page: None,
            }),
            BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 10,
                row_id: 30,
                payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                overflow_page: None,
            }),
        ];
        
        for cell in &cells {
            node.insert_cell(cell.clone()).unwrap();
        }
        
        // Find existing rowids
        let (found, idx) = node.find_table_rowid(10).unwrap();
        assert!(found);
        assert_eq!(idx, 0);
        
        let (found, idx) = node.find_table_rowid(20).unwrap();
        assert!(found);
        assert_eq!(idx, 1);
        
        let (found, idx) = node.find_table_rowid(30).unwrap();
        assert!(found);
        assert_eq!(idx, 2);
        
        // Find non-existing rowids
        let (found, idx) = node.find_table_rowid(5).unwrap();
        assert!(!found);
        assert_eq!(idx, 0); // Should be inserted before rowid 10
        
        let (found, idx) = node.find_table_rowid(15).unwrap();
        assert!(!found);
        assert_eq!(idx, 1); // Should be inserted between 10 and 20
        
        let (found, idx) = node.find_table_rowid(40).unwrap();
        assert!(!found);
        assert_eq!(idx, 3); // Should be inserted after rowid 30
        
        // Test error with non-leaf node
        let interior_node = BTreeNode::create_interior(PageType::TableInterior, None, Rc::clone(&pager)).unwrap();
        let result = interior_node.find_table_rowid(10);
        assert!(result.is_err());
    }


    #[test]
    fn test_find_table_key() {
        let pager = create_test_pager();
        
        // Create an interior node
        let node = BTreeNode::create_interior(PageType::TableInterior,None, Rc::clone(&pager)).unwrap();
        
        // Set right-most child
        node.set_right_most_child(40).unwrap();
        
        // Insert some cells with different keys
        let cells = [
            BTreeCell::TableInterior(TableInteriorCell {
                left_child_page: 10,
                key: 15,
            }),
            BTreeCell::TableInterior(TableInteriorCell {
                left_child_page: 20,
                key: 25,
            }),
            BTreeCell::TableInterior(TableInteriorCell {
                left_child_page: 30,
                key: 35,
            }),
        ];
        
        for cell in &cells {
            node.insert_cell(cell.clone()).unwrap();
            
        }
        
        // Find existing keys
        let (found, child, idx) = node.find_table_key(15).unwrap();
        assert!(found);
        assert_eq!(child, 10);
        assert_eq!(idx, 0);
        println!("Found key 15, child: {}, index: {}", child, idx);
        let (found, child, idx) = node.find_table_key(25).unwrap();
        assert!(found);
        assert_eq!(child, 20);
        assert_eq!(idx, 1);
        println!("Found key 15, child: {}, index: {}", child, idx);
        let (found, child, idx) = node.find_table_key(35).unwrap();
        assert!(found);
        assert_eq!(child, 30);
        assert_eq!(idx, 2);
        println!("Found key 15, child: {}, index: {}", child, idx);
        // Find non-existing keys
        let (found, child, _) = node.find_table_key(10).unwrap();
        assert!(!found);
        assert_eq!(child, 10); // Should go to the left child of the first cell
        println!("Found key 15, child: {}, index: {}", child, idx);
        let (found, child, _) = node.find_table_key(20).unwrap();
        assert!(!found);
        assert_eq!(child, 10); // Should go to the left child of the first cell
        
        let (found, child, _) = node.find_table_key(30).unwrap();
        assert!(!found);
        assert_eq!(child, 20); // Should go to the left child of the second cell
        
        let (found, child, _) = node.find_table_key(39).unwrap();
        assert!(!found);
        assert_eq!(child, 30); // Should go to the left child of the third cell
        
        let (found, child, _) = node.find_table_key(50).unwrap();
        assert!(!found);
        assert_eq!(child, 40); // Should go
    }

    #[test]
    fn test_find_index_key() {
        let pager = create_test_pager();
        
        // Create an index leaf node
        let node = BTreeNode::create_leaf(PageType::IndexLeaf, Rc::clone(&pager)).unwrap();
        
        // Create payloads with different keys
        let create_index_payload = |key: i64| -> Vec<u8> {
            let mut payload = Vec::new();
            crate::utils::serialization::serialize_values(
                &[crate::utils::serialization::SqliteValue::Integer(key)],
                &mut payload
            ).unwrap();
            payload
        };
        
        // Insert cells with different keys
        let keys = [10, 20, 30];
        for &key in &keys {
            let payload = create_index_payload(key);
            let cell = BTreeCell::IndexLeaf(IndexLeafCell {
                payload_size: payload.len() as u64,
                payload,
                overflow_page: None,
            });
            node.insert_cell(cell).unwrap();
        }
        
        // Find existing keys
        let (found, idx) = node.find_index_key(&KeyValue::Integer(10)).unwrap();
        assert!(found);
        assert_eq!(idx, 0);
        
        let (found, idx) = node.find_index_key(&KeyValue::Integer(20)).unwrap();
        assert!(found);
        assert_eq!(idx, 1);
        
        let (found, idx) = node.find_index_key(&KeyValue::Integer(30)).unwrap();
        assert!(found);
        assert_eq!(idx, 2);
        
        // Find non-existing keys
        let (found, idx) = node.find_index_key(&KeyValue::Integer(5)).unwrap();
        assert!(!found);
        assert_eq!(idx, 0); // Should be inserted before key 10
        
        let (found, idx) = node.find_index_key(&KeyValue::Integer(15)).unwrap();
        assert!(!found);
        assert_eq!(idx, 1); // Should be inserted between 10 and 20
        
        let (found, idx) = node.find_index_key(&KeyValue::Integer(40)).unwrap();
        assert!(!found);
        assert_eq!(idx, 3); // Should be inserted after key 30
    }

    #[test]
    fn test_insert_cell_ordered_table_leaf() {
        let pager = create_test_pager();
        
        // Create a leaf node
        let node = BTreeNode::create_leaf(PageType::TableLeaf, Rc::clone(&pager)).unwrap();
        
        // Insert cells with rowids in random order
        let rowids = [30, 10, 50, 20, 40];
        for &rowid in &rowids {
            let cell = BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 10,
                row_id: rowid,
                payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                overflow_page: None,
            });
            
            let result= node.insert_cell_ordered(cell);
            if result.is_err() {
               println!("Error inserting cell with rowid {}: {:?}", rowid, result.err());
            } else {
                // If no error, check the result
                // The node should not have split
                assert!(result.is_ok());
                // Check the returned values
            let (split, median_key, new_node) = result.unwrap();
                assert!(!split);
                assert!(median_key.is_none());
                assert!(new_node.is_none());
            }
        }
        
        // Verify cells are ordered by rowid
        let cell_count = node.cell_count().unwrap();
        assert_eq!(cell_count, 5);
        
        for i in 0..cell_count {
            let cell = node.get_cell_owned(i).unwrap();
            match cell {
                BTreeCell::TableLeaf(leaf) => {
                    assert_eq!(leaf.row_id, (i as i64 + 1) * 10);
                },
                _ => panic!("Expected TableLeaf cell"),
            }
        }
    }

    #[test]
    fn test_insert_cell_ordered_table_interior() {
        let pager = create_test_pager();
        
        // Create an interior node
        let node = BTreeNode::create_interior(PageType::TableInterior, None,Rc::clone(&pager)).unwrap();
        
        // Set right-most child
        node.set_right_most_child(100).unwrap();
        
        // Insert cells with keys in random order
        let keys = [(30, 45), (10, 15), (50, 75), (20, 30), (40,60)];
        for &(left_child, key) in &keys {
            let cell = BTreeCell::TableInterior(TableInteriorCell {
                left_child_page: left_child,
                key,
            });
            println!("Inserting cell with left_child: {}, key: {}", left_child, key);
            
            let (split, median_key, new_node) = node.insert_cell_ordered(cell).unwrap();
            assert!(!split);
            assert!(median_key.is_none());
            assert!(new_node.is_none());
        }
        
        // Verify cells are ordered by key
        let cell_count = node.cell_count().unwrap();
        assert_eq!(cell_count, 5);
        
        for i in 0..cell_count {
            println!("Verifying cell at index {}", i);
            // Get the cell at index i
            let cell = node.get_cell_owned(i).unwrap();
            match cell {
                BTreeCell::TableInterior(interior) => {
                    println!("Cell at index {}: left_child: {}, key: {}", i, interior.left_child_page, interior.key);
                    assert_eq!(interior.key, (i as i64 + 1) * 15);
                    assert_eq!(interior.left_child_page, (i as u32 + 1) * 10);
                },
                _ => panic!("Expected TableInterior cell"),
            }
        }
        
        // Verify right-most child is still set
        let right_most = node.get_right_most_child().unwrap();
        assert_eq!(right_most, 100);
    }

    #[test]
    fn test_node_split() {
        let pager = create_test_pager();
        
        // Create a leaf node
        let node = BTreeNode::create_leaf(PageType::TableLeaf, pager).unwrap();
        
        // Insert enough cells to cause a split
        // First, fill the node with 10 cells
        for i in 0..12 {
            let rowid = i;
            let cell = BTreeCell::TableLeaf(TableLeafCell {
                payload_size: 300, // Large payload to fill the node quickly
                row_id: rowid,
                payload: vec![i as u8; 300],
                overflow_page: None,
            });
            
            let (split, _, _) = node.insert_cell_ordered(cell).unwrap();
            assert!(!split); // No split yet
            println!("Inserted cell with rowid: {}", rowid);
        }
        println!("Node filled with cells, cell count: {}", node.cell_count().unwrap());
        // Now insert one more cell to cause a split
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 500,
            row_id: 13,
            payload: vec![10; 500],
            overflow_page: None,
        });
        
        let (split, median_key, new_node) = node.insert_cell_ordered(cell).unwrap();
        
        // The node should have split
        assert!(split);
        assert!(median_key.is_some());
        assert!(new_node.is_some());
        
        let median = median_key.unwrap();
        let right_node = new_node.unwrap();
        
        // Verify the median key
        assert!(median > 0);
        
        // Original node should have cells with rowid < median
        let left_cells = node.cell_count().unwrap();
        for i in 0..left_cells {
            let cell = node.get_cell_owned(i).unwrap();
            match cell {
                BTreeCell::TableLeaf(leaf) => {
                    assert!(leaf.row_id < median);
                },
                _ => panic!("Expected TableLeaf cell"),
            }
        }
        
        // New node should have cells with rowid >= median
        let right_cells = right_node.cell_count().unwrap();
        for i in 0..right_cells {
            let cell = right_node.get_cell_owned(i).unwrap();
            match cell {
                BTreeCell::TableLeaf(leaf) => {
                    assert!(leaf.row_id >= median);
                },
                _ => panic!("Expected TableLeaf cell"),
            }
        }
    }

    #[test]
    fn test_free_space() {
        let pager = create_test_pager();
        
        // Create a leaf node
        let node = BTreeNode::create_leaf(PageType::TableLeaf, Rc::clone(&pager)).unwrap();
        
        // Get initial free space
        let initial_free_space = node.free_space().unwrap();
        assert!(initial_free_space > 0);
        
        // Add a cell
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 100,
            row_id: 1,
            payload: vec![0; 100],
            overflow_page: None,
        });
        
        node.insert_cell(cell).unwrap();
        
        // Get free space after adding cell
        let free_space_after = node.free_space().unwrap();
        
        // Free space should have decreased
        assert!(free_space_after < initial_free_space);
        
        // The difference should be the cell size plus the cell index size (2 bytes)
        let cell_size = 100 + 1 + 1; // payload + varint(payload_size) + varint(rowid)
        let expected_diff = cell_size + 2;
        
        // Due to alignment and other factors, we may not get exact match,
        // but the difference should be close
        let actual_diff = initial_free_space - free_space_after;
        assert!(actual_diff >= expected_diff - 5 && actual_diff <= expected_diff + 5);
    }

    #[test]
    fn test_open_node() {
        let pager = create_test_pager();
        
        // Create a leaf node
        let node = BTreeNode::create_leaf(PageType::TableLeaf, Rc::clone(&pager)).unwrap();
        let page_number = node.page_number;
        
        // Add a cell to the node
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 42,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            overflow_page: None,
        });
        
        node.insert_cell(cell).unwrap();
        
        // Open the node again
        let reopened_node = BTreeNode::open(page_number, PageType::TableLeaf, Rc::clone(&pager)).unwrap();
        
        // Verify the node type and page number
        assert_eq!(reopened_node.node_type, PageType::TableLeaf);
        assert_eq!(reopened_node.page_number, page_number);
        
        // Verify the cell count
        assert_eq!(reopened_node.cell_count().unwrap(), 1);
        
        // Verify the cell content
        let cell = reopened_node.get_cell_owned(0).unwrap();
        match cell {
            BTreeCell::TableLeaf(leaf) => {
                assert_eq!(leaf.row_id, 42);
                assert_eq!(leaf.payload, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
            },
            _ => panic!("Expected TableLeaf cell"),
        }
        
        // Test opening with incorrect page type
        let result = BTreeNode::open(page_number, PageType::TableInterior, Rc::clone(&pager));
        assert!(result.is_err());
    }
}

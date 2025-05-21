//! # B-Tree Node Module
//!
//! This module defines the `BTreeNode` struct and its associated methods.
//! It provides functionality for creating, opening, and manipulating B-Tree nodes.
//!

use crate::page::{BTreeCell, BTreePage, Page, PageType, TableInteriorCell};
use crate::storage::Pager;
use crate::utils::cmp::KeyValue;

use std::hash::Hasher;
use std::io;
use std::io::Cursor;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

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
    use crate::utils::serialization::{deserialize_values, SqliteValue};

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
    /// Opted to use `Arc<Mutex<Pager>>` because each node needs a mutable reference to the pager,
    /// However, there is going to be only one pager in the whole B-Tree, and there will be only one writer at a time.
    /// This allows us to share the pager across multiple nodes while still allowing for mutable access.
    
    pager: Arc<Mutex<Pager>>, // Decided to switch to Arc<Mutex<Pager>> for thread safety and interior mutability. 
    // In SQLite, there is a limit of one writer at a time, so this is not a problem.
    // If we wanted to have multiple writers, we would need to use a more complex synchronization mechanism, maybe using `Mutex` or `RwLock`.
    // An issue is that we currently require a mutable reference to the pager just to read the page, which is not ideal.
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
    pub fn new(page_number: u32, node_type: PageType, shared_pager: Arc<Mutex<Pager>>) -> Self {
        BTreeNode {
            page_number,
            node_type,
            pager: Arc::clone(&shared_pager),
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
    pub fn open(
        page_number: u32,
        node_type: PageType,
        shared_pager: Arc<Mutex<Pager>>,
    ) -> io::Result<Self> {
        // Verify that the page exists and is of the correct type
            
            let pager_ref = shared_pager.lock().map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
                })?;
            pager_ref.get_page(page_number, Some(node_type), |page|{
                // Check if the page is of the correct type
                match page {
                    Page::BTree(btree_page) => {
                        if btree_page.header.page_type != node_type {
                            panic!(
                                "Unexpected page type: expected {:?}, obtained {:?}",
                                node_type, btree_page.header.page_type
                            );
                        }
                    }
                    _ => panic!("Expected BTree page"),
                };
                
            });
            

        // Creates a new BTreeNode with the given page number and type
        Ok(BTreeNode {
            page_number,
            node_type,
            pager: Arc::clone(&shared_pager),
        })
    }

    /// Obtains the number of cells in the node.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues or if the page is not of BTree type.
    pub fn cell_count(&self) -> io::Result<u16> {
        let pager_ref = self.pager.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
        })?;
        
        pager_ref.get_page(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => btree_page.header.cell_count,
                _ => panic!("Expected BTree page"),
            }
        })
    }


        /// Gets the free space in the node.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    pub fn free_space(&self) -> io::Result<usize> {
        let pager_ref = self.pager.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
        })?;
        
        pager_ref.get_page(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => btree_page.free_space(),
                _ => panic!("Expected BTree page"),
            }
        })
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

        let pager_ref = self.pager.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
        })?;

        pager_ref.get_page(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => {
                    match btree_page.header.right_most_page {
                        Some(page_number) => page_number,
                        None => panic!("Right-most child not found"),
                    }
                },
                _ => panic!("Expected BTree page"),
            }
        })
    }

    /// Sets the right-most child of the node (only for interior nodes).
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

        let pager_ref = self.pager.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
        })?;

        pager_ref.get_page_mut(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => {
                    btree_page.header.right_most_page = Some(page_number);
                    Ok(())
                },
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData, 
                    "Page is not of BTree type"
                )),
            }
        })?;
    Ok(())
    }

 

   

    // Creates a new Leaf btree node.
    ///
    /// # Parameters
    /// * `node_type` - Type of node (TableLeaf or IndexLeaf).
    /// * `pager` - Reference to the pager for I/O operations.
    ///
    /// # Errors
    /// Returns an error if the page cannot be created or if the type is not leaf.
    pub fn create_leaf(node_type: PageType, pager: Arc<Mutex<Pager>>) -> io::Result<Self> {
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
            pager: Arc::clone(&pager),
        };

        let page_number = {
            new_node
                .pager
                .lock().map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
                })?
                .create_btree_page(node_type, None)?
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
    pub fn create_interior(
        node_type: PageType,
        right_most_page: Option<u32>, // Decided to make this optional because it aids flexibility.
        pager: Arc<Mutex<Pager>>,
    ) -> io::Result<Self> {
        if !node_type.is_interior() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("The BTreeNode is not of type interior: {:?}", node_type),
            ));
        }

        let mut new_node = BTreeNode {
            page_number: 0,
            node_type,
            pager: Arc::clone(&pager),
        };

        let mut right_most_page = right_most_page;
        // If the right-most page is None, we set it to 0. As ponting to zero means pointing to nothing.
        if right_most_page.is_none() {
            right_most_page = Some(0);
        }

        let page_number = {
            new_node
                .pager
                .lock().map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
                })?
                .create_btree_page(node_type, right_most_page)?
        };

        new_node.page_number = page_number;
        Ok(new_node)
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

        let pager_ref = self.pager.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
        })?;

        pager_ref.get_page(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => {
                    let cell_count = btree_page.header.cell_count;

                    // Binary search
                    let mut left = 0;
                    let mut right = cell_count.saturating_sub(1) as i32;

                    while left <= right {
                        let mid = left + (right - left) / 2;
                        let mid_idx = mid as u16;

                        let cell = &btree_page.cells[mid as usize];
                        let cell_key = match cell {
                            BTreeCell::IndexLeaf(leaf_cell) => 
                                extract_key_from_payload(&leaf_cell.payload)?,
                            BTreeCell::IndexInterior(interior_cell) => 
                                extract_key_from_payload(&interior_cell.payload)?,
                            _ => return Err(io::Error::new(
                                io::ErrorKind::InvalidData, 
                                "Incorrect cell type"
                            )),
                        };

                        match cell_key.partial_cmp(index_key) {
                            Some(std::cmp::Ordering::Equal) => {
                                return Ok((true, mid_idx));
                            }
                            Some(std::cmp::Ordering::Greater) => {
                                right = mid - 1;
                            }
                            Some(std::cmp::Ordering::Less) => {
                                left = mid + 1;
                            }
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
                },
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData, 
                    "Page is not of BTree type"
                )),
            }
        })?
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

        let pager_ref = self.pager.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
        })?;

        pager_ref.get_page(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => {
                    let cell_count = btree_page.header.cell_count;

                    // If the node is empty, return immediately
                    if cell_count == 0 {
                        return Ok((false, 0));
                    }

                    // Binary search
                    let mut left = 0;
                    let mut right = cell_count.saturating_sub(1) as i32;

                    while left <= right {
                        let mid = left + (right - left) / 2;
                        let mid_idx = mid as u16;

                        let cell = &btree_page.cells[mid as usize];
                        let mid_rowid = match cell {
                            BTreeCell::TableLeaf(cell) => cell.row_id,
                            _ => return Err(io::Error::new(
                                io::ErrorKind::InvalidData, 
                                "Incorrect cell type"
                            )),
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
                    Ok((false, left as u16))
                },
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData, 
                    "Page is not of BTree type"
                )),
            }
        })?
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

        let pager_ref = self.pager.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
        })?;

        pager_ref.get_page(self.page_number, Some(self.node_type), |page| {
            match page {
                Page::BTree(btree_page) => {
                    let cell_count = btree_page.header.cell_count;

                    // Binary search
                    let mut left = 0;
                    let mut right = cell_count.saturating_sub(1) as i32;

                    while left <= right {
                        let mid = left + (right - left) / 2;
                        let mid_idx = mid as u16;

                        let cell = &btree_page.cells[mid as usize];
                        let mid_key = match cell {
                            BTreeCell::TableInterior(cell) => cell.key,
                            _ => return Err(io::Error::new(
                                io::ErrorKind::InvalidData, 
                                "Incorrect cell type"
                            )),
                        };

                        match mid_key.partial_cmp(&key) {
                            Some(std::cmp::Ordering::Equal) => {
                                // Found an exact match
                                let left_child = match cell {
                                    BTreeCell::TableInterior(cell) => cell.left_child_page,
                                    _ => unreachable!("Incorrect cell type"),
                                };

                                return Ok((true, left_child, mid_idx));
                            }
                            Some(std::cmp::Ordering::Greater) => {
                                // The key is to the left
                                right = mid - 1;
                            }
                            Some(std::cmp::Ordering::Less) => {
                                // The key is to the right
                                left = mid + 1;
                            }
                            None => {
                                // This should not happen with our implementation
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "Incomparable key types",
                                ));
                            }
                        }
                    }

                    if let Some(right_most_page) = btree_page.header.right_most_page {
                        // The key is greater than the largest key
                        if key > right_most_page as i64 {
                            return Ok((false, right_most_page, cell_count));
                        }
                    }

                    if cell_count == 0 || right < 0 {
                        // The node is empty or the key is less than the smallest key
                        if cell_count == 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::NotFound,
                                "Node is empty"
                            ));
                        }
                        
                        match &btree_page.cells[0] {
                            BTreeCell::TableInterior(cell) => {
                                let left_child = cell.left_child_page;
                                Ok((false, left_child, 0))
                            }
                            _ => Err(io::Error::new(
                                io::ErrorKind::InvalidData, 
                                "Incorrect cell type"
                            )),
                        }
                    } else {
                        // The key is between two keys
                        let idx = right as u16;
                        let cell = &btree_page.cells[idx as usize];
                        match cell {
                            BTreeCell::TableInterior(cell) => {
                                let left_child = cell.left_child_page;
                                Ok((false, left_child, idx))
                            }
                            _ => Err(io::Error::new(
                                io::ErrorKind::InvalidData, 
                                "Incorrect cell type"
                            )),
                        }
                    }
                },
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData, 
                    "Page is not of BTree type"
                )),
            }
        })?
    }


    // Inserts a cell into the node.
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
    // Verify that the cell type matches the node type
    match (&self.node_type, &cell) {
        (PageType::TableLeaf, BTreeCell::TableLeaf(_)) => {}
        (PageType::TableInterior, BTreeCell::TableInterior(_)) => {}
        (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {}
        (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {}
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Type of cell is not compatible with the type of node: {:?}",
                    self.node_type
                ),
            ));
        }
    }

    let pager_ref = self.pager.lock().map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
    })?;

    // Create a cloned cell that will be moved into the closure
    let cell_clone = cell.clone();
    
    // Use the get_page_mut method with a callback that handles errors properly
    pager_ref.get_page_mut(self.page_number, Some(self.node_type), move |page| {
        match page {
            Page::BTree(btree_page) => {
                // Add the cell to the page
                btree_page.add_cell(cell_clone)?;
                
                // Return the index of the newly inserted cell
                Ok(btree_page.header.cell_count - 1)
            },
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                "Page is not of BTree type"
            )),
        }
    })?
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
    // First, get basic information from the original node
    let mut pager_ref = self.pager.lock().map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
    })?;
    
    // Get cell count and determine split point
    let (cell_count, is_interior) = pager_ref.get_page(self.page_number, Some(self.node_type), |page| {
        match page {
            Page::BTree(btree_page) => (btree_page.header.cell_count, self.node_type.is_interior()),
            _ => panic!("Expected BTree page"),
        }
    })?;

    // Find the splitting point (middle of the node)
    let split_point = cell_count / 2;
    
    // Check that we have enough cells to split
    if cell_count <= 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Not enough cells to split node"
        ));
    }

    // Create a new node of the same type
    let new_node = match self.node_type {
        PageType::TableLeaf | PageType::IndexLeaf => {
            BTreeNode::create_leaf(self.node_type, Arc::clone(&self.pager))?
        },
        PageType::TableInterior | PageType::IndexInterior => {
            BTreeNode::create_interior(self.node_type, None, Arc::clone(&self.pager))?
        },
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot split a non-B-Tree page",
            ));
        }
    };

    // This complex operation needs to be broken into steps for the callback-based API
    
    // Step 1: Extract cells to move and determine the median key
    let (cells_to_move, median_info) = self.prepare_split_data(split_point)?;
    
    // Step 2: Move the cells to the new node
    self.move_cells_to_new_node(&new_node, cells_to_move)?;
    
    // Step 3: If necessary, set the right-most page of the new node
    if is_interior {
        // For interior nodes, update the right-most child of the new node
        let mut new_pager_ref = self.pager.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
        })?;
        
        // Here we specify that the callback returns Option<u32> to match the expected return type
        let right_most_opt: Option<u32> = new_pager_ref.get_page(self.page_number, Some(self.node_type), |page| {
            if let Page::BTree(btree_page) = page {
                btree_page.header.right_most_page
            } else {
                None
            }
        })?;
        
        // Now use the right_most value if it exists
        if let Some(right_most) = right_most_opt {
            new_node.set_right_most_child(right_most)?;
        }
    }
    
    Ok((new_node, median_info.0, median_info.1))
}

/// Helper method to prepare data for splitting a node
/// Returns the cells to move and the median key info
fn prepare_split_data(&self, split_point: u16) -> io::Result<(Vec<BTreeCell>, (i64, u16))> {
    let pager_ref = self.pager.lock().map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
    })?;
    
    let result = pager_ref.get_page_mut(self.page_number, Some(self.node_type), |page| {
        match page {
            Page::BTree(btree_page) => {
                // Check if there are enough cells to split
                if btree_page.cells.len() <= split_point as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Not enough cells to split",
                    ));
                }
                
                // Extract cells to move to the new node
                let mut cells_to_move = Vec::new();
                
                // For interior nodes, the splitting is different than for leaf nodes
                let median_info = if self.node_type.is_interior() {
                    // For interior nodes, we need the median key and to update right-most pointer
                    let right_most_page = btree_page.header.right_most_page;
                    
                    if right_most_page.is_none() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Interior node without right-most page pointer",
                        ));
                    }
                    
                    // The median is the middle cell (which will be promoted to parent)
                    let mid_cell_idx = split_point - 1;
                    let median_key = match &btree_page.cells[mid_cell_idx as usize] {
                        BTreeCell::TableInterior(cell) => {
                            // Update right-most pointer of original node
                            btree_page.header.right_most_page = Some(cell.left_child_page);
                            (cell.key, mid_cell_idx)
                        },
                        BTreeCell::IndexInterior(cell) => {
                            // Update right-most pointer of original node
                            btree_page.header.right_most_page = Some(cell.left_child_page);
                            
                            // Extract key from payload
                            let key_value = extract_key_from_payload(&cell.payload)?;
                            let key = match key_value {
                                KeyValue::Integer(i) => i,
                                KeyValue::Float(f) => f as i64,
                                _ => {
                                    // Hash other types for comparison
                                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                                    std::hash::Hash::hash(&key_value, &mut hasher);
                                    hasher.finish() as i64
                                }
                            };
                            (key, mid_cell_idx)
                        },
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Incorrect cell type for interior node",
                            ));
                        }
                    };
                    
                    // Collect cells to move (excluding the median)
                    for i in (mid_cell_idx as usize + 1)..btree_page.cells.len() {
                        cells_to_move.push(btree_page.cells[i].clone());
                    }
                    
                    // Remove the cells (including the median) from the original node
                    btree_page.cells.truncate(mid_cell_idx as usize);
                    btree_page.cell_indices.truncate(mid_cell_idx as usize);
                    
                    median_key
                } else {
                    // For leaf nodes, the median is the first cell in the second half
                    let median_cell = &btree_page.cells[split_point as usize];
                    
                    let median_key = match (self.node_type, median_cell) {
                        (PageType::TableLeaf, BTreeCell::TableLeaf(cell)) => {
                            (cell.row_id, split_point)
                        },
                        (PageType::IndexLeaf, BTreeCell::IndexLeaf(cell)) => {
                            // Extract key from payload
                            let key_value = extract_key_from_payload(&cell.payload)?;
                            let key = match key_value {
                                KeyValue::Integer(i) => i,
                                KeyValue::Float(f) => f as i64,
                                _ => {
                                    // Hash other types for comparison
                                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                                    std::hash::Hash::hash(&key_value, &mut hasher);
                                    hasher.finish() as i64
                                }
                            };
                            (key, split_point)
                        },
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Incorrect cell type for leaf node",
                            ));
                        }
                    };
                    
                    // Collect cells to move
                    for i in (split_point as usize)..btree_page.cells.len() {
                        cells_to_move.push(btree_page.cells[i].clone());
                    }
                    
                    // Remove the cells from the original node
                    btree_page.cells.truncate(split_point as usize);
                    btree_page.cell_indices.truncate(split_point as usize);
                    
                    median_key
                };
                
                // Update cell count in original node
                btree_page.header.cell_count = btree_page.cells.len() as u16;
                
                // Update content start offset
                btree_page.update_content_start_offset();
                
                return Ok((cells_to_move, median_info))
            },
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData, 
                "Page is not of BTree type"
            )),
        }
    })?;
    result
}

/// Helper method to move cells to a new node during split
fn move_cells_to_new_node(&self, new_node: &BTreeNode, cells: Vec<BTreeCell>) -> io::Result<()> {
    // Add each cell to the new node
    for cell in cells {
        new_node.insert_cell(cell)?;
    }
    
    Ok(())
}
    

    /// Inserts a cell in order according to the key.
///
/// # Parameters
/// * `cell` - Cell to insert.
///
/// # Errors
/// Returns an error if the cell type does not match the node type,
/// if there's not enough space, or if there are I/O issues.
///
/// # Returns
/// Tuple with:
/// - `true` if the node was split, `false` otherwise
/// - Median key (if the node was split)
/// - New node (if the node was split)
pub fn insert_cell_ordered(
    &self,
    cell: BTreeCell,
) -> io::Result<(bool, Option<i64>, Option<BTreeNode>)> {
    // Verify that the cell type matches the node type
    match (&self.node_type, &cell) {
        (PageType::TableLeaf, BTreeCell::TableLeaf(_)) => {}
        (PageType::TableInterior, BTreeCell::TableInterior(_)) => {}
        (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {}
        (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {}
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Cell type incompatible with node type: {:?}",
                    self.node_type
                ),
            ));
        }
    }

    // Calculate cell size
    let cell_size = cell.size();
    let cell_index_size = 2; // 2 bytes for the cell index
    
    // Get the free space in the node
    let free_space = self.free_space()?;
    
    // Check if there's enough space for the new cell
    if free_space < cell_size + cell_index_size {
        // Not enough space, split the node
        let (new_node, median_key, _) = self.split()?;
        
        // Determine which node should receive the new cell
        let insert_in_new = match (&self.node_type, &cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(table_cell)) => {
                table_cell.row_id >= median_key
            }
            (PageType::TableInterior, BTreeCell::TableInterior(table_cell)) => {
                table_cell.key >= median_key
            }
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
                        // Fallback for incomparable types
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        std::hash::Hash::hash(&key_value, &mut hasher);
                        let hashed_key = hasher.finish() as i64;
                        hashed_key >= median_key
                    }
                }
            }
            (PageType::IndexInterior, BTreeCell::IndexInterior(index_cell)) => {
                // Similar to IndexLeaf
                let key_value = extract_key_from_payload(&index_cell.payload)?;
                let median_key_value = KeyValue::Integer(median_key);

                match key_value.partial_cmp(&median_key_value) {
                    Some(std::cmp::Ordering::Less) => false,
                    Some(_) => true,
                    None => {
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        std::hash::Hash::hash(&key_value, &mut hasher);
                        let hashed_key = hasher.finish() as i64;
                        hashed_key >= median_key
                    }
                }
            }
            _ => unreachable!("Cell type already verified"),
        };

        // Insert the cell into the appropriate node
        if insert_in_new {
            new_node.insert_cell_ordered(cell)?;
        } else {
            self.insert_cell_ordered(cell)?;
        }

        return Ok((true, Some(median_key), Some(new_node)));
    }

    // There's enough space, find the correct position to insert
    let position = self.find_position_for_cell(&cell)?;
    
    // Insert the cell at the calculated position
    let pager_ref = self.pager.lock().map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
    })?;
    
    let cell_clone = cell.clone();
    pager_ref.get_page_mut(self.page_number, Some(self.node_type), move |page| {
        match page {
            Page::BTree(btree_page) => {
                // Calculate appropriate cell offset in the page
                let offset = if btree_page.header.cell_count > 0 && position < btree_page.header.cell_count {
                    // For interior cells, try to maintain proper spacing
                    let existing_offset = btree_page.cell_indices[position as usize];
                    existing_offset - cell_size as u16
                } else {
                    // Calculate a new offset for the end of the page
                    btree_page.header.content_start_offset - cell_size as u16
                };

                // Insert the cell and its index
                if position < btree_page.header.cell_count {
                    btree_page.cells.insert(position as usize, cell_clone);
                    btree_page.cell_indices.insert(position as usize, offset);
                } else {
                    btree_page.cells.push(cell_clone);
                    btree_page.cell_indices.push(offset);
                }

                // Update page metadata
                btree_page.header.cell_count += 1;
                btree_page.header.content_start_offset = btree_page
                    .cell_indices
                    .iter()
                    .min()
                    .copied()
                    .unwrap_or(btree_page.header.content_start_offset);
                
                Ok(())
            },
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Page is not of BTree type"
            )),
        }
    })?;

    Ok((false, None, None))
}

/// Finds the appropriate position to insert a cell based on its key
fn find_position_for_cell(&self, cell: &BTreeCell) -> io::Result<u16> {
    match (self.node_type, cell) {
        (PageType::TableLeaf, BTreeCell::TableLeaf(table_cell)) => {
            // Position based on rowid
            let (_, idx) = self.find_table_rowid(table_cell.row_id)?;
            Ok(idx)
        },
        (PageType::TableInterior, BTreeCell::TableInterior(table_cell)) => {
            let pager_ref = self.pager.lock().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e))
            })?;
            
            pager_ref.get_page(self.page_number, Some(self.node_type), |page| {
                match page {
                    Page::BTree(btree_page) => {
                        if btree_page.header.cell_count == 0 {
                            return Ok(0);
                        }
                        
                        // Binary search for position
                        let mut left = 0;
                        let mut right = btree_page.header.cell_count as i32 - 1;
                        let mut pos = 0;
                        
                        while left <= right {
                            let mid = left + (right - left) / 2;
                            let mid_cell = &btree_page.cells[mid as usize];
                            
                            match mid_cell {
                                BTreeCell::TableInterior(interior_cell) => {
                                    if interior_cell.key == table_cell.key {
                                        // Exact match, replace cell
                                        return Ok(mid as u16);
                                    } else if interior_cell.key > table_cell.key {
                                        right = mid - 1;
                                    } else {
                                        left = mid + 1;
                                        pos = left as u16;
                                    }
                                },
                                _ => return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "Incorrect cell type"
                                )),
                            }
                        }
                        
                        Ok(pos)
                    },
                    _ => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Page is not of BTree type"
                    )),
                }
            })?
        },
        (PageType::IndexLeaf, BTreeCell::IndexLeaf(index_cell)) => {
            // Extract key from payload
            let key_value = extract_key_from_payload(&index_cell.payload)?;
            
            // Find position based on key
            let (found, idx) = self.find_index_key(&key_value)?;
            
            // If found, return the exact position (for replacement)
            if found {
                Ok(idx)
            } else {
                Ok(idx) // Insert at this position
            }
        },
        (PageType::IndexInterior, BTreeCell::IndexInterior(index_cell)) => {
            // Extract key from payload
            let key_value = extract_key_from_payload(&index_cell.payload)?;
            
            // Find position based on key
            let (found, idx) = self.find_index_key(&key_value)?;
            
            // If found, return the exact position (for replacement)
            if found {
                Ok(idx)
            } else {
                Ok(idx) // Insert at this position
            }
        },
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Cell type incompatible with node type"
        )),
    }
}
}

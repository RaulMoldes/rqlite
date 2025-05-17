//! # B-Tree Implementation Module
//! 
//! This module implements the complete B-Tree structure for SQLite,
//! providing operations for creating, searching, inserting, and deleting.
//! It supports both table B-trees (which store rows) and index B-trees
//! (which store indexed values).

use std::io;
use std::rc::Rc;
use std::cell::RefCell;

use crate::page::{Page, PageType, BTreeCell};
use crate::storage::Pager;
use crate::tree::node::BTreeNode;
use crate::tree::cell::BTreeCellFactory;
use crate::tree::record::Record;
use crate::utils::cmp::KeyValue;
use crate::tree::node::extract_key_from_payload;
use std::hash::Hasher;
/// Represents a B-Tree in SQLite.
///
/// A B-Tree is a self-balancing tree data structure that maintains
/// sorted data and allows searches, insertions, and deletions in
/// logarithmic time.
pub struct BTree {
    /// Number of the root page of the tree
    root_page: u32,
    /// Type of B-Tree (table or index)
    tree_type: TreeType,
    /// Shared reference to the pager for I/O operations
    pager: Rc<RefCell<Pager>>,
    /// Size of each page in bytes
    page_size: u32,
    /// Reserved space at the end of each page
    reserved_space: u8,
    /// Maximum fraction of page that can be used by a payload
    max_payload_fraction: u8,
    /// Minimum fraction of page that must be used by a payload
    min_payload_fraction: u8,
}

/// Type of B-Tree
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeType {
    /// B-Tree for a table (stores rows)
    Table,
    /// B-Tree for an index (stores index entries)
    Index,
}

impl BTree {
    /// Creates a new B-Tree instance.
    ///
    /// # Parameters
    /// * `root_page` - Number of the root page.
    /// * `tree_type` - Type of the tree (table or index).
    /// * `pager` - Reference to the pager for I/O operations.
    /// * `page_size` - Size of page in bytes.
    /// * `reserved_space` - Reserved space at the end of each page.
    /// * `max_payload_fraction` - Maximum fraction of a page that can be occupied by a payload.
    /// * `min_payload_fraction` - Minimum fraction of a page that must be occupied by a payload.
    ///
    /// # Returns
    /// A new B-Tree instance.
    pub fn new(
        root_page: u32,
        tree_type: TreeType,
        pager: Rc<RefCell<Pager>>,
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

    /// Creates a new empty B-Tree.
    ///
    /// # Parameters
    /// * `tree_type` - Type of tree (table or index).
    /// * `pager` - Reference to the pager for I/O operations.
    /// * `page_size` - Size of page in bytes.
    /// * `reserved_space` - Reserved space at the end of each page.
    /// * `max_payload_fraction` - Maximum fraction of a page that can be occupied by a payload.
    /// * `min_payload_fraction` - Minimum fraction of a page that must be occupied by a payload.
    ///
    /// # Errors
    /// Returns an error if the root page cannot be created.
    ///
    /// # Returns
    /// A new B-Tree instance.
    pub fn create(
        tree_type: TreeType,
        pager: Rc<RefCell<Pager>>,
        page_size: u32,
        reserved_space: u8,
        max_payload_fraction: u8,
        min_payload_fraction: u8,
    ) -> io::Result<Self> {
        // Create the root page (always a leaf)
        let page_type = match tree_type {
            TreeType::Table => PageType::TableLeaf,
            TreeType::Index => PageType::IndexLeaf,
        };
        
        // Create the root node
        let node = BTreeNode::create_leaf(page_type, Rc::clone(&pager))?;
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

    /// Opens an existing B-Tree.
    ///
    /// # Parameters
    /// * `root_page` - Number of the root page.
    /// * `tree_type` - Type of tree (table or index).
    /// * `pager` - Reference to the pager for I/O operations.
    /// * `page_size` - Size of page in bytes.
    /// * `reserved_space` - Reserved space at the end of each page.
    /// * `max_payload_fraction` - Maximum fraction of a page that can be occupied by a payload.
    /// * `min_payload_fraction` - Minimum fraction of a page that must be occupied by a payload.
    ///
    /// # Errors
    /// Returns an error if the root page does not exist or is not valid.
    ///
    /// # Returns
    /// A B-Tree instance representing the existing tree.
    pub fn open(
        root_page: u32,
        tree_type: TreeType,
        pager: Rc<RefCell<Pager>>,
        page_size: u32,
        reserved_space: u8,
        max_payload_fraction: u8,
        min_payload_fraction: u8,
    ) -> io::Result<Self> {
        // Verify that the root page exists and is valid
        {
            let mut pager_ref = pager.borrow_mut();
            let page = pager_ref.get_page(root_page, None)?;
            
            match page {
                Page::BTree(btree_page) => {
                    match (tree_type, btree_page.header.page_type) {
                        (TreeType::Table, PageType::TableLeaf) | (TreeType::Table, PageType::TableInterior) => {},
                        (TreeType::Index, PageType::IndexLeaf) | (TreeType::Index, PageType::IndexInterior) => {},
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Root page type does not match tree type",
                            ));
                        }
                    }
                },
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Root page is not a B-Tree page",
                    ));
                }
            }
        }
        
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
    
    /// Gets the maximum payload size that can be stored locally in a page.
    ///
    /// # Returns
    /// Maximum payload size in bytes.
    fn max_local_payload(&self) -> usize {
        let usable_size = self.page_size as usize - self.reserved_space as usize;
        BTreeCellFactory::max_local_payload(usable_size, self.max_payload_fraction)
    }

    /// Gets the minimum payload size that must be stored locally in a page.
    ///
    /// # Returns
    /// Minimum payload size in bytes.
    fn min_local_payload(&self) -> usize {
        let usable_size = self.page_size as usize - self.reserved_space as usize;
        BTreeCellFactory::min_local_payload(usable_size, self.min_payload_fraction)
    }

    /// Gets the usable size of a page (excluding reserved space).
    ///
    /// # Returns
    /// Usable size in bytes.
    fn usable_page_size(&self) -> usize {
        self.page_size as usize - self.reserved_space as usize
    }

    /// Finds a record in a table B-Tree by its rowid.
    ///
    /// # Parameters
    /// * `rowid` - Row ID to search for.
    ///
    /// # Errors
    /// Returns an error if the tree is not a table tree or if there are I/O issues.
    ///
    /// # Returns
    /// The record found, or `None` if no record exists with the given rowid.
    pub fn find(&self, rowid: i64) -> io::Result<Option<Record>> {
        if self.tree_type != TreeType::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot find record in an index tree",
            ));
        }
        
        // Start at the root page
        let mut current_page = self.root_page;
   
        let mut current_type = self.get_page_type(current_page)?;
        let mut is_leaf = current_type.is_leaf();
        
        // Traverse the tree until we reach a leaf node
        while !is_leaf {
            let node = BTreeNode::open(current_page, current_type, Rc::clone(&self.pager))?;
            
            // Find the child that may contain the key
            let (_, child_page, _) = node.find_table_key(rowid)?;
            
            // Move to the child
            current_page = child_page;
            current_type = self.get_page_type(current_page)?;
            is_leaf = current_type.is_leaf();
        }
        
        // We're at a leaf node, look for the key
        let leaf_node = BTreeNode::open(current_page, current_type, Rc::clone(&self.pager))?;
        let (found, idx) = leaf_node.find_table_rowid(rowid)?;
        
        if !found {
            // Key not found
            return Ok(None);
        }
        
        // Get the record from the cell
        let cell = leaf_node.get_cell_owned(idx)?;
        
        match cell {
            BTreeCell::TableLeaf(leaf_cell) => {
                // Get payload from the cell
                let mut payload = leaf_cell.payload.clone();
                
                // Handle overflow chain if present
                if let Some(overflow_page) = leaf_cell.overflow_page {
                    payload.extend_from_slice(&self.read_overflow_chain(overflow_page)?);
                }
                
                // Deserialize the record
                let (record, _) = Record::from_bytes(&payload)?;
                Ok(Some(record))
            },
            _ => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Expected a table leaf cell",
                ))
            }
        }
    }

    /// Finds a key in an index B-Tree.
    ///
    /// # Parameters
    /// * `key` - Key to search for.
    ///
    /// # Errors
    /// Returns an error if the tree is not an index tree or if there are I/O issues.
    ///
    /// # Returns
    /// Tuple with:
    /// - `true` if the key was found, `false` otherwise
    /// - leaf page where the key is or should be
    /// - index of the cell in the leaf page
    pub fn find_index_key(&self, key: &KeyValue) -> io::Result<(bool, u32, u16)> {
        if self.tree_type != TreeType::Index {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot find index key in a table tree",
            ));
        }
        
        // Start at the root page
        let mut current_page = self.root_page;
        
        let mut current_type = self.get_page_type(current_page)?;
        let mut is_leaf = current_type.is_leaf();
        
        // Traverse the tree until we reach a leaf node
        while !is_leaf {
            let node = BTreeNode::open(current_page, current_type, Rc::clone(&self.pager))?;
            
            // Find the position of the key in the interior node
            let (found, idx) = node.find_index_key(key)?;
            
            if found {
                // If we found the key in an interior node, we need to follow the left child
                let cell = node.get_cell_owned(idx)?;
                match cell {
                    BTreeCell::IndexInterior(interior_cell) => {
                        current_page = interior_cell.left_child_page;
                    },
                    _ => unreachable!("Expected an index interior cell"),
                }
            } else if idx == 0 {
                // The key is smaller than all keys in this node
                // Get the leftmost child from the first cell
                let cell = node.get_cell_owned(0)?;
                match cell {
                    BTreeCell::IndexInterior(interior_cell) => {
                        current_page = interior_cell.left_child_page;
                    },
                    _ => unreachable!("Expected an index interior cell"),
                }
            } else if idx >= node.cell_count()? {
                // The key is larger than all keys in this node
                // Get the rightmost child
                current_page = node.get_right_most_child()?;
            } else {
                // The key falls between two keys
                // Get the left child of the next cell
                let cell = node.get_cell_owned(idx)?;
                match cell {
                    BTreeCell::IndexInterior(interior_cell) => {
                        current_page = interior_cell.left_child_page;
                    },
                    _ => unreachable!("Expected an index interior cell"),
                }
            }
            
            // Update the page type for the next iteration
            current_type = self.get_page_type(current_page)?;
            is_leaf = current_type.is_leaf();
        }
        
        // We're at a leaf node, look for the key
        let leaf_node = BTreeNode::open(current_page, current_type, Rc::clone(&self.pager))?;
        let (found, idx) = leaf_node.find_index_key(key)?;
        
        Ok((found, current_page, idx))
    }

    /// Inserts a record into a table B-Tree.
    ///
    /// # Parameters
    /// * `rowid` - Row ID for the record.
    /// * `record` - Record to insert.
    ///
    /// # Errors
    /// Returns an error if the tree is not a table tree or if there are I/O issues.
    pub fn insert(&mut self, rowid: i64, record: &Record) -> io::Result<()> {
        if self.tree_type != TreeType::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot insert record into an index tree",
            ));
        }
        
        // Serialize the record
        let payload = record.to_bytes()?;
        
        // Create a table leaf cell
        let (cell, overflow_data) = BTreeCellFactory::create_table_leaf_cell(
            rowid,
            payload,
            self.max_local_payload(),
            self.min_local_payload(),
            self.usable_page_size(),
        )?;
        
        // Handle overflow if needed
        let cell = if let Some(overflow_data) = overflow_data {
            match cell {
                BTreeCell::TableLeaf(mut leaf_cell) => {
                    // Create overflow pages for the overflow data
                    let overflow_page = self.create_overflow_chain(overflow_data)?;
                    leaf_cell.overflow_page = Some(overflow_page);
                    BTreeCell::TableLeaf(leaf_cell)
                },
                _ => unreachable!("Expected a table leaf cell"),
            }
        } else {
            cell
        };
        
        // Find the leaf node where the record should be inserted
        let (leaf_page, path) = self.find_leaf_for_insert(rowid)?;
        let leaf_node = BTreeNode::open(leaf_page, PageType::TableLeaf, Rc::clone(&self.pager))?;
        
        // Try to insert the cell
        let (split, median_key, new_node) = leaf_node.insert_cell_ordered(cell)?;
        
        if split {
            // Propagate the split up the tree
            self.propagate_split(leaf_node, new_node.unwrap(), median_key.unwrap(), path)?;
        }
        
        Ok(())
    }

    /// Inserts a key into an index B-Tree.
    ///
    /// # Parameters
    /// * `key` - Key to insert (usually a serialized form of the indexed column).
    /// * `rowid` - Row ID associated with the key.
    ///
    /// # Errors
    /// Returns an error if the tree is not an index tree or if there are I/O issues.
    pub fn insert_index(&mut self, key: &[u8], rowid: i64) -> io::Result<()> {
        if self.tree_type != TreeType::Index {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot insert index entry into a table tree",
            ));
        }
        
        // Create a payload that contains both the key and rowid
        // Format: [key, rowid]
        
        // Extract key value for comparison
        let key_value = extract_key_from_payload(key)?;
        
        // Create a payload containing the key and rowid
        let mut payload = key.to_vec();
        
        // Add the rowid at the end of the payload
        let rowid_record = Record::with_values(vec![crate::utils::serialization::SqliteValue::Integer(rowid)]);
        let rowid_bytes = rowid_record.to_bytes()?;
        payload.extend_from_slice(&rowid_bytes);
        
        // Create an index leaf cell
        let (cell, overflow_data) = BTreeCellFactory::create_index_leaf_cell(
            payload,
            self.max_local_payload(),
            self.min_local_payload(),
            self.usable_page_size(),
        )?;
        
        // Handle overflow if needed
        let cell = if let Some(overflow_data) = overflow_data {
            match cell {
                BTreeCell::IndexLeaf(mut leaf_cell) => {
                    // Create overflow pages for the overflow data
                    let overflow_page = self.create_overflow_chain(overflow_data)?;
                    leaf_cell.overflow_page = Some(overflow_page);
                    BTreeCell::IndexLeaf(leaf_cell)
                },
                _ => unreachable!("Expected an index leaf cell"),
            }
        } else {
            cell
        };
        
        // Find the leaf node where the key should be inserted
        let (_found, leaf_page, _index) = self.find_index_key(&key_value)?;
        let leaf_node = BTreeNode::open(leaf_page, PageType::IndexLeaf, Rc::clone(&self.pager))?;
        
        // Try to insert the cell
        let (split, median_key, new_node) = leaf_node.insert_cell_ordered(cell)?;
        
        if split {
            // Propagate the split up the tree
            let path = self.get_path_to_leaf(leaf_page, &key_value)?;
            self.propagate_split(leaf_node, new_node.unwrap(), median_key.unwrap(), path)?;
        }
        
        Ok(())
    }

    /// Deletes a record from a table B-Tree.
    ///
    /// # Parameters
    /// * `rowid` - Row ID of the record to delete.
    ///
    /// # Errors
    /// Returns an error if the tree is not a table tree or if there are I/O issues.
    ///
    /// # Returns
    /// `true` if a record was deleted, `false` if the record was not found.
    pub fn delete(&mut self, rowid: i64) -> io::Result<bool> {
        if self.tree_type != TreeType::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot delete record from an index tree",
            ));
        }
        
        // Find the leaf node containing the record
        let (leaf_page, path) = self.find_leaf_for_insert(rowid)?;
        let leaf_node = BTreeNode::open(leaf_page, PageType::TableLeaf, Rc::clone(&self.pager))?;
        
        // Find the record in the leaf
        let (found, idx) = leaf_node.find_table_rowid(rowid)?;
        
        if !found {
            // Record not found
            return Ok(false);
        }
        
        // Get the cell to handle overflow pages
        let cell = leaf_node.get_cell_owned(idx)?;
        if let BTreeCell::TableLeaf(leaf_cell) = &cell {
            if let Some(overflow_page) = leaf_cell.overflow_page {
                // Free overflow pages
                self.free_overflow_chain(overflow_page)?;
            }
        }
        
        // Delete the cell from the page
        {
            let mut pager = self.pager.borrow_mut();
            let page = pager.get_page_mut(leaf_page, Some(PageType::TableLeaf))?;
            
            if let Page::BTree(btree_page) = page {
                // Remove the cell and its index
                btree_page.cells.remove(idx as usize);
                btree_page.cell_indices.remove(idx as usize);
                btree_page.header.cell_count -= 1;
                
                // Update the content start offset
                btree_page.update_content_start_offset();
            } else {
                unreachable!("Expected a B-Tree page");
            }
        }
        
        // Check if the node is underfilled and needs rebalancing
        self.rebalance_after_delete(leaf_page, path)?;
        
        Ok(true)
    }

    /// Deletes a key from an index B-Tree.
    ///
    /// # Parameters
    /// * `key` - Key to delete.
    ///
    /// # Errors
    /// Returns an error if the tree is not an index tree or if there are I/O issues.
    ///
    /// # Returns
    /// `true` if a key was deleted, `false` if the key was not found.
    pub fn delete_index(&mut self, key: &KeyValue) -> io::Result<bool> {
        if self.tree_type != TreeType::Index {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot delete index entry from a table tree",
            ));
        }
        
        // Find the leaf node containing the key
        let (found, leaf_page, idx) = self.find_index_key(key)?;
        
        if !found {
            // Key not found
            return Ok(false);
        }
        
        // Get the cell to handle overflow pages
        let leaf_node = BTreeNode::open(leaf_page, PageType::IndexLeaf, Rc::clone(&self.pager))?;
        let cell = leaf_node.get_cell_owned(idx)?;
        
        if let BTreeCell::IndexLeaf(leaf_cell) = &cell {
            if let Some(overflow_page) = leaf_cell.overflow_page {
                // Free overflow pages
                self.free_overflow_chain(overflow_page)?;
            }
        }
        
        // Delete the cell from the page
        {
            let mut pager = self.pager.borrow_mut();
            let page = pager.get_page_mut(leaf_page, Some(PageType::IndexLeaf))?;
            
            if let Page::BTree(btree_page) = page {
                // Remove the cell and its index
                btree_page.cells.remove(idx as usize);
                btree_page.cell_indices.remove(idx as usize);
                btree_page.header.cell_count -= 1;
                
                // Update the content start offset
                btree_page.update_content_start_offset();
            } else {
                unreachable!("Expected a B-Tree page");
            }
        }
        
        // Get the path to the leaf for rebalancing
        let path = self.get_path_to_leaf(leaf_page, key)?;
        
        // Check if the node is underfilled and needs rebalancing
        self.rebalance_after_delete(leaf_page, path)?;
        
        Ok(true)
    }

    /// Creates a chain of overflow pages to store additional data.
    ///
    /// # Parameters
    /// * `data` - Data to store in overflow pages.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// Page number of the first overflow page.
    fn create_overflow_chain(&self, data: Vec<u8>) -> io::Result<u32> {
        let mut pager = self.pager.borrow_mut();
        
        // Calculate how much data can fit in each overflow page
        let data_per_page = self.page_size as usize - 4; // 4 bytes for next_page pointer
        
        // Split the data into chunks
        let chunks: Vec<_> = data.chunks(data_per_page).collect();
        let chunk_count = chunks.len();
        
        if chunk_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No data to store in overflow chain",
            ));
        }
        
        // Create the last page first (with next_page = 0)
        let last_chunk = chunks[chunk_count - 1];
        let last_page = pager.create_overflow_page(0, last_chunk.to_vec())?;
        
        if chunk_count == 1 {
            return Ok(last_page);
        }
        
        // Create the remaining pages in reverse order
        let mut next_page = last_page;
        
        for i in (0..chunk_count - 1).rev() {
            let page = pager.create_overflow_page(next_page, chunks[i].to_vec())?;
            next_page = page;
        }
        
        Ok(next_page)
    }

    /// Reads the data from a chain of overflow pages.
    ///
    /// # Parameters
    /// * `first_page` - Page number of the first overflow page.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// Data stored in the overflow chain.
    fn read_overflow_chain(&self, first_page: u32) -> io::Result<Vec<u8>> {
        let mut pager = self.pager.borrow_mut();
        let mut result = Vec::new();
        let mut current_page = first_page;
        
        while current_page != 0 {
            let page = pager.get_page(current_page, Some(PageType::Overflow))?;
            
            match page {
                Page::Overflow(overflow) => {
                    result.extend_from_slice(&overflow.data);
                    current_page = overflow.next_page;
                },
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Expected overflow page, got something else: {}", current_page),
                    ));
                }
            }
        }
        
        Ok(result)
    }

    /// Frees a chain of overflow pages.
    ///
    /// # Parameters
    /// * `first_page` - Page number of the first overflow page.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    fn free_overflow_chain(&self, first_page: u32) -> io::Result<()> {
        // Currently, we just mark the pages as free
        // In a complete implementation, you would add them to the freelist
        let mut pager = self.pager.borrow_mut();
        let mut current_page = first_page;
        
        while current_page != 0 {
            let page = pager.get_page(current_page, Some(PageType::Overflow))?;
            
            match page {
                Page::Overflow(overflow) => {
                    let next_page = overflow.next_page;
                    
                    // Mark the page as free (add to freelist)
                    // This would be implemented differently in a full SQLite implementation
                    // For now, we just create a free page
                    pager.create_free_page(0)?;
                    
                    current_page = next_page;
                },
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Expected overflow page, got something else: {}", current_page),
                    ));
                }
            }
        }
        
        Ok(())
    }

    /// Finds the leaf node where a key should be inserted and the path from the root.
    ///
    /// # Parameters
    /// * `key` - Key to insert.
    ///
    /// # Errors
    /// Returns an error if there are I/O issues.
    ///
    /// # Returns
    /// Tuple with:
    /// - Page number of the leaf node
    /// - Path from root to the leaf (excluding the leaf itself)
    fn find_leaf_for_insert(&self, key: i64) -> io::Result<(u32, Vec<u32>)> {
        let mut current_page = self.root_page;
        let mut path = Vec::new();
     
        // Get the root page type
        let root_type = self.get_page_type(current_page)?;
        let mut is_leaf = root_type.is_leaf();
        let mut current_type = root_type;
        
        // Traverse the tree until we reach a leaf
        while !is_leaf {
            path.push(current_page);
            
            let node = BTreeNode::open(current_page, current_type, Rc::clone(&self.pager))?;
            
            // Find which child might contain the key
            let (_, child_page, _) = node.find_table_key(key)?;
            current_page = child_page;
            
            // Update the page type
            current_type = self.get_page_type(current_page)?;
            is_leaf = current_type.is_leaf();
        }
        
        Ok((current_page, path))
    }

    /// Gets the path from the root/// Gets the path from the root to a leaf node for a specific key in an index tree.
///
/// # Parameters
/// * `leaf_page` - Page number of the leaf node.
/// * `key` - Key to find the path for.
///
/// # Errors
/// Returns an error if there are I/O issues.
///
/// # Returns
/// Path from root to the leaf (excluding the leaf itself).
fn get_path_to_leaf(&self, leaf_page: u32, key: &KeyValue) -> io::Result<Vec<u32>> {
    if leaf_page == self.root_page {
        // If the leaf is the root, there's no path
        return Ok(Vec::new());
    }
    
    let mut current_page = self.root_page;
    let mut path = Vec::new();
    
    
    // Get the root page type
    let root_type = self.get_page_type(current_page)?;
    let mut is_leaf = root_type.is_leaf();
    let mut current_type = root_type;
    
    // Traverse the tree until we reach a leaf
    while !is_leaf {
        path.push(current_page);
        
        let node = BTreeNode::open(current_page, current_type, Rc::clone(&self.pager))?;
        
        // Find which child might contain the key
        let (found, idx) = node.find_index_key(key)?;
        
        let child_page = if found {
            // If the key is found, follow the left child
            let cell = node.get_cell_owned(idx)?;
            match cell {
                BTreeCell::IndexInterior(cell) => cell.left_child_page,
                _ => unreachable!("Expected an index interior cell"),
            }
        } else if idx == 0 {
            // Key is smaller than all keys in the node, take the leftmost child
            let cell = node.get_cell_owned(0)?;
            match cell {
                BTreeCell::IndexInterior(cell) => cell.left_child_page,
                _ => unreachable!("Expected an index interior cell"),
            }
        } else if idx >= node.cell_count()? {
            // Key is larger than all keys in the node, take the rightmost child
            node.get_right_most_child()?
        } else {
            // Key is in between keys in the node, take the appropriate child
            let cell = node.get_cell_owned(idx)?;
            match cell {
                BTreeCell::IndexInterior(cell) => cell.left_child_page,
                _ => unreachable!("Expected an index interior cell"),
            }
        };
        
        // If we've reached our target leaf, we're done
        if child_page == leaf_page {
            return Ok(path);
        }
        
        // Continue to the next level
        current_page = child_page;
        current_type = self.get_page_type(current_page)?;
        is_leaf = current_type.is_leaf();
        
        // If we've reached a leaf that isn't our target, something went wrong
        if is_leaf && current_page != leaf_page {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Could not find path to leaf",
            ));
        }
    }
    
    // If we get here, we didn't find the right path
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Could not find path to leaf",
    ))
}

/// Propagates a node split up the tree.
///
/// # Parameters
/// * `left_node` - Left node after the split (original node).
/// * `right_node` - Right node after the split (new node).
/// * `median_key` - Key that separates the two nodes.
/// * `path` - Path from root to the split node.
///
/// # Errors
/// Returns an error if there are I/O issues.
fn propagate_split(&mut self, left_node: BTreeNode, right_node: BTreeNode, median_key: i64, mut path: Vec<u32>) -> io::Result<()> {
    // If path is empty, we're splitting the root
    if path.is_empty() {
        self.create_new_root(left_node, right_node, median_key)?;
        return Ok(());
    }
    
    // Get the parent node
    let parent_page = path.pop().unwrap();
    let parent_type = if self.tree_type == TreeType::Table {
        PageType::TableInterior
    } else {
        PageType::IndexInterior
    };
    
    let parent_node = BTreeNode::open(parent_page, parent_type, Rc::clone(&self.pager))?;
    
    // Create a new cell for the parent
    let cell = if self.tree_type == TreeType::Table {
        BTreeCellFactory::create_table_interior_cell(
            left_node.page_number,
            median_key,
        )
    } else {
        // For index trees, we need to create an interior cell with payload
        // This is simplified - in a real implementation, you'd extract the key from the median
        let payload = vec![]; // Simplified - should be proper payload
        
        let (cell, overflow) = BTreeCellFactory::create_index_interior_cell(
            left_node.page_number,
            payload,
            self.max_local_payload(),
            self.min_local_payload(),
            self.usable_page_size(),
        )?;
        
        // Handle overflow if needed
        if let Some(overflow_data) = overflow {
            match cell {
                BTreeCell::IndexInterior(mut interior_cell) => {
                    let overflow_page = self.create_overflow_chain(overflow_data)?;
                    interior_cell.overflow_page = Some(overflow_page);
                    BTreeCell::IndexInterior(interior_cell)
                },
                _ => unreachable!("Expected an index interior cell"),
            }
        } else {
            cell
        }
    };
    
    // Update the parent's right-most child to point to the right node
    parent_node.set_right_most_child(right_node.page_number)?;
    
    // Insert the cell into the parent
    let (split, new_median, new_parent) = parent_node.insert_cell_ordered(cell)?;
    
    if split {
        // Recursively propagate the split up the tree
        self.propagate_split(
            parent_node,
            new_parent.unwrap(),
            new_median.unwrap(),
            path,
        )?;
    }
    
    Ok(())
}

/// Creates a new root node after the root splits.
///
/// # Parameters
/// * `left_node` - Left node after the split (original root).
/// * `right_node` - Right node after the split (new node).
/// * `median_key` - Key that separates the two nodes.
///
/// # Errors
/// Returns an error if there are I/O issues.
fn create_new_root(&mut self, left_node: BTreeNode, right_node: BTreeNode, median_key: i64) -> io::Result<()> {
    // Create a new interior node to be the new root
    let root_type = if self.tree_type == TreeType::Table {
        PageType::TableInterior
    } else {
        PageType::IndexInterior
    };
    
    // Create the new root node with right_node as its rightmost child
    let new_root = BTreeNode::create_interior(root_type, Rc::clone(&self.pager))?;
    
    // Set the rightmost child
    new_root.set_right_most_child(right_node.page_number)?;
    
    // Create a cell pointing to the left node
    let cell = if self.tree_type == TreeType::Table {
        BTreeCellFactory::create_table_interior_cell(
            left_node.page_number,
            median_key,
        )
    } else {
        // For index trees, we need to create an interior cell with payload
        // This is simplified - in a real implementation, you'd extract the key from the median
        let payload = vec![]; // Simplified - should be proper payload
        
        let (cell, overflow) = BTreeCellFactory::create_index_interior_cell(
            left_node.page_number,
            payload,
            self.max_local_payload(),
            self.min_local_payload(),
            self.usable_page_size(),
        )?;
        
        // Handle overflow if needed
        if let Some(overflow_data) = overflow {
            match cell {
                BTreeCell::IndexInterior(mut interior_cell) => {
                    let overflow_page = self.create_overflow_chain(overflow_data)?;
                    interior_cell.overflow_page = Some(overflow_page);
                    BTreeCell::IndexInterior(interior_cell)
                },
                _ => unreachable!("Expected an index interior cell"),
            }
        } else {
            cell
        }
    };
    
    // Insert the cell into the new root
    new_root.insert_cell(cell)?;
    
    // Update the tree's root page
    self.root_page = new_root.page_number;
    
    Ok(())
}

/// Rebalances the tree after a deletion.
///
/// # Parameters
/// * `leaf_page` - Page number of the leaf node where deletion occurred.
/// * `path` - Path from root to the leaf node.
///
/// # Errors
/// Returns an error if there are I/O issues.
fn rebalance_after_delete(&mut self, leaf_page: u32, path: Vec<u32>) -> io::Result<()> {
    // If this is the root or if the path is empty, no rebalancing needed
    if path.is_empty() || leaf_page == self.root_page {
        return Ok(());
    }
    
    // Define minimum cell count for a node (50% full)
    // In SQLite, this is typically fill_factor / 2
    let min_cell_count = 1; // Simplified - should be based on page size
    
    // Check if the node needs rebalancing
    let leaf_type = if self.tree_type == TreeType::Table {
        PageType::TableLeaf
    } else {
        PageType::IndexLeaf
    };
    
    let leaf_node = BTreeNode::open(leaf_page, leaf_type, Rc::clone(&self.pager))?;
    let cell_count = leaf_node.cell_count()?;
    
    if cell_count >= min_cell_count {
        // Node has enough cells, no rebalancing needed
        return Ok(());
    }
    
    // Node is underfilled, try to borrow or merge
    let parent_page = *path.last().unwrap();
    let parent_type = if self.tree_type == TreeType::Table {
        PageType::TableInterior
    } else {
        PageType::IndexInterior
    };
    
    let parent_node = BTreeNode::open(parent_page, parent_type, Rc::clone(&self.pager))?;
    
    // Find the index of the current node in the parent
    let (left_sibling, right_sibling) = self.find_siblings(parent_node, leaf_page)?;
    
    // Try to borrow from the right sibling first
    if let Some(right_page) = right_sibling {
        if self.borrow_from_sibling(leaf_node, right_page, parent_page, false)? {
            return Ok(());
        }
    }
    
    // Try to borrow from the left sibling
    if let Some(left_page) = left_sibling {
        // Reopen the leaf_node since it was moved in the previous call
        let leaf_node = BTreeNode::open(leaf_page, leaf_type, Rc::clone(&self.pager))?;
        if self.borrow_from_sibling(leaf_node, left_page, parent_page, true)? {
            return Ok(());
        }
    }
    
    // Borrowing failed, need to merge
    // Prefer merging with left sibling if available
    if let Some(left_page) = left_sibling {
        self.merge_nodes(left_page, leaf_page, parent_page)?;
    } else if let Some(right_page) = right_sibling {
        self.merge_nodes(leaf_page, right_page, parent_page)?;
    } else {
        // No siblings available, this should never happen in a valid B-tree
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Node has no siblings for merging",
        ));
    }
    
    // Check if parent needs rebalancing (recursive)
    let new_path = if path.len() > 1 {
        path[0..path.len()-1].to_vec()
    } else {
        Vec::new()
    };
    
    self.rebalance_after_delete(parent_page, new_path)?;
    
    Ok(())
}

/// Finds the sibling nodes of a given node.
///
/// # Parameters
/// * `parent_node` - Parent node.
/// * `node_page` - Page number of the node to find siblings for.
///
/// # Errors
/// Returns an error if there are I/O issues.
///
/// # Returns
/// Tuple with:
/// - Option with page number of left sibling (None if no left sibling)
/// - Option with page number of right sibling (None if no right sibling)
fn find_siblings(&self, parent_node: BTreeNode, node_page: u32) -> io::Result<(Option<u32>, Option<u32>)> {
    let cell_count = parent_node.cell_count()?;
    
    if cell_count == 0 {
        // Parent has no cells, node must be the only child
        return Ok((None, None));
    }
    
    // Find the position of node_page in the parent
    let mut pos = None;
   // let mut is_rightmost = false;
    
    // Check if node is the rightmost child
    let rightmost = parent_node.get_right_most_child()?;
    if rightmost == node_page {
       // let is_rightmost = true;
        pos = Some(cell_count);
    }
    
    // If not rightmost, find the position by scanning cells
    if pos.is_none() {
        for i in 0..cell_count {
            let cell = parent_node.get_cell_owned(i)?;
            
            let child_page = match cell {
                BTreeCell::TableInterior(ref interior) => interior.left_child_page,
                BTreeCell::IndexInterior(ref interior) => interior.left_child_page,
                _ => unreachable!("Expected an interior cell"),
            };
            
            if child_page == node_page {
                pos = Some(i);
                break;
            }
        }
    }
    
    let pos = match pos {
        Some(p) => p,
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Node not found in parent",
            ));
        }
    };
    
    // Determine siblings
    let left_sibling = if pos > 0 {
        // Get the left sibling (left child of the previous cell)
        if pos == cell_count {
            // Node is rightmost child, its left sibling is the right child of the last cell
            let last_cell = parent_node.get_cell_owned(cell_count - 1)?;
            
            match self.tree_type {
                TreeType::Table => {
                    match last_cell {
                        BTreeCell::TableInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected a table interior cell"),
                    }
                },
                TreeType::Index => {
                    match last_cell {
                        BTreeCell::IndexInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected an index interior cell"),
                    }
                },
            }
        } else {
            // Get the left child of the previous cell
            let prev_cell = parent_node.get_cell_owned(pos - 1)?;
            
            match self.tree_type {
                TreeType::Table => {
                    match prev_cell {
                        BTreeCell::TableInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected a table interior cell"),
                    }
                },
                TreeType::Index => {
                    match prev_cell {
                        BTreeCell::IndexInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected an index interior cell"),
                    }
                },
            }
        }
    } else {
        None
    };
    
    let right_sibling = if pos < cell_count {
        // Get the right sibling
        if pos == 0 {
            // Node is leftmost child, its right sibling is the left child of the first cell
            let first_cell = parent_node.get_cell_owned(0)?;
            
            match self.tree_type {
                TreeType::Table => {
                    match first_cell {
                        BTreeCell::TableInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected a table interior cell"),
                    }
                },
                TreeType::Index => {
                    match first_cell {
                        BTreeCell::IndexInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected an index interior cell"),
                    }
                },
            }
        } else if pos == cell_count {
            // Node is rightmost child, it has no right sibling
            None
        } else {
            // Get the left child of the next cell
            let next_cell = parent_node.get_cell_owned(pos)?;
            
            match self.tree_type {
                TreeType::Table => {
                    match next_cell {
                        BTreeCell::TableInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected a table interior cell"),
                    }
                },
                TreeType::Index => {
                    match next_cell {
                        BTreeCell::IndexInterior(interior) => Some(interior.left_child_page),
                        _ => unreachable!("Expected an index interior cell"),
                    }
                },
            }
        }
    } else {
        None
    };
    
    Ok((left_sibling, right_sibling))
}

/// Tries to borrow cells from a sibling node.
///
/// # Parameters
/// * `target_node` - Node that needs more cells.
/// * `sibling_page` - Page number of the sibling node.
/// * `parent_page` - Page number of the parent node.
/// * `from_left` - `true` if borrowing from left sibling, `false` if from right.
///
/// # Errors
/// Returns an error if there are I/O issues.
///
/// # Returns
/// `true` if borrowing succeeded, `false` otherwise.
fn borrow_from_sibling(&self, target_node: BTreeNode, sibling_page: u32, parent_page: u32, from_left: bool) -> io::Result<bool> {
    // Define minimum cell count for a node after borrowing
    let min_cell_count = 1; // Simplified - should be based on page size
    
    // Open sibling node
    let sibling_type = target_node.node_type;
    let sibling_node = BTreeNode::open(sibling_page, sibling_type, Rc::clone(&self.pager))?;
    
    // Check if sibling has enough cells to spare
    let sibling_cell_count = sibling_node.cell_count()?;
    
    if sibling_cell_count <= min_cell_count {
        // Sibling doesn't have enough cells to spare
        return Ok(false);
    }
    
    // Get the separator key from the parent
    let parent_type = if self.tree_type == TreeType::Table {
        PageType::TableInterior
    } else {
        PageType::IndexInterior
    };
    
    let parent_node = BTreeNode::open(parent_page, parent_type, Rc::clone(&self.pager))?;
    
    if from_left {
        // Borrow from the left sibling
        
        // Get the rightmost cell from the left sibling
        let sibling_cell = sibling_node.get_cell_owned(sibling_cell_count - 1)?;
        
        // Remove the cell from the sibling
        {
            let mut pager = self.pager.borrow_mut();
            let page = pager.get_page_mut(sibling_page, Some(sibling_type))?;
            
            if let Page::BTree(btree_page) = page {
                btree_page.cells.pop();
                btree_page.cell_indices.pop();
                btree_page.header.cell_count -= 1;
            }
        }
        
        // Update the separator key in the parent
        // Find the separator key in the parent
        let separator_idx = self.find_separator_index(&parent_node, target_node.page_number, true)?;
        
        // Update the separator key
        let new_separator = match (self.tree_type, &sibling_cell) {
            (TreeType::Table, BTreeCell::TableLeaf(leaf)) => leaf.row_id,
            (TreeType::Index, BTreeCell::IndexLeaf(leaf)) => {
                let key_value = extract_key_from_payload(&leaf.payload)?;
                match key_value {
                    KeyValue::Integer(i) => i,
                    KeyValue::Float(f) => f as i64,
                    _ => {
                        // For string and blob, hash the key
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        std::hash::Hash::hash(&key_value, &mut hasher);
                        hasher.finish() as i64
                    }
                }
            },
            _ => unreachable!("Unexpected cell type"),
        };
        
        // Update the separator key in the parent
        {
            let mut cell = parent_node.get_cell_mut(separator_idx)?;
            
            match self.tree_type {
                TreeType::Table => {
                    match *cell {
                        BTreeCell::TableInterior(ref mut interior) => {
                            interior.key = new_separator;
                        },
                        _ => unreachable!("Expected a table interior cell"),
                    }
                },
                TreeType::Index => {
                    // For index trees, we need to update the payload
                    // This is simplified - in a real implementation, you'd handle this properly
                }
            }
        }
        
        // Insert the cell into the target node
        target_node.insert_cell_ordered(sibling_cell)?;
        
        Ok(true)
    } else {
        // Borrow from the right sibling
        
        // Get the leftmost cell from the right sibling
        let sibling_cell = sibling_node.get_cell_owned(0)?;
        
        // Remove the cell from the sibling
        {
            let mut pager = self.pager.borrow_mut();
            let page = pager.get_page_mut(sibling_page, Some(sibling_type))?;
            
            if let Page::BTree(btree_page) = page {
                btree_page.cells.remove(0);
                btree_page.cell_indices.remove(0);
                btree_page.header.cell_count -= 1;
            }
        }
        
        // Update the separator key in the parent
        // Find the separator key in the parent
        let separator_idx = self.find_separator_index(&parent_node, target_node.page_number, false)?;
        
        // Update the separator key
        let new_separator = match (self.tree_type, &sibling_cell) {
            (TreeType::Table, BTreeCell::TableLeaf(leaf)) => leaf.row_id,
            (TreeType::Index, BTreeCell::IndexLeaf(leaf)) => {
                let key_value = extract_key_from_payload(&leaf.payload)?;
                match key_value {
                    KeyValue::Integer(i) => i,
                    KeyValue::Float(f) => f as i64,
                    _ => {
                        // For string and blob, hash the key
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        std::hash::Hash::hash(&key_value, &mut hasher);
                        hasher.finish() as i64
                    }
                }
            },
            _ => unreachable!("Unexpected cell type"),
        };
        
        // Update the separator key in the parent
        {
            let mut cell = parent_node.get_cell_mut(separator_idx)?;
            
            match self.tree_type {
                TreeType::Table => {
                    match *cell {
                        BTreeCell::TableInterior(ref mut interior) => {
                            interior.key = new_separator;
                        },
                        _ => unreachable!("Expected a table interior cell"),
                    }
                },
                TreeType::Index => {
                    // For index trees, we need to update the payload
                    // This is simplified - in a real implementation, you'd handle this properly
                }
            }
        }
        
        // Insert the cell into the target node
        target_node.insert_cell_ordered(sibling_cell)?;
        
        Ok(true)
    }
}

/// Finds the index of the separator between two nodes in the parent node.
///
/// # Parameters
/// * `parent_node` - Parent node.
/// * `child_page` - Page number of one of the child nodes.
/// * `is_left` - `true` if child_page is the left node, `false` if it's the right node.
///
/// # Errors
/// Returns an error if there are I/O issues.
///
/// # Returns
/// Index of the separator cell in the parent.
fn find_separator_index(&self, parent_node: &BTreeNode, child_page: u32, is_left: bool) -> io::Result<u16> {
    let cell_count = parent_node.cell_count()?;
    
    // If child_page is the rightmost child, there's no separator (it's after the last key)
    let rightmost = parent_node.get_right_most_child()?;
    if child_page == rightmost {
        if is_left {
            // The separator is the last key in the parent
            return Ok(cell_count - 1);
        } else {
            // There's no separator if child_page is rightmost and is_left is false
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No separator for rightmost child",
            ));
        }
    }
    
    // Find the cell that has child_page as its left child
    for i in 0..cell_count {
        let cell = parent_node.get_cell_owned(i)?;
        
        let cell_child = match cell {
            BTreeCell::TableInterior(ref interior) => interior.left_child_page,
            BTreeCell::IndexInterior(ref interior) => interior.left_child_page,
            _ => unreachable!("Expected an interior cell"),
        };
        
        if cell_child == child_page {
            if is_left {
                // The separator is this cell (since child_page is the left child)
                return Ok(i);
            } else {
                // The separator is the previous cell (since child_page is the right child)
                if i == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "No separator for leftmost child",
                    ));
                }
                return Ok(i - 1);
            }
        }
    }
    
    // Child page not found in parent
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Child page not found in parent",
    ))
}

/// Merges two adjacent nodes.
///
/// # Parameters
/// * `left_page` - Page number of the left node.
/// * `right_page` - Page number of the right node.
/// * `parent_page` - Page number of the parent node.
///
/// # Errors
/// Returns an error if there are I/O issues.
fn merge_nodes(&self, left_page: u32, right_page: u32, parent_page: u32) -> io::Result<()> {
    // Open the nodes
    let leaf_type = if self.tree_type == TreeType::Table {
        PageType::TableLeaf
    } else {
        PageType::IndexLeaf
    };
    
    let left_node = BTreeNode::open(left_page, leaf_type, Rc::clone(&self.pager))?;
    let right_node = BTreeNode::open(right_page, leaf_type, Rc::clone(&self.pager))?;
    
    // Get the separator key from the parent
    let parent_type = if self.tree_type == TreeType::Table {
        PageType::TableInterior
    } else {
        PageType::IndexInterior
    };
    
    let parent_node = BTreeNode::open(parent_page, parent_type, Rc::clone(&self.pager))?;
    
    // Find the separator key in the parent
    let separator_idx = self.find_separator_index(&parent_node, left_page, true)?;
    
    // Move all cells from right to left
    let right_cell_count = right_node.cell_count()?;
    
    for i in 0..right_cell_count {
        let cell = right_node.get_cell_owned(i)?;
        left_node.insert_cell_ordered(cell)?;
    }
    
    // Remove the separator from the parent
    {
        let mut pager = self.pager.borrow_mut();
        let page = pager.get_page_mut(parent_page, Some(parent_type))?;
        
        if let Page::BTree(btree_page) = page {
            // Remove the separator cell
            btree_page.cells.remove(separator_idx as usize);
            btree_page.cell_indices.remove(separator_idx as usize);
            btree_page.header.cell_count -= 1;
            
            // If the right node was the right-most child, update to point to the left node
            if let Some(right_most) = btree_page.header.right_most_page {
                if right_most == right_page {
                    btree_page.header.right_most_page = Some(left_page);
                }
            }
        }
    }
    
    // Mark the right node as free
    // This would normally add it to the free list
    // For simplicity, we're just creating a free page (the page itself isn't actually freed)
    let mut pager = self.pager.borrow_mut();
    pager.create_free_page(0)?;
    
    Ok(())
}

/// Gets the page type of a specific page.
///
/// # Parameters
/// * `page_number` - Page number to check.
///
/// # Errors
/// Returns an error if there are I/O issues.
///
/// # Returns
/// The page type.
fn get_page_type(&self, page_number: u32) -> io::Result<PageType> {
    let mut pager = self.pager.borrow_mut();
    let page = pager.get_page(page_number, None)?;
    
    match page {
        Page::BTree(btree_page) => Ok(btree_page.header.page_type),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Page is not a B-Tree page",
        )),
    }
}
}


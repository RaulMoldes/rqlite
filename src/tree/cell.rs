//! # B-Tree Cell Factory Module
//! 
//! This module provides a factory for creating B-Tree cells. For details on the implementation of cells, refer to page.rs module where all the data structures are placed.

use std::io;

use crate::page::{BTreeCell, TableLeafCell, TableInteriorCell, IndexLeafCell, IndexInteriorCell};


/// Factory for creating B-Tree cells.
///
/// This struct provides methods to create different types of B-Tree cells, including table leaf cells, table interior cells, index leaf cells, and index interior cells.
/// It also provides methods to calculate the maximum and minimum local payload sizes for cells based on the usable size of the page and the specified fractions.
pub struct BTreeCellFactory;

impl BTreeCellFactory {
    /// Crates a new table leaf cell.
    ///
    /// # Parameters
    /// * `rowid` - Row ID of the cell.
    /// * `payload` - Data to be stored in the cell.
    /// * `max_local_payload` - Maximum size of payload that can be stored locally.
    /// * `min_local_payload` - Minimum size of payload that must be stored locally.
    /// * `usable_size` - Usable size of the page (excluding header and reserved space).
    /// 
    /// # Errors
    /// Returns an error if there are issues creating the cell.
    ///
    /// # Returns
    /// A tuple containing:
    /// - The created cell
    /// - Data that doesn't fit in the local cell and needs to be stored in overflow pages (if any)
    pub fn create_table_leaf_cell(
        rowid: i64,
        payload: Vec<u8>,
        max_local_payload: usize,
        min_local_payload: usize,
        
        usable_size: usize,
    ) -> io::Result<(BTreeCell, Option<Vec<u8>>)> {
        let payload_size = payload.len();
        
        // Determine how much of the payload is stored locally
        // (similar to index leaf cell)
        // If the payload is small enough, store it all locally.
        // Otherwise, calculate the minimum local payload size (M) and store part of it locally and part in overflow.
        let local_payload_size = if payload_size <= max_local_payload {
            // Store all locally
            payload_size
        } else {
            // Calculate M (minimum local payload size)
            // Formula for M: (usable_size - 12) * X / 255
            // where X is the minimum payload fraction.
            // In sqlite, X is 32 (12.5%).
            // This means that the minimum local payload size is 12.5% of the usable size.
            // This way, if the data is too large, just keep the minimum local payload size and store the rest in overflow.
            let m = min_local_payload.min((usable_size - 35) / 4);
            
            if payload_size <= m {
                // This is redundant as we have already checked but I am starting to become a bit paranoic 
                // with all this database programming stuff
                // Store all locally
                payload_size
            } else {
                // Store part locally and part in overflow
                // Formula: M + ((payload_size - M) % (usable_size - 4))
                m + ((payload_size - m) % (usable_size - 4))
            }
        };
        
        // Part of the payload that is stored locally
        let local_payload = payload[0..local_payload_size].to_vec();
        
        // Part of the payload that is stored in overflow (if any)
        let overflow_payload = if local_payload_size < payload_size {
            Some(payload[local_payload_size..].to_vec())
        } else {
            None
        };
        
        // Create the cell
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: payload_size as u64,
            row_id: rowid,
            payload: local_payload,
            overflow_page: None, // Will be set later.
        });
        
        Ok((cell, overflow_payload))
    }

    /// Creates a new table interior cell.
    /// 
    /// # Parameters
    /// * `left_child_page` - Page number of the left child.
    /// * `key` - Key of the cell.
    /// 
    /// # Returns
    /// The created cell.
    /// 
    /// Note that interior cells do not have a payload, so the payload size is not relevant here.
    /// (This is because I am actually implementing a B+ tree and not a B tree, so the interior cells do not have a payload).
    pub fn create_table_interior_cell(
        left_child_page: u32,
        key: i64,
    ) -> BTreeCell {
        BTreeCell::TableInterior(TableInteriorCell {
            left_child_page,
            key,
        })
    }

    /// Creates a new index leaf cell.
    ///
    /// # Parameters
    /// * `payload` - Data to be stored in the cell.
    /// * `max_local_payload` - Maximum size of payload that can be stored locally.
    /// * `min_local_payload` - Minimum size of payload that must be stored locally.
    /// 
    /// # Returns
    /// A tuple containing:
    /// - The created cell
    /// - Data that doesn't fit in the local cell and needs to be stored in overflow pages (if any)
    pub fn create_index_leaf_cell(
        payload: Vec<u8>,
        max_local_payload: usize,
        min_local_payload: usize,
        usable_size: usize,
    ) -> io::Result<(BTreeCell, Option<Vec<u8>>)> {
        let payload_size = payload.len();
        
        // Determine how much of the payload is stored locally
        let local_payload_size = if payload_size <= max_local_payload {
            payload_size
        } else {
            // Calculate M again
            let m = min_local_payload.min((usable_size - 35) / 4);
            
            if payload_size <= m {
                payload_size
            } else {
                m + ((payload_size - m) % (usable_size - 4))
            }
        };
        
        // Part of the payload that is stored locally
        let local_payload = payload[0..local_payload_size].to_vec();
        
        // Part of the payload that is stored in overflow (if any)
        let overflow_payload = if local_payload_size < payload_size {
            Some(payload[local_payload_size..].to_vec())
        } else {
            None
        };
        
        // Create the cell
        let cell = BTreeCell::IndexLeaf(IndexLeafCell {
            payload_size: payload_size as u64,
            payload: local_payload,
            overflow_page: None, // Will be set later.
        });
        
        Ok((cell, overflow_payload))
    }

    /// Creates a new index interior cell.
    ///
    /// # Parameters
    /// * `left_child_page` - Page number of the left child.
    /// * `payload` - Data to be stored in the cell.
    /// * `max_local_payload` - Maximum size of payload that can be stored locally.
    /// * `min_local_payload` - Minimum size of payload that must be stored locally.
    /// * `usable_size` - Usable size of the page (excluding header and reserved space).
    ///
    /// # Errors
    /// Returns an error if there are issues creating the cell.
    ///
    /// # Returns
    /// A tuple with:
    /// - The created cell
    /// - Data that doesn't fit in the local cell and needs to be stored in overflow pages (if any)
    pub fn create_index_interior_cell(
        left_child_page: u32,
        payload: Vec<u8>,
        max_local_payload: usize,
        min_local_payload: usize,
        usable_size: usize,
    ) -> io::Result<(BTreeCell, Option<Vec<u8>>)> {
        let payload_size = payload.len();
        
        // Determine how much of the payload is stored locally
        // If the payload is small enough, store it all locally.
        let local_payload_size = if payload_size <= max_local_payload {
            payload_size
        } else {
            // Calculate M (minimum local payload size)
            let m = min_local_payload.min((usable_size - 35) / 4);
            
            if payload_size <= m {
                payload_size
            } else {
                m + ((payload_size - m) % (usable_size - 4))
            }
        };
        
        // Part of the payload that is stored locally
        // (similar to index leaf cell)
        let local_payload = payload[0..local_payload_size].to_vec();
        
        // Part of the payload that is stored in overflow (if any)
        let overflow_payload = if local_payload_size < payload_size {
            Some(payload[local_payload_size..].to_vec())
        } else {
            None
        };
        
        // Create the cell
        let cell = BTreeCell::IndexInterior(IndexInteriorCell {
            left_child_page,
            payload_size: payload_size as u64,
            payload: local_payload,
            overflow_page: None, // Se establecerá más tarde
        });
        
        Ok((cell, overflow_payload))
    }

    /// Calculates the maximum size of payload that can be stored locally.
    /// 
    /// # Parameters
    /// * `usable_size` - Usable size in the page.
    /// * `max_payload_fraction` - Maximum fraction of a page that can be occupied by a cell. 
    /// 
    /// # Returns
    /// Maximum size in bytes.
    /// 
    /// On SQLite, the `MAX_PAYLOAD_FRACTION` is 255 (100%). I decided to make it a parameter to make it more flexible.
    pub fn max_local_payload(usable_size: usize, max_payload_fraction: u8) -> usize {
        let max_fraction = (usable_size - 12) * max_payload_fraction as usize / 255;
        let absolute_max = usable_size - 35;
        max_fraction.min(absolute_max)
    }

    /// Calculates the minimum size of payload that must be stored locally.
    /// 
    /// # Parameters
    /// * `usable_size` - Usable size in the page.
    /// * `min_payload_fraction` - Minimum fraction of a page that must be occupied by a cell.
    /// 
    /// # Returns
    /// Minimum size in bytes.
    /// 
    /// SQLITE USES `MIN_PAYLOAD_FRACTION` = 32 (12.5%). I decided to make it a parameter to make it more flexible.
    pub fn min_local_payload(usable_size: usize, min_payload_fraction: u8) -> usize {
        // Fórmula: (usable_size - 12) * X / 255
        (usable_size - 12) * min_payload_fraction as usize / 255
    }
}

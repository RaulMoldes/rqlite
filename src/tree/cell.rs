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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{BTreeCell, TableLeafCell, TableInteriorCell, IndexLeafCell, IndexInteriorCell};

    // Test helper function to create a simple payload
    fn create_test_payload(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 256) as u8).collect()
    }

    #[test]
    fn test_create_table_leaf_cell_small_payload() {
        let rowid = 42;
        let payload = create_test_payload(100);
        let max_local = 200;
        let min_local = 50;
        let usable_size = 1000;
        
        let (cell, overflow) = BTreeCellFactory::create_table_leaf_cell(
            rowid, 
            payload.clone(), 
            max_local, 
            min_local, 
            usable_size
        ).unwrap();
        
        // With small payload, everything should fit locally
        assert!(overflow.is_none());
        
        // Check cell type and contents
        match cell {
            BTreeCell::TableLeaf(leaf) => {
                assert_eq!(leaf.row_id, rowid);
                assert_eq!(leaf.payload_size as usize, payload.len());
                assert_eq!(leaf.payload, payload);
                assert!(leaf.overflow_page.is_none());
            },
            _ => panic!("Expected TableLeaf cell"),
        }
    }

    #[test]
    fn test_create_table_leaf_cell_large_payload() {
        let rowid = 42;
        let payload = create_test_payload(300);
        let max_local = 100;
        let min_local = 50;
        let usable_size = 1000;
        
        let (cell, overflow) = BTreeCellFactory::create_table_leaf_cell(
            rowid, 
            payload.clone(), 
            max_local, 
            min_local, 
            usable_size
        ).unwrap();
        
        // With large payload, some data should go to overflow
        assert!(overflow.is_some());
        let overflow_data = overflow.unwrap();
        
        // Check cell type and contents
        match cell {
            BTreeCell::TableLeaf(leaf) => {
                assert_eq!(leaf.row_id, rowid);
                assert_eq!(leaf.payload_size as usize, payload.len());
                
                // Check that the cell contains the first part of payload
                assert_eq!(leaf.payload.len(), min_local);
                assert_eq!(&leaf.payload[..], &payload[..min_local]);
                
                // Check overflow data contains the rest
                assert_eq!(overflow_data, payload[min_local..].to_vec());
                
                // No overflow page yet
                assert!(leaf.overflow_page.is_none());
            },
            _ => panic!("Expected TableLeaf cell"),
        }
    }

    #[test]
    fn test_create_table_interior_cell() {
        let left_child_page = 123;
        let key = 456;
        
        let cell = BTreeCellFactory::create_table_interior_cell(left_child_page, key);
        
        match cell {
            BTreeCell::TableInterior(interior) => {
                assert_eq!(interior.left_child_page, left_child_page);
                assert_eq!(interior.key, key);
            },
            _ => panic!("Expected TableInterior cell"),
        }
    }

    #[test]
    fn test_create_index_leaf_cell_small_payload() {
        let payload = create_test_payload(100);
        let max_local = 200;
        let min_local = 50;
        let usable_size = 1000;
        
        let (cell, overflow) = BTreeCellFactory::create_index_leaf_cell(
            payload.clone(), 
            max_local, 
            min_local, 
            usable_size
        ).unwrap();
        
        // With small payload, everything should fit locally
        assert!(overflow.is_none());
        
        // Check cell type and contents
        match cell {
            BTreeCell::IndexLeaf(leaf) => {
                assert_eq!(leaf.payload_size as usize, payload.len());
                assert_eq!(leaf.payload, payload);
                assert!(leaf.overflow_page.is_none());
            },
            _ => panic!("Expected IndexLeaf cell"),
        }
    }

    #[test]
    fn test_create_index_leaf_cell_large_payload() {
        let payload = create_test_payload(300);
        let max_local = 100;
        let min_local = 50;
        let usable_size = 1000;
        
        let (cell, overflow) = BTreeCellFactory::create_index_leaf_cell(
            payload.clone(), 
            max_local, 
            min_local, 
            usable_size
        ).unwrap();
        
        // With large payload, some data should go to overflow
        assert!(overflow.is_some());
        let overflow_data = overflow.unwrap();
        
        // Check cell type and contents
        match cell {
            BTreeCell::IndexLeaf(leaf) => {
                assert_eq!(leaf.payload_size as usize, payload.len());
                
                // Check that the cell contains the first part of payload
                assert_eq!(leaf.payload.len(), min_local);
                assert_eq!(&leaf.payload[..], &payload[..min_local]);
                
                // Check overflow data contains the rest
                assert_eq!(overflow_data, payload[min_local..].to_vec());
                
                // No overflow page yet
                assert!(leaf.overflow_page.is_none());
            },
            _ => panic!("Expected IndexLeaf cell"),
        }
    }

    #[test]
    fn test_create_index_interior_cell_small_payload() {
        let left_child_page = 123;
        let payload = create_test_payload(100);
        let max_local = 200;
        let min_local = 50;
        let usable_size = 1000;
        
        let (cell, overflow) = BTreeCellFactory::create_index_interior_cell(
            left_child_page,
            payload.clone(), 
            max_local, 
            min_local, 
            usable_size
        ).unwrap();
        
        // With small payload, everything should fit locally
        assert!(overflow.is_none());
        
        // Check cell type and contents
        match cell {
            BTreeCell::IndexInterior(interior) => {
                assert_eq!(interior.left_child_page, left_child_page);
                assert_eq!(interior.payload_size as usize, payload.len());
                assert_eq!(interior.payload, payload);
                assert!(interior.overflow_page.is_none());
            },
            _ => panic!("Expected IndexInterior cell"),
        }
    }

    #[test]
    fn test_create_index_interior_cell_large_payload() {
        let left_child_page = 123;
        let payload = create_test_payload(300);
        let max_local = 100;
        let min_local = 50;
        let usable_size = 1000;
        
        let (cell, overflow) = BTreeCellFactory::create_index_interior_cell(
            left_child_page,
            payload.clone(), 
            max_local, 
            min_local, 
            usable_size
        ).unwrap();
        
        // With large payload, some data should go to overflow
        assert!(overflow.is_some());
        let overflow_data = overflow.unwrap();
        
        // Check cell type and contents
        match cell {
            BTreeCell::IndexInterior(interior) => {
                assert_eq!(interior.left_child_page, left_child_page);
                assert_eq!(interior.payload_size as usize, payload.len());
                
                // Check that the cell contains the first part of payload
                assert_eq!(interior.payload.len(), min_local);
                assert_eq!(&interior.payload[..], &payload[..min_local]);
                
                // Check overflow data contains the rest
                assert_eq!(overflow_data, payload[min_local..].to_vec());
                
                // No overflow page yet
                assert!(interior.overflow_page.is_none());
            },
            _ => panic!("Expected IndexInterior cell"),
        }
    }

    #[test]
    fn test_max_local_payload() {
        let usable_size = 1000;
        
        // Max fraction = 255 (100%)
        let max_local_100_percent = BTreeCellFactory::max_local_payload(usable_size, 255);
        assert_eq!(max_local_100_percent, 965); // (1000-12)*255/255 capped at 1000-35
        
        // Max fraction = 128 (50%)
        let max_local_50_percent = BTreeCellFactory::max_local_payload(usable_size, 128);
        let expected = (usable_size - 12) * 128 / 255;
        assert_eq!(max_local_50_percent, expected);
        
        // Max fraction = 0 (0%)
        let max_local_0_percent = BTreeCellFactory::max_local_payload(usable_size, 0);
        assert_eq!(max_local_0_percent, 0);
    }

    #[test]
    fn test_min_local_payload() {
        let usable_size = 1000;
        
        // Min fraction = 32 (12.5%)
        let min_local_12_5_percent = BTreeCellFactory::min_local_payload(usable_size, 32);
        let expected = (usable_size - 12) * 32 / 255;
        assert_eq!(min_local_12_5_percent, expected);
        
        // Min fraction = 0 (0%)
        let min_local_0_percent = BTreeCellFactory::min_local_payload(usable_size, 0);
        assert_eq!(min_local_0_percent, 0);
        
        // Min fraction = 255 (100%)
        let min_local_100_percent = BTreeCellFactory::min_local_payload(usable_size, 255);
        let expected = (usable_size - 12) * 255 / 255;
        assert_eq!(min_local_100_percent, expected);
    }
}
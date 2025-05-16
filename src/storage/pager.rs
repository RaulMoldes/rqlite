//! # Pager Module
//! 
//! This module implements the `Pager` struct, which is responsible for managing
//! the loading and caching of database pages. It interacts with the `DiskManager`
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;


use super::disk::DiskManager;
use super::cache::BufferPool;
use crate::page::{BTreePage, BTreePageHeader, BTreeCell, OverflowPage, FreePage, Page, PageType, IndexInteriorCell, IndexLeafCell, TableInteriorCell, TableLeafCell };
use crate::header::Header;

/// Handles the loading and caching of database pages.
/// Maintains a buffer pool of pages in memory and manages the interaction with the disk.
/// It is responsible for reading and writing pages to and from the disk, as well as
/// managing the page cache.
/// The `Pager` struct is the main interface for interacting with the database pages.
/// I decided to implement the page_cache as a HashMap instead of making some kind of 
/// BufferPool, because the pages are not going to be used in a LRU-K way.
/// To keep it simple, the pages are going to be used in a FIFO way, so a HashMap facilitates that.
/// The `Pager` struct is the main interface for interacting with the database pages.



pub struct Pager {
    /// Disk manager.
    disk_manager: DiskManager,
    /// Page Cache. This should be a BufferPool, but for now it is just a HashMap.
    page_cache: BufferPool,
    /// Size of each page in bytes.
    /// This is the size of the page that is going to be used in the database.
    page_size: u32,
    /// Reserved space at the end of each page.
    reserved_space: u8,
    /// Indicates if the pager has unsaved changes. That must be written to disk.
    /// Tis could be handled in a more fine-grained way when the BufferPool is implemented.
    /// For now, if the Pager has any dirty page, all its cached pages must be written to disk.
    dirty: bool,
}

impl Pager {
    /// Opens an existing database file.
    /// Basically the same as `DiskManager::open`, but it also initializes the page cache.
    /// 
    /// # Parameters
    /// * `path` - Path to the database file.
    /// 
    /// # Errors
    /// Returns an error if the file cannot be opened or if the header is invalid.
    /// 
    /// # Returns
    /// A `Pager` instance with the disk manager and page cache initialized.
    pub fn open<P: AsRef<Path>>(path: P, buffer_pool_size: Option<usize>) -> io::Result<Self> {
        let mut disk_manager = DiskManager::open(path)?;
        let header = disk_manager.read_header()?;
        
        Ok(Pager {
            disk_manager,
            page_cache: BufferPool::new(buffer_pool_size.unwrap_or(1000)),
            page_size: header.page_size,
            reserved_space: header.reserved_space,
            dirty: false,
        })
    }

    /// Creates a new database file.
    /// 
    /// # Parameters
    /// * `path` - Path to the database file.
    /// * `page_size` - Size of each page in bytes.
    /// * `reserved_space` - Reserved space at the end of each page.
    /// 
    /// # Errors
    /// Returns an error if the file cannot be created or if the header is invalid.
    ///
    /// # Returns
    /// A `Pager` instance with the disk manager and page cache initialized.
    pub fn create<P: AsRef<Path>>(path: P, page_size: u32, buffer_pool_size: Option<usize>, reserved_space: u8) -> io::Result<Self> {
        let mut disk_manager = DiskManager::create(path, page_size)?;
        
        // Upadate the header with the reserved space
        let mut header = disk_manager.read_header()?;
        header.reserved_space = reserved_space;
        disk_manager.write_header(&header)?;
        
        Ok(Pager {
            disk_manager,
            page_cache: BufferPool::new(buffer_pool_size.unwrap_or(1000)),
            page_size,
            reserved_space,
            dirty: false,
        })
    }

    /// Obtains the header of the database from the disk manager. 
    pub fn get_header(&mut self) -> io::Result<Header> {
        self.disk_manager.read_header()
    }

    /// Updates the header of the database in the disk manager.
    /// 
    /// # Parameters
    /// * `header` - Header to update.
    ///
    /// # Errors
    /// Returns an error if the header cannot be written to disk.
    /// 
    /// # Returns
    /// A result indicating success or failure.
    pub fn update_header(&mut self, header: &Header) -> io::Result<()> {
        self.disk_manager.write_header(header)?;
        self.dirty = true; // Not really needed i think, but let's keep it for consistency
        Ok(())
    }

    pub fn unpin_page(&mut self, page_number: u32) -> bool {
        self.page_cache.unpin_page(page_number)
    }

    /// Obtains a page from the database, loading it from disk if necessary.
    /// Attempts to load the page from the cache first, and if it is not found,
    /// it loads it from disk and caches it.
    /// 
    /// # Parameters
    /// * `page_number` - Number of the page to obtain.
    /// * `page_type` - Expected type of the page.
    /// 
    /// # Errors
    /// Returns an error if the page does not exist, cannot be read, or if the type does not match.
    /// 
    /// # Returns
    /// A reference to the page.
    pub fn get_page(&mut self, page_number: u32, page_type: Option<PageType>) -> io::Result<&Page> {
         // Try to get from BufferPool
        if !self.page_cache.contains_page(page_number) {
           // Load from disk into buffer pool
                self.load_page(page_number)?;
                // Get from buffer pool
            }
        
        // Verify the type of the page if it was specified
        if let Some(expected_type) = page_type {
            match self.page_cache.get_page(page_number) {
                Some(Page::BTree(btree_page)) => {
                    if btree_page.header.page_type != expected_type {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Type of page not supported: expected {:?}, obtained {:?}",
                                expected_type, btree_page.header.page_type),
                        ));
                    }
                },
                Some(Page::Overflow(_)) => {
                    if expected_type != PageType::Overflow {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Type of page not supported: expected {:?}, obtained Overflow",
                                expected_type),
                        ));
                    }
                },
                Some(Page::Free(_)) => {
                    if expected_type != PageType::Free {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Type of page not supported: expected {:?}, obtained Free",
                                expected_type),
                        ));
                    }
                },
                None => unreachable!("The page should be in the cache at this point!"),
            }
        }
        
        // Mark the page as dirty (modified)
        self.dirty = true; // ?? Not sure of this. It can be improved when the BufferPool is implemented
        
        // Return the reference to the page
        self.page_cache.get_page(page_number)
            .ok_or_else(|| io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page not found: {}", page_number),
            ))
    }

    /// Similar to `get_page`, but returns a mutable reference to the page.
    /// This is useful for modifying the page in place.
    /// 
    /// # Parameters
    /// * `page_number` - Number of the page to obtain.
    /// * `page_type` - Expected type of the page.
    /// 
    /// # Errors
    /// Returns an error if the page does not exist, cannot be read, or if the type does not match.
    /// 
    /// # Returns
    /// A mutable reference to the page.
    pub fn get_page_mut(&mut self, page_number: u32, page_type: Option<PageType>) -> io::Result<&mut Page> {
        if !self.page_cache.contains_page(page_number) {
            // Load the page from disk if it is not in the cache
            self.load_page(page_number)?;
        }
        
        // Verify the type of page if it was specified
        if let Some(expected_type) = page_type {
            match self.page_cache.get_page(page_number) {
                Some(Page::BTree(btree_page)) => {
                    if btree_page.header.page_type != expected_type {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Type of page incorrect: expected {:?}, obtained {:?}",
                                expected_type, btree_page.header.page_type),
                        ));
                    }
                },
                Some(Page::Overflow(_)) => {
                    if expected_type != PageType::Overflow {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Type of page incorrect: expected {:?}, obtained Overflow",
                                expected_type),
                        ));
                    }
                },
                Some(Page::Free(_)) => {
                    if expected_type != PageType::Free {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Type of page incorrect: expected {:?}, obtained Free",
                                expected_type),
                        ));
                    }
                },
                None => unreachable!("Page should be in the cache at this point!"),
            }
        }
        
        // Mark the page as dirty (modified)
        self.dirty = true;
        
        // Return the mutable reference to the page
        self.page_cache.get_page_mut(page_number)
            .ok_or_else(|| io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page not found: {}", page_number),
            ))
    }


    /// Caches a page in memory.
    /// Can be improved to use a more sophisticated caching strategy in the future.
    /// Actually, SQLite uses a LRU-K strategy, but for now we are going to use a simple FIFO strategy.
    /// 
    /// # Parameters
    /// * `page_number` - Number of the page to cache.
    /// 
    /// # Errors
    /// Returns an error if the page cannot be loaded from disk.
    fn load_page(&mut self, page_number: u32) -> io::Result<()> {
        // Verify if the page number is valid
        let page_count = self.disk_manager.page_count()?;
        if (page_number) > (page_count + 1) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page number out of range: {}, maximum {}", page_number, page_count + 1),
            ));
        }
        
        // Retrieve the page from disk
        let mut buffer = vec![0u8; self.page_size as usize]; // Allocate a buffer for the page
        self.disk_manager.read_page(page_number, &mut buffer)?;
        
        // Interpret the page based on its type
        // The first byte of the page indicates the type of page. Page 1 is always a B-Tree page.
        let page = if page_number == 1 || self.is_btree_page(&buffer)? {
            self.parse_btree_page(page_number, &buffer)?
        } else if self.is_overflow_page(&buffer)? {
            self.parse_overflow_page(page_number, &buffer)?
        } else {
            self.parse_free_page(page_number, &buffer)?
        };
        
        // Add the page to the cache
        self.add_page_to_cache(page_number, page)?;
        Ok(())
        
    }

    /// Adds a page to the cache.
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to add.
    /// * `page` - Page to add.
    /// 
    /// # Errors
    /// Returns an error if the page cannot be added to the cache.
    /// 
    /// # Returns
    /// A result indicating success or failure.
    fn add_page_to_cache(&mut self, page_number: u32, page: Page) -> io::Result<()> {
        // Add the page to the cache
        match self.page_cache.add_page(page_number, page, true){
            Some((evicted_page_number, _)) => {
                if evicted_page_number == page_number {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Page {} was rejected by the cache (OOM Error)", page_number),
                    ));
                } else {
                    return Ok(());
                }
                
            },
            _ => {
                return Ok(());
            }
        }
    }
    /// Determines if a page is a B-Tree page.
    /// 
    /// # Parameters
    /// * `buffer` - Data of the page.
    /// 
    /// # Errors
    /// Returns an error if there are problems interpreting the data.
    /// 
    /// # Returns
    /// `true` if the page is a B-Tree page, `false` otherwise.
    fn is_btree_page(&self, buffer: &[u8]) -> io::Result<bool> {
        if buffer.len() < 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small to determine page type",
            ));
        }
        
        // The first byte of the page indicates the type of page
        match buffer[0] {
            0x02 | 0x05 | 0x0A | 0x0D => Ok(true),
            _ => Ok(false),
        }
    }

    /// Determines if a page is an overflow page.
    /// 
    /// # Parameters
    /// * `buffer` - Data of the page.
    /// 
    /// # Errors
    /// Returns an error if there are problems interpreting the data or if the buffer is too small.
    fn is_overflow_page(&self, buffer: &[u8]) -> io::Result<bool> {
        if buffer.len() < 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small to determine page type",
            ));
        }
        
        // The first byte of the page indicates the type of page
        match buffer[0] {
            0x10 => Ok(true),
            _ => Ok(false),
        }
    }

    /// Parses a B-Tree page.
///
/// # Parameters
/// * `page_number` - Page number.
/// * `buffer` - Page data.
///
/// # Errors
/// Returns an error if there are problems interpreting the data.
///
/// # Return
/// The parsed B-Tree page.
fn parse_btree_page(&self, page_number: u32, buffer: &[u8]) -> io::Result<Page> {
    if buffer.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Buffer too small for a B-Tree page",
        ));
    }
    
    let mut cursor = std::io::Cursor::new(buffer);
    let header = BTreePageHeader::read_from(&mut cursor)?;
    
    // Create a B-Tree page with the header
    let mut btree_page = BTreePage {
        header: header.clone(),
        cell_indices: Vec::new(),
        cells: Vec::new(),
        page_size: self.page_size,
        page_number,
        reserved_space: self.reserved_space,
    };
    
    // Calculate header size
    let header_size = header.size();
    
    // Read cell indices
    let cell_count = header.cell_count as usize;
    let mut cell_indices = Vec::with_capacity(cell_count);
    
    for i in 0..cell_count {
        let offset = header_size + i * 2;
        if offset + 2 > buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small to read cell indices",
            ));
        }
        
        let index = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        cell_indices.push(index);
    }
    
    btree_page.cell_indices = cell_indices;
    
    // Read cells
    for &cell_offset in &btree_page.cell_indices {
        let cell_offset = cell_offset as usize;
        if cell_offset >= buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Cell offset out of range: {}", cell_offset),
            ));
        }
        
        // Parse cells based on page type
        let cell = match header.page_type {
            PageType::TableLeaf => {
                let mut cell_cursor = std::io::Cursor::new(&buffer[cell_offset..]);
                
                // Read payload size
                let (payload_size, payload_size_bytes) = crate::utils::decode_varint(&mut cell_cursor)?;
                
                // Read rowid
                let (row_id, rowid_bytes) = crate::utils::decode_varint(&mut cell_cursor)?;
                
                // Calculate position after reading varint
                let header_bytes = payload_size_bytes + rowid_bytes;
                
                // Determine if there's an overflow page
                let (payload, overflow_page) = if payload_size as usize <= buffer.len() - cell_offset - header_bytes {
                    // Payload fits entirely in this page
                    let payload = buffer[cell_offset + header_bytes..cell_offset + header_bytes + payload_size as usize].to_vec();
                    (payload, None)
                } else {
                    // Payload is split with overflow
                    let local_size = (buffer.len() - cell_offset - header_bytes).min(payload_size as usize);
                    let payload = buffer[cell_offset + header_bytes..cell_offset + header_bytes + local_size].to_vec();
                    
                    // Read overflow page number
                    let overflow_offset = cell_offset + header_bytes + local_size;
                    if overflow_offset + 4 <= buffer.len() {
                        let overflow_page = u32::from_be_bytes([
                            buffer[overflow_offset],
                            buffer[overflow_offset + 1],
                            buffer[overflow_offset + 2],
                            buffer[overflow_offset + 3],
                        ]);
                        (payload, Some(overflow_page))
                    } else {
                        // Invalid format
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid overflow page pointer",
                        ));
                    }
                };
                
                BTreeCell::TableLeaf(TableLeafCell {
                    payload_size: payload_size as u64,
                    row_id,
                    payload,
                    overflow_page,
                })
            },
            PageType::TableInterior => {
                if cell_offset + 4 > buffer.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Buffer too small to read interior cell",
                    ));
                }
                
                // Read left child page
                let left_child_page = u32::from_be_bytes([
                    buffer[cell_offset],
                    buffer[cell_offset + 1],
                    buffer[cell_offset + 2],
                    buffer[cell_offset + 3],
                ]);
                
                // Read key
                let mut cell_cursor = std::io::Cursor::new(&buffer[cell_offset + 4..]);
                let (key, _) = crate::utils::decode_varint(&mut cell_cursor)?;
                
                BTreeCell::TableInterior(TableInteriorCell {
                    left_child_page,
                    key,
                })
            },
            PageType::IndexLeaf => {
                let mut cell_cursor = std::io::Cursor::new(&buffer[cell_offset..]);
                
                // Read payload size
                let (payload_size, payload_size_bytes) = crate::utils::decode_varint(&mut cell_cursor)?;
                
                // Calculate position after reading varint
                let header_bytes = payload_size_bytes;
                
                // Determine if there's an overflow page
                let (payload, overflow_page) = if payload_size as usize <= buffer.len() - cell_offset - header_bytes {
                    // Payload fits entirely in this page
                    let payload = buffer[cell_offset + header_bytes..cell_offset + header_bytes + payload_size as usize].to_vec();
                    (payload, None)
                } else {
                    // Payload is split with overflow
                    let local_size = (buffer.len() - cell_offset - header_bytes).min(payload_size as usize);
                    let payload = buffer[cell_offset + header_bytes..cell_offset + header_bytes + local_size].to_vec();
                    
                    // Read overflow page number
                    let overflow_offset = cell_offset + header_bytes + local_size;
                    if overflow_offset + 4 <= buffer.len() {
                        let overflow_page = u32::from_be_bytes([
                            buffer[overflow_offset],
                            buffer[overflow_offset + 1],
                            buffer[overflow_offset + 2],
                            buffer[overflow_offset + 3],
                        ]);
                        (payload, Some(overflow_page))
                    } else {
                        // Invalid format
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid overflow page pointer",
                        ));
                    }
                };
                
                BTreeCell::IndexLeaf(IndexLeafCell {
                    payload_size: payload_size as u64,
                    payload,
                    overflow_page,
                })
            },
            PageType::IndexInterior => {
                if cell_offset + 4 > buffer.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Buffer too small to read interior cell",
                    ));
                }
                
                // Read left child page
                let left_child_page = u32::from_be_bytes([
                    buffer[cell_offset],
                    buffer[cell_offset + 1],
                    buffer[cell_offset + 2],
                    buffer[cell_offset + 3],
                ]);
                
                // Read payload size
                let mut cell_cursor = std::io::Cursor::new(&buffer[cell_offset + 4..]);
                let (payload_size, payload_size_bytes) = crate::utils::decode_varint(&mut cell_cursor)?;
                
                // Calculate position after reading varint
                let header_bytes = 4 + payload_size_bytes;
                
                // Determine if there's an overflow page
                let (payload, overflow_page) = if payload_size as usize <= buffer.len() - cell_offset - header_bytes {
                    // Payload fits entirely in this page
                    let payload = buffer[cell_offset + header_bytes..cell_offset + header_bytes + payload_size as usize].to_vec();
                    (payload, None)
                } else {
                    // Payload is split with overflow
                    let local_size = (buffer.len() - cell_offset - header_bytes).min(payload_size as usize);
                    let payload = buffer[cell_offset + header_bytes..cell_offset + header_bytes + local_size].to_vec();
                    
                    // Read overflow page number
                    let overflow_offset = cell_offset + header_bytes + local_size;
                    if overflow_offset + 4 <= buffer.len() {
                        let overflow_page = u32::from_be_bytes([
                            buffer[overflow_offset],
                            buffer[overflow_offset + 1],
                            buffer[overflow_offset + 2],
                            buffer[overflow_offset + 3],
                        ]);
                        (payload, Some(overflow_page))
                    } else {
                        // Invalid format
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid overflow page pointer",
                        ));
                    }
                };
                
                BTreeCell::IndexInterior(IndexInteriorCell {
                    left_child_page,
                    payload_size: payload_size as u64,
                    payload,
                    overflow_page,
                })
            },
            PageType::Overflow | PageType::Free => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid page type for B-Tree: {:?}", header.page_type),
                ));
            }
        };
        
        btree_page.cells.push(cell);
    }
    
    Ok(Page::BTree(btree_page))
}

    /// Parses an overflow page.
    /// 
    /// # Parameters
    /// * `page_number` - Page number.
    /// * `buffer` - Page data.
    /// 
    /// # Errors
    /// Returns an error if there are problems interpreting the data or if the buffer is too small.
    fn parse_overflow_page(&self, page_number: u32, buffer: &[u8]) -> io::Result<Page> {
        if buffer.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small for an overflow page",
            ));
        }
        
        // The first 4 bytes contain the number of the next overflow page
        let next_page = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        
        // The rest of the buffer contains the data
        let data = buffer[4..].to_vec();
        
        let overflow_page = OverflowPage::new(
            next_page,
            data,
            self.page_size,
            page_number,
        )?;
        
        Ok(Page::Overflow(overflow_page))
    }

    /// Parses a free page from a buffer.
    /// 
    /// # Parameters
    /// * `page_number` - Page number.
    /// * `buffer` - Page data.
    /// 
    /// # Errors
    /// Returns an error if there are problems interpreting the data or if the buffer is too small.
    /// 
    /// # Returns
    /// A `Page` instance representing the parsed free page.
    fn parse_free_page(&self, page_number: u32, buffer: &[u8]) -> io::Result<Page> {
        if buffer.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer demasiado pequeño para una página libre",
            ));
        }
        
        // The first 4 bytes contain the number of the next free page
        let next_page = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        
        let free_page = FreePage::new(
            next_page,
            self.page_size,
            page_number,
        );
        
        Ok(Page::Free(free_page))
    }

    /// Creates a new B-Tree page.
    ///
    /// # Parameters
    /// * `page_type` - Type of the B-Tree page (e.g., TableLeaf, IndexLeaf, etc.).
    /// * `right_most_page` - Number of the rightmost page (if applicable).
    /// 
    /// # Errors
    /// Returns an error if the page cannot be created or if the page type is invalid.
    /// 
    /// # Returns
    /// Number of the created page.
    pub fn create_btree_page(&mut self, page_type: PageType, right_most_page: Option<u32>) -> io::Result<u32> {
        // Asign a new page
        let page_number = self.disk_manager.allocate_pages(1)?;
        
        // Create the B-Tree page
        let btree_page = BTreePage::new(
            page_type,
            self.page_size,
            page_number,
            self.reserved_space,
            right_most_page,
        )?;
        
        // Add the page to the cache
        self.add_page_to_cache(page_number, Page::BTree(btree_page))?;
        self.dirty = true;
        
        Ok(page_number)
    }

    /// Creates a new overflow page.
    /// 
    /// # Parameters
    /// * `next_page` - Number of the next overflow page (0 if it is the last).
    /// * `data` - Data to store in the overflow page.
    /// 
    /// # Errors
    /// Returns an error if the page cannot be created or if the data is too large.
    pub fn create_overflow_page(&mut self, next_page: u32, data: Vec<u8>) -> io::Result<u32> {
        // Verify the size of the data
        // The maximum size of the data is the page size minus 4 bytes for the next_page
        let max_data_size = self.page_size as usize - 4; // 4 bytes para next_page
        if data.len() > max_data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Data is too big for an overflow page: {} bytes, maximum {} bytes",
                    data.len(), max_data_size),
            ));
        }
        
        // Asign a new page
        let page_number = self.disk_manager.allocate_pages(1)?;
        
        // Create the overflow page
        let overflow_page = OverflowPage::new(
            next_page,
            data,
            self.page_size,
            page_number,
        )?;
        
        // Add the page to the cache
        self.add_page_to_cache(page_number, Page::Overflow(overflow_page))?;
        self.dirty = true;
        
        Ok(page_number)
    }

    /// Create a new free page.
    /// 
    /// # Parameters
    /// * `next_page` - Number of the next free page (0 if it is the last).
    /// 
    /// # Errors
    /// Returns an error if the page cannot be created.
    /// 
    /// # Returns
    /// Number of the created page.
    pub fn create_free_page(&mut self, next_page: u32) -> io::Result<u32> {
        // Add a new page
        let page_number = self.disk_manager.allocate_pages(1)?;
        
        // Create the free page
        let free_page = FreePage::new(
            next_page,
            self.page_size,
            page_number,
        );
        
        // Agdd the page to the cache
        self.add_page_to_cache(page_number, Page::Free(free_page))?;
        self.dirty = true;
        
        Ok(page_number)
    }

    /// Flushes the dirty pages to disk.
    /// Currently, it writes all the pages in the cache to disk.
    /// This could be improved to only write the dirty pages, but for now we are lazy.
    pub fn flush(&mut self) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }

        let dirty_pages = self.page_cache.get_dirty_pages_referenced();
        if dirty_pages.is_empty() {
            return Ok(());
        }

        

        for (page_number,page) in dirty_pages{
            let buffer = self.serialize_page(page)?;
            
            // Write the page to disk
            self.disk_manager.write_page(page_number, &buffer)?;
        }
        
        // Sync the disk manager to ensure all data is written
        self.disk_manager.sync()?;
        
        self.dirty = false;
        Ok(())
    }

    /// Serializes a page into a byte buffer.
///
/// # Parameters
/// * `page` - Page to serialize.
///
/// # Errors
/// Returns an error if there are problems during serialization.
///
/// # Return
/// Buffer with the serialized data.
fn serialize_page(&self, page: &Page) -> io::Result<Vec<u8>> {
    let mut buffer = vec![0u8; self.page_size as usize];
    
    match page {
        Page::BTree(btree_page) => {
            // Write the header
            let mut cursor = std::io::Cursor::new(&mut buffer[..]);
            btree_page.header.write_to(&mut cursor)?;
            
            // Calculate header size
            let header_size = btree_page.header.size();
            
            // Write cell indices
            let mut offset = header_size;
            for (_, &cell_index) in btree_page.cell_indices.iter().enumerate() {
                let index_bytes = cell_index.to_be_bytes();
                buffer[offset..offset + 2].copy_from_slice(&index_bytes);
                offset += 2;
            }
            
            // Write cells (content area)
            for (i, cell) in btree_page.cells.iter().enumerate() {
                let cell_offset = btree_page.cell_indices[i] as usize;
                
                match cell {
                    BTreeCell::TableLeaf(leaf_cell) => {
                        let mut cell_cursor = std::io::Cursor::new(&mut buffer[cell_offset..]);
                        // Write payload size as varint
                        crate::utils::encode_varint(leaf_cell.payload_size as i64, &mut cell_cursor)?;
                        
                        // Write rowid as varint
                        crate::utils::encode_varint(leaf_cell.row_id, &mut cell_cursor)?;
                        
                        // Calculate current position
                        let pos = cell_cursor.position() as usize;
                        
                        // Write payload
                        let payload_size = leaf_cell.payload.len();
                        buffer[cell_offset + pos..cell_offset + pos + payload_size]
                            .copy_from_slice(&leaf_cell.payload);
                        
                        // Write overflow page if present
                        if let Some(overflow_page) = leaf_cell.overflow_page {
                            let overflow_offset = cell_offset + pos + payload_size;
                            buffer[overflow_offset..overflow_offset + 4]
                                .copy_from_slice(&overflow_page.to_be_bytes());
                        }
                    },
                    BTreeCell::TableInterior(interior_cell) => {
                        let mut cell_cursor = std::io::Cursor::new(&mut buffer[cell_offset..]);
                        
                        // Write left child page
                        cell_cursor.write_all(&interior_cell.left_child_page.to_be_bytes())?;
                        
                        // Write key as varint
                        crate::utils::encode_varint(interior_cell.key, &mut cell_cursor)?;
                    },
                    BTreeCell::IndexLeaf(leaf_cell) => {
                        let mut cell_cursor = std::io::Cursor::new(&mut buffer[cell_offset..]);
                        
                        // Write payload size as varint
                        crate::utils::encode_varint(leaf_cell.payload_size as i64, &mut cell_cursor)?;
                        
                        // Calculate current position
                        let pos = cell_cursor.position() as usize;
                        
                        // Write payload
                        let payload_size = leaf_cell.payload.len();
                        buffer[cell_offset + pos..cell_offset + pos + payload_size]
                            .copy_from_slice(&leaf_cell.payload);
                        
                        // Write overflow page if present
                        if let Some(overflow_page) = leaf_cell.overflow_page {
                            let overflow_offset = cell_offset + pos + payload_size;
                            buffer[overflow_offset..overflow_offset + 4]
                                .copy_from_slice(&overflow_page.to_be_bytes());
                        }
                    },
                    BTreeCell::IndexInterior(interior_cell) => {
                        let mut cell_cursor = std::io::Cursor::new(&mut buffer[cell_offset..]);
                        
                        // Write left child page
                        cell_cursor.write_all(&interior_cell.left_child_page.to_be_bytes())?;
                        
                        // Write payload size as varint
                        crate::utils::encode_varint(interior_cell.payload_size as i64, &mut cell_cursor)?;
                        
                        // Calculate current position
                        let pos = cell_cursor.position() as usize;
                        
                        // Write payload
                        let payload_size = interior_cell.payload.len();
                        buffer[cell_offset + pos..cell_offset + pos + payload_size]
                            .copy_from_slice(&interior_cell.payload);
                        
                        // Write overflow page if present
                        if let Some(overflow_page) = interior_cell.overflow_page {
                            let overflow_offset = cell_offset + pos + payload_size;
                            buffer[overflow_offset..overflow_offset + 4]
                                .copy_from_slice(&overflow_page.to_be_bytes());
                        }
                    },
                }
            }
        },
        Page::Overflow(overflow_page) => {
            // Write next overflow page number
            let next_page_bytes = overflow_page.next_page.to_be_bytes();
            buffer[0..4].copy_from_slice(&next_page_bytes);
            
            // Write data
            let data_len = overflow_page.data.len().min(self.page_size as usize - 4);
            buffer[4..4 + data_len].copy_from_slice(&overflow_page.data[0..data_len]);
        },
        Page::Free(free_page) => {
            // Write next free page number
            let next_page_bytes = free_page.next_page.to_be_bytes();
            buffer[0..4].copy_from_slice(&next_page_bytes);
        },
    }
    
    Ok(buffer)
}

    /// Obtains the number of pages in the database.
    /// 
    /// # Errors
    /// Returns an error if the page count cannot be obtained.
    /// 
    /// # Returns
    /// The number of pages in the database.
    pub fn page_count(&self) -> io::Result<u32> {
        self.disk_manager.page_count()
    }

    /// Closes the pager and flushes any dirty pages to disk.
    pub fn close(&mut self) -> io::Result<()> {
        self.flush()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_create_pager() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Create a pager
        let result = Pager::create(&db_path, 4096, None, 0);
        assert!(result.is_ok());
        
        // Verify that the file exists
        assert!(db_path.exists());
        
        // Verify that the file size is at least one page
        let metadata = fs::metadata(&db_path).unwrap();
        assert!(metadata.len() >= 4096);
    }

    #[test]
    fn test_open_pager() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Create a pager
        {
            let _pager = Pager::create(&db_path, 4096, None, 0).unwrap();
        }
        
        // Open the existing pager
        let result = Pager::open(&db_path, None);
        assert!(result.is_ok());
        
        let pager = result.unwrap();
        assert_eq!(pager.page_size, 4096);
        assert_eq!(pager.reserved_space, 0);
    }

    #[test]
    fn test_create_btree_page() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Create a pager
        let mut pager = Pager::create(&db_path, 4096, None, 0).unwrap();
        
        // Create a table leaf B-Tree page
        let page_number = pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        assert_eq!(page_number, 3); // The first page is the header
        
        // Verify that the page exists in the cache
        assert!(pager.page_cache.contains_page(page_number));
        
        // Flush to write the page to disk
        pager.flush().unwrap();
        
        // Open a new pager and verify the page is readable
        let mut pager2 = Pager::open(&db_path, None).unwrap();
        let page_result = pager2.get_page(page_number, Some(PageType::TableLeaf));
        println!("Page result: {:?}", page_result);
        assert!(page_result.is_ok());
    }

    #[test]
    fn test_header_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Create a pager
        let mut pager = Pager::create(&db_path, 4096, None, 0).unwrap();
        
        // Read the header
        let mut header = pager.get_header().unwrap();
        assert_eq!(header.page_size, 4096);
        assert_eq!(header.reserved_space, 0);
        
        // Modify the header
        header.user_version = 42;
        pager.update_header(&header).unwrap();
        
        // Read it again and verify
        let header2 = pager.get_header().unwrap();
        assert_eq!(header2.user_version, 42);
    }

    #[test]
    fn test_page_count() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Create a pager
        let mut pager = Pager::create(&db_path, 4096, None, 0).unwrap();
        
        // Verify there is one page initially
        assert_eq!(pager.page_count().unwrap(), 2); // 1 for the header and 1 for the first page
        
        // Create a B-Tree page
        pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        
        // Verify there are now three pages
        assert_eq!(pager.page_count().unwrap(), 3);
    }

    #[test]
    fn test_btree_page_serialization() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Create a pager
        let mut pager = Pager::create(&db_path, 4096, None, 0).unwrap();
        
        // Create a B-Tree page
        let page_number = pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        
        // Add a cell to the page
        {
            let page = pager.get_page_mut(page_number, Some(PageType::TableLeaf)).unwrap();
            match page {
                Page::BTree(btree_page) => {
                    let cell = BTreeCell::TableLeaf(TableLeafCell {
                        payload_size: 10,
                        row_id: 42,
                        payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        overflow_page: None,
                    });
                    
                    // Add the cell manually to the page
                    btree_page.cell_indices.push(4000); // Near the end of the page
                    btree_page.cells.push(cell);
                    btree_page.header.cell_count = 1;
                },
                _ => panic!("Unexpected page type"),
            }
        }
        
        // Unpin the page after modification
        pager.page_cache.unpin_page(page_number);
        
        // Flush to disk
        pager.flush().unwrap();
        
        // Create a new pager to read the page back
        let mut pager2 = Pager::open(&db_path, None).unwrap();
        
        // Read the page
        let page = pager2.get_page(page_number, Some(PageType::TableLeaf)).unwrap();
        
        // Verify the page
        match page {
            Page::BTree(btree_page) => {
                assert_eq!(btree_page.header.page_type, PageType::TableLeaf);
                assert_eq!(btree_page.header.cell_count, 1);
                assert_eq!(btree_page.cell_indices.len(), 1);
                assert_eq!(btree_page.cells.len(), 1);
                
                // Verify the cell
                match &btree_page.cells[0] {
                    BTreeCell::TableLeaf(cell) => {
                        assert_eq!(cell.payload_size, 10);
                        assert_eq!(cell.row_id, 42);
                        assert_eq!(cell.payload, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
                        assert_eq!(cell.overflow_page, None);
                    },
                    _ => panic!("Unexpected cell type"),
                }
            },
            _ => panic!("Unexpected page type"),
        }
        
        // Don't forget to unpin the page after reading
        pager2.page_cache.unpin_page(page_number);
    }
    
    #[test]
    fn test_page_pinning_and_dirty_flags() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Create a pager with a small buffer pool to test eviction
        let mut pager = Pager::create(&db_path, 4096, Some(2), 0).unwrap();
        
        // Create two pages
        let page1 = pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        let page2 = pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        
        // Both pages should be pinned after creation
        assert!(pager.page_cache.is_pinned(page1));
        assert!(pager.page_cache.is_pinned(page2));
        
        // Unpin page1
        assert!(pager.page_cache.unpin_page(page1));
        assert!(!pager.page_cache.is_pinned(page1));
        assert!(pager.page_cache.is_pinned(page2));
        
        // Page2 should be marked as dirty when we get it for writing
        let _ = pager.get_page_mut(page2, Some(PageType::TableLeaf)).unwrap();
        assert!(pager.page_cache.is_dirty(page2));
        
        // Create a third page, which should evict page1 (unpinned)
        let page3 = pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        
        // Page1 should not be in the cache anymore (evicted)
        assert!(!pager.page_cache.contains_page(page1));
        assert!(pager.page_cache.contains_page(page2));
        assert!(pager.page_cache.contains_page(page3));
        
        // Verify pages are still accessible after flushing
        pager.flush().unwrap();
        
        // We should be able to load page1 again, even though it was evicted
        let _ = pager.get_page(page1, Some(PageType::TableLeaf)).unwrap();
        assert!(pager.page_cache.contains_page(page1));
        
        // Clean up
        pager.page_cache.unpin_page(page1);
        pager.page_cache.unpin_page(page2);
        pager.page_cache.unpin_page(page3);
    }
}
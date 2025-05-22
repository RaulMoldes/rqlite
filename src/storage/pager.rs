//! # Pager Module
//!
//! This module implements the `Pager` struct, which is responsible for managing
//! the loading and caching of database pages. It interacts with the `DiskManager`


use core::panic;
use std::io::{self, Write};
use std::path::Path;

use super::cache::BufferPool;
use super::disk::{self, DiskManager};
use crate::header::Header;
use crate::page::{Page,
    self, BTreeCell, BTreePage, BTreePageHeader, ByteSerializable, FreePage, IndexInteriorCell, IndexLeafCell, OverflowPage, PageType, TableInteriorCell, TableLeafCell
};

use std::cell::{Ref, RefCell, RefMut};

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
    disk_manager: DiskManager, // Switched to RefCell for interior mutability
    /// Page Cache. This should be a BufferPool, but for now it is just a HashMap.
    page_cache: BufferPool,
    /// Size of each page in bytes.
    /// This is the size of the page that is going to be used in the database.
    page_size: u32,

    journal_pages: Vec<(u32,Page)>,
    /// Reserved space at the end of each page.
    reserved_space: u8,
    /// Indicates if the pager has unsaved changes. That must be written to disk.
    /// Tis could be handled in a more fine-grained way when the BufferPool is implemented.
    /// For now, if the Pager has any dirty page, all its cached pages must be written to disk.
    dirty: bool,
}

impl Drop for Pager {
    fn drop(&mut self) {

       // If we are in the middle of a transaction, we need to rollback
        if self.dirty {
            // Rollback the transaction
            if let Err(e) = self.rollback_transaction() {
                eprintln!("Error rolling back transaction: {}", e);
            }
        }
       
    }
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
            journal_pages: vec![],
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
    pub fn create<P: AsRef<Path>>(
        path: P,
        page_size: u32,
        buffer_pool_size: Option<usize>,
        reserved_space: u8,
    ) -> io::Result<Self> {
        let mut disk_manager = DiskManager::create(path, page_size)?;

        // Upadate the header with the reserved space
        let mut header = disk_manager.read_header()?;
        header.reserved_space = reserved_space;
        disk_manager.write_header(&header)?;

        Ok(Pager {
            disk_manager,
            page_cache: BufferPool::new(buffer_pool_size.unwrap_or(1000)),
            page_size,
            journal_pages: vec![],  // For now the journal is kept on memory
            reserved_space,
            dirty: false,
        })
    }

    /// Obtains the header of the database from the disk manager.
    pub fn get_header(&mut self) -> io::Result<Header> {
        // This goes straight to the disk
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
        //Again, straight to the disk to rewrite the header
         
        self.disk_manager.write_header(header)?;
        self.dirty = true; // Not really needed i think, but let's keep it for consistency
        Ok(())
    }




    /// Obtains a page from the database, loading it from disk if necessary.
    /// Attempts to load the page from the cache first, and if it is not found,
    /// it loads it from disk and caches it.
    /// I think using a callback function is a good idea, because it allows to use the page without cloning it.
    /// This way, after the callback function is executed, the page is unpinned.
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
        }
        
        if let Some(expected_type) = page_type {
            self.page_cache.validate_page_type(page_number, expected_type).map_err(|opt_err|
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Type of page incorrect: expected {:?}, obtained {:?}",
                        expected_type, opt_err
                    ),
                )
            )?;
        }
        
        // Get page pins the page. We will have to unpin it once we are done with it.
        let page = self.page_cache.get_page(page_number).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            )
        })?;
        
       
        Ok(page)
    }

    pub fn begin_transaction(&mut self) -> io::Result<()> {
        // This is a no-op for now, but we could do some initialization here if needed
        // We are going to use the journal pages to keep track of the dirty pages
        self.journal_pages.clear();
        self.dirty = true; // We are dirty now
        Ok(())
    }

    pub fn unpin_page(&mut self, page_number: u32) -> io::Result<()> {
        // Unpin the page from the cache
        if !self.page_cache.unpin_page(page_number) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            ));
        }
        Ok(())
    }


    pub fn commit_transaction(&mut self) -> io::Result<()> {
        // Flush the dirty pages to disk
        // Kind of a two-phase commit, but not really
        if  self.flush().is_ok() {
            self.clear_journal();  
            Ok(()) 

        } else {
            // Rollback the transaction if there was an error
            self.rollback_transaction()?;
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Transaction failed",
            ))
        }

        
    }

    fn clear_journal(&mut self) {
        // Clear the journal pages
        if self.dirty {
            panic!("Cannot clear journal pages, we are in the middle of a transaction");
        }
        self.journal_pages.clear();
    }

    fn rollback_transaction(&mut self) -> io::Result<()> {
        // Rolling back is just a matter of removing the pages from the journal and rewriting them back to disk.
        for (page_number, page) in self.journal_pages.iter() {
            // Write the page to disk
            let buffer = self.serialize_page(page)?;
            self.disk_manager.write_page(*page_number, &buffer)?;
        }
        // The transaction was invalidated, so we need to clear the journal
        self.page_cache.mark_clean_all(); // Mark all pages as clean
        self.dirty = false; // We are not dirty anymore
        self.clear_journal(); // Clear the journal pages
        Ok(())
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
        
        
        // Try to get from BufferPool
        if !self.page_cache.contains_page(page_number) {
            // Load from disk into buffer pool
            self.load_page(page_number)?;
        }
        
        if let Some(expected_type) = page_type {
            self.page_cache.validate_page_type(page_number, expected_type).map_err(|opt_err|
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Type of page incorrect: expected {:?}, obtained {:?}",
                        expected_type, opt_err
                    ),
                )
            )?;
        }
        
        // Get page pins the page. We will have to unpin it once we are done with it.
        let page = self.page_cache.get_page_mut(page_number).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            )
        })?;

        // Loading the page mutably implies adding it to the journal list
        self.journal_pages.push((page_number, page.clone()));
        
       // This actually marks the initiation of a transaction
        self.dirty = true; // We are dirty now
        Ok(page)
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
        
        {
            
            let page_count = self.disk_manager.page_count()?;
            
        if (page_number) > (page_count + 1) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Page number out of range: {}, maximum {}",
                    page_number,
                    page_count + 1
                ),
            ));
        }
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
        if let Some((evicted_page_number, evicted_page)) = self.add_page_to_cache(page_number, page)?{
           
            let buffer = self.serialize_page(&evicted_page)?;
            // Write the page to disk. This should be safe because the buffer pool wont evict a page that is marked as dirty.
            self.disk_manager.write_page(evicted_page_number, &buffer)?;
            
        }
        
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
    fn add_page_to_cache(&mut self, page_number: u32, page: Page) -> io::Result<Option<(u32, Page)>> {
        // Add the page to the cache

        match self.page_cache.add_page(page_number, page, true) { 
            // We pin it because in most cases if we add a page to the cache we are going to use it
            Some((evicted_page_number, evicted_page)) => {
                if evicted_page_number == page_number {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Page {} was rejected by the cache (OOM Error)", page_number),
                    ))
                } else {
                    Ok(Some((evicted_page_number, evicted_page))) // Return the evicted page number, so that it can be flushed
                }
            }
            _ => Ok(None), // No page was evicted
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
        if buffer.is_empty() {
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
        if buffer.is_empty() {
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

        // Use the ByteSerializable trait to read the page
        let mut page = BTreePage::read_from(&mut cursor)?;

        // Set the page_number, page_size and reserved_space
        page.page_number = page_number;
        page.page_size = self.page_size;
        page.reserved_space = self.reserved_space;

        Ok(Page::BTree(page))
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

        let mut cursor = std::io::Cursor::new(buffer);

        // Use the ByteSerializable trait to read the page
        let mut page = OverflowPage::read_from(&mut cursor)?;

        // Set the page_number, page_size and reserved_space
        page.page_number = page_number;
        page.page_size = self.page_size;

        Ok(Page::Overflow(page))
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
                "Buffer too small for a free page",
            ));
        }

        let mut cursor = std::io::Cursor::new(buffer);

        // Use the ByteSerializable trait to read the page
        let mut page = FreePage::read_from(&mut cursor)?;

        // Set the page_number, page_size and reserved_space
        page.page_number = page_number;
        page.page_size = self.page_size;

        Ok(Page::Free(page))
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
    pub fn create_btree_page(
        &mut self,
        page_type: PageType,
        right_most_page: Option<u32>,
    ) -> io::Result<u32> {
        // This goes straight to the disk, as interacting with the cache here overcomplicates things
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
        let buffer = self.serialize_page(&Page::BTree(btree_page))?;

        self.disk_manager.write_page(page_number, &buffer)?;

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
    pub fn create_overflow_page(&mut self, next_page: u32, 
        data: Vec<u8>,
    ) -> io::Result<u32> {
        // Verify the size of the data
        // The maximum size of the data is the page size minus 4 bytes for the next_page
        let max_data_size = self.page_size as usize - 4; // 4 bytes para next_page
        if data.len() > max_data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Data is too big for an overflow page: {} bytes, maximum {} bytes",
                    data.len(),
                    max_data_size
                ),
            ));
        }

        // Asign a new page
        let page_number = self.disk_manager.allocate_pages(1)?;

        // Create the overflow page
        let overflow_page = OverflowPage::new(next_page, data, self.page_size, page_number)?;

        let buffer = self.serialize_page(&Page::Overflow(overflow_page))?;
        self.disk_manager.write_page(page_number, &buffer)?;
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
    pub fn create_free_page(&mut self, 
        next_page: u32,
    ) -> io::Result<u32> {
        // Add a new page
        let page_number = self.disk_manager.allocate_pages(1)?;

        // Create the free page
        let free_page = FreePage::new(next_page, self.page_size, page_number);

        let buffer = self.serialize_page(&Page::Free(free_page))?;
        self.disk_manager.write_page(page_number, &buffer)?;
        Ok(page_number)
        
    }

    /// Flushes the dirty pages to disk.
    /// Currently, it writes all the pages in the cache to disk.
    /// This could be improved to only write the dirty pages, but for now we are lazy.
    pub fn flush(&mut self) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }

    
        let dirty_pages = self.page_cache.get_dirty_pages();

        if dirty_pages.is_empty() {
            return Ok(());
        }

        for (page_number, page) in dirty_pages {
            let buffer = self.serialize_page(page)?;

            // Write the page to disk
            self.disk_manager.write_page(page_number, &buffer)?;
            
        }

        // Sync the disk manager to ensure all data is written
        self.disk_manager.sync()?;
        self.page_cache.mark_clean_all();
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
        let mut cursor = std::io::Cursor::new(&mut buffer[..]);

        // Use the ByteSerializable trait to write the page
        page.write_to(&mut cursor)?;

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

}

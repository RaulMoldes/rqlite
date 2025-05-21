//! # Pager Module
//!
//! This module implements the `Pager` struct, which is responsible for managing
//! the loading and caching of database pages. It interacts with the `DiskManager`


use std::io::{self, Write};
use std::path::Path;

use super::cache::BufferPool;
use super::disk::DiskManager;
use crate::header::Header;
use crate::page::{
    self, BTreeCell, BTreePage, BTreePageHeader, ByteSerializable, FreePage, IndexInteriorCell, IndexLeafCell, OverflowPage, Page, PageType, TableInteriorCell, TableLeafCell
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
    disk_manager: RefCell<DiskManager>, // Switched to RefCell for interior mutability
    /// Page Cache. This should be a BufferPool, but for now it is just a HashMap.
    page_cache: RefCell<BufferPool>,
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

impl Drop for Pager {
    fn drop(&mut self) {
        // Flush the dirty pages to disk when the pager is dropped
        if let Err(e) = self.flush() {
            eprintln!("Error flushing pager: {}", e);
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
            disk_manager: RefCell::new(disk_manager),
            page_cache: RefCell::new(BufferPool::new(buffer_pool_size.unwrap_or(1000))),
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
            disk_manager: RefCell::new(disk_manager),
            page_cache: RefCell::new(BufferPool::new(buffer_pool_size.unwrap_or(1000))),
            page_size,
            reserved_space,
            dirty: false,
        })
    }

    /// Obtains the header of the database from the disk manager.
    pub fn get_header(&self) -> io::Result<Header> {
        let mut dmanager = self.disk_manager.try_borrow_mut().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow DiskManager for reading header",
            )
        })?;
        dmanager.read_header()
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
        let mut disk_manager = self.disk_manager.try_borrow_mut().map_err(
            |_| io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow DiskManager for updating header",
            ),
        )?;
        disk_manager.write_header(header)?;
        self.dirty = true; // Not really needed i think, but let's keep it for consistency
        Ok(())
    }

    pub fn unpin_page(&self, page_number: u32) -> io::Result<()> {
        let result = {
            let mut page_cache = self.page_cache.try_borrow_mut().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to borrow page cache for unpinning page",
                )
            })?;
    
            if !page_cache.contains_page(page_number) {
                return Ok(()); // terminamos temprano
            }
    
            page_cache.unpin_page(page_number)
        }; // <<-- aquí se libera el RefMut (page_cache)
        
       
        // fuera del scope de borrow_mut
        if result {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            ))
        }
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
    pub fn get_page<F, R>(&self, page_number: u32, page_type: Option<PageType>, f: F) -> io::Result<R>
where
    F: FnOnce(&Page) -> R,
{   

    let is_on_cache = {

        let page_cache = self.page_cache.try_borrow().map_err(
            |_| io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow page cache for page type verification",
            ),
        )?;
        page_cache.contains_page(page_number)
    };
    
    // Try to get from BufferPool
    if !is_on_cache{
        // Load from disk into buffer pool
        self.load_page(page_number)?;
    }

    let mut page_cache = self.page_cache.try_borrow_mut().map_err(
        |_| io::Error::new(
            io::ErrorKind::Other,
            "Failed to borrow page cache for page reading",
        ),
    )?;
    
    
    if let Some(expected_type) = page_type {
        page_cache.validate_page_type(page_number, expected_type).map_err(|opt_err|
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Type of page incorrect: expected {:?}, obtained {:?}",
                    expected_type, opt_err
                ),
            )
        )?;
    }
    
    let page = page_cache.get_page(page_number).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Page {} not found in cache", page_number),
        )
    })?;
    
    
    Ok(f(page))
    // Unpin the page after using it
    
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
    pub fn get_page_mut<F, R>(&self, page_number: u32, page_type: Option<PageType>, f: F) -> io::Result<R>
where
    F: FnOnce(&mut Page) -> R,
{
    let mut page_cache = self.page_cache.try_borrow_mut().map_err(
        |_| io::Error::new(
            io::ErrorKind::Other,
            "Failed to borrow page cache for page type verification",
        ),
    )?;
    
    // Try to get from BufferPool
    if !page_cache.contains_page(page_number) {
        // Load from disk into buffer pool
        self.load_page(page_number)?;
    }
    
    if let Some(expected_type) = page_type {
        page_cache.validate_page_type(page_number, expected_type).map_err(|opt_err|
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Type of page incorrect: expected {:?}, obtained {:?}",
                    expected_type, opt_err
                ),
            )
        )?;
    }
    
    let page = page_cache.get_page_mut(page_number).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Page {} not found in cache", page_number),
        )
    })?;
    
    // Ejecutar la función callback con la referencia a la página
    Ok(f(page))
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
    fn load_page(&self, page_number: u32) -> io::Result<()> {
        // Verify if the page number is valid
        
        {
            let disk_manager = self.disk_manager.borrow();
            let page_count = disk_manager.page_count()?;

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
        self.disk_manager.try_borrow_mut().map_err(
            |_| io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow DiskManager for reading page",
            ),
        )?.read_page(page_number, &mut buffer)?;
      
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
    fn add_page_to_cache(&self, page_number: u32, page: Page) -> io::Result<()> {
        // Add the page to the cache

        match self.page_cache.try_borrow_mut().map_err(
            |_| io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow page cache for adding page",
            )
        )?.add_page(page_number, page, true) {
            Some((evicted_page_number, _)) => {
                if evicted_page_number == page_number {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Page {} was rejected by the cache (OOM Error)", page_number),
                    ))
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
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
    fn create_btree_page(
        &self,
        page_type: PageType,
        right_most_page: Option<u32>,
        page_cache: &mut RefMut<'_,BufferPool>,
    ) -> io::Result<u32> {
        // Asign a new page
        let page_number = self.disk_manager.try_borrow_mut().map_err(
            |_| io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow DiskManager for allocating page",
            ),
        )?.allocate_pages(1)?;
      

        // Create the B-Tree page
        let btree_page = BTreePage::new(
            page_type,
            self.page_size,
            page_number,
            self.reserved_space,
            right_most_page,
        )?;

        // Directamente usar el `page_cache` pasado
    match page_cache.add_page(page_number, Page::BTree(btree_page), true) {
        Some((evicted, _)) if evicted == page_number => {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Page {} rejected by cache", page_number),
            ))
        }
        _ => {
            
            Ok(page_number)
        }
    }
    }

    /// Creates a new overflow page.
    ///
    /// # Parameters
    /// * `next_page` - Number of the next overflow page (0 if it is the last).
    /// * `data` - Data to store in the overflow page.
    ///
    /// # Errors
    /// Returns an error if the page cannot be created or if the data is too large.
    fn create_overflow_page(& self, next_page: u32, 
        data: Vec<u8>,
        page_cache: &mut RefMut<'_,BufferPool>,
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
        let page_number = self.disk_manager.try_borrow_mut().map_err(
            |_| io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow DiskManager for allocating page",
            ),
        )?.allocate_pages(1)?;

        // Create the overflow page
        let overflow_page = OverflowPage::new(next_page, data, self.page_size, page_number)?;

        // Add the page to the cache
        
        match page_cache.add_page(page_number, Page::Overflow(overflow_page), true) {
            Some((evicted, _)) if evicted == page_number => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Page {} rejected by cache", page_number),
                ))
            }
            _ => {
                
                Ok(page_number)
            }
        }
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
    fn create_free_page(&self, 
        next_page: u32,
        page_cache: &mut RefMut<'_,BufferPool>,
    ) -> io::Result<u32> {
        // Add a new page
        let page_number = self.disk_manager.try_borrow_mut().map_err(
            |_| io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow DiskManager for allocating page",
            ),
        )?.allocate_pages(1)?;

        // Create the free page
        let free_page = FreePage::new(next_page, self.page_size, page_number);

        // Directamente usar el `page_cache` pasado
        match page_cache.add_page(page_number, Page::Free(free_page), true) {
        Some((evicted, _)) if evicted == page_number => {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Page {} rejected by cache", page_number),
            ))
        }
        _ => {
          
            Ok(page_number)
        }
    }
    }

    // Creates  single page of the given type.
    pub fn create_page(
        &mut self,
        page_type: PageType,
        right_most_page: Option<u32>,
        next_page: Option<u32>,
        data: Option<Vec<u8>>,
    ) -> io::Result<u32>{
        let mut page_cache = self.page_cache.try_borrow_mut().map_err(
            |_| io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow page cache for creating pages",
            ),
        )?;
        let page_number = match page_type {
            PageType::TableLeaf | PageType::TableInterior | PageType::IndexLeaf | PageType::IndexInterior => {
                self.create_btree_page(page_type, right_most_page, &mut page_cache)?
            }
            PageType::Overflow => {
                if let Some(data) = data {
                    self.create_overflow_page(next_page.unwrap_or(0), data, &mut page_cache)?
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Data is required for overflow page",
                    ));
                }
            }
            PageType::Free => {
                self.create_free_page(next_page.unwrap_or(0), &mut page_cache)?
            }
        };
        page_cache.unpin_page(page_number);
        Ok(page_number)
    }

    // Wrapper function to create multiple pages of the same type.
    pub fn create_pages(
        &mut self,
        page_type: PageType,
        right_most_page: Option<u32>,
        next_page: Option<u32>,
        data: Option<Vec<u8>>,
        num_pages: u32,
    ) -> io::Result<Vec<u32>> {
        let mut page_cache = self.page_cache.try_borrow_mut().map_err(
            |_| io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow page cache for creating pages",
            ),
        )?;
        let mut page_numbers = Vec::with_capacity(num_pages as usize);
        for _ in 0..num_pages {
            let page_number = match page_type {
                PageType::TableLeaf | PageType::TableInterior | PageType::IndexLeaf | PageType::IndexInterior => {
                    self.create_btree_page(page_type, right_most_page, &mut page_cache)?

                    
                }
                PageType::Overflow => {
                    if let Some(data) = data.clone() {

                        self.create_overflow_page(next_page.unwrap_or(0), data, &mut page_cache)?
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Data is required for overflow page",
                        ));
                    }
                    
                   
                    
                }
                PageType::Free => {
                    self.create_free_page(next_page.unwrap_or(0), &mut page_cache)?
                   
                  
                }
            };

            page_cache.unpin_page(page_number);
            page_numbers.push(page_number);
            
        }
        Ok(page_numbers)
       
        
    }
    /// Flushes the dirty pages to disk.
    /// Currently, it writes all the pages in the cache to disk.
    /// This could be improved to only write the dirty pages, but for now we are lazy.
    pub fn flush(&mut self) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }

        let cache = self.page_cache.borrow();
        let dirty_pages = cache.get_dirty_pages_referenced();

        if dirty_pages.is_empty() {
            return Ok(());
        }

        for (page_number, page) in dirty_pages {
            let buffer = self.serialize_page(page)?;

            // Write the page to disk
            self.disk_manager.try_borrow_mut().map_err(
                |_| io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to borrow DiskManager for writing page",
                ),
            )?.write_page(page_number, &buffer)?;
        }

        // Sync the disk manager to ensure all data is written
        self.disk_manager.try_borrow_mut().map_err(
            |_| io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow DiskManager for Sync",
            ),
        )?.sync()?;

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
       
        let disk_manager = self.disk_manager.try_borrow().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Failed to borrow DiskManager for page count",
            )
        })?;
        disk_manager.page_count()
      
    }

    /// Closes the pager and flushes any dirty pages to disk.
    pub fn close(&mut self) -> io::Result<()> {
        self.flush()
    }
}


#[cfg(test)]
mod pager_tests {
    use super::*;

    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn create_test_pager() -> Arc<Mutex<Pager>> {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(db_path, 4096, Some(100), 0).unwrap();
        Arc::new(Mutex::new(pager))
    }

    #[test]
    fn test_open_create_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Test create
        {
            let pager = Pager::create(&db_path, 4096, Some(10), 0);
            assert!(pager.is_ok());
            
            let mut pager = pager.unwrap();
            
            // Create a btree page
            let page_num = pager.create_page(PageType::TableLeaf, None, None, None).unwrap();
            assert_eq!(page_num, 2); // First page should be 2 (page 1 is header)
            
            // Flush changes
            pager.flush().unwrap();
        }
        
        // Test open
        {
            let pager = Pager::open(&db_path, Some(10));
            assert!(pager.is_ok());
            
            let pager = pager.unwrap();
            
            // Verify page count
            let count = pager.page_count().unwrap();
            assert_eq!(count, 2); // Should have 2 pages
            
            // Verify we can read page 2
            pager.get_page(2, Some(PageType::TableLeaf), |page| {
                match page {
                    Page::BTree(btree_page) => {
                        assert_eq!(btree_page.header.page_type, PageType::TableLeaf);
                        assert_eq!(btree_page.page_number, 2);
                    },
                    _ => panic!("Expected BTree page"),
                }
            }).unwrap();
        }
    }

    #[test]
    fn test_page_crud_operations() {
        let pager = create_test_pager();
        
        // Create different types of pages
        {
            let mut pager_guard = pager.lock().unwrap();
            
            // Create BTree page
            let btree_page = pager_guard.create_page(PageType::TableLeaf, None, None, None).unwrap();
            
            // Create overflow page
            let data = vec![1, 2, 3, 4, 5];
            let overflow_page = pager_guard.create_page(PageType::Overflow, None, Some(0), Some(data.clone())).unwrap();
            
            // Create free page
            let free_page = pager_guard.create_page(PageType::Free, None, Some(0), None).unwrap();
            
            // Verify pages were created with correct numbers
            assert_eq!(btree_page, 2);
            assert_eq!(overflow_page, 3);
            assert_eq!(free_page, 4);
        }
        
        // Read pages
        {
            let pager_guard = pager.lock().unwrap();
            
            // Read BTree page
            pager_guard.get_page(2, Some(PageType::TableLeaf), |page| {
                match page {
                    Page::BTree(btree_page) => {
                        assert_eq!(btree_page.header.page_type, PageType::TableLeaf);
                        assert_eq!(btree_page.header.cell_count, 0);
                    },
                    _ => panic!("Expected BTree page"),
                }
            }).unwrap();
            
            // Read overflow page
            pager_guard.get_page(3, Some(PageType::Overflow), |page| {
                match page {
                    Page::Overflow(overflow_page) => {
                        assert_eq!(overflow_page.next_page, 0);
                        assert_eq!(overflow_page.data, vec![1, 2, 3, 4, 5]);
                    },
                    _ => panic!("Expected Overflow page"),
                }
            }).unwrap();
            
            // Read free page
            pager_guard.get_page(4, Some(PageType::Free), |page| {
                match page {
                    Page::Free(free_page) => {
                        assert_eq!(free_page.next_page, 0);
                    },
                    _ => panic!("Expected Free page"),
                }
            }).unwrap();
        }
        
        // Modify pages
        {
            let pager_guard = pager.lock().unwrap();
            
            // Modify BTree page
            pager_guard.get_page_mut(2, Some(PageType::TableLeaf), |page| {
                match page {
                    Page::BTree(btree_page) => {
                        // Create a dummy cell
                        let cell = BTreeCell::TableLeaf(TableLeafCell {
                            payload_size: 5,
                            row_id: 1,
                            payload: vec![10, 20, 30, 40, 50],
                            overflow_page: None,
                        });
                        
                        // Add cell to page
                        btree_page.add_cell(cell).unwrap();
                    },
                    _ => panic!("Expected BTree page"),
                }
            }).unwrap();
            
            // Verify modification
            pager_guard.get_page(2, Some(PageType::TableLeaf), |page| {
                match page {
                    Page::BTree(btree_page) => {
                        assert_eq!(btree_page.header.cell_count, 1);
                        
                        match &btree_page.cells[0] {
                            BTreeCell::TableLeaf(cell) => {
                                assert_eq!(cell.row_id, 1);
                                assert_eq!(cell.payload, vec![10, 20, 30, 40, 50]);
                            },
                            _ => panic!("Expected TableLeaf cell"),
                        }
                    },
                    _ => panic!("Expected BTree page"),
                }
            }).unwrap();
        }
        
        // Test flush
        {
            let mut pager_guard = pager.lock().unwrap();
            assert!(pager_guard.flush().is_ok());
        }
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;
        let pager = create_test_pager();
        
        // Create a page
        {
            let mut pager_guard = pager.lock().unwrap();
            pager_guard.create_page(PageType::TableLeaf, None, None, None).unwrap();
        }
        
        // Spawn multiple threads to read the page
        let mut handles = Vec::new();
        for _ in 0..10 {
            let pager_clone = Arc::clone(&pager);
            let handle = thread::spawn(move || {
                let pager_guard = pager_clone.lock().unwrap();
                pager_guard.get_page(2, Some(PageType::TableLeaf), |page| {
                    match page {
                        Page::BTree(btree_page) => {
                            assert_eq!(btree_page.header.page_type, PageType::TableLeaf);
                            assert_eq!(btree_page.page_number, 2);
                        },
                        _ => panic!("Expected BTree page"),
                    }
                }).unwrap();
            });
            handles.push(handle);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_page_type_validation() {
        let pager = create_test_pager();
        
        // Create different page types
        {
            let mut pager_guard = pager.lock().unwrap();
            pager_guard.create_page(PageType::TableLeaf, None, None, None).unwrap();
        }
        
        // Try to access with wrong page type
        {
            let pager_guard = pager.lock().unwrap();
            let result = pager_guard.get_page(2, Some(PageType::TableInterior), |_| {});
            assert!(result.is_err());
            
            // Right page type should work
            let result = pager_guard.get_page(2, Some(PageType::TableLeaf), |_| {});
            assert!(result.is_ok());
            
            // None page type should also work
            let result = pager_guard.get_page(2, None, |_| {});
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_large_data_operations() {
        let pager = create_test_pager();
        
        // Create a page with large data
        {
            let mut pager_guard = pager.lock().unwrap();
            
            // Create a large payload that will span multiple overflow pages
            let large_data = vec![42u8; 8000]; // 8KB data
            
            // Create btree page
            let btree_page = pager_guard.create_page(PageType::TableLeaf, None, None, None).unwrap();
            
            // Add a cell with the large payload
            pager_guard.get_page_mut(btree_page, Some(PageType::TableLeaf), |page| {
                match page {
                    Page::BTree(btree_page) => {
                        // Create a large cell that will require overflow pages
                        let cell = BTreeCell::TableLeaf(TableLeafCell {
                            payload_size: large_data.len() as u64,
                            row_id: 1,
                            payload: large_data[0..3000].to_vec(), // First 3KB in main page
                            overflow_page: Some(3), // Point to overflow page
                        });
                        
                        btree_page.add_cell(cell).unwrap();
                    },
                    _ => panic!("Expected BTree page"),
                }
            }).unwrap();
            
            // Create overflow pages for the rest of the data
            let overflow1 = pager_guard.create_page(PageType::Overflow, None, Some(4), Some(large_data[3000..6000].to_vec())).unwrap();
            let overflow2 = pager_guard.create_page(PageType::Overflow, None, Some(0), Some(large_data[6000..].to_vec())).unwrap();
            
            assert_eq!(overflow1, 3);
            assert_eq!(overflow2, 4);
        }
        
        // Verify the large cell
        {
            let pager_guard = pager.lock().unwrap();
            
            pager_guard.get_page(2, Some(PageType::TableLeaf), |page| {
                match page {
                    Page::BTree(btree_page) => {
                        assert_eq!(btree_page.header.cell_count, 1);
                        
                        match &btree_page.cells[0] {
                            BTreeCell::TableLeaf(cell) => {
                                assert_eq!(cell.row_id, 1);
                                assert_eq!(cell.payload_size, 8000);
                                assert_eq!(cell.payload.len(), 3000);
                                assert_eq!(cell.overflow_page, Some(3));
                            },
                            _ => panic!("Expected TableLeaf cell"),
                        }
                    },
                    _ => panic!("Expected BTree page"),
                }
            }).unwrap();
            
            // Verify overflow pages
            pager_guard.get_page(3, Some(PageType::Overflow), |page| {
                match page {
                    Page::Overflow(overflow_page) => {
                        assert_eq!(overflow_page.next_page, 4);
                        assert_eq!(overflow_page.data.len(), 3000);
                    },
                    _ => panic!("Expected Overflow page"),
                }
            }).unwrap();
            
            pager_guard.get_page(4, Some(PageType::Overflow), |page| {
                match page {
                    Page::Overflow(overflow_page) => {
                        assert_eq!(overflow_page.next_page, 0);
                        assert_eq!(overflow_page.data.len(), 2000);
                    },
                    _ => panic!("Expected Overflow page"),
                }
            }).unwrap();
        }
        
        // Test flush
        {
            let mut pager_guard = pager.lock().unwrap();
            assert!(pager_guard.flush().is_ok());
        }
    }

    #[test]
    fn test_buffer_pool_page_eviction() {
        // Create a pager with a small buffer pool (only 2 pages)
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let pager = Pager::create(db_path, 4096, Some(2), 0).unwrap();
        let pager = Arc::new(Mutex::new(pager));
        
        // Create 3 pages
        {
            let mut pager_guard = pager.lock().unwrap();
            pager_guard.create_pages(PageType::TableLeaf, None, None, None, 3).unwrap();
            // Create 3 pages

        }
        println!("Created 3 pages");
        
        
        // Access pages in sequence to test LRU behavior
        {
            let pager_guard = pager.lock().unwrap();
            
            // Access page 2 (should add to cache)
            pager_guard.get_page(2, Some(PageType::TableLeaf), |_| {}).unwrap();
            println!("Accessed page 2");
            // Access page 3 (should add to cache)
            pager_guard.get_page(3, Some(PageType::TableLeaf), |_| {}).unwrap();
            pager_guard.unpin_page(2).unwrap();
            // Access page 4 (should evict page 2)
            pager_guard.get_page(4, Some(PageType::TableLeaf), |_| {}).unwrap();
            pager_guard.unpin_page(4).unwrap();
            // Access page 2 again (should be re-loaded, evicting page 4)
            pager_guard.get_page(2, Some(PageType::TableLeaf), |_| {}).unwrap();
            
            // Access page 3 again (should still be in cache)
            pager_guard.get_page(3, Some(PageType::TableLeaf), |_| {}).unwrap();
            
            // The above test confirms the buffer pool evicts pages based on LRU policy
        }
    }

    #[test]
    fn test_unpin_page() {
        let pager = create_test_pager();
        
        // Create a page
        {
            let mut pager_guard = pager.lock().unwrap();
            pager_guard.create_page(PageType::TableLeaf, None, None, None).unwrap();
        }
        
        // Access the page (which pins it)
        {
            let pager_guard = pager.lock().unwrap();
            pager_guard.get_page(2, Some(PageType::TableLeaf), |_| {}).unwrap();
            
            // Unpin the page
            let result = pager_guard.unpin_page(2);
            assert!(result.is_ok());
            
            
            // Unpin a non-existent page
            let result = pager_guard.unpin_page(999);
            assert!(result.is_ok());
           
        }
    }

    #[test]
    fn test_header_operations() {
        let pager = create_test_pager();
        
        // Get and modify header
        {
            let mut pager_guard = pager.lock().unwrap();
            
            // Get header
            let mut header = pager_guard.get_header().unwrap();
            
            // Modify header
            header.user_version = 42;
            
            // Update header
            pager_guard.update_header(&header).unwrap();
            
            // Get header again to verify
            let header2 = pager_guard.get_header().unwrap();
            assert_eq!(header2.user_version, 42);
        }
    }
}
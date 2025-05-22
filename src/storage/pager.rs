//! # Improved Pager Implementation with RAII Guards
//!
//! This implementation provides automatic page unpinning through RAII guards,
//! ensuring pages are properly unpinned when they go out of scope.

use std::ops::{Deref, DerefMut};
use std::io;
use std::path::Path;
use std::rc::Rc;
use std::cell::RefCell;

use super::cache::BufferPool;
use super::disk::DiskManager;
use crate::header::Header;
use crate::page::{Page, PageType, BTreePage, OverflowPage, FreePage, ByteSerializable};

/// RAII guard for immutable page access
/// Automatically unpins the page when dropped using BufferPool
pub struct PageGuard {
    page_number: u32,
    page: *const Page, // Raw pointer to avoid borrow checker issues
    buffer_pool: Rc<RefCell<BufferPool>>,
}

impl PageGuard {
    /// Creates a new PageGuard
    fn new(page_number: u32, page: &Page, buffer_pool: Rc<RefCell<BufferPool>>) -> Self {
        PageGuard {
            page_number,
            page: page as *const Page,
            buffer_pool,
        }
    }
}

impl Deref for PageGuard {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // Safe because the BufferPool guarantees the page stays alive while pinned
        unsafe { &*self.page }
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        // Automatically unpin the page when the guard is dropped
        if let Ok(mut pool) = self.buffer_pool.try_borrow_mut() {
            let _ = pool.unpin_page(self.page_number);
        }
    }
}

/// RAII guard for mutable page access
/// Automatically unpins the page when dropped and marks it as dirty
pub struct PageGuardMut {
    page_number: u32,
    page: *mut Page, // Raw pointer to avoid borrow checker issues
    buffer_pool: Rc<RefCell<BufferPool>>,
}

impl PageGuardMut {
    /// Creates a new PageGuardMut
    fn new(page_number: u32, page: &mut Page, buffer_pool: Rc<RefCell<BufferPool>>) -> Self {
        PageGuardMut {
            page_number,
            page: page as *mut Page,
            buffer_pool,
        }
    }
}

impl Deref for PageGuardMut {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // Safe because the BufferPool guarantees the page stays alive while pinned
        unsafe { &*self.page }
    }
}

impl DerefMut for PageGuardMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Safe because the BufferPool guarantees the page stays alive while pinned
        unsafe { &mut *self.page }
    }
}

impl Drop for PageGuardMut {
    fn drop(&mut self) {
        // Automatically unpin the page when the guard is dropped
        if let Ok(mut pool) = self.buffer_pool.try_borrow_mut() {
            let _ = pool.unpin_page(self.page_number);
        }
    }
}

/// Enhanced Pager with RAII guards for automatic page management
pub struct Pager {
    disk_manager: DiskManager,
    page_cache: Rc<RefCell<BufferPool>>, // Shared reference to allow guards to access it
    page_size: u32,
    journal_pages: Vec<(u32, Page)>,
    reserved_space: u8,
    dirty: bool,
}

impl Drop for Pager {
    fn drop(&mut self) {
        if self.dirty {
            if let Err(e) = self.rollback_transaction() {
                eprintln!("Error rolling back transaction: {}", e);
            }
        }
    }
}

impl Pager {
    /// Opens an existing database file with RAII page management
    pub fn open<P: AsRef<Path>>(path: P, buffer_pool_size: Option<usize>) -> io::Result<Self> {
        let mut disk_manager = DiskManager::open(path)?;
        let header = disk_manager.read_header()?;

        Ok(Pager {
            disk_manager,
            page_cache: Rc::new(RefCell::new(BufferPool::new(buffer_pool_size.unwrap_or(1000)))),
            journal_pages: vec![],
            page_size: header.page_size,
            reserved_space: header.reserved_space,
            dirty: false,
        })
    }

    /// Creates a new database file with RAII page management
    pub fn create<P: AsRef<Path>>(
        path: P,
        page_size: u32,
        buffer_pool_size: Option<usize>,
        reserved_space: u8,
    ) -> io::Result<Self> {
        let mut disk_manager = DiskManager::create(path, page_size)?;

        let mut header = disk_manager.read_header()?;
        header.reserved_space = reserved_space;
        disk_manager.write_header(&header)?;

        Ok(Pager {
            disk_manager,
            page_cache: Rc::new(RefCell::new(BufferPool::new(buffer_pool_size.unwrap_or(1000)))),
            page_size,
            journal_pages: vec![],
            reserved_space,
            dirty: false,
        })
    }

    /// Gets a page with RAII guard for automatic unpinning
    /// This is the recommended way to access pages
    pub fn get_page_guard(&mut self, page_number: u32, page_type: Option<PageType>) -> io::Result<PageGuard> {
        // Load page if not in cache
        let is_on_cache = {
            let mut page_cache = self.page_cache.borrow_mut();
            !page_cache.contains_page(page_number)
        };
        if !is_on_cache {
            self.load_page(page_number)?;
        }
     

        // Validate page type if specified
        if let Some(expected_type) = page_type {
            let page_cache = self.page_cache.borrow();
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

        // Get page from cache and create guard
        let mut page_cache = self.page_cache.borrow_mut();
        let page = page_cache.get_page(page_number).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            )
        })?;

        // Create the guard with a shared reference to the buffer pool
        Ok(PageGuard::new(page_number, page, Rc::clone(&self.page_cache)))
    }

    /// Gets a mutable page with RAII guard for automatic unpinning
    pub fn get_page_guard_mut(&mut self, page_number: u32, page_type: Option<PageType>) -> io::Result<PageGuardMut> {
        // Load page if not in cache
        let is_on_cache = {
            let mut page_cache = self.page_cache.borrow_mut();
            !page_cache.contains_page(page_number)
        };
        if !is_on_cache {
            self.load_page(page_number)?;
        }

        // Validate page type if specified
        if let Some(expected_type) = page_type {
            let page_cache = self.page_cache.borrow();
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

        // Add to journal for transaction support
        {
            let mut page_cache = self.page_cache.borrow_mut();
            if let Some(page) = page_cache.get_page(page_number) {
                self.journal_pages.push((page_number, page.clone()));
            }
        }

        // Get mutable page and create guard
        let mut page_cache = self.page_cache.borrow_mut();
        let page = page_cache.get_page_mut(page_number).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            )
        })?;

        self.dirty = true;

        // Create the guard with a shared reference to the buffer pool
        Ok(PageGuardMut::new(page_number, page, Rc::clone(&self.page_cache)))
    }

    /// Legacy method - use get_page_guard instead
    /// 
    /// # Deprecated
    /// This method requires manual unpinning. Use `get_page_guard` instead for automatic memory management.
    pub fn get_page(&mut self, page_number: u32, page_type: Option<PageType>) -> io::Result<&Page> {
        // Load page if not in cache
        let is_on_cache = {
            let mut page_cache = self.page_cache.borrow_mut();
            !page_cache.contains_page(page_number)
        };
        if !is_on_cache {
            self.load_page(page_number)?;
        }
        
        // Validate page type if specified
        if let Some(expected_type) = page_type {
            let page_cache = self.page_cache.borrow();
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
        
        // This is problematic because we return a reference that outlives the RefCell borrow
        // Users should use get_page_guard instead
        let mut page_cache = self.page_cache.borrow_mut();
        let page = page_cache.get_page(page_number).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            )
        })?;
        
        // SAFETY: This is unsafe because we're returning a reference that outlives the borrow
        // The caller must ensure they unpin the page manually
        unsafe { Ok(&*(page as *const Page)) }
    }

    /// Legacy method - use get_page_guard_mut instead
    /// 
    /// # Deprecated
    /// This method requires manual unpinning. Use `get_page_guard_mut` instead for automatic memory management.
    pub fn get_page_mut(&mut self, page_number: u32, page_type: Option<PageType>) -> io::Result<&mut Page> {
        // Load page if not in cache
        let is_on_cache = {
            let mut page_cache = self.page_cache.borrow_mut();
            !page_cache.contains_page(page_number)
        };
        if !is_on_cache {
            self.load_page(page_number)?;
        }
        
        // Validate page type if specified
        if let Some(expected_type) = page_type {
            let page_cache = self.page_cache.borrow();
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
        
        // Add to journal for transaction support
        {
            let mut page_cache = self.page_cache.borrow_mut();
            if let Some(page) = page_cache.get_page(page_number) {
                self.journal_pages.push((page_number, page.clone()));
            }
        }
        
        // This is problematic because we return a reference that outlives the RefCell borrow
        // Users should use get_page_guard_mut instead
        let mut page_cache = self.page_cache.borrow_mut();
        let page = page_cache.get_page_mut(page_number).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            )
        })?;

        self.dirty = true;
        
        // SAFETY: This is unsafe because we're returning a reference that outlives the borrow
        // The caller must ensure they unpin the page manually
        unsafe { Ok(&mut *(page as *mut Page)) }
    }

    /// Manually unpin a page (for backward compatibility with legacy methods)
    pub fn unpin_page(&mut self, page_number: u32) -> io::Result<()> {
        let mut page_cache = self.page_cache.borrow_mut();
        if !page_cache.unpin_page(page_number) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            ));
        }
        Ok(())
    }

    /// Gets the header of the database
    pub fn get_header(&mut self) -> io::Result<Header> {
        self.disk_manager.read_header()
    }

    /// Updates the header of the database
    pub fn update_header(&mut self, header: &Header) -> io::Result<()> {
        self.disk_manager.write_header(header)?;
        self.dirty = true;
        Ok(())
    }

    /// Begins a transaction
    pub fn begin_transaction(&mut self) -> io::Result<()> {
        self.journal_pages.clear();
        self.dirty = true;
        Ok(())
    }

    /// Commits the current transaction
    pub fn commit_transaction(&mut self) -> io::Result<()> {
        if self.flush().is_ok() {
            self.clear_journal();
            Ok(())
        } else {
            self.rollback_transaction()?;
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Transaction failed",
            ))
        }
    }

    /// Rolls back the current transaction
    pub fn rollback_transaction(&mut self) -> io::Result<()> {
        for (page_number, page) in self.journal_pages.iter() {
            let buffer = self.serialize_page(page)?;
            self.disk_manager.write_page(*page_number, &buffer)?;
        }
        
        let mut page_cache = self.page_cache.borrow_mut();
        page_cache.mark_clean_all();
        self.dirty = false;
        self.journal_pages.clear();
        Ok(())
    }

    /// Clears the journal
    fn clear_journal(&mut self) {
        if self.dirty {
            panic!("Cannot clear journal pages, we are in the middle of a transaction");
        }
        self.journal_pages.clear();
    }

    /// Creates a new B-Tree page
    pub fn create_btree_page(
        &mut self,
        page_type: PageType,
        right_most_page: Option<u32>,
    ) -> io::Result<u32> {
        let page_number = self.disk_manager.allocate_pages(1)?;
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

    /// Creates a new overflow page
    pub fn create_overflow_page(&mut self, next_page: u32, data: Vec<u8>) -> io::Result<u32> {
        let max_data_size = self.page_size as usize - 4;
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

        let page_number = self.disk_manager.allocate_pages(1)?;
        let overflow_page = OverflowPage::new(next_page, data, self.page_size, page_number)?;
        let buffer = self.serialize_page(&Page::Overflow(overflow_page))?;
        self.disk_manager.write_page(page_number, &buffer)?;
        Ok(page_number)
    }

    /// Creates a new free page
    pub fn create_free_page(&mut self, next_page: u32) -> io::Result<u32> {
        let page_number = self.disk_manager.allocate_pages(1)?;
        let free_page = FreePage::new(next_page, self.page_size, page_number);
        let buffer = self.serialize_page(&Page::Free(free_page))?;
        self.disk_manager.write_page(page_number, &buffer)?;
        Ok(page_number)
    }

    /// Flushes dirty pages to disk
    pub fn flush(&mut self) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }

        let dirty_pages = {
            let page_cache = self.page_cache.borrow();
            page_cache.get_dirty_pages().into_iter().map(|(n, p)| (n, p.clone())).collect::<Vec<_>>()
        };

        if dirty_pages.is_empty() {
            return Ok(());
        }

        for (page_number, page) in dirty_pages {
            let buffer = self.serialize_page(&page)?;
            self.disk_manager.write_page(page_number, &buffer)?;
        }

        self.disk_manager.sync()?;
        let mut page_cache = self.page_cache.borrow_mut();
        page_cache.mark_clean_all();
        self.dirty = false;
        Ok(())
    }

    /// Gets the number of pages in the database
    pub fn page_count(&self) -> io::Result<u32> {
        self.disk_manager.page_count()
    }

    // Private helper methods...

    fn load_page(&mut self, page_number: u32) -> io::Result<()> {
        let page_count = self.disk_manager.page_count()?;
        if page_number > page_count + 1 {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Page number out of range: {}, maximum {}",
                    page_number,
                    page_count + 1
                ),
            ));
        }

        let mut buffer = vec![0u8; self.page_size as usize];
        self.disk_manager.read_page(page_number, &mut buffer)?;
        
        let page = if page_number == 1 || self.is_btree_page(&buffer)? {
            self.parse_btree_page(page_number, &buffer)?
        } else if self.is_overflow_page(&buffer)? {
            self.parse_overflow_page(page_number, &buffer)?
        } else {
            self.parse_free_page(page_number, &buffer)?
        };

        if let Some((evicted_page_number, evicted_page)) = self.add_page_to_cache(page_number, page)? {
            let buffer = self.serialize_page(&evicted_page)?;
            self.disk_manager.write_page(evicted_page_number, &buffer)?;
        }
        
        Ok(())
    }

    fn add_page_to_cache(&mut self, page_number: u32, page: Page) -> io::Result<Option<(u32, Page)>> {
        let mut page_cache = self.page_cache.borrow_mut();
        match page_cache.add_page(page_number, page, true) {
            Some((evicted_page_number, evicted_page)) => {
                if evicted_page_number == page_number {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Page {} was rejected by the cache (OOM Error)", page_number),
                    ))
                } else {
                    Ok(Some((evicted_page_number, evicted_page)))
                }
            }
            _ => Ok(None),
        }
    }

    fn is_btree_page(&self, buffer: &[u8]) -> io::Result<bool> {
        if buffer.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small to determine page type",
            ));
        }

        match buffer[0] {
            0x02 | 0x05 | 0x0A | 0x0D => Ok(true),
            _ => Ok(false),
        }
    }

    fn is_overflow_page(&self, buffer: &[u8]) -> io::Result<bool> {
        if buffer.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small to determine page type",
            ));
        }

        match buffer[0] {
            0x10 => Ok(true),
            _ => Ok(false),
        }
    }

    fn parse_btree_page(&self, page_number: u32, buffer: &[u8]) -> io::Result<Page> {
        if buffer.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small for a B-Tree page",
            ));
        }
        let mut cursor = std::io::Cursor::new(buffer);
        let mut page = BTreePage::read_from(&mut cursor)?;
        page.page_number = page_number;
        page.page_size = self.page_size;
        page.reserved_space = self.reserved_space;
        Ok(Page::BTree(page))
    }

    fn parse_overflow_page(&self, page_number: u32, buffer: &[u8]) -> io::Result<Page> {
        if buffer.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small for an overflow page",
            ));
        }

        let mut cursor = std::io::Cursor::new(buffer);
        let mut page = OverflowPage::read_from(&mut cursor)?;
        page.page_number = page_number;
        page.page_size = self.page_size;
        Ok(Page::Overflow(page))
    }

    fn parse_free_page(&self, page_number: u32, buffer: &[u8]) -> io::Result<Page> {
        if buffer.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer too small for a free page",
            ));
        }

        let mut cursor = std::io::Cursor::new(buffer);
        let mut page = FreePage::read_from(&mut cursor)?;
        page.page_number = page_number;
        page.page_size = self.page_size;
        Ok(Page::Free(page))
    }

    fn serialize_page(&self, page: &Page) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0u8; self.page_size as usize];
        let mut cursor = std::io::Cursor::new(&mut buffer[..]);
        page.write_to(&mut cursor)?;
        Ok(buffer)
    }
}



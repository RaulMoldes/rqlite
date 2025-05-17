// src/storage/buffer_pool.rs

use std::collections::{HashMap, VecDeque};
use crate::page::Page;

/// Represents a frame in the buffer pool that holds a page.
/// The frame contains metadata about the page, such as pin count and dirty flag.
pub struct BufferFrame {
    /// Page data stored in this frame
    page: Page,
    /// Number of clients using this page
    pin_count: u32,
    /// True if the page has been modified and needs to be written back to disk
    is_dirty: bool,
}

impl BufferFrame {
    /// Creates a new buffer frame with the given page
    pub fn new(page: Page) -> Self {
        BufferFrame {
            page,
            pin_count: 0,
            is_dirty: false,
        }
    }

    /// Increments the pin count for this frame
    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    /// Decrements the pin count for this frame
    /// Returns true if the pin count reached zero
    pub fn unpin(&mut self) -> bool {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
        self.pin_count == 0
    }

    /// Marks the page as dirty
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }

    /// Checks if the page is currently pinned
    pub fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    /// Checks if the page is dirty
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Resets the dirty flag
    pub fn reset_dirty(&mut self) {
        self.is_dirty = false;
    }

    /// Gets a reference to the page
    pub fn page(&self) -> &Page {
        &self.page
    }

    /// Gets a mutable reference to the page
    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.page
    }
}

/// A buffer pool that caches pages in memory using a LRU (Least Recently Used) eviction policy.
/// The buffer pool is responsible for managing the loading and caching of database pages.
pub struct BufferPool {
    /// Maximum number of pages that can be held in memory
    max_pages: usize,
    /// Frames holding the actual page data
    frames: HashMap<u32, BufferFrame>,
    /// LRU queue for page replacement (contains page_numbers)
    lru_list: VecDeque<u32>,
}

impl BufferPool {
    /// Creates a new buffer pool with the specified maximum number of pages
    ///
    /// # Parameters
    /// * `max_pages` - Maximum number of pages that can be held in memory
    ///
    /// # Returns
    /// A new buffer pool instance
    pub fn new(max_pages: usize) -> Self {
        BufferPool {
            max_pages,
            frames: HashMap::with_capacity(max_pages),
            lru_list: VecDeque::with_capacity(max_pages),
        }
    }

    /// Checks if a page exists in the buffer pool
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to check
    ///
    /// # Returns
    /// true if the page is in the buffer pool, false otherwise
    pub fn contains_page(&self, page_number: u32) -> bool {
        self.frames.contains_key(&page_number)
    }

    /// Gets a reference to a page from the buffer pool
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to get
    ///
    /// # Returns
    /// Some reference to the page if it exists in the buffer pool, None otherwise
    pub fn get_page(&mut self, page_number: u32) -> Option<&Page> {
        if self.frames.contains_key(&page_number) {
            // Update the page's position in the LRU list
            self.touch_page(page_number);
            
            // Pin the page
            if let Some(frame) = self.frames.get_mut(&page_number) {
                frame.pin();
                return Some(frame.page());
            }
        }
        
        None
    }

    /// Gets a mutable reference to a page from the buffer pool
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to get
    ///
    /// # Returns
    /// Some mutable reference to the page if it exists in the buffer pool, None otherwise
    pub fn get_page_mut(&mut self, page_number: u32) -> Option<&mut Page> {
        if self.frames.contains_key(&page_number) {
            // Update the page's position in the LRU list
            self.touch_page(page_number);
            
            // Pin the page and mark it as dirty
            if let Some(frame) = self.frames.get_mut(&page_number) {
                frame.pin();
                frame.mark_dirty();
                return Some(frame.page_mut());
            }
        }
        
        None
    }

    /// Adds a page to the buffer pool
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to add
    /// * `page` - Page to add
    /// * `pin` - Whether to pin the page
    ///
    /// # Returns
    /// Option containing page number and page that was evicted, if any
    pub fn add_page(&mut self, page_number: u32, page: Page, pin: bool) -> Option<(u32, Page)> {
        // Check if we need to evict a page
     
        let evicted = if self.frames.len() >= self.max_pages && !self.frames.contains_key(&page_number) {
            // ATTEMPT TO EVICT A PAGE
            self.evict_page()
        } else {
            None
        };
       

        if evicted.is_none() && self.frames.len() >= self.max_pages && !self.frames.contains_key(&page_number) {
            // If we couldn't evict a page, return the requested page (rejected)
            return Some((page_number, page));
        }
        
        
        // Create a new frame for this page
        let mut frame = BufferFrame::new(page);
        
        // Pin the page if requested
        if pin {
            frame.pin();
        } else {
            // If not pinned, add to LRU list
            self.lru_list.push_back(page_number);
        }
        
        // Add the frame to the buffer pool
        self.frames.insert(page_number, frame);
        
        evicted
    }

    /// Unpins a page, allowing it to be evicted from the buffer pool
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to unpin
    ///
    /// # Returns
    /// true if the page was successfully unpinned, false if the page was not in the buffer pool
    pub fn unpin_page(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            let is_unpinned = frame.unpin();
            if is_unpinned {
                // If the pin count reached zero, ensure it's in the LRU list
                if !self.lru_list.contains(&page_number) {
                    self.lru_list.push_back(page_number);
                }
            }
            return true;
        }
        
        false
    }

    /// Marks a page as dirty
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to mark as dirty
    ///
    /// # Returns
    /// true if the page was successfully marked as dirty, false if the page was not in the buffer pool
    pub fn mark_dirty(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.mark_dirty();
            return true;
        }
        
        false
    }

    /// Marks a page as clean (not dirty)
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to mark as clean
    ///
    /// # Returns
    /// true if the page was successfully marked as clean, false if the page was not in the buffer pool
    pub fn mark_clean(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.reset_dirty();
            return true;
        }
        
        false
    }

    /// Gets a list of all dirty pages in the buffer pool
    ///
    /// # Returns
    /// Vector of page numbers for dirty pages
    pub fn get_dirty_pages(&self) -> Vec<u32> {
        self.frames.iter()
            .filter(|(_, frame)| frame.is_dirty())
            .map(|(page_number, _)| *page_number)
            .collect()
    }

     /// Gets a list of all dirty pages in the buffer pool
    ///
    /// # Returns
    /// Vector of page refrences for dirty pages
    pub fn get_dirty_pages_referenced(&self) -> Vec<(u32, &Page)> {
        self.frames.iter()
            .filter(|(_, frame)| frame.is_dirty())
            .map(|(page_number, frame)| (*page_number, &frame.page))
            .collect()
        
    }

    /// Removes a page from the buffer pool
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to remove
    ///
    /// # Returns
    /// The removed page if it was in the buffer pool and not pinned, None otherwise
    pub fn remove_page(&mut self, page_number: u32) -> Option<Page> {
        // Check if the page is in the buffer pool
        if let Some(frame) = self.frames.get(&page_number) {
            // Cannot remove a pinned page
            if frame.is_pinned() {
                return None;
            }
        } else {
            return None;
        }
        
        // Remove the page from the LRU list
        self.lru_list.retain(|&p| p != page_number);
        
        // Remove and return the page
        self.frames.remove(&page_number).map(|frame| frame.page)
    }

    /// Evicts a page from the buffer pool using LRU policy
    ///
    /// # Returns
    /// The evicted page number and page if successful, None if no page could be evicted
    fn evict_page(&mut self) -> Option<(u32, Page)> {
        // Try to find an unpinned page in the LRU list
        while let Some(page_number) = self.lru_list.pop_front() {
          
            // Check if the page is still in the buffer pool
            if let Some(frame) = self.frames.get(&page_number) {
                // Cannot evict a pinned page
                if frame.is_pinned() {
                    // Push the page to the back of the LRU list
                    self.lru_list.push_back(page_number);
            
                    continue;
                }
                
                // Remove the page from the buffer pool
                if let Some(frame) = self.frames.remove(&page_number) {
                  
                    return Some((page_number, frame.page));
                }
            }
        }
        
        // If we get here, all pages are pinned
        None
    }

    /// Updates the position of a page in the LRU list
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to update
    fn touch_page(&mut self, page_number: u32) {
        // Remove the page from the LRU list
        self.lru_list.retain(|&p| p != page_number);
        
        // Add the page to the back of the LRU list if it's not pinned
        if let Some(frame) = self.frames.get(&page_number) {
            if !frame.is_pinned() {
                self.lru_list.push_back(page_number);
            }
        }
    }

    /// Gets the maximum number of pages this buffer pool can hold
    pub fn max_pages(&self) -> usize {
        self.max_pages
    }

    /// Gets the current number of pages in the buffer pool
    pub fn page_count(&self) -> usize {
        self.frames.len()
    }

    /// Checks if a page is dirty
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to check
    ///
    /// # Returns
    /// true if the page is dirty, false if the page is not dirty or not in the buffer pool
    pub fn is_dirty(&self, page_number: u32) -> bool {
        self.frames.get(&page_number)
            .map_or(false, |frame| frame.is_dirty())
    }

    /// Checks if a page is pinned
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to check
    ///
    /// # Returns
    /// true if the page is pinned, false if the page is not pinned or not in the buffer pool
    pub fn is_pinned(&self, page_number: u32) -> bool {
        self.frames.get(&page_number)
            .map_or(false, |frame| frame.is_pinned())
    }

    /// Gets the pin count of a page
    ///
    /// # Parameters
    /// * `page_number` - Number of the page to check
    ///
    /// # Returns
    /// The pin count of the page, or 0 if the page is not in the buffer pool
    pub fn pin_count(&self, page_number: u32) -> u32 {
        self.frames.get(&page_number)
            .map_or(0, |frame| frame.pin_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{PageType, BTreePage, BTreePageHeader, Page};

    // Helper function to create a test page
    fn create_test_page(page_number: u32) -> Page {
        let header = BTreePageHeader::new_leaf(PageType::TableLeaf);
        let btree_page = BTreePage {
            header,
            cell_indices: Vec::new(),
            cells: Vec::new(),
            page_size: 4096,
            page_number,
            reserved_space: 0,
        };
        Page::BTree(btree_page)
    }

    #[test]
    fn test_buffer_pool_new() {
        let pool = BufferPool::new(10);
        assert_eq!(pool.max_pages(), 10);
        assert_eq!(pool.page_count(), 0);
    }

    #[test]
    fn test_add_page() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);

        // Add first page
        let evicted = pool.add_page(1, page1, false);
        assert!(evicted.is_none());
        assert_eq!(pool.page_count(), 1);
        assert!(pool.contains_page(1));

        // Add second page
        let evicted = pool.add_page(2, page2, true);
        assert!(evicted.is_none());
        assert_eq!(pool.page_count(), 2);
        assert!(pool.contains_page(2));
        
        // Page 2 should be pinned
        assert!(pool.is_pinned(2));
        assert!(!pool.is_pinned(1));
    }

    #[test]
    fn test_get_page() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        // Add a page
        pool.add_page(1, page1, false);
        
        // Get the page
        let page = pool.get_page(1);
        assert!(page.is_some());
        
        // Page should be pinned after get
        assert!(pool.is_pinned(1));
        
        // Get a non-existent page
        let page = pool.get_page(2);
        assert!(page.is_none());
    }

    #[test]
    fn test_get_page_mut() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        // Add a page
        pool.add_page(1, page1, false);
        
        // Get the page mutably
        let page = pool.get_page_mut(1);
        assert!(page.is_some());
        
        // Page should be pinned and dirty after get_mut
        assert!(pool.is_pinned(1));
        assert!(pool.is_dirty(1));
        
        // Get a non-existent page
        let page = pool.get_page_mut(2);
        assert!(page.is_none());
    }

    #[test]
    fn test_unpin_page() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        // Add a page and pin it
        pool.add_page(1, page1, true);
        assert!(pool.is_pinned(1));
        
        // Unpin the page
        let result = pool.unpin_page(1);
        assert!(result);
        assert!(!pool.is_pinned(1));
        
        // Unpin a non-existent page
        let result = pool.unpin_page(2);
        assert!(!result);
    }

    #[test]
    fn test_dirty_flags() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        // Add a page
        pool.add_page(1, page1, false);
        assert!(!pool.is_dirty(1));
        
        // Mark the page as dirty
        let result = pool.mark_dirty(1);
        assert!(result);
        assert!(pool.is_dirty(1));
        
        // Mark the page as clean
        let result = pool.mark_clean(1);
        assert!(result);
        assert!(!pool.is_dirty(1));
        
        // Mark a non-existent page as dirty
        let result = pool.mark_dirty(2);
        assert!(!result);
    }

    #[test]
    fn test_get_dirty_pages() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        let page3 = create_test_page(3);
        
        // Add pages
        pool.add_page(1, page1, false);
        pool.add_page(2, page2, false);
        pool.add_page(3, page3, false);
        
        // Mark some pages as dirty
        pool.mark_dirty(1);
        pool.mark_dirty(3);
        
        // Get dirty pages
        let dirty_pages = pool.get_dirty_pages();
        assert_eq!(dirty_pages.len(), 2);
        assert!(dirty_pages.contains(&1));
        assert!(dirty_pages.contains(&3));
    }

    #[test]
    fn test_remove_page() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        // Add a page
        pool.add_page(1, page1, false);
        
        // Remove the page
        let removed = pool.remove_page(1);
        assert!(removed.is_some());
        assert!(!pool.contains_page(1));
        
        // Remove a non-existent page
        let removed = pool.remove_page(1);
        assert!(removed.is_none());
        
        // Add a page and pin it
        let page2 = create_test_page(2);
        pool.add_page(2, page2, true);
        
        // Try to remove a pinned page
        let removed = pool.remove_page(2);
        assert!(removed.is_none());
        assert!(pool.contains_page(2));
    }

    #[test]
    fn test_eviction() {
        let mut pool = BufferPool::new(2);
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        let page3 = create_test_page(3);
        
        // Add pages
        pool.add_page(1, page1, false);
        pool.add_page(2, page2, false);
        
        // Touch page 1 to make it more recently used
        pool.get_page(1);
        pool.unpin_page(1);
        
        // Add another page, which should evict page 2 (least recently used)
        let evicted = pool.add_page(3, page3, false);
        assert!(evicted.is_some());
        let (evicted_page_number, _) = evicted.unwrap();
        assert_eq!(evicted_page_number, 2);
        
        // Verify pages 1 and 3 are in the pool, but not 2
        assert!(pool.contains_page(1));
        assert!(!pool.contains_page(2));
        assert!(pool.contains_page(3));
    }

    #[test]
    fn test_eviction_with_pinned_pages() {
        let mut pool = BufferPool::new(2);
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        let page3 = create_test_page(3);
        
        // Add pages and pin them
        let evicted = pool.add_page(1, page1, true);
        assert!(evicted.is_none());
        let evicted = pool.add_page(2, page2, true);
        assert!(evicted.is_none());
        
        // Try to add another page, which should fail to evict any page because all are pinned
        let evicted = pool.add_page(3, page3.clone(), true);
        assert!(evicted.is_some()); // We should have rejected the page
        let (evicted_page_number, _) = evicted.unwrap();
        assert_eq!(evicted_page_number, 3); // The page we tried to add was rejected
        assert_eq!(pool.page_count(), 2); // Pool should still have 2 pages
        
        // Pool should still have pages 1 and 2
        assert!(pool.contains_page(1));
        assert!(pool.contains_page(2));
      
        
        // Unpin page 1
        pool.unpin_page(1);

        
        // Try to add page 3 again, which should evict page 1
        let evicted = pool.add_page(3, page3, true);
        assert!(evicted.is_some());
        let (evicted_page_number, _) = evicted.unwrap();
        assert_eq!(evicted_page_number, 1);
        
        // Pool should now have pages 2 and 3
        assert!(!pool.contains_page(1));
        assert!(pool.contains_page(2));
        assert!(pool.contains_page(3));
    }
}
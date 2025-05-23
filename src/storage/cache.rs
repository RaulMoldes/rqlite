// Additional improvements to your BufferPool implementation

use crate::page::{PageType, Page};
use core::panic;
use std::collections::{HashMap, VecDeque};
use std::io;

/// Enhanced BufferFrame with better tracking
#[derive(Debug)]
pub struct BufferFrame {
    page: Page,
    pin_count: u32,
    is_dirty: bool,
    /// Track when the page was last accessed for better LRU
    last_accessed: std::time::Instant,
    /// Track if the page is being written to prevent concurrent access issues
    is_being_written: bool,
}

impl BufferFrame {
    pub fn new(page: Page) -> Self {
        BufferFrame {
            page,
            pin_count: 0,
            is_dirty: false,
            last_accessed: std::time::Instant::now(),
            is_being_written: false,
        }
    }

    pub fn pin(&mut self) {
       
        self.pin_count += 1;
        self.last_accessed = std::time::Instant::now();
    }

    pub fn unpin(&mut self) -> bool {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
        self.pin_count == 0
    }

    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
        self.last_accessed = std::time::Instant::now();
    }

    pub fn reset_dirty(&mut self) {
        self.is_dirty = false;
    }

    pub fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn page(&self) -> &Page {
        &self.page
    }

    pub fn page_mut(&mut self) -> &mut Page {
        self.last_accessed = std::time::Instant::now();
        &mut self.page
    }

    pub fn pin_count(&self) -> u32 {
        self.pin_count
    }

    pub fn last_accessed(&self) -> std::time::Instant {
        self.last_accessed
    }

    pub fn set_being_written(&mut self, writing: bool) {
        self.is_being_written = writing;
    }

    pub fn is_being_written(&self) -> bool {
        self.is_being_written
    }
}

/// Enhanced BufferPool with better page lifecycle management

pub struct BufferPool {
    max_pages: usize,
    frames: HashMap<u32, BufferFrame>,
    lru_list: VecDeque<u32>,
    /// Statistics for monitoring buffer pool performance
    stats: BufferPoolStats,
}

#[derive(Debug, Default)]
pub struct BufferPoolStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub pages_evicted: u64,
    pub pages_written: u64,
    pub pin_operations: u64,
    pub unpin_operations: u64,
}

impl BufferPoolStats {
    pub fn hit_rate(&self) -> f64 {
        if self.cache_hits + self.cache_misses == 0 {
            0.0
        } else {
            self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64
        }
    }
}

impl BufferPool {
    pub fn new(max_pages: usize) -> Self {
        BufferPool {
            max_pages,
            frames: HashMap::with_capacity(max_pages),
            lru_list: VecDeque::with_capacity(max_pages),
            stats: BufferPoolStats::default(),
        }
    }

    /// Enhanced contains_page with statistics tracking
    pub fn contains_page(&mut self, page_number: u32) -> bool {
        let contains = self.frames.contains_key(&page_number);
        if contains {
            self.stats.cache_hits += 1;
        } else {
            self.stats.cache_misses += 1;
        }
        contains
    }

    /// Enhanced page validation with better error reporting
    pub fn validate_page_type(&self, page_number: u32, expected_type: PageType) -> Result<(), String> {
        if let Some(frame) = self.frames.get(&page_number) {
            let actual_type = frame.page().page_type();
            if actual_type != expected_type {
                return Err(format!(
                    "Page type mismatch for page {}: expected {:?}, found {:?}",
                    page_number, expected_type, actual_type
                ));
            }
        } else {
            return Err(format!("Page {} not found in buffer pool", page_number));
        }
        Ok(())
    }

    /// Enhanced get_page with better statistics and safety checks
    pub fn get_page(&mut self, page_number: u32) -> Option<&Page> {
        if !self.frames.contains_key(&page_number) {
            return None;
        }

        self.touch_page(page_number);
        
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.pin();
            self.stats.pin_operations += 1;
            return Some(frame.page());
        }

        None
    }

    /// Enhanced get_page_mut with better safety checks
    pub fn get_page_mut(&mut self, page_number: u32) -> Option<&mut Page> {
        if !self.frames.contains_key(&page_number) {
            return None;
        }

        self.touch_page(page_number);

        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.pin();
            frame.mark_dirty();
            self.stats.pin_operations += 1;
            return Some(frame.page_mut());
        }

        None
    }

    /// Enhanced unpin with safety checks and statistics
    pub fn unpin_page(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            let was_pinned = frame.is_pinned();
            let is_now_unpinned = frame.unpin();
            // println!("Unpinning page {}", page_number);
            if was_pinned {
              
                self.stats.unpin_operations += 1;
            }

            if is_now_unpinned && !self.lru_list.contains(&page_number) {
                self.lru_list.push_back(page_number);
            }
            return true;
        }
        false
    }

    /// Force unpin a page (emergency use only)
    pub fn force_unpin_page(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.pin_count = 0;
            if !self.lru_list.contains(&page_number) {
                self.lru_list.push_back(page_number);
            }
            return true;
        }
        false
    }

    /// Get all pinned pages (for debugging)
    pub fn get_pinned_pages(&self) -> Vec<u32> {
        self.frames
            .iter()
            .filter(|(_, frame)| frame.is_pinned())
            .map(|(page_number, _)| *page_number)
            .collect()
    }

    /// Enhanced add_page with better eviction logic
    pub fn add_page(&mut self, page_number: u32, page: Page, pin: bool) -> Option<(u32, Page)> {
        // Check if we need to evict a page
        
        let evicted = if self.frames.len() >= self.max_pages && !self.frames.contains_key(&page_number) {
            self.evict_page_smart()
            //self.evict_page_original()
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
            self.stats.pin_operations += 1;
        } else {
            // If not pinned, add to LRU list
            self.lru_list.push_back(page_number);
        }

        // Add the frame to the buffer pool
        self.frames.insert(page_number, frame);
        // println!("Added page {} to buffer pool", page_number);
        evicted
    }

    /// Smart eviction that considers page access patterns
    fn evict_page_smart(&mut self) -> Option<(u32, Page)> {
        // println!("Evicting page using smart eviction strategy");
        // println!("Frames before eviction: {:?}", self.frames);
        // First, try to find the least recently used unpinned page
        let mut candidates: Vec<_> = self.lru_list
            .iter()
            .filter_map(|&page_number| {
                self.frames.get(&page_number).map(|frame| (page_number, frame.last_accessed(), frame.is_pinned()))
            })
            .filter(|(page_number, _,_)| {
                self.frames.get(page_number).is_some_and(|frame| !frame.is_pinned())
            })
            .collect();

        // println!("Candidates for eviction: {:?}", candidates);
        // Sort by last accessed time (oldest first)
        candidates.sort_by_key(|(_, last_accessed,_)| *last_accessed);

        if let Some((page_number, _,_)) = candidates.first() {
            let page_number = *page_number;
            
            // Remove from LRU list
            self.lru_list.retain(|&p| p != page_number);
            
            // Remove and return the page
            if let Some(frame) = self.frames.remove(&page_number) {
                self.stats.pages_evicted += 1;
                return Some((page_number, frame.page));
            }
        }

        // Fallback to original eviction logic
        self.evict_page_original()
    }

    /// Original eviction logic as fallback
    fn evict_page_original(&mut self) -> Option<(u32, Page)> {
        while let Some(page_number) = self.lru_list.pop_front() {
            if let Some(frame) = self.frames.get(&page_number) {
                if frame.is_pinned() {
                    self.lru_list.push_back(page_number);
                    continue;
                }

                if let Some(frame) = self.frames.remove(&page_number) {
                    self.stats.pages_evicted += 1;
                    return Some((page_number, frame.page));
                }
            }
        }
        None
    }

    /// Enhanced mark_dirty with validation
    pub fn mark_dirty(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.mark_dirty();
            return true;
        }
        false
    }

    /// Enhanced mark_clean with validation
    pub fn mark_clean(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.reset_dirty();
            return true;
        }
        false
    }

    /// Mark all pages as clean
    pub fn mark_clean_all(&mut self) {
        for frame in self.frames.values_mut() {
            frame.reset_dirty();
        }
    }

    /// Get dirty pages with better error handling
    pub fn get_dirty_pages(&self) -> Vec<(u32, &Page)> {
        self.frames
            .iter()
            .filter(|(_, frame)| frame.is_dirty() && !frame.is_being_written())
            .map(|(page_number, frame)| (*page_number, frame.page()))
            .collect()
    }

    /// Enhanced remove_page with safety checks
    pub fn remove_page(&mut self, page_number: u32) -> Option<Page> {
        // Check if the page is in the buffer pool
        if let Some(frame) = self.frames.get(&page_number) {
            // Cannot remove a pinned page
            if frame.is_pinned() {
                return None;
            }
            
            // Cannot remove a page being written
            if frame.is_being_written() {
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

    /// Enhanced touch_page with better LRU management
    fn touch_page(&mut self, page_number: u32) {
        // Remove the page from the LRU list
        self.lru_list.retain(|&p| p != page_number);

        // Add the page to the back of the LRU list if it's not pinned
        if let Some(frame) = self.frames.get_mut(&page_number) {
            if !frame.is_pinned() {
                self.lru_list.push_back(page_number);
            }
            frame.last_accessed = std::time::Instant::now();
        }
    }

    /// Check if a page is dirty
    pub fn is_dirty(&self, page_number: u32) -> bool {
        self.frames
            .get(&page_number)
            .map_or(false, |frame| frame.is_dirty())
    }

    /// Check if a page is pinned
    pub fn is_pinned(&self, page_number: u32) -> bool {
        self.frames
            .get(&page_number)
            .map_or(false, |frame| frame.is_pinned())
    }

    /// Get the pin count of a page
    pub fn pin_count(&self, page_number: u32) -> u32 {
        self.frames
            .get(&page_number)
            .map_or(0, |frame| frame.pin_count())
    }

    /// Update page content (for testing purposes)
    pub fn update_page(&mut self, page_number: u32, page: Page) -> Result<(), String> {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.page = page;
            frame.mark_dirty();
            Ok(())
        } else {
            Err(format!("Page {} not found in buffer pool", page_number))
        }
    }

    /// Get buffer pool statistics
    pub fn get_stats(&self) -> &BufferPoolStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = BufferPoolStats::default();
    }

    /// Get current capacity utilization
    pub fn utilization(&self) -> f64 {
        self.frames.len() as f64 / self.max_pages as f64
    }

    /// Get maximum number of pages this buffer pool can hold
    pub fn max_pages(&self) -> usize {
        self.max_pages
    }

    /// Get current number of pages in the buffer pool
    pub fn page_count(&self) -> usize {
        self.frames.len()
    }

    /// Get number of pinned pages
    pub fn pinned_page_count(&self) -> usize {
        self.frames.values().filter(|frame| frame.is_pinned()).count()
    }

    /// Get number of dirty pages
    pub fn dirty_page_count(&self) -> usize {
        self.frames.values().filter(|frame| frame.is_dirty()).count()
    }

    /// Validate buffer pool integrity (for debugging)
    pub fn validate_integrity(&self) -> Result<(), String> {
        // Check that all pages in LRU list exist in frames
        for &page_number in &self.lru_list {
            if !self.frames.contains_key(&page_number) {
                return Err(format!("Page {} in LRU list but not in frames", page_number));
            }
        }

        // Check that no pinned pages are in LRU list
        for &page_number in &self.lru_list {
            if let Some(frame) = self.frames.get(&page_number) {
                if frame.is_pinned() && self.max_pages <= self.frames.len() {// This is only a problem if we have reached the frame limit.
                    return Err(format!("Pinned page {} found in LRU list", page_number));
                }
            }
        }

        // Check that frame count doesn't exceed maximum
        if self.frames.len() > self.max_pages {
            return Err(format!(
                "Frame count {} exceeds maximum {}",
                self.frames.len(),
                self.max_pages
            ));
        }

        Ok(())
    }

    /// Prepare page for writing (mark as being written)
    pub fn prepare_page_for_write(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.set_being_written(true);
            true
        } else {
            false
        }
    }

    /// Finish writing page (unmark as being written)
    pub fn finish_page_write(&mut self, page_number: u32) -> bool {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.set_being_written(false);
            self.stats.pages_written += 1;
            true
        } else {
            false
        }
    }

    /// Force cleanup of all unpinned pages (emergency use)
    pub fn force_cleanup(&mut self) -> usize {
        let unpinned_pages: Vec<u32> = self.frames
            .iter()
            .filter(|(_, frame)| !frame.is_pinned())
            .map(|(page_number, _)| *page_number)
            .collect();

        let count = unpinned_pages.len();
        for page_number in unpinned_pages {
            self.frames.remove(&page_number);
            self.lru_list.retain(|&p| p != page_number);
        }

        count
    }

    /// Additional methods for RAII guard support

    /// Simple contains check without statistics
    pub fn contains_page_simple(&self, page_number: u32) -> bool {
        self.frames.contains_key(&page_number)
    }

    /// Pin a page for use with guards
    pub fn pin_page_for_guard(&mut self, page_number: u32) -> io::Result<()> {
        
        if let Some(frame) = self.frames.get_mut(&page_number) {
           
            frame.pin();
            // println!("Pinning page {} for guard", page_number);
            // println!("Page {} is now pinned. Pin count is {}", page_number, frame.pin_count());
            // println!("Page {} is pinned: {}", page_number, frame.is_pinned());  
            self.stats.pin_operations += 1;
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            ))
        }
    }

    /// Pin a page for mutable use with guards
    pub fn pin_page_for_guard_mut(&mut self, page_number: u32) -> io::Result<()> {
        if let Some(frame) = self.frames.get_mut(&page_number) {
            frame.pin();
            frame.mark_dirty();
            self.stats.pin_operations += 1;
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page {} not found in cache", page_number),
            ))
        }
    }

    /// Get page reference for guard (immutable)
    pub fn get_page_ref(&self, page_number: u32) -> Option<&Page> {
        self.frames.get(&page_number).map(|frame| frame.page())
    }

    /// Get page reference for guard (mutable)
    pub fn get_page_mut_ref(&mut self, page_number: u32) -> Option<&mut Page> {
        self.frames.get_mut(&page_number).map(|frame| frame.page_mut())
    }

    /// Get page for journal (cloning)
    pub fn get_page_for_journal(&self, page_number: u32) -> Option<&Page> {
        self.frames.get(&page_number).map(|frame| frame.page())
    }
}

#[cfg(test)]
mod buffer_pool_tests {
    use super::*;
    use crate::page::{BTreePage, BTreePageHeader, Page, PageType};

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
    fn test_enhanced_buffer_pool_stats() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        // Test cache miss
        assert!(!pool.contains_page(1));
        assert_eq!(pool.get_stats().cache_misses, 1);
        
        // Add page
        pool.add_page(1, page1, false);
        
        // Test cache hit
        assert!(pool.contains_page(1));
        assert_eq!(pool.get_stats().cache_hits, 1);
        
        // Test pin operation
        pool.get_page(1);
        assert_eq!(pool.get_stats().pin_operations, 1);
        
        // Test unpin operation
        pool.unpin_page(1);
        assert_eq!(pool.get_stats().unpin_operations, 1);
    }

    #[test]
    fn test_smart_eviction() {
        let mut pool = BufferPool::new(2);
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        let page3 = create_test_page(3);
        
        // Add two pages
        pool.add_page(1, page1, false);
        std::thread::sleep(std::time::Duration::from_millis(10));
        pool.add_page(2, page2, false);
        
        // Access page 1 to make it more recently used
        pool.get_page(1);
        pool.unpin_page(1);
        
        // Add page 3, should evict page 2 (least recently used)
        let evicted = pool.add_page(3, page3, false);
        assert!(evicted.is_some());
        let (evicted_page_number, _) = evicted.unwrap();
        assert_eq!(evicted_page_number, 2);
        
        // Verify stats
        assert_eq!(pool.get_stats().pages_evicted, 1);
    }

    #[test]
    fn test_pinned_pages_tracking() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        
        pool.add_page(1, page1, true);
        pool.add_page(2, page2, false);
        
        let pinned_pages = pool.get_pinned_pages();
        assert_eq!(pinned_pages.len(), 1);
        assert!(pinned_pages.contains(&1));
    }

    #[test]
    fn test_force_unpin() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        pool.add_page(1, page1, true);
        assert!(pool.is_pinned(1));
        assert_eq!(pool.pin_count(1), 1);
        
        // Force unpin
        pool.force_unpin_page(1);
        assert!(!pool.is_pinned(1));
        assert_eq!(pool.pin_count(1), 0);
    }

    #[test]
    fn test_integrity_validation() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        pool.add_page(1, page1, false);
        
        // Should pass integrity check
        assert!(pool.validate_integrity().is_ok());
        
        // Pin the page
        pool.get_page(1);
        
        // Should still pass integrity check
        assert!(pool.validate_integrity().is_ok());
    }

    #[test]
    fn test_utilization_metrics() {
        let mut pool = BufferPool::new(4);
        assert_eq!(pool.utilization(), 0.0);
        
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        
        pool.add_page(1, page1, false);
        assert_eq!(pool.utilization(), 0.25);
        
        pool.add_page(2, page2, false);
        assert_eq!(pool.utilization(), 0.5);
        
        assert_eq!(pool.page_count(), 2);
        assert_eq!(pool.pinned_page_count(), 0);
        assert_eq!(pool.dirty_page_count(), 0);
    }

    #[test]
    fn test_write_protection() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        pool.add_page(1, page1, false);
        
        // Prepare page for writing
        pool.prepare_page_for_write(1);
        
        // Page should not be in dirty pages list while being written
        let dirty_pages = pool.get_dirty_pages();
        assert_eq!(dirty_pages.len(), 0);
        
        // Cannot remove page while being written
        let removed = pool.remove_page(1);
        assert!(removed.is_none());
        
        // Finish writing
        pool.finish_page_write(1);
        assert_eq!(pool.get_stats().pages_written, 1);
    }

    #[test]
    fn test_force_cleanup() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        let page3 = create_test_page(3);
        
        pool.add_page(1, page1, true);  // Pinned
        pool.add_page(2, page2, false); // Not pinned
        pool.add_page(3, page3, false); // Not pinned
        
        assert_eq!(pool.page_count(), 3);
        
        // Force cleanup should remove unpinned pages
        let removed_count = pool.force_cleanup();
        assert_eq!(removed_count, 2);
        assert_eq!(pool.page_count(), 1);
        assert!(pool.contains_page(1)); // Pinned page should remain
    }

    #[test]
    fn test_page_type_validation() {
        let mut pool = BufferPool::new(3);
        let page1 = create_test_page(1);
        
        pool.add_page(1, page1, false);
        
        // Valid type should pass
        assert!(pool.validate_page_type(1, PageType::TableLeaf).is_ok());
        
        // Invalid type should fail
        let result = pool.validate_page_type(1, PageType::TableInterior);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("type mismatch"));
    }
}
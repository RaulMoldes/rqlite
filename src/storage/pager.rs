//! # Pager Module
//! 
//! This module implements the `Pager` struct, which is responsible for managing
//! the loading and caching of database pages. It interacts with the `DiskManager`
use std::collections::HashMap;
use std::io::{self, Cursor};
use std::path::Path;


use super::disk::DiskManager;
use crate::page::{BTreePage, BTreePageHeader, OverflowPage, FreePage, Page, PageType};
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
    page_cache: HashMap<u32, Page>,
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
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut disk_manager = DiskManager::open(path)?;
        let header = disk_manager.read_header()?;
        
        Ok(Pager {
            disk_manager,
            page_cache: HashMap::new(),
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
    pub fn create<P: AsRef<Path>>(path: P, page_size: u32, reserved_space: u8) -> io::Result<Self> {
        let mut disk_manager = DiskManager::create(path, page_size)?;
        
        // Upadate the header with the reserved space
        let mut header = disk_manager.read_header()?;
        header.reserved_space = reserved_space;
        disk_manager.write_header(&header)?;
        
        Ok(Pager {
            disk_manager,
            page_cache: HashMap::new(),
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
        if !self.page_cache.contains_key(&page_number) {
            // If it is not in the cache, load it from disk
            // This is a bit tricky, because we need to check if the page exists
            // and if it is a valid page. If it is not, we need to return an error.
            self.load_page(page_number)?;
        }
        
        // Verifiy the type of the page if it was specified
        if let Some(expected_type) = page_type {
            match self.page_cache.get(&page_number) {
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
        self.page_cache.get(&page_number)
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
        if !self.page_cache.contains_key(&page_number) {
            // Load the page from disk if it is not in the cache
            self.load_page(page_number)?;
        }
        
        // Verify the type of page if it was specified
        if let Some(expected_type) = page_type {
            match self.page_cache.get(&page_number) {
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
        self.page_cache.get_mut(&page_number)
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
        if page_number > page_count {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Page number out of range: {}, maximum {}", page_number, page_count),
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
        self.page_cache.insert(page_number, page);
        
        Ok(())
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
    /// # Returns
    /// A `Page` instance representing the parsed B-Tree page.
    fn parse_btree_page(&self, page_number: u32, buffer: &[u8]) -> io::Result<Page> {

        
        let mut cursor = Cursor::new(buffer);
        let header = BTreePageHeader::read_from(&mut cursor)?;
        
        // Por ahora, simplemente creamos una página B-Tree vacía con el encabezado leído
        let btree_page = BTreePage {
            header,
            cell_indices: Vec::new(),
            cells: Vec::new(),
            page_size: self.page_size,
            page_number,
            reserved_space: self.reserved_space,
        };
        
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
        self.page_cache.insert(page_number, Page::BTree(btree_page));
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
        self.page_cache.insert(page_number, Page::Overflow(overflow_page));
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
        self.page_cache.insert(page_number, Page::Free(free_page));
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
        
        // Save all the pages in the cache to disk
        for (page_number, page) in &self.page_cache {
            // Serialize the page to a buffer
            let buffer = self.serialize_page(page)?;
            
            // Write the page to disk
            self.disk_manager.write_page(*page_number, &buffer)?;
        }
        
        // Sync the disk manager to ensure all data is written
        self.disk_manager.sync()?;
        
        self.dirty = false;
        Ok(())
    }

    /// Serializa una página en un buffer de bytes.
    ///
    /// # Parámetros
    /// * `page` - Página a serializar.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al serializar.
    ///
    /// # Retorno
    /// Buffer con los datos serializados.
    fn serialize_page(&self, page: &Page) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0u8; self.page_size as usize];
        
        match page {
            Page::BTree(btree_page) => {
                // Por ahora, simplemente establecemos el tipo de página
                buffer[0] = btree_page.header.page_type as u8;
                
                // Missing serialization of the BTreePage
            },
            Page::Overflow(overflow_page) => {
                // Write the number of the next overflow page
                let next_page_bytes = overflow_page.next_page.to_be_bytes();
                buffer[0..4].copy_from_slice(&next_page_bytes);
                
                // Write the data of the overflow page
                let data_len = overflow_page.data.len().min(self.page_size as usize - 4);
                buffer[4..4 + data_len].copy_from_slice(&overflow_page.data[0..data_len]);
            },
            Page::Free(free_page) => {
                // Write the number of the next free page
                // The first 4 bytes of the page are reserved for the next page
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
        
        // Crear un pager
        let result = Pager::create(&db_path, 4096, 0);
        assert!(result.is_ok());
        
        // Verificar que el archivo existe
        assert!(db_path.exists());
        
        // Verificar que el tamaño del archivo es al menos el de una página
        let metadata = fs::metadata(&db_path).unwrap();
        assert!(metadata.len() >= 4096);
    }

    #[test]
    fn test_open_pager() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear un pager
        {
            let _pager = Pager::create(&db_path, 4096, 0).unwrap();
        }
        
        // Abrir el pager existente
        let result = Pager::open(&db_path);
        assert!(result.is_ok());
        
        let pager = result.unwrap();
        assert_eq!(pager.page_size, 4096);
        assert_eq!(pager.reserved_space, 0);
    }

    #[test]
    fn test_create_btree_page() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear un pager
        let mut pager = Pager::create(&db_path, 4096, 0).unwrap();
        
        // Crear una página B-Tree de tabla hoja
        let page_number = pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        assert_eq!(page_number, 3); // La primera página es el encabezado
        
        // Verificar que la página existe en la caché
        assert!(pager.page_cache.contains_key(&page_number));
        
        // Flush para escribir la página a disco
        pager.flush().unwrap();
        
        // Abrir un nuevo pager y verificar que la página es legible
        let mut pager2 = Pager::open(&db_path).unwrap();
        let page_result = pager2.get_page(page_number, Some(PageType::TableLeaf));
        assert!(page_result.is_ok());
    }

    #[test]
    fn test_header_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear un pager
        let mut pager = Pager::create(&db_path, 4096, 0).unwrap();
        
        // Leer el encabezado
        let mut header = pager.get_header().unwrap();
        assert_eq!(header.page_size, 4096);
        assert_eq!(header.reserved_space, 0);
        
        // Modificar el encabezado
        header.user_version = 42;
        pager.update_header(&header).unwrap();
        
        // Leer de nuevo y verificar
        let header2 = pager.get_header().unwrap();
        assert_eq!(header2.user_version, 42);
    }

    #[test]
    fn test_page_count() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear un pager
        let mut pager = Pager::create(&db_path, 4096, 0).unwrap();
        
        // Verificar que hay una página inicialmente
        assert_eq!(pager.page_count().unwrap(), 2); // 1 para el encabezado y 1 para la primera página
        
        // Crear una página B-Tree
        pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        
        // Verificar que ahora hay dos páginas
        assert_eq!(pager.page_count().unwrap(), 3);
    }
}
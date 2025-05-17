//! # Page Module
//! 
//! This module defines the structures and functions for handling B-Tree pages, overflow pages, and free pages in a SQLite database.
//! It includes the representation of page headers, cell types, and methods for reading and writing pages.
//! 
//! BASIC PAGE STRUCTURE
//! Each page in a SQLite database consists of a header and a series of cells. The header contains metadata about the page, such as its type, size, and offsets to the cells.
//! After the header, the cells are stored in a contiguous block of memory. Each cell has an associated offset in the page, which is stored in a separate array (slot array).
//! The slot array is a list of offsets that point to the start of each cell in the page. Therefore to access a cell, we only need its physical identifier, which is made up of the page_number + the slot_number.
//! IMPORTANT: The slots grow from the beginning of the page to the end, while the cells grow from the end of the page to the beginning.
//! Therefore a page becomes full when the slot array reaches the point where the cells start (free space pointer).
//! --------------------------------------------------------
//! | Page Header (B-Tree)                                 |
//! |------------------------------------------------------|
//! | Slot Array (Offsets to Cells)                        |
//! |------------------------------------------------------|
//! |                                                      |
//! |                        DATA                          |
//! |                                                      |
//! |------------------------------------------------------|
use std::fmt;
use std::io::{self, Read, Write};
use crate::header::HEADER_SIZE;
use std::io::Cursor;

/// Page Types on SQLite. There are basically two types of pages: 
/// Table pages: A table page is a B-Tree page that stores data from a table. 
/// Index pages: An index page is a B-Tree page that stores data from an index.
/// Each btree page can be either a leaf page or an interior page (see btree module).
/// Special pages include overflow pages and free pages.
/// Overflow pages are used to store data that does not fit in the main page.
/// Free pages are used to keep track of the free space in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Index interior page (Interior of the B-Tree index)
    IndexInterior = 0x02,
    /// Table interior page (Interior of the B-Tree table)
    TableInterior = 0x05,
    /// Leaf index page (Leaf of the B-Tree index)
    IndexLeaf = 0x0A,
    /// Leaf table page (Leaf of the B-Tree table)
    TableLeaf = 0x0D,
    /// Overflow page (used for large data)
    Overflow = 0x10,
    /// Free page (used for free space management)
    Free = 0x00,
}

impl PageType {
    /// Builds a `PageType` from a byte marker.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x02 => Some(PageType::IndexInterior),
            0x05 => Some(PageType::TableInterior),
            0x0A => Some(PageType::IndexLeaf),
            0x0D => Some(PageType::TableLeaf),
            0x10 => Some(PageType::Overflow), 
            0x00 => Some(PageType::Free),
            _ => None, // Not a valid page type
        }
    }

    /// Returns true if the page is an interior page.
    pub fn is_interior(&self) -> bool {
        matches!(self, PageType::IndexInterior | PageType::TableInterior)
    }

    /// Returns true if the page is a leaf page.
    pub fn is_leaf(&self) -> bool {
        matches!(self, PageType::IndexLeaf | PageType::TableLeaf)
    }

    /// Returns true if the page is part of an index.
    pub fn is_index(&self) -> bool {
        matches!(self, PageType::IndexInterior | PageType::IndexLeaf)
    }

    /// Returns true if the page is part of a table.
    pub fn is_table(&self) -> bool {
        matches!(self, PageType::TableInterior | PageType::TableLeaf)
    }

    /// Returns true if the page is an overflow page.
    pub fn is_overflow(&self) -> bool {
        matches!(self, PageType::Overflow)
    }

    /// Returns true if the page is a free page.
    pub fn is_free(&self) -> bool {
        matches!(self, PageType::Free)
    }
}


/// Custom trait for serializing and deserializing data to and from byte streams.
/// This trait is used to read and write data in a binary format.
/// It is implemented for various types, including B-Tree page headers and cells.
pub trait ByteSerializable {
    /// Reads a value from a byte stream.
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> where Self: Sized;
    /// Writes a value to a byte stream.
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()>;
}

/// Represents the header of a B-Tree page.
#[derive(Debug, Clone)]
pub struct BTreePageHeader {
    /// Type of btree page (table, index, leaf or interior).
    pub page_type: PageType,
    /// Offset to the first free block in the page (free space pointer).
    pub first_free_block_offset: u16,
    /// Total number of slots (cells) in the page.
    pub cell_count: u16,
    /// Offset to the point where the content starts (after the header).
    pub content_start_offset: u16,
    /// Number of fragmented free bytes.
    pub fragmented_free_bytes: u8,
    /// For Btree interior pages, the page number of the rightmost child.
    pub right_most_page: Option<u32>,
}

impl BTreePageHeader {
    /// Creates a new header for a Leaf B-Tree page.
    /// # Parameters
    /// * `page_type` - Type of the page (must be a leaf type).
    /// # Panics
    /// Panics if the page type is not a leaf type.
    pub fn new_leaf(page_type: PageType) -> Self {
        if !page_type.is_leaf() {
            panic!("Expected a leaf page type");
        }

        BTreePageHeader {
            page_type,
            first_free_block_offset: 0,
            cell_count: 0,
            content_start_offset: 0, // Se actualizará al añadir celdas
            fragmented_free_bytes: 0,
            right_most_page: None,
        }
    }

    /// Creates a new header for an interior page type.
    /// # Parameters
    /// * `page_type` - Type of the page (must be an interior type).
    /// * right_most_page - Page number of the rightmost child.
    /// # Panics
    /// Panics if the page type is not an interior type.
    pub fn new_interior(page_type: PageType, right_most_page: u32) -> Self {
        if !page_type.is_interior() {
            panic!("Expected an interior page type");
        }

        BTreePageHeader {
            page_type,
            first_free_block_offset: 0,
            cell_count: 0,
            content_start_offset: 0, // Se actualizará al añadir celdas
            fragmented_free_bytes: 0,
            right_most_page: Some(right_most_page),
        }
    }

    /// Calculates the total size of the page header in bytes.
    pub fn size(&self) -> usize {
        if self.page_type.is_leaf() {
            8 // Leaf pages: type (1) + first_free (2) + cell_count (2) + content_start (2) + fragmented_bytes (1)
        } else {
            12 //For interior pages we need to add the right_most_page (4)
        }
    }
}


impl ByteSerializable for BTreePageHeader {
    /// Reads a header from a source, which must implement the trait Read.
    /// # Parameters
    /// * `reader` - Source from which to read the header.
    /// # Errors
    /// Returns an error if the header cannot be read or if the page type is unknown.
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = [0u8; 12]; // Buffer to read the header
        reader.read_exact(&mut buffer[0..1])?; // Read the page type

        let page_type = PageType::from_byte(buffer[0]).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid btree page type: {:#04x}", buffer[0]),
            )
        })?;

        // Read common fields
        // Read the first free block offset, cell count, content start offset, and fragmented free bytes
        reader.read_exact(&mut buffer[1..8])?;
        let first_free_block_offset = u16::from_be_bytes([buffer[1], buffer[2]]);
        let cell_count = u16::from_be_bytes([buffer[3], buffer[4]]);
        let content_start_offset = u16::from_be_bytes([buffer[5], buffer[6]]);
        let fragmented_free_bytes = buffer[7];

        // Read the rightmost page number if the page type is interior
        let right_most_page = if page_type.is_interior() {
            reader.read_exact(&mut buffer[8..12])?;
            Some(u32::from_be_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]))
        } else {
            None
        };

        Ok(BTreePageHeader {
            page_type,
            first_free_block_offset,
            cell_count,
            content_start_offset,
            fragmented_free_bytes,
            right_most_page,
        })
    }

    /// Write the header to a writer, which must implement the trait Write.
    /// # Parameters
    /// * `writer` - Writer to which to write the header.
    /// # Errors
    /// Returns an error if the header cannot be written.
    /// 
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write the page type
        writer.write_all(&[self.page_type as u8])?;

        // Write common fields
        writer.write_all(&self.first_free_block_offset.to_be_bytes())?;
        writer.write_all(&self.cell_count.to_be_bytes())?;
        writer.write_all(&self.content_start_offset.to_be_bytes())?;
        writer.write_all(&[self.fragmented_free_bytes])?;

        // Write the rightmost page number if the page type is interior
        if let Some(right_most) = self.right_most_page {
            writer.write_all(&right_most.to_be_bytes())?;
        }

        Ok(())
    }
}


/// Display trait implementation for BTreePageHeader
impl fmt::Display for BTreePageHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "B-Tree Page Header:")?;
        writeln!(f, "  Type: {:?}", self.page_type)?;
        writeln!(f, "  Cell Count: {}", self.cell_count)?;
        writeln!(f, "  Content Start Offset: {}", self.content_start_offset)?;
        
        if let Some(right_most) = self.right_most_page {
            writeln!(f, "  Right Most Page: {}", right_most)?;
        }
        
        Ok(())
    }
}




/// Each cell on a B-Tree page can be of different types.
/// Table leaf cells is where the actual data is stored.
#[derive(Debug, Clone)]
pub struct TableLeafCell {
    /// Size of the payload in bytes.
    pub payload_size: u64,
    /// Physical row_id (row identifier).
    pub row_id: i64,
    /// Payload content in bytes. The payload is the actual data stored in the cell.
    pub payload: Vec<u8>,
    /// Pointer to the overflow page that stores therest of the data if it does not fit in this page.
    pub overflow_page: Option<u32>,
}


/// Table interior cells are used to store the keys that define the boundaries between child pages.
/// Used for efficient searching and navigation in the B-Tree structure.
/// (See BTree module for more details).
#[derive(Debug, Clone)]
pub struct TableInteriorCell {
    /// Page_number (pointer) to the left_child.
    pub left_child_page: u32,
    /// Key that defines the boundary between the left and right child..
    pub key: i64,
}

/// Each cell in a B-Tree index leaf page contains a payload and a rowid.
#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    /// Size of the payload in bytes.
    pub payload_size: u64,
    /// Payload content in bytes. The payload is the actual data stored in the cell.
    /// In this case it is the index key.
    /// (If you created an index on column a of table t the payload will be the value of a for each row).
    pub payload: Vec<u8>,
    /// References to the page of overflow (if the payload does not fit in this page). This is very rare but can happen if we try to index on very large columns.
    pub overflow_page: Option<u32>,
}

/// Each cell in a B-Tree index interior page contains a pointer to the left child and a key.
#[derive(Debug, Clone)]
pub struct IndexInteriorCell {
    /// Page_number (pointer) to the left child.
    pub left_child_page: u32,
    /// Payload size in bytes. In index cells, we also store the key. This is the main difference from the table cells.
    pub payload_size: u64,
    /// Payload content in bytes. The payload is the actual data stored in the cell.
    pub payload: Vec<u8>,
    /// References to the page of overflow (if the payload does not fit in this page). 
    pub overflow_page: Option<u32>,
}

/// Represents a cell in a B-Tree page.
#[derive(Debug, Clone)]
pub enum BTreeCell {
    /// Table leaf cell.
    TableLeaf(TableLeafCell),
    /// Interior table cell.
    TableInterior(TableInteriorCell),
    /// Index leaf cell.
    IndexLeaf(IndexLeafCell),
    /// Interior index cell.
    IndexInterior(IndexInteriorCell),
}

impl BTreeCell {
    /// Calculates the size of the cell in bytes.
    pub fn size(&self) -> usize {
        match self {
            BTreeCell::TableLeaf(cell) => {
                // Calculate the varint size for the payload size and row_id (See varint module for details).
                let varint_size = crate::utils::varint_size(cell.payload_size as i64);
                let rowid_size = crate::utils::varint_size(cell.row_id);
                
                varint_size + rowid_size + cell.payload.len() + 
                    if cell.overflow_page.is_some() { 4 } else { 0 } // We add 4 bytes for the overflow page if it exists
            },
            BTreeCell::TableInterior(cell) => {
                4 + crate::utils::varint_size(cell.key) // Add 4 bytes for the left_child_page and varint size for the key
            },
            BTreeCell::IndexLeaf(cell) => {
                let varint_size = crate::utils::varint_size(cell.payload_size as i64);
                
                varint_size + cell.payload.len() + 
                    if cell.overflow_page.is_some() { 4 } else { 0 }
            },
            BTreeCell::IndexInterior(cell) => {
                let varint_size = crate::utils::varint_size(cell.payload_size as i64);
                
                4 + varint_size + cell.payload.len() + // 4 bytes for the left_child_page
                // Add the varint size for the payload
                    if cell.overflow_page.is_some() { 4 } else { 0 }
            },
        }
    }
}

/// Represents a B-Tree page.
#[derive(Debug, Clone)]
pub struct BTreePage {
    /// Header of the B-Tree page.
    pub header: BTreePageHeader,
    /// SLot array (offsets to the cells).
    pub cell_indices: Vec<u16>,
    /// Cells stored in the page.
    pub cells: Vec<BTreeCell>,
    /// Page size in bytes.
    pub page_size: u32,
    /// Page number.
    pub page_number: u32,
    /// Reserved space at the end of each page.
    pub reserved_space: u8,
}

impl BTreePage {
    /// Creates a new btree page 
    /// # Parameters: 
    /// * page_type - Type of the page (must be a leaf or interior type).
    /// * page_size - Size of the page in bytes.
    /// * page_number - Page number.
    /// * reserved_space - Reserved space at the end of each page.
    /// * right_most_page - Page number of the rightmost child (only for interior pages).
    /// # Errors
    /// Returns an error if the page type is not valid or if the right_most_page is not set for a leaf page.
    /// # Panics
    /// Panics if the page type is not valid or if the right_most_page is not set for a leaf page.
    pub fn new(
        page_type: PageType,
        page_size: u32,
        page_number: u32,
        reserved_space: u8,
        right_most_page: Option<u32>,
    ) -> io::Result<Self> {
        // Check if the page type is valid
        let header = if page_type.is_leaf() {
            if right_most_page.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "The right_most_page should not be set for leaf pages",
                ));
            }
            BTreePageHeader::new_leaf(page_type) // Create a new leaf page header
        } else if let Some(right_most) = right_most_page  {
         
                BTreePageHeader::new_interior(page_type, right_most)
            
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "The right_most_page should be set for interior pages",
                ));
            
        };

        // Initialize the B-Tree page
        let mut page = BTreePage {
            header,
            cell_indices: Vec::new(),
            cells: Vec::new(),
            page_size,
            page_number,
            reserved_space,
        };

        // Inicialize the content start offset
        page.update_content_start_offset();

        Ok(page)
    }

    /// Sets the content start offset based on the page size and reserved space.
    fn update_content_start_offset(&mut self) {
        self.header.content_start_offset = self.page_size as u16 - self.reserved_space as u16;
    }

    /// Addse a cell to the B-Tree page.
    /// # Parameters
    /// * `cell` - The cell to add to the page.
    /// # Errors
    /// Returns an error if the cell cannot be added due to insufficient space or if the cell type is incompatible with the page type.
    /// # Panics
    /// Panics if the cell type is incompatible with the page type.
    /// # Notes
    /// The cell is added to the page and the content start offset is updated. T
    pub fn add_cell(&mut self, cell: BTreeCell) -> io::Result<()> {
        // Verify the type compatibility
        // Check if the cell type is compatible with the page type
        match (&self.header.page_type, &cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(_)) => {},
            (PageType::TableInterior, BTreeCell::TableInterior(_)) => {},
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {},
            (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {},
            _ => {
                // You cannot add a cell to an overflow page or free page
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Cell type incompatible with this page: {:?}", self.header.page_type),
                ));
            }
        }

        // Compute the required space to store the cell
        let cell_size = cell.size();
        let cell_index_size = 2; // 2 additional bytes for the cell offset in the slot array

        // Compute the available space in the page
        let header_size = self.header.size();
        let cell_indices_size = self.cell_indices.len() * cell_index_size; // Current space occupied by indices
        // Total used space in the page. If we are at page 1 we need to add the header size, as the database header is stored on page 1.
        let used_space = if self.page_number == 1 {
            HEADER_SIZE + header_size + cell_indices_size
        } else {
            header_size + cell_indices_size
        };

        // Calculate the available space for the new cell
        // The content start offset is the point where the content starts (after the header and the slot array).
        let content_start = self.header.content_start_offset as usize;
        let available_space = content_start - used_space - cell_index_size; // Restar el espacio para el nuevo índice

        if cell_size > available_space {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Not enough bytes to store the cell: needed {} bytes, available {} bytes", 
                    cell_size, available_space),
            ));
        }

        // Update the content start offset
        self.header.content_start_offset -= cell_size as u16;
        
        // Append the cell index to the slot array
        self.cell_indices.push(self.header.content_start_offset);
        
        // Append the cell to the cells vector
        self.cells.push(cell);
        
        // Update the first free block offset
        self.header.cell_count += 1;

        Ok(())
    }

    /// Returns the free space on the page .
    pub fn free_space(&self) -> usize {
        let header_size = self.header.size();
        let cell_indices_size = self.cell_indices.len() * 2; // 2 bytes per index
     
        let used_space = if self.page_number == 1 {
            HEADER_SIZE + header_size + cell_indices_size
            
        } else {
            header_size + cell_indices_size
        };
       
        
        let content_size = self.page_size as usize - self.header.content_start_offset as usize;
        
        self.page_size as usize - used_space - content_size - self.reserved_space as usize
    }
}

// Updated implementation for BTreePage
impl ByteSerializable for BTreePage {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read the header
        let header = BTreePageHeader::read_from(reader)?;
        
        // Create a new BTreePage with default values
        let mut page = BTreePage {
            header: header.clone(),
            cell_indices: Vec::new(),
            cells: Vec::new(),
            page_size: 0, // Will be set later
            page_number: 0, // Will be set later
            reserved_space: 0, // Will be set later
        };
        
        // Read cell indices
        let cell_count = header.cell_count as usize;
        let mut cell_indices = Vec::with_capacity(cell_count);
        
        for _ in 0..cell_count {
            let mut buffer = [0u8; 2];
            reader.read_exact(&mut buffer)?;
            let index = u16::from_be_bytes(buffer);
            cell_indices.push(index);
        }
        
        page.cell_indices = cell_indices;
        
        // Read the entire remaining data to have access to all cells
        let mut remaining_data = Vec::new();
        reader.read_to_end(&mut remaining_data)?;
        
        // Deserialize each cell using its offset
        let header_size = page.header.size();
        let indices_size = page.cell_indices.len() * 2; // 2 bytes per index
        let content_start = header_size + indices_size;
        
        for &cell_index in &page.cell_indices {
            // Calculate the actual offset in the remaining_data
            let cell_offset = cell_index as usize - content_start;
            
            if cell_offset >= remaining_data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Cell offset out of range: {}", cell_offset),
                ));
            }
            
            // Create a cursor at the cell position
            let mut cell_cursor = Cursor::new(&remaining_data[cell_offset..]);
            
            // Deserialize the cell based on the page type
            let cell = match header.page_type {
                PageType::TableLeaf => {
                    // Read payload size
                    let (payload_size, payload_size_bytes) = crate::utils::decode_varint(&mut cell_cursor)?;
                    
                    // Read rowid
                    let (row_id, rowid_bytes) = crate::utils::decode_varint(&mut cell_cursor)?;
                    
                    // Calculate the position after reading varints
                    let header_bytes = payload_size_bytes + rowid_bytes;
                    
                    // Read the payload
                    let payload_size = payload_size as usize;
                    let available_bytes = remaining_data.len() - cell_offset - header_bytes;
                    let local_size = payload_size.min(available_bytes);
                    
                    let mut payload = vec![0u8; local_size];
                    let payload_start = cell_cursor.position() as usize;
                    payload.copy_from_slice(&remaining_data[cell_offset + payload_start..][..local_size]);
                    
                    // Check if there's an overflow page (if payload doesn't fit)
                    let overflow_page = if local_size < payload_size {
                        let overflow_offset = cell_offset + header_bytes + local_size;
                        if overflow_offset + 4 <= remaining_data.len() {
                            Some(u32::from_be_bytes([
                                remaining_data[overflow_offset],
                                remaining_data[overflow_offset + 1],
                                remaining_data[overflow_offset + 2],
                                remaining_data[overflow_offset + 3],
                            ]))
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    
                    BTreeCell::TableLeaf(TableLeafCell {
                        payload_size: payload_size as u64,
                        row_id,
                        payload,
                        overflow_page,
                    })
                },
                PageType::TableInterior => {
                    if cell_offset + 4 > remaining_data.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Buffer too small to read interior cell",
                        ));
                    }
                    
                    // Read left child page
                    let left_child_page = u32::from_be_bytes([
                        remaining_data[cell_offset],
                        remaining_data[cell_offset + 1],
                        remaining_data[cell_offset + 2],
                        remaining_data[cell_offset + 3],
                    ]);
                    
                    // Read key
                    let mut key_cursor = Cursor::new(&remaining_data[cell_offset + 4..]);
                    let (key, _) = crate::utils::decode_varint(&mut key_cursor)?;
                    
                    BTreeCell::TableInterior(TableInteriorCell {
                        left_child_page,
                        key,
                    })
                },
                PageType::IndexLeaf => {
                    // Read payload size
                    let (payload_size, payload_size_bytes) = crate::utils::decode_varint(&mut cell_cursor)?;
                    
                    // Calculate header bytes
                    let header_bytes = payload_size_bytes;
                    
                    // Read the payload
                    let payload_size = payload_size as usize;
                    let available_bytes = remaining_data.len() - cell_offset - header_bytes;
                    let local_size = payload_size.min(available_bytes);
                    
                    let mut payload = vec![0u8; local_size];
                    let payload_start = cell_cursor.position() as usize;
                    payload.copy_from_slice(&remaining_data[cell_offset + payload_start..][..local_size]);
                    
                    // Check if there's an overflow page
                    let overflow_page = if local_size < payload_size {
                        let overflow_offset = cell_offset + header_bytes + local_size;
                        if overflow_offset + 4 <= remaining_data.len() {
                            Some(u32::from_be_bytes([
                                remaining_data[overflow_offset],
                                remaining_data[overflow_offset + 1],
                                remaining_data[overflow_offset + 2],
                                remaining_data[overflow_offset + 3],
                            ]))
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    
                    BTreeCell::IndexLeaf(IndexLeafCell {
                        payload_size: payload_size as u64,
                        payload,
                        overflow_page,
                    })
                },
                PageType::IndexInterior => {
                    if cell_offset + 4 > remaining_data.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Buffer too small to read interior cell",
                        ));
                    }
                    
                    // Read left child page
                    let left_child_page = u32::from_be_bytes([
                        remaining_data[cell_offset],
                        remaining_data[cell_offset + 1],
                        remaining_data[cell_offset + 2],
                        remaining_data[cell_offset + 3],
                    ]);
                    
                    // Read payload size
                    let mut payload_cursor = Cursor::new(&remaining_data[cell_offset + 4..]);
                    let (payload_size, payload_size_bytes) = crate::utils::decode_varint(&mut payload_cursor)?;
                    
                    // Calculate header bytes
                    let header_bytes = 4 + payload_size_bytes;
                    
                    // Read the payload
                    let payload_size = payload_size as usize;
                    let available_bytes = remaining_data.len() - cell_offset - header_bytes;
                    let local_size = payload_size.min(available_bytes);
                    
                    let mut payload = vec![0u8; local_size];
                    payload.copy_from_slice(&remaining_data[cell_offset + header_bytes..][..local_size]);
                    
                    // Check if there's an overflow page
                    let overflow_page = if local_size < payload_size {
                        let overflow_offset = cell_offset + header_bytes + local_size;
                        if overflow_offset + 4 <= remaining_data.len() {
                            Some(u32::from_be_bytes([
                                remaining_data[overflow_offset],
                                remaining_data[overflow_offset + 1],
                                remaining_data[overflow_offset + 2],
                                remaining_data[overflow_offset + 3],
                            ]))
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    
                    BTreeCell::IndexInterior(IndexInteriorCell {
                        left_child_page,
                        payload_size: payload_size as u64,
                        payload,
                        overflow_page,
                    })
                },
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unexpected page type for B-Tree: {:?}", header.page_type),
                    ));
                }
            };
            
            page.cells.push(cell);
        }
        
        Ok(page)
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write the header
        self.header.write_to(writer)?;
        
        // Write cell indices
        for &idx in &self.cell_indices {
            writer.write_all(&idx.to_be_bytes())?;
        }
        
        // Calculate the start of the content area
        let header_size = self.header.size();
        let indices_size = self.cell_indices.len() * 2; // 2 bytes per index
        let content_start = header_size + indices_size;
        
        // Create a buffer for the content area - we'll fill in cells at their specific offsets
        let mut content_buffer = vec![0u8; self.page_size as usize - content_start];
        
        // Write each cell to its position in the content buffer
        for (i, cell) in self.cells.iter().enumerate() {
            if i >= self.cell_indices.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Cell index out of range",
                ));
            }
            
            let cell_index = self.cell_indices[i] as usize;
            if cell_index < content_start || cell_index >= self.page_size as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid cell index: {}", cell_index),
                ));
            }
            
            let buffer_offset = cell_index - content_start;
            
            // We need to keep track of the size of the buffer.
            let mut buffer_size = content_buffer.len();

            // Create a cursor at the cell position
            let mut cell_cursor = Cursor::new(&mut content_buffer[buffer_offset..]);
            
            // Write the cell - note this doesn't include the cell type as it's determined by the page type
            match cell {
                BTreeCell::TableLeaf(leaf_cell) => {
                    // Write payload size
                    crate::utils::encode_varint(leaf_cell.payload_size as i64, &mut cell_cursor)?;
                    buffer_size -= crate::utils::varint_size(leaf_cell.payload_size as i64);
                    // Write rowid
                    crate::utils::encode_varint(leaf_cell.row_id, &mut cell_cursor)?;
                    buffer_size -= crate::utils::varint_size(leaf_cell.row_id);
                    // Write payload
                    let current_pos = cell_cursor.position() as usize;
                    let available_space = buffer_size - buffer_offset - current_pos;
                    let payload_size = leaf_cell.payload.len().min(available_space);
                    
                    if payload_size > 0 {
                        cell_cursor.write_all(&leaf_cell.payload[..payload_size])?;
                    }
                    
                    // Write overflow page if present
                    if let Some(overflow_page) = leaf_cell.overflow_page {
                        cell_cursor.write_all(&overflow_page.to_be_bytes())?;
                    }
                },
                BTreeCell::TableInterior(interior_cell) => {
                    // Write left child page
                    cell_cursor.write_all(&interior_cell.left_child_page.to_be_bytes())?;
                    
                    // Write key
                    crate::utils::encode_varint(interior_cell.key, &mut cell_cursor)?;
                },
                BTreeCell::IndexLeaf(leaf_cell) => {
                    // Write payload size
                    crate::utils::encode_varint(leaf_cell.payload_size as i64, &mut cell_cursor)?;
                    buffer_size -= crate::utils::varint_size(leaf_cell.payload_size as i64);
                    // Write payload
                    let current_pos = cell_cursor.position() as usize;
                    let available_space = buffer_size - buffer_offset - current_pos;
                    let payload_size = leaf_cell.payload.len().min(available_space);
                    
                    if payload_size > 0 {
                        cell_cursor.write_all(&leaf_cell.payload[..payload_size])?;
                    }
                    
                    // Write overflow page if present
                    if let Some(overflow_page) = leaf_cell.overflow_page {
                        cell_cursor.write_all(&overflow_page.to_be_bytes())?;
                    }
                },
                BTreeCell::IndexInterior(interior_cell) => {
                    // Write left child page
                    cell_cursor.write_all(&interior_cell.left_child_page.to_be_bytes())?;
                    buffer_size -= 4;
                    // Write payload size
                    crate::utils::encode_varint(interior_cell.payload_size as i64, &mut cell_cursor)?;
                    buffer_size -= crate::utils::varint_size(interior_cell.payload_size as i64);
                    // Write payload
                    let current_pos = cell_cursor.position() as usize;
                    let available_space = buffer_size- buffer_offset - current_pos;
                    let payload_size = interior_cell.payload.len().min(available_space);
                    
                    if payload_size > 0 {
                        cell_cursor.write_all(&interior_cell.payload[..payload_size])?;
                    }
                    
                    // Write overflow page if present
                    if let Some(overflow_page) = interior_cell.overflow_page {
                        cell_cursor.write_all(&overflow_page.to_be_bytes())?;
                    }
                },
            }
        }
        
        // Write the content buffer
        writer.write_all(&content_buffer)?;
        
        Ok(())
    }
}






/// Represents an overflow page.
#[derive(Debug, Clone)]
pub struct OverflowPage {
    /// Next overflow page number (0 if it is the last one). Overflow pages are linked together. in a linked list, allowing us to store super-large tuples.
    /// The first page is the one that stores the first part of the data.
    pub next_page: u32,
    /// Data stored in the overflow page.
    pub data: Vec<u8>,
    /// Page size in bytes.
    pub page_size: u32,
    /// Page number.
    pub page_number: u32,
}

impl OverflowPage {
    /// Creates a new overflow page.
    ///
    /// # Parameters
    /// * `next_page` - Number of the next overflow page (0 if it is the last one), no one can point to page 0 as this page is not used.
    /// * `data` - Data to be stored in the overflow page.
    /// * `page_size` - Size of the page in bytes.
    /// * `page_number` - Page number.
    /// # Errors
    /// Returns an error if the data size exceeds the maximum page size.
    /// # Notes
    /// The maximum size of the data is limited by the page size minus 4 bytes for the next_page pointer.
    pub fn new(
        next_page: u32,
        data: Vec<u8>,
        page_size: u32,
        page_number: u32,
    ) -> io::Result<Self> {
        let max_data_size = page_size as usize - 4; // 4 bytes para next_page
        
        if data.len() > max_data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Data too big for the overflow page: {} bytes, max is {} bytes",
                    data.len(), max_data_size),
            ));
        }
        
        Ok(OverflowPage {
            next_page,
            data,
            page_size,
            page_number,
        })
    }
}

// Implementation for OverflowPage
impl ByteSerializable for OverflowPage {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read next page pointer
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        let next_page = u32::from_be_bytes(buffer);
        
        // Read data
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        
        // Create overflow page
        // Note: page_size and page_number need to be set by the caller
        Ok(OverflowPage {
            next_page,
            data,
            page_size: 0,
            page_number: 0,
        })
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write next page pointer
        writer.write_all(&self.next_page.to_be_bytes())?;
        
        // Write data
        writer.write_all(&self.data)?;
        
        Ok(())
    }
}


/// Represents a free page in the database.
#[derive(Debug, Clone)]
pub struct FreePage {
    /// Next free page number (0 if it is the last one).
    /// Free pages are also a linked list. This mimics the behaviour of MMAP in modern operating systems.
    /// However on memory allocators, the free list are a double linked list.
    pub next_page: u32,
    /// Page size in bytes.
    pub page_size: u32,
    /// Page number.
    pub page_number: u32,
}

impl FreePage {
    /// Creates a new free page.
    ///
    /// # Parameters
    /// * `next_page` - Number of the next free page (0 if it is the last one).
    /// * `page_size` - Size of the page in bytes.
    /// * `page_number` - Page number or page id.
    pub fn new(
        next_page: u32,
        page_size: u32,
        page_number: u32,
    ) -> Self {
        FreePage {
            next_page,
            page_size,
            page_number,
        }
    }
}

// Implementation for FreePage
impl ByteSerializable for FreePage {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read next page pointer
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        let next_page = u32::from_be_bytes(buffer);
        
        // Create free page
        // Note: page_size and page_number need to be set by the caller
        Ok(FreePage {
            next_page,
            page_size: 0,
            page_number: 0,
        })
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write next page pointer
        writer.write_all(&self.next_page.to_be_bytes())?;
        
        Ok(())
    }
}


/// Represents a generic page in the database of any type.
#[derive(Debug, Clone)]
pub enum Page {
    /// Btree page.
    BTree(BTreePage),
    /// Overflow page.
    Overflow(OverflowPage),
    /// Free page.
    Free(FreePage),
}

impl Page {
    /// Returns the page_number (Just a Java getter)
    pub fn page_number(&self) -> u32 {
        match self {
            Page::BTree(page) => page.page_number,
            Page::Overflow(page) => page.page_number,
            Page::Free(page) => page.page_number,
        }
    }

    /// Returns tha page size.
    pub fn page_size(&self) -> u32 {
        match self {
            Page::BTree(page) => page.page_size,
            Page::Overflow(page) => page.page_size,
            Page::Free(page) => page.page_size,
        }
    }
}


// Implementation for Page enum
impl ByteSerializable for Page {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read the first byte to determine the page type
        let mut buffer = [0u8; 1];
        reader.read_exact(&mut buffer)?;
        
        match buffer[0] {
            // B-Tree page types
            0x02 | 0x05 | 0x0A | 0x0D => {
                // Put back the first byte
                let mut combined_reader = std::io::Cursor::new(buffer.to_vec())
                    .chain(reader);
                    
                // Parse as B-Tree page
                let btree_page = BTreePage::read_from(&mut combined_reader)?;
                Ok(Page::BTree(btree_page))
            },
            // Overflow page
            0x10 => {
                let overflow_page = OverflowPage::read_from(reader)?;
                Ok(Page::Overflow(overflow_page))
            },
            // Free page
            0x00 => {
                let free_page = FreePage::read_from(reader)?;
                Ok(Page::Free(free_page))
            },
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid page type: {}", buffer[0]),
            )),
        }
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Page::BTree(btree_page) => btree_page.write_to(writer),
            Page::Overflow(overflow_page) => overflow_page.write_to(writer),
            Page::Free(free_page) => free_page.write_to(writer),
        }
    }
}


// Implementations for converting Page to BTreePage, OverflowPage and FreePage
impl From<Page> for BTreePage {
    fn from(page: Page) -> Self {
        match page {
            Page::BTree(btree_page) => btree_page,
            _ => panic!("Cannot convert to BTreePage: page is not of type BTree"),
        }
    }
}
// Implementation to convert Page to OverflowPage and FreePage
impl From<Page> for OverflowPage {
    fn from(page: Page) -> Self {
        match page {
            Page::Overflow(overflow_page) => overflow_page,
            _ => panic!("Cannot Convert to OverflowPage: page is not of type Overflow"),
        }
    }
}
// Implementation to convert from Page to FreePage
impl From<Page> for FreePage {
    fn from(page: Page) -> Self {
        match page {
            Page::Free(free_page) => free_page,
            _ => panic!("Cannot convert to FreePage: page is not of type Free"),
        }
    }
}

// The same traits to be able to convert to &BTreePage, &OverflowPage and &FreePage for &Page.
// This is useful to avoid copying the page when we only need a reference to it.
impl<'a> From<&'a Page> for &'a BTreePage {
    fn from(page: &'a Page) -> Self {
        match page {
            Page::BTree(btree_page) => btree_page,
            _ => panic!("Cannot convert to &BTreePage: page is not of type BTree"),
        }
    }
}
// Same for a &mut reference
impl<'a> From<&'a mut Page> for &'a mut BTreePage {
    fn from(page: &'a mut Page) -> Self {
        match page {
            Page::BTree(btree_page) => btree_page,
            _ => panic!("Cannot convert to &mut BTreePage: page is not of type BTree"),
        }
    }
}

// Same for OverflowPage and FreePage
impl<'a> From<&'a Page> for &'a OverflowPage {
    fn from(page: &'a Page) -> Self {
        match page {
            Page::Overflow(overflow_page) => overflow_page,
            _ => panic!("Cannot convert to &OverflowPage: page is not of type Overflow"),
        }
    }
}

impl<'a> From<&'a mut Page> for &'a mut OverflowPage {
    fn from(page: &'a mut Page) -> Self {
        match page {
            Page::Overflow(overflow_page) => overflow_page,
            _ => panic!("Cannot convert to &mut OverflowPage: page is not of type Overflow"),
        }
    }
}

impl<'a> From<&'a Page> for &'a FreePage {
    fn from(page: &'a Page) -> Self {
        match page {
            Page::Free(free_page) => free_page,
            _ => panic!("Cannot convert to &FreePage: page is not of type Free"),
        }
    }
}

impl<'a> From<&'a mut Page> for &'a mut FreePage {
    fn from(page: &'a mut Page) -> Self {
        match page {
            Page::Free(free_page) => free_page,
            _ => panic!("Cannot convert to &mut FreePage: page is not of type Free"),
        }
    }
}

/// Tests for the page module.
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_page_type_from_byte() {
        assert_eq!(PageType::from_byte(0x02), Some(PageType::IndexInterior));
        assert_eq!(PageType::from_byte(0x05), Some(PageType::TableInterior));
        assert_eq!(PageType::from_byte(0x0A), Some(PageType::IndexLeaf));
        assert_eq!(PageType::from_byte(0x0D), Some(PageType::TableLeaf));
        assert_eq!(PageType::from_byte(0x10), Some(PageType::Overflow));
        assert_eq!(PageType::from_byte(0x00), Some(PageType::Free)); 
    }

    #[test]
    fn test_page_type_properties() {
        assert!(PageType::IndexInterior.is_interior());
        assert!(PageType::TableInterior.is_interior());
        assert!(!PageType::IndexLeaf.is_interior());
        assert!(!PageType::TableLeaf.is_interior());

        assert!(!PageType::IndexInterior.is_leaf());
        assert!(!PageType::TableInterior.is_leaf());
        assert!(PageType::IndexLeaf.is_leaf());
        assert!(PageType::TableLeaf.is_leaf());

        assert!(PageType::IndexInterior.is_index());
        assert!(PageType::IndexLeaf.is_index());
        assert!(!PageType::TableInterior.is_index());
        assert!(!PageType::TableLeaf.is_index());

        assert!(!PageType::IndexInterior.is_table());
        assert!(!PageType::IndexLeaf.is_table());
        assert!(PageType::TableInterior.is_table());
        assert!(PageType::TableLeaf.is_table());
    }

    #[test]
    fn test_btree_page_header_new_leaf() {
        let header = BTreePageHeader::new_leaf(PageType::TableLeaf);
        assert_eq!(header.page_type, PageType::TableLeaf);
        assert_eq!(header.cell_count, 0);
        assert_eq!(header.right_most_page, None);
    }

    #[test]
    fn test_btree_page_header_new_interior() {
        let header = BTreePageHeader::new_interior(PageType::TableInterior, 42);
        assert_eq!(header.page_type, PageType::TableInterior);
        assert_eq!(header.cell_count, 0);
        assert_eq!(header.right_most_page, Some(42));
    }

    #[test]
    #[should_panic]
    fn test_btree_page_header_new_leaf_with_wrong_type() {
        BTreePageHeader::new_leaf(PageType::TableInterior);
    }

    #[test]
    #[should_panic]
    fn test_btree_page_header_new_interior_with_wrong_type() {
        BTreePageHeader::new_interior(PageType::TableLeaf, 42);
    }

    #[test]
    fn test_btree_page_header_size() {
        let leaf_header = BTreePageHeader::new_leaf(PageType::TableLeaf);
        assert_eq!(leaf_header.size(), 8);

        let interior_header = BTreePageHeader::new_interior(PageType::TableInterior, 42);
        assert_eq!(interior_header.size(), 12);
    }

    #[test]
    fn test_btree_page_header_serialization() {
        // Probar encabezado de página hoja
        let leaf_header = BTreePageHeader {
            page_type: PageType::TableLeaf,
            first_free_block_offset: 0x1234,
            cell_count: 42,
            content_start_offset: 0x5678,
            fragmented_free_bytes: 5,
            right_most_page: None,
        };

        let mut buffer = Vec::new();
        leaf_header.write_to(&mut buffer).unwrap();
        
        assert_eq!(buffer.len(), 8);
        assert_eq!(buffer[0], PageType::TableLeaf as u8);
        
        let mut cursor = Cursor::new(buffer);
        let read_header = BTreePageHeader::read_from(&mut cursor).unwrap();
        
        assert_eq!(read_header.page_type, PageType::TableLeaf);
        assert_eq!(read_header.first_free_block_offset, 0x1234);
        assert_eq!(read_header.cell_count, 42);
        assert_eq!(read_header.content_start_offset, 0x5678);
        assert_eq!(read_header.fragmented_free_bytes, 5);
        assert_eq!(read_header.right_most_page, None);

        // Probar encabezado de página interior
        let interior_header = BTreePageHeader {
            page_type: PageType::TableInterior,
            first_free_block_offset: 0x1234,
            cell_count: 42,
            content_start_offset: 0x5678,
            fragmented_free_bytes: 5,
            right_most_page: Some(0x12345678),
        };

        let mut buffer = Vec::new();
        interior_header.write_to(&mut buffer).unwrap();
        
        assert_eq!(buffer.len(), 12);
        assert_eq!(buffer[0], PageType::TableInterior as u8);
        
        let mut cursor = Cursor::new(buffer);
        let read_header = BTreePageHeader::read_from(&mut cursor).unwrap();
        
        assert_eq!(read_header.page_type, PageType::TableInterior);
        assert_eq!(read_header.first_free_block_offset, 0x1234);
        assert_eq!(read_header.cell_count, 42);
        assert_eq!(read_header.content_start_offset, 0x5678);
        assert_eq!(read_header.fragmented_free_bytes, 5);
        assert_eq!(read_header.right_most_page, Some(0x12345678));
    }

    #[test]
    fn test_btree_page_new() {
        // Crear una página hoja
        let leaf_page = BTreePage::new(
            PageType::TableLeaf,
            4096,
            1,
            0,
            None,
        ).unwrap();
        
        assert_eq!(leaf_page.header.page_type, PageType::TableLeaf);
        assert_eq!(leaf_page.header.right_most_page, None);
        assert_eq!(leaf_page.header.content_start_offset, 4096);
        assert_eq!(leaf_page.page_size, 4096);
        assert_eq!(leaf_page.page_number, 1);
        assert_eq!(leaf_page.reserved_space, 0);
        
        // Crear una página interior
        let interior_page = BTreePage::new(
            PageType::TableInterior,
            4096,
            2,
            0,
            Some(42),
        ).unwrap();
        
        assert_eq!(interior_page.header.page_type, PageType::TableInterior);
        assert_eq!(interior_page.header.right_most_page, Some(42));
        assert_eq!(interior_page.header.content_start_offset, 4096);
        assert_eq!(interior_page.page_size, 4096);
        assert_eq!(interior_page.page_number, 2);
        assert_eq!(interior_page.reserved_space, 0);
    }

    #[test]
    fn test_btree_page_with_invalid_parameters() {
        // Intentar crear una página hoja con right_most_page
        let result = BTreePage::new(
            PageType::TableLeaf,
            4096,
            1,
            0,
            Some(42),
        );
        
        assert!(result.is_err());
        
        // Intentar crear una página interior sin right_most_page
        let result = BTreePage::new(
            PageType::TableInterior,
            4096,
            2,
            0,
            None,
        );
        
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_page_free_space() {
        let mut page = BTreePage::new(
            PageType::TableLeaf,
            4096,
            1,
            0,
            None,
        ).unwrap();
        
        // Verificar espacio libre inicial
        let header_size = page.header.size();
        let initial_free_space = 4096 - HEADER_SIZE - header_size;
        assert_eq!(page.free_space(), initial_free_space);
        
        // Crear una celda TableLeaf dummy para pruebas
        let cell = BTreeCell::TableLeaf(TableLeafCell {
            payload_size: 10,
            row_id: 1,
            payload: vec![0; 10],
            overflow_page: None,
        });
        
        // Añadir la celda y verificar que el espacio libre disminuye
        page.add_cell(cell).unwrap();
        
        // Calcular el espacio que debería haberse utilizado
        let varint_size = crate::utils::varint_size(10); // payload_size
        let rowid_size = crate::utils::varint_size(1);   // row_id
        let payload_size = 10;                          // payload
        let cell_size = varint_size + rowid_size + payload_size;
        let cell_index_size = 2; // 2 bytes para el índice de la celda
        
        let expected_free_space = initial_free_space - cell_size - cell_index_size;
        assert_eq!(page.free_space(), expected_free_space);
    }

    #[test]
    fn test_overflow_page_new() {
        // Datos que caben en la página
        let data = vec![0; 4092]; // 4096 - 4 bytes para next_page
        let result = OverflowPage::new(0, data, 4096, 3);
        assert!(result.is_ok());
        
        // Datos demasiado grandes
        let data = vec![0; 4093]; // 1 byte más de lo que cabe
        let result = OverflowPage::new(0, data, 4096, 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_free_page_new() {
        let page = FreePage::new(42, 4096, 3);
        assert_eq!(page.next_page, 42);
        assert_eq!(page.page_size, 4096);
        assert_eq!(page.page_number, 3);
    }

    #[test]
    fn test_page_methods() {
        // Probar Page::page_number
        let btree_page = Page::BTree(
            BTreePage::new(PageType::TableLeaf, 4096, 1, 0, None).unwrap()
        );
        assert_eq!(btree_page.page_number(), 1);
        
        let overflow_page = Page::Overflow(
            OverflowPage::new(0, vec![0; 100], 4096, 2).unwrap()
        );
        assert_eq!(overflow_page.page_number(), 2);
        
        let free_page = Page::Free(
            FreePage::new(0, 4096, 3)
        );
        assert_eq!(free_page.page_number(), 3);
        
        // Probar Page::page_size
        assert_eq!(btree_page.page_size(), 4096);
        assert_eq!(overflow_page.page_size(), 4096);
        assert_eq!(free_page.page_size(), 4096);
    }
}
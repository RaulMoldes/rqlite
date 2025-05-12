//! # Page Module
//! 
//! Este módulo define las estructuras y tipos relacionados con las páginas de la base de datos.
//! En SQLite, una base de datos está dividida en páginas de tamaño fijo, y cada página puede
//! ser de diferentes tipos (B-Tree, overflow, free).

use std::fmt;
use std::io::{self, Read, Write};
use crate::header::HEADER_SIZE;

/// Tipos de página en una base de datos SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Página de índice interior (Interior del B-Tree de índice)
    IndexInterior = 0x02,
    /// Página de tabla interior (Interior del B-Tree de tabla)
    TableInterior = 0x05,
    /// Página de índice hoja (Hoja del B-Tree de índice)
    IndexLeaf = 0x0A,
    /// Página de tabla hoja (Hoja del B-Tree de tabla)
    TableLeaf = 0x0D,
    /// Página de desbordamiento (almacena datos que no caben en una página)
    Overflow = 0x10,
    /// Página libre (no utilizada)
    Free = 0x00,
}

impl PageType {
    /// Construye un `PageType` a partir de un byte.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x02 => Some(PageType::IndexInterior),
            0x05 => Some(PageType::TableInterior),
            0x0A => Some(PageType::IndexLeaf),
            0x0D => Some(PageType::TableLeaf),
            0x10 => Some(PageType::Overflow), // Añadido el caso para Overflow
            0x00 => Some(PageType::Free),
            _ => None, // No se puede distinguir automáticamente
        }
    }

    /// Devuelve si la página es una página interior.
    pub fn is_interior(&self) -> bool {
        matches!(self, PageType::IndexInterior | PageType::TableInterior)
    }

    /// Devuelve si la página es una página hoja.
    pub fn is_leaf(&self) -> bool {
        matches!(self, PageType::IndexLeaf | PageType::TableLeaf)
    }

    /// Devuelve si la página es parte de un índice.
    pub fn is_index(&self) -> bool {
        matches!(self, PageType::IndexInterior | PageType::IndexLeaf)
    }

    /// Devuelve si la página es parte de una tabla.
    pub fn is_table(&self) -> bool {
        matches!(self, PageType::TableInterior | PageType::TableLeaf)
    }
}

/// Representa el encabezado de una página B-Tree.
#[derive(Debug, Clone)]
pub struct BTreePageHeader {
    /// Tipo de página B-Tree.
    pub page_type: PageType,
    /// Desplazamiento hasta la primera celda libre.
    pub first_free_block_offset: u16,
    /// Número de celdas en la página.
    pub cell_count: u16,
    /// Offset del inicio del área de contenido celular.
    pub content_start_offset: u16,
    /// Número de bytes fragmentados dentro de la página.
    pub fragmented_free_bytes: u8,
    /// Para páginas interiores, el número de página del hijo más a la derecha.
    pub right_most_page: Option<u32>,
}

impl BTreePageHeader {
    /// Crea un nuevo encabezado para una página B-Tree hoja.
    pub fn new_leaf(page_type: PageType) -> Self {
        if !page_type.is_leaf() {
            panic!("Se esperaba un tipo de página hoja");
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

    /// Crea un nuevo encabezado para una página B-Tree interior.
    pub fn new_interior(page_type: PageType, right_most_page: u32) -> Self {
        if !page_type.is_interior() {
            panic!("Se esperaba un tipo de página interior");
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

    /// Calcula el tamaño del encabezado en bytes.
    pub fn size(&self) -> usize {
        if self.page_type.is_leaf() {
            8 // Páginas hoja: tipo (1) + first_free (2) + cell_count (2) + content_start (2) + fragmented_bytes (1)
        } else {
            12 // Páginas interiores: todo lo anterior + right_most_page (4)
        }
    }

    /// Lee un encabezado de página B-Tree desde un origen de datos.
    ///
    /// # Parámetros
    /// * `reader` - Origen de datos que implementa `Read`.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al leer los datos o si el formato no es válido.
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = [0u8; 12]; // Tamaño máximo del encabezado
        reader.read_exact(&mut buffer[0..1])?; // Leer el tipo de página

        let page_type = PageType::from_byte(buffer[0]).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Tipo de página B-Tree desconocido: {:#04x}", buffer[0]),
            )
        })?;

        // Leer campos comunes
        reader.read_exact(&mut buffer[1..8])?;
        let first_free_block_offset = u16::from_be_bytes([buffer[1], buffer[2]]);
        let cell_count = u16::from_be_bytes([buffer[3], buffer[4]]);
        let content_start_offset = u16::from_be_bytes([buffer[5], buffer[6]]);
        let fragmented_free_bytes = buffer[7];

        // Leer campo adicional para páginas interiores
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

    /// Escribe el encabezado de página B-Tree en un destino.
    ///
    /// # Parámetros
    /// * `writer` - Destino donde se escribirá el encabezado.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al escribir los datos.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Escribir el tipo de página
        writer.write_all(&[self.page_type as u8])?;

        // Escribir campos comunes
        writer.write_all(&self.first_free_block_offset.to_be_bytes())?;
        writer.write_all(&self.cell_count.to_be_bytes())?;
        writer.write_all(&self.content_start_offset.to_be_bytes())?;
        writer.write_all(&[self.fragmented_free_bytes])?;

        // Escribir campo adicional para páginas interiores
        if let Some(right_most) = self.right_most_page {
            writer.write_all(&right_most.to_be_bytes())?;
        }

        Ok(())
    }
}

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

/// Cada celda en una página B-Tree de tabla hoja contiene un número de campos.
#[derive(Debug, Clone)]
pub struct TableLeafCell {
    /// Tamaño del payload en bytes.
    pub payload_size: u64,
    /// ID de la fila (rowid).
    pub row_id: i64,
    /// Contenido del payload.
    pub payload: Vec<u8>,
    /// Referencia a página de overflow (si el payload no cabe en esta página).
    pub overflow_page: Option<u32>,
}

/// Cada celda en una página B-Tree de tabla interior contiene una clave y un hijo.
#[derive(Debug, Clone)]
pub struct TableInteriorCell {
    /// Número de página del hijo izquierdo.
    pub left_child_page: u32,
    /// Clave (rowid) que define el límite entre los hijos izquierdo y derecho.
    pub key: i64,
}

/// Cada celda en una página B-Tree de índice hoja contiene un payload.
#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    /// Tamaño del payload en bytes.
    pub payload_size: u64,
    /// Contenido del payload.
    pub payload: Vec<u8>,
    /// Referencia a página de overflow (si el payload no cabe en esta página).
    pub overflow_page: Option<u32>,
}

/// Cada celda en una página B-Tree de índice interior contiene un payload y un hijo.
#[derive(Debug, Clone)]
pub struct IndexInteriorCell {
    /// Número de página del hijo izquierdo.
    pub left_child_page: u32,
    /// Tamaño del payload en bytes.
    pub payload_size: u64,
    /// Contenido del payload.
    pub payload: Vec<u8>,
    /// Referencia a página de overflow (si el payload no cabe en esta página).
    pub overflow_page: Option<u32>,
}

/// Representa una celda de una página B-Tree, que puede ser de diferentes tipos.
#[derive(Debug, Clone)]
pub enum BTreeCell {
    /// Celda de tabla hoja.
    TableLeaf(TableLeafCell),
    /// Celda de tabla interior.
    TableInterior(TableInteriorCell),
    /// Celda de índice hoja.
    IndexLeaf(IndexLeafCell),
    /// Celda de índice interior.
    IndexInterior(IndexInteriorCell),
}

impl BTreeCell {
    /// Calcula el tamaño de la celda en bytes.
    pub fn size(&self) -> usize {
        match self {
            BTreeCell::TableLeaf(cell) => {
                let varint_size = crate::utils::varint_size(cell.payload_size as i64);
                let rowid_size = crate::utils::varint_size(cell.row_id);
                
                varint_size + rowid_size + cell.payload.len() + 
                    if cell.overflow_page.is_some() { 4 } else { 0 }
            },
            BTreeCell::TableInterior(cell) => {
                4 + crate::utils::varint_size(cell.key) // 4 bytes para left_child_page + tamaño de key
            },
            BTreeCell::IndexLeaf(cell) => {
                let varint_size = crate::utils::varint_size(cell.payload_size as i64);
                
                varint_size + cell.payload.len() + 
                    if cell.overflow_page.is_some() { 4 } else { 0 }
            },
            BTreeCell::IndexInterior(cell) => {
                let varint_size = crate::utils::varint_size(cell.payload_size as i64);
                
                4 + varint_size + cell.payload.len() + // 4 bytes para left_child_page + varint + payload
                    if cell.overflow_page.is_some() { 4 } else { 0 }
            },
        }
    }
}

/// Representa una página B-Tree de la base de datos.
#[derive(Debug, Clone)]
pub struct BTreePage {
    /// Encabezado de la página.
    pub header: BTreePageHeader,
    /// Vector de índices (offsets) de celdas.
    pub cell_indices: Vec<u16>,
    /// Vector de celdas.
    pub cells: Vec<BTreeCell>,
    /// Tamaño de la página en bytes.
    pub page_size: u32,
    /// Número de página.
    pub page_number: u32,
    /// Espacio reservado al final de cada página.
    pub reserved_space: u8,
}

impl BTreePage {
    /// Crea una nueva página B-Tree.
    ///
    /// # Parámetros
    /// * `page_type` - Tipo de la página B-Tree.
    /// * `page_size` - Tamaño de la página en bytes.
    /// * `page_number` - Número de página.
    /// * `reserved_space` - Espacio reservado al final de cada página.
    /// * `right_most_page` - Para páginas interiores, el número de página del hijo más a la derecha.
    ///
    /// # Errores
    /// Retorna un error si el tipo de página no es válido para el valor de right_most_page.
    pub fn new(
        page_type: PageType,
        page_size: u32,
        page_number: u32,
        reserved_space: u8,
        right_most_page: Option<u32>,
    ) -> io::Result<Self> {
        let header = if page_type.is_leaf() {
            if right_most_page.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Las páginas hoja no deben tener right_most_page",
                ));
            }
            BTreePageHeader::new_leaf(page_type)
        } else {
            if let Some(right_most) = right_most_page {
                BTreePageHeader::new_interior(page_type, right_most)
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Las páginas interiores deben tener right_most_page",
                ));
            }
        };

        // Inicializar el content_start_offset al tamaño de la página menos el espacio reservado
        let mut page = BTreePage {
            header,
            cell_indices: Vec::new(),
            cells: Vec::new(),
            page_size,
            page_number,
            reserved_space,
        };

        // Inicializar el content_start_offset
        page.update_content_start_offset();

        Ok(page)
    }

    /// Actualiza el offset de inicio de contenido.
    fn update_content_start_offset(&mut self) {
        self.header.content_start_offset = self.page_size as u16 - self.reserved_space as u16;
    }

    /// Añade una celda a la página B-Tree.
    ///
    /// # Parámetros
    /// * `cell` - Celda a añadir.
    ///
    /// # Errores
    /// Retorna un error si la celda no es compatible con el tipo de página o si no hay espacio.
    pub fn add_cell(&mut self, cell: BTreeCell) -> io::Result<()> {
        // Verificar compatibilidad de tipo
        match (&self.header.page_type, &cell) {
            (PageType::TableLeaf, BTreeCell::TableLeaf(_)) => {},
            (PageType::TableInterior, BTreeCell::TableInterior(_)) => {},
            (PageType::IndexLeaf, BTreeCell::IndexLeaf(_)) => {},
            (PageType::IndexInterior, BTreeCell::IndexInterior(_)) => {},
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Tipo de celda incompatible con el tipo de página: {:?}", self.header.page_type),
                ));
            }
        }

        // Calcular el espacio necesario para la celda
        let cell_size = cell.size();
        let cell_index_size = 2; // 2 bytes para el índice (offset) de la celda

        // Calcular espacio disponible
        let header_size = self.header.size();
        let cell_indices_size = self.cell_indices.len() * cell_index_size;
        let used_space = if self.page_number == 1 {
            HEADER_SIZE + header_size + cell_indices_size
        } else {
            header_size + cell_indices_size
        };

        // Espacio para nuevos datos
        let content_start = self.header.content_start_offset as usize;
        let available_space = content_start - used_space - cell_index_size; // Restar el espacio para el nuevo índice

        if cell_size > available_space {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("No hay suficiente espacio para la celda: necesita {} bytes, disponible {} bytes", 
                    cell_size, available_space),
            ));
        }

        // Actualizar el content_start_offset
        self.header.content_start_offset -= cell_size as u16;
        
        // Añadir el índice de la celda
        self.cell_indices.push(self.header.content_start_offset);
        
        // Añadir la celda
        self.cells.push(cell);
        
        // Actualizar el contador de celdas
        self.header.cell_count += 1;

        Ok(())
    }

    /// Devuelve el espacio libre en la página.
    pub fn free_space(&self) -> usize {
        let header_size = self.header.size();
        let cell_indices_size = self.cell_indices.len() * 2; // 2 bytes por índice
        
        let used_space = if self.page_number == 1 {
            HEADER_SIZE + header_size + cell_indices_size
        } else {
            header_size + cell_indices_size
        };
        
        let content_size = self.page_size as usize - self.header.content_start_offset as usize;
        
        self.page_size as usize - used_space - content_size - self.reserved_space as usize
    }
}

/// Representa una página de desbordamiento.
#[derive(Debug, Clone)]
pub struct OverflowPage {
    /// Número de la siguiente página de desbordamiento (0 si es la última).
    pub next_page: u32,
    /// Datos almacenados en esta página.
    pub data: Vec<u8>,
    /// Tamaño de la página en bytes.
    pub page_size: u32,
    /// Número de página.
    pub page_number: u32,
}

impl OverflowPage {
    /// Crea una nueva página de desbordamiento.
    ///
    /// # Parámetros
    /// * `next_page` - Número de la siguiente página de desbordamiento (0 si es la última).
    /// * `data` - Datos a almacenar en la página.
    /// * `page_size` - Tamaño de la página en bytes.
    /// * `page_number` - Número de página.
    ///
    /// # Errores
    /// Retorna un error si los datos no caben en la página.
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
                format!("Datos demasiado grandes para la página: {} bytes, máximo {} bytes",
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

/// Representa una página libre.
#[derive(Debug, Clone)]
pub struct FreePage {
    /// Número de la siguiente página libre (0 si es la última).
    pub next_page: u32,
    /// Tamaño de la página en bytes.
    pub page_size: u32,
    /// Número de página.
    pub page_number: u32,
}

impl FreePage {
    /// Crea una nueva página libre.
    ///
    /// # Parámetros
    /// * `next_page` - Número de la siguiente página libre (0 si es la última).
    /// * `page_size` - Tamaño de la página en bytes.
    /// * `page_number` - Número de página.
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

/// Representa una página genérica de la base de datos SQLite.
#[derive(Debug, Clone)]
pub enum Page {
    /// Página B-Tree (tabla o índice).
    BTree(BTreePage),
    /// Página de desbordamiento.
    Overflow(OverflowPage),
    /// Página libre.
    Free(FreePage),
}

impl Page {
    /// Devuelve el número de página.
    pub fn page_number(&self) -> u32 {
        match self {
            Page::BTree(page) => page.page_number,
            Page::Overflow(page) => page.page_number,
            Page::Free(page) => page.page_number,
        }
    }

    /// Devuelve el tamaño de la página en bytes.
    pub fn page_size(&self) -> u32 {
        match self {
            Page::BTree(page) => page.page_size,
            Page::Overflow(page) => page.page_size,
            Page::Free(page) => page.page_size,
        }
    }
}

// Implementaciones de From<Page> para los diferentes tipos de páginas
impl From<Page> for BTreePage {
    fn from(page: Page) -> Self {
        match page {
            Page::BTree(btree_page) => btree_page,
            _ => panic!("No se puede convertir a BTreePage: la página no es de tipo BTree"),
        }
    }
}

impl From<Page> for OverflowPage {
    fn from(page: Page) -> Self {
        match page {
            Page::Overflow(overflow_page) => overflow_page,
            _ => panic!("No se puede convertir a OverflowPage: la página no es de tipo Overflow"),
        }
    }
}

impl From<Page> for FreePage {
    fn from(page: Page) -> Self {
        match page {
            Page::Free(free_page) => free_page,
            _ => panic!("No se puede convertir a FreePage: la página no es de tipo Free"),
        }
    }
}

// También necesitamos implementar From<Page> para las referencias a estos tipos
impl<'a> From<&'a Page> for &'a BTreePage {
    fn from(page: &'a Page) -> Self {
        match page {
            Page::BTree(btree_page) => btree_page,
            _ => panic!("No se puede convertir a &BTreePage: la página no es de tipo BTree"),
        }
    }
}

impl<'a> From<&'a mut Page> for &'a mut BTreePage {
    fn from(page: &'a mut Page) -> Self {
        match page {
            Page::BTree(btree_page) => btree_page,
            _ => panic!("No se puede convertir a &mut BTreePage: la página no es de tipo BTree"),
        }
    }
}

// Implementaciones similares para OverflowPage y FreePage
impl<'a> From<&'a Page> for &'a OverflowPage {
    fn from(page: &'a Page) -> Self {
        match page {
            Page::Overflow(overflow_page) => overflow_page,
            _ => panic!("No se puede convertir a &OverflowPage: la página no es de tipo Overflow"),
        }
    }
}

impl<'a> From<&'a mut Page> for &'a mut OverflowPage {
    fn from(page: &'a mut Page) -> Self {
        match page {
            Page::Overflow(overflow_page) => overflow_page,
            _ => panic!("No se puede convertir a &mut OverflowPage: la página no es de tipo Overflow"),
        }
    }
}

impl<'a> From<&'a Page> for &'a FreePage {
    fn from(page: &'a Page) -> Self {
        match page {
            Page::Free(free_page) => free_page,
            _ => panic!("No se puede convertir a &FreePage: la página no es de tipo Free"),
        }
    }
}

impl<'a> From<&'a mut Page> for &'a mut FreePage {
    fn from(page: &'a mut Page) -> Self {
        match page {
            Page::Free(free_page) => free_page,
            _ => panic!("No se puede convertir a &mut FreePage: la página no es de tipo Free"),
        }
    }
}

/// Módulo de pruebas.
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
        assert_eq!(PageType::from_byte(0x00), None); // No se puede distinguir automáticamente
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
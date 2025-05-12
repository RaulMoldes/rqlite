//! # Pager Module
//! 
//! Este módulo implementa el sistema de paginación que permite cargar y gestionar
//! páginas de la base de datos en memoria.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use super::disk::DiskManager;
use crate::page::{BTreePage, BTreePageHeader, OverflowPage, FreePage, Page, PageType};
use crate::header::Header;

/// Gestiona la carga y caché de páginas de la base de datos.
///
/// El Pager actúa como intermediario entre el sistema de almacenamiento y el
/// motor de base de datos, gestionando qué páginas están en memoria y sincronizando
/// los cambios con el disco.
pub struct Pager {
    /// Gestor de operaciones de disco.
    disk_manager: DiskManager,
    /// Caché de páginas cargadas.
    page_cache: HashMap<u32, Page>,
    /// Tamaño de página en bytes.
    page_size: u32,
    /// Espacio reservado al final de cada página.
    reserved_space: u8,
    /// Indica si hay cambios pendientes que necesitan ser escritos a disco.
    dirty: bool,
}

impl Pager {
    /// Abre un archivo de base de datos existente.
    ///
    /// # Parámetros
    /// * `path` - Ruta al archivo de base de datos.
    ///
    /// # Errores
    /// Retorna un error si el archivo no existe, no se puede abrir, o no es un archivo SQLite válido.
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

    /// Crea un nuevo archivo de base de datos.
    ///
    /// # Parámetros
    /// * `path` - Ruta donde crear el archivo.
    /// * `page_size` - Tamaño de página en bytes.
    /// * `reserved_space` - Espacio reservado al final de cada página.
    ///
    /// # Errores
    /// Retorna un error si no se puede crear el archivo o escribir en él.
    pub fn create<P: AsRef<Path>>(path: P, page_size: u32, reserved_space: u8) -> io::Result<Self> {
        let mut disk_manager = DiskManager::create(path, page_size)?;
        
        // Actualizar el encabezado con el espacio reservado
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

    /// Obtiene el encabezado de la base de datos.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al leer los datos o si el encabezado no es válido.
    pub fn get_header(&mut self) -> io::Result<Header> {
        self.disk_manager.read_header()
    }

    /// Actualiza el encabezado de la base de datos.
    ///
    /// # Parámetros
    /// * `header` - Nuevo encabezado.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al escribir los datos.
    pub fn update_header(&mut self, header: &Header) -> io::Result<()> {
        self.disk_manager.write_header(header)?;
        self.dirty = true;
        Ok(())
    }

    // Obtiene una página de la base de datos, cargándola desde disco si es necesario.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página a obtener.
    /// * `page_type` - Tipo esperado de la página.
    ///
    /// # Errores
    /// Retorna un error si la página no existe, no se puede leer, o si el tipo no coincide.
    pub fn get_page(&mut self, page_number: u32, page_type: Option<PageType>) -> io::Result<&Page> {
        if !self.page_cache.contains_key(&page_number) {
            // Cargar la página desde disco
            self.load_page(page_number)?;
        }
        
        // Verificar el tipo de la página si se especificó
        if let Some(expected_type) = page_type {
            match self.page_cache.get(&page_number) {
                Some(Page::BTree(btree_page)) => {
                    if btree_page.header.page_type != expected_type {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Tipo de página incorrecto: esperado {:?}, obtenido {:?}",
                                expected_type, btree_page.header.page_type),
                        ));
                    }
                },
                Some(Page::Overflow(_)) => {
                    if expected_type != PageType::Overflow {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Tipo de página incorrecto: esperado {:?}, obtenido Overflow",
                                expected_type),
                        ));
                    }
                },
                Some(Page::Free(_)) => {
                    if expected_type != PageType::Free {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Tipo de página incorrecto: esperado {:?}, obtenido Free",
                                expected_type),
                        ));
                    }
                },
                None => unreachable!("La página debe estar en la caché en este punto"),
            }
        }
        
        // Marcar la página como sucia (modificada)
        self.dirty = true;
        
        // Devolver la referencia a la página
        self.page_cache.get(&page_number)
            .ok_or_else(|| io::Error::new(
                io::ErrorKind::NotFound,
                format!("Página no encontrada: {}", page_number),
            ))
    }

    /// Obtiene una página mutable de la base de datos, cargándola desde disco si es necesario.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página a obtener.
    /// * `page_type` - Tipo esperado de la página.
    ///
    /// # Errores
    /// Retorna un error si la página no existe, no se puede leer, o si el tipo no coincide.
    pub fn get_page_mut(&mut self, page_number: u32, page_type: Option<PageType>) -> io::Result<&mut Page> {
        if !self.page_cache.contains_key(&page_number) {
            // Cargar la página desde disco
            self.load_page(page_number)?;
        }
        
        // Verificar el tipo de la página si se especificó
        if let Some(expected_type) = page_type {
            match self.page_cache.get(&page_number) {
                Some(Page::BTree(btree_page)) => {
                    if btree_page.header.page_type != expected_type {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Tipo de página incorrecto: esperado {:?}, obtenido {:?}",
                                expected_type, btree_page.header.page_type),
                        ));
                    }
                },
                Some(Page::Overflow(_)) => {
                    if expected_type != PageType::Overflow {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Tipo de página incorrecto: esperado {:?}, obtenido Overflow",
                                expected_type),
                        ));
                    }
                },
                Some(Page::Free(_)) => {
                    if expected_type != PageType::Free {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Tipo de página incorrecto: esperado {:?}, obtenido Free",
                                expected_type),
                        ));
                    }
                },
                None => unreachable!("La página debe estar en la caché en este punto"),
            }
        }
        
        // Marcar la página como sucia (modificada)
        self.dirty = true;
        
        // Devolver la referencia mutable a la página
        self.page_cache.get_mut(&page_number)
            .ok_or_else(|| io::Error::new(
                io::ErrorKind::NotFound,
                format!("Página no encontrada: {}", page_number),
            ))
    }
    /// Carga una página desde el disco en la caché.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página a cargar.
    ///
    /// # Errores
    /// Retorna un error si la página no existe o no se puede leer.
    fn load_page(&mut self, page_number: u32) -> io::Result<()> {
        // Verificar si la página existe
        let page_count = self.disk_manager.page_count()?;
        if page_number > page_count {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Página fuera de rango: {}, máximo {}", page_number, page_count),
            ));
        }
        
        // Leer la página desde el disco
        let mut buffer = vec![0u8; self.page_size as usize];
        self.disk_manager.read_page(page_number, &mut buffer)?;
        
        // Interpretar el tipo de página y crear la estructura adecuada
        let page = if page_number == 1 || self.is_btree_page(&buffer)? {
            self.parse_btree_page(page_number, &buffer)?
        } else if self.is_overflow_page(&buffer)? {
            self.parse_overflow_page(page_number, &buffer)?
        } else {
            self.parse_free_page(page_number, &buffer)?
        };
        
        // Agregar la página a la caché
        self.page_cache.insert(page_number, page);
        
        Ok(())
    }

    
    /// Determina si una página es una página B-Tree.
    ///
    /// # Parámetros
    /// * `buffer` - Datos de la página.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al interpretar los datos.
    ///
    /// # Retorno
    /// `true` si la página es una página B-Tree, `false` en caso contrario.
    fn is_btree_page(&self, buffer: &[u8]) -> io::Result<bool> {
        if buffer.len() < 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer demasiado pequeño para determinar el tipo de página",
            ));
        }
        
        // Los tipos de página B-Tree son 0x02, 0x05, 0x0A y 0x0D
        match buffer[0] {
            0x02 | 0x05 | 0x0A | 0x0D => Ok(true),
            _ => Ok(false),
        }
    }

    /// Determina si una página es una página de desbordamiento.
    ///
    /// # Parámetros
    /// * `buffer` - Datos de la página.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al interpretar los datos.
    ///
    /// # Retorno
    /// `true` si la página es una página de desbordamiento, `false` en caso contrario.
    fn is_overflow_page(&self, buffer: &[u8]) -> io::Result<bool> {
        // No hay una forma directa de determinar si una página es de desbordamiento,
        // se necesitaría información adicional. En una implementación real, esto
        // dependería de la estructura de la base de datos y de cómo se gestiona la lista
        // de desbordamiento.
        
        // Para simplificar, asumimos que no es una página de desbordamiento
        Ok(false)
    }

    /// Parsea una página B-Tree.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página.
    /// * `buffer` - Datos de la página.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al interpretar los datos.
    ///
    /// # Retorno
    /// La página B-Tree parseada.
    fn parse_btree_page(&self, page_number: u32, buffer: &[u8]) -> io::Result<Page> {
        use std::io::Cursor;
        
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

    /// Parsea una página de desbordamiento.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página.
    /// * `buffer` - Datos de la página.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al interpretar los datos.
    ///
    /// # Retorno
    /// La página de desbordamiento parseada.
    fn parse_overflow_page(&self, page_number: u32, buffer: &[u8]) -> io::Result<Page> {
        if buffer.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer demasiado pequeño para una página de desbordamiento",
            ));
        }
        
        // Los primeros 4 bytes contienen el número de la siguiente página
        let next_page = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        
        // El resto son los datos
        let data = buffer[4..].to_vec();
        
        let overflow_page = OverflowPage::new(
            next_page,
            data,
            self.page_size,
            page_number,
        )?;
        
        Ok(Page::Overflow(overflow_page))
    }

    /// Parsea una página libre.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página.
    /// * `buffer` - Datos de la página.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al interpretar los datos.
    ///
    /// # Retorno
    /// La página libre parseada.
    fn parse_free_page(&self, page_number: u32, buffer: &[u8]) -> io::Result<Page> {
        if buffer.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Buffer demasiado pequeño para una página libre",
            ));
        }
        
        // Los primeros 4 bytes contienen el número de la siguiente página libre
        let next_page = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        
        let free_page = FreePage::new(
            next_page,
            self.page_size,
            page_number,
        );
        
        Ok(Page::Free(free_page))
    }

    /// Crea una nueva página B-Tree.
    ///
    /// # Parámetros
    /// * `page_type` - Tipo de la página B-Tree.
    /// * `right_most_page` - Para páginas interiores, el número de página del hijo más a la derecha.
    ///
    /// # Errores
    /// Retorna un error si no se puede asignar la página o si el tipo no es válido.
    ///
    /// # Retorno
    /// Número de la página creada.
    pub fn create_btree_page(&mut self, page_type: PageType, right_most_page: Option<u32>) -> io::Result<u32> {
        // Asignar una nueva página
        let page_number = self.disk_manager.allocate_pages(1)?;
        
        // Crear la página B-Tree
        let btree_page = BTreePage::new(
            page_type,
            self.page_size,
            page_number,
            self.reserved_space,
            right_most_page,
        )?;
        
        // Agregar la página a la caché
        self.page_cache.insert(page_number, Page::BTree(btree_page));
        self.dirty = true;
        
        Ok(page_number)
    }

    /// Crea una nueva página de desbordamiento.
    ///
    /// # Parámetros
    /// * `next_page` - Número de la siguiente página de desbordamiento (0 si es la última).
    /// * `data` - Datos a almacenar en la página.
    ///
    /// # Errores
    /// Retorna un error si no se puede asignar la página o si los datos son demasiado grandes.
    ///
    /// # Retorno
    /// Número de la página creada.
    pub fn create_overflow_page(&mut self, next_page: u32, data: Vec<u8>) -> io::Result<u32> {
        // Verificar el tamaño de los datos
        let max_data_size = self.page_size as usize - 4; // 4 bytes para next_page
        if data.len() > max_data_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Datos demasiado grandes para la página: {} bytes, máximo {} bytes",
                    data.len(), max_data_size),
            ));
        }
        
        // Asignar una nueva página
        let page_number = self.disk_manager.allocate_pages(1)?;
        
        // Crear la página de desbordamiento
        let overflow_page = OverflowPage::new(
            next_page,
            data,
            self.page_size,
            page_number,
        )?;
        
        // Agregar la página a la caché
        self.page_cache.insert(page_number, Page::Overflow(overflow_page));
        self.dirty = true;
        
        Ok(page_number)
    }

    /// Crea una nueva página libre.
    ///
    /// # Parámetros
    /// * `next_page` - Número de la siguiente página libre (0 si es la última).
    ///
    /// # Errores
    /// Retorna un error si no se puede asignar la página.
    ///
    /// # Retorno
    /// Número de la página creada.
    pub fn create_free_page(&mut self, next_page: u32) -> io::Result<u32> {
        // Asignar una nueva página
        let page_number = self.disk_manager.allocate_pages(1)?;
        
        // Crear la página libre
        let free_page = FreePage::new(
            next_page,
            self.page_size,
            page_number,
        );
        
        // Agregar la página a la caché
        self.page_cache.insert(page_number, Page::Free(free_page));
        self.dirty = true;
        
        Ok(page_number)
    }

    /// Guarda todas las páginas modificadas en disco.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al escribir los datos.
    pub fn flush(&mut self) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        
        // Guardar cada página en la caché
        for (page_number, page) in &self.page_cache {
            // Serializar la página
            let buffer = self.serialize_page(page)?;
            
            // Escribir la página en disco
            self.disk_manager.write_page(*page_number, &buffer)?;
        }
        
        // Sincronizar con el disco
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
                
                // En una implementación completa, aquí se escribirían las celdas y demás datos
            },
            Page::Overflow(overflow_page) => {
                // Escribir el número de la siguiente página
                let next_page_bytes = overflow_page.next_page.to_be_bytes();
                buffer[0..4].copy_from_slice(&next_page_bytes);
                
                // Escribir los datos
                let data_len = overflow_page.data.len().min(self.page_size as usize - 4);
                buffer[4..4 + data_len].copy_from_slice(&overflow_page.data[0..data_len]);
            },
            Page::Free(free_page) => {
                // Escribir el número de la siguiente página libre
                let next_page_bytes = free_page.next_page.to_be_bytes();
                buffer[0..4].copy_from_slice(&next_page_bytes);
            },
        }
        
        Ok(buffer)
    }

    /// Obtiene el número de páginas en la base de datos.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al obtener los metadatos.
    ///
    /// # Retorno
    /// Número total de páginas.
    pub fn page_count(&self) -> io::Result<u32> {
        self.disk_manager.page_count()
    }

    /// Cierra el pager, guardando todos los cambios pendientes.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al guardar los datos.
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
        assert_eq!(page_number, 2); // La primera página es el encabezado
        
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
        assert_eq!(pager.page_count().unwrap(), 1);
        
        // Crear una página B-Tree
        pager.create_btree_page(PageType::TableLeaf, None).unwrap();
        
        // Verificar que ahora hay dos páginas
        assert_eq!(pager.page_count().unwrap(), 2);
    }
}
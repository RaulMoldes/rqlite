//! # Disk Module
//! 
//! Este módulo proporciona funcionalidades para acceder al sistema de archivos
//! y realizar operaciones de lectura y escritura en el archivo de base de datos.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::header::{Header, HEADER_SIZE, SQLITE_HEADER_STRING};

/// Gestiona las operaciones de bajo nivel sobre el archivo de base de datos.
///
/// Esta estructura se encarga de abrir, cerrar, leer y escribir en el archivo
/// de base de datos de SQLite, proporcionando una interfaz para acceder a páginas
/// específicas del archivo.
pub struct DiskManager {
    /// Ruta al archivo de base de datos.
    pub path: PathBuf,
    /// Manejador del archivo.
    file: File,
    /// Tamaño de página en bytes.
    page_size: u32,
}

impl DiskManager {
    /// Abre un archivo de base de datos existente.
    ///
    /// # Parámetros
    /// * `path` - Ruta al archivo de base de datos.
    ///
    /// # Errores
    /// Retorna un error si el archivo no existe, no se puede abrir, o no es un archivo SQLite válido.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        
        let mut disk_manager = DiskManager {
            path: path.as_ref().to_path_buf(),
            file,
            page_size: 0, // Se actualizará después
        };
        
        // Leer el encabezado para obtener el tamaño de página
        let header = disk_manager.read_header()?;
        disk_manager.page_size = header.page_size;
        
        Ok(disk_manager)
    }

    /// Crea un nuevo archivo de base de datos.
    ///
    /// # Parámetros
    /// * `path` - Ruta donde crear el archivo.
    /// * `page_size` - Tamaño de página en bytes.
    ///
    /// # Errores
    /// Retorna un error si no se puede crear el archivo o escribir en él.
    pub fn create<P: AsRef<Path>>(path: P, page_size: u32) -> io::Result<Self> {
        // Crear el archivo
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        
        let mut disk_manager = DiskManager {
            path: path.as_ref().to_path_buf(),
            file,
            page_size,
        };
        
        // Crear y escribir el encabezado
        let header = Header::with_page_size(page_size)?;
        disk_manager.write_header(&header)?;
        
        // Escribir la primera página completa (necesario para que SQLite considere el archivo válido)
        disk_manager.allocate_pages(1)?;
        
        Ok(disk_manager)
    }

    /// Lee el encabezado de la base de datos.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al leer los datos o si el encabezado no es válido.
    pub fn read_header(&mut self) -> io::Result<Header> {
        self.file.seek(SeekFrom::Start(0))?;
        
        // Verificar primero la firma
        let mut signature = [0u8; 16];
        self.file.read_exact(&mut signature)?;
        
        if &signature != SQLITE_HEADER_STRING {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No es un archivo SQLite válido: firma incorrecta",
            ));
        }
        
        // Volver al inicio para leer el encabezado completo
        self.file.seek(SeekFrom::Start(0))?;
        Header::read_from(&mut self.file)
    }

    /// Escribe el encabezado de la base de datos.
    ///
    /// # Parámetros
    /// * `header` - Encabezado a escribir.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al escribir los datos.
    pub fn write_header(&mut self, header: &Header) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        header.write_to(&mut self.file)
    }

    /// Lee una página completa desde el archivo.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página a leer (comenzando desde 1).
    /// * `buffer` - Buffer donde se escribirán los datos leídos.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al leer los datos o si el número de página es inválido.
    pub fn read_page(&mut self, page_number: u32, buffer: &mut [u8]) -> io::Result<()> {
        if page_number == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Número de página inválido: 0 (las páginas comienzan desde 1)",
            ));
        }
        
        let offset = self.page_offset(page_number);
        self.file.seek(SeekFrom::Start(offset))?;
        
        if buffer.len() != self.page_size as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Tamaño de buffer incorrecto: esperado {}, obtenido {}", 
                    self.page_size, buffer.len()),
            ));
        }
        
        self.file.read_exact(buffer)
    }

    /// Escribe una página completa en el archivo.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página a escribir (comenzando desde 1).
    /// * `buffer` - Buffer con los datos a escribir.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al escribir los datos o si el número de página es inválido.
    pub fn write_page(&mut self, page_number: u32, buffer: &[u8]) -> io::Result<()> {
        if page_number == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Número de página inválido: 0 (las páginas comienzan desde 1)",
            ));
        }
        
        let offset = self.page_offset(page_number);
        self.file.seek(SeekFrom::Start(offset))?;
        
        if buffer.len() != self.page_size as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Tamaño de buffer incorrecto: esperado {}, obtenido {}", 
                    self.page_size, buffer.len()),
            ));
        }
        
        self.file.write_all(buffer)
    }

    /// Asigna nuevas páginas al final del archivo.
    ///
    /// # Parámetros
    /// * `count` - Número de páginas a asignar.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al escribir en el archivo.
    ///
    /// # Retorno
    /// Número de la primera página asignada.
    pub fn allocate_pages(&mut self, count: u32) -> io::Result<u32> {
        // Obtener el tamaño actual del archivo
        let file_size = self.file.metadata()?.len();
        
        // Calcular el número de página actual
        let current_pages = (file_size + self.page_size as u64 - 1) / self.page_size as u64;
        let first_new_page = current_pages as u32 + 1;
        
        // Calcular el nuevo tamaño del archivo
        let new_size = file_size + (count as u64 * self.page_size as u64);
        
        // Cambiar el tamaño del archivo
        self.file.set_len(new_size)?;
        
        // Inicializar las nuevas páginas con ceros
        let zeros = vec![0u8; self.page_size as usize];
        for page_number in first_new_page..(first_new_page + count) {
            self.write_page(page_number, &zeros)?;
        }
        
        // Actualizar el número de páginas en el encabezado
        let mut header = self.read_header()?;
        header.database_size = first_new_page + count - 1;
        self.write_header(&header)?;
        
        Ok(first_new_page)
    }

    /// Calcula el desplazamiento en bytes para una página específica.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página (comenzando desde 1).
    ///
    /// # Retorno
    /// Desplazamiento en bytes desde el inicio del archivo.
    fn page_offset(&self, page_number: u32) -> u64 {
        (page_number as u64 - 1) * self.page_size as u64
    }

    /// Obtiene el tamaño actual de la base de datos en páginas.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al obtener los metadatos del archivo.
    ///
    /// # Retorno
    /// Número total de páginas en la base de datos.
    pub fn page_count(&self) -> io::Result<u32> {
        let file_size = self.file.metadata()?.len();
        Ok((file_size / self.page_size as u64) as u32)
    }

    /// Sincroniza los cambios al disco, asegurando que todos los datos sean escritos.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al sincronizar.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_create_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        let result = DiskManager::create(&db_path, 4096);
        assert!(result.is_ok());
        
        // Verificar que el archivo existe
        assert!(db_path.exists());
        
        // Verificar que el tamaño del archivo es al menos el de una página
        let metadata = fs::metadata(&db_path).unwrap();
        assert!(metadata.len() >= 4096);
    }

    #[test]
    fn test_open_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        {
            let _disk_manager = DiskManager::create(&db_path, 4096).unwrap();
        }
        
        // Abrir la base de datos existente
        let result = DiskManager::open(&db_path);
        assert!(result.is_ok());
        
        let disk_manager = result.unwrap();
        assert_eq!(disk_manager.page_size, 4096);
    }

    #[test]
    fn test_read_write_header() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        let mut disk_manager = DiskManager::create(&db_path, 4096).unwrap();
        
        // Leer el encabezado
        let mut header = disk_manager.read_header().unwrap();
        assert_eq!(header.page_size, 4096);
        
        // Modificar el encabezado
        header.user_version = 42;
        
        // Escribir el encabezado modificado
        disk_manager.write_header(&header).unwrap();
        
        // Leer de nuevo y verificar
        let header2 = disk_manager.read_header().unwrap();
        assert_eq!(header2.user_version, 42);
    }

    #[test]
    fn test_read_write_page() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        let mut disk_manager = DiskManager::create(&db_path, 4096).unwrap();
        
        // Preparar datos para escribir
        let mut data = vec![0u8; 4096];
        for i in 0..100 {
            data[i] = i as u8;
        }
        
        // Escribir en la página 1
        disk_manager.write_page(1, &data).unwrap();
        
        // Leer de nuevo
        let mut buffer = vec![0u8; 4096];
        disk_manager.read_page(1, &mut buffer).unwrap();
        
        // Verificar que los datos coinciden
        assert_eq!(&buffer[0..100], &data[0..100]);
    }

    #[test]
    fn test_allocate_pages() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        let mut disk_manager = DiskManager::create(&db_path, 4096).unwrap();
        
        // Verificar que solo hay una página
        assert_eq!(disk_manager.page_count().unwrap(), 1);
        
        // Asignar 2 páginas más
        let first_new_page = disk_manager.allocate_pages(2).unwrap();
        assert_eq!(first_new_page, 2);
        
        // Verificar que ahora hay 3 páginas
        assert_eq!(disk_manager.page_count().unwrap(), 3);
        
        // Verificar que el encabezado refleja el nuevo tamaño
        let header = disk_manager.read_header().unwrap();
        assert_eq!(header.database_size, 3);
    }

    #[test]
    fn test_invalid_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        
        // Crear una base de datos
        let mut disk_manager = DiskManager::create(&db_path, 4096).unwrap();
        
        // Intentar leer la página 0 (inválido)
        let mut buffer = vec![0u8; 4096];
        let result = disk_manager.read_page(0, &mut buffer);
        assert!(result.is_err());
        
        // Intentar escribir la página 0 (inválido)
        let result = disk_manager.write_page(0, &buffer);
        assert!(result.is_err());
        
        // Intentar leer con un buffer de tamaño incorrecto
        let mut small_buffer = vec![0u8; 2048];
        let result = disk_manager.read_page(1, &mut small_buffer);
        assert!(result.is_err());
        
        // Intentar escribir con un buffer de tamaño incorrecto
        let result = disk_manager.write_page(1, &small_buffer);
        assert!(result.is_err());
    }
}
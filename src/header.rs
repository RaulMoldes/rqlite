//! # Header Module
//! 
//! Este módulo implementa la estructura del encabezado de un archivo de base de datos SQLite.
//! El encabezado ocupa los primeros 100 bytes del archivo y contiene metadatos importantes
//! sobre la estructura y el estado de la base de datos.

use std::fmt;
use std::io::{self, Read, Write};

/// Tamaño del encabezado en bytes.
pub const HEADER_SIZE: usize = 100;

/// Cadena de inicio que identifica un archivo SQLite válido.
pub const SQLITE_HEADER_STRING: &[u8; 16] = b"SQLite format 3\0";

/// Representa el encabezado de un archivo de base de datos SQLite.
///
/// El encabezado contiene información importante sobre la configuración de la base de datos,
/// como el tamaño de página, versiones del formato, y diversas configuraciones y contadores.
#[derive(Debug, Clone)]
pub struct Header {
    /// Tamaño de página en bytes. Debe ser una potencia de 2 entre 512 y 65536.
    pub page_size: u32,
    /// Versión del formato para escritura.
    pub write_version: u8,
    /// Versión del formato para lectura.
    pub read_version: u8,
    /// Bytes reservados al final de cada página.
    pub reserved_space: u8,
    /// Máximo orden de empaquetado de entradas fraccionales.
    pub max_payload_fraction: u8,
    /// Mínimo orden de empaquetado de entradas fraccionales.
    pub min_payload_fraction: u8,
    /// Orden de empaquetado de hoja.
    pub leaf_payload_fraction: u8,
    /// Número de cambios al archivo.
    pub change_counter: u32,
    /// Tamaño de la base de datos en páginas.
    pub database_size: u32,
    /// Número de página de la primera página de la lista libre.
    pub first_freelist_trunk_page: u32,
    /// Número total de páginas en la lista libre.
    pub freelist_pages: u32,
    /// Cookie del esquema.
    pub schema_cookie: u32,
    /// Número de formato del esquema.
    pub schema_format_number: u32,
    /// Tamaño de caché por defecto.
    pub default_cache_size: u32,
    /// Número de página de la raíz del árbol más grande de autoincrementos.
    pub largest_root_btree_page: u32,
    /// Codificación de texto de la base de datos.
    pub text_encoding: u32,
    /// Versión de usuario.
    pub user_version: u32,
    /// Modo para vacío incremental.
    pub incremental_vacuum_mode: u32,
    /// ID de aplicación.
    pub application_id: u32,
    /// Reservado para expansión futura.
    pub reserved: [u8; 20],
    /// Número de versión de SQLite que modificó la base de datos por última vez.
    pub version_valid_for: u32,
    /// Número de versión de SQLite.
    pub sqlite_version_number: u32,
}

impl Default for Header {
    /// Crea un encabezado con valores predeterminados.
    fn default() -> Self {
        Header {
            page_size: 4096,
            write_version: 1,
            read_version: 1,
            reserved_space: 0,
            max_payload_fraction: 64,
            min_payload_fraction: 32,
            leaf_payload_fraction: 32,
            change_counter: 0,
            database_size: 0,
            first_freelist_trunk_page: 0,
            freelist_pages: 0,
            schema_cookie: 0,
            schema_format_number: 4,
            default_cache_size: 0,
            largest_root_btree_page: 0,
            text_encoding: 1, // 1 = UTF-8
            user_version: 0,
            incremental_vacuum_mode: 0,
            application_id: 0,
            reserved: [0; 20],
            version_valid_for: 0,
            sqlite_version_number: 0,
        }
    }
}

impl Header {
    /// Crea un nuevo encabezado con valores predeterminados.
    pub fn new() -> Self {
        Default::default()
    }

    /// Crea un nuevo encabezado con un tamaño de página específico.
    ///
    /// # Parámetros
    /// * `page_size` - Tamaño de página en bytes. Debe ser una potencia de 2 entre 512 y 65536.
    ///
    /// # Errores
    /// Retorna un error si el tamaño de página no es válido.
    pub fn with_page_size(page_size: u32) -> io::Result<Self> {
        if !is_valid_page_size(page_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Tamaño de página inválido: {}. Debe ser una potencia de 2 entre 512 y 65536", page_size),
            ));
        }

        let mut header = Self::new();
        header.page_size = page_size;
        Ok(header)
    }

    /// Lee el encabezado desde un origen de datos.
    ///
    /// # Parámetros
    /// * `reader` - Origen de datos que implementa `Read`.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al leer los datos o si el encabezado no es válido.
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = [0u8; HEADER_SIZE];
        reader.read_exact(&mut buffer)?;

        // Verificar la cadena de inicio
        if &buffer[0..16] != SQLITE_HEADER_STRING {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No es un archivo SQLite válido: encabezado incorrecto",
            ));
        }

        let page_size = match u16::from_be_bytes([buffer[16], buffer[17]]) {
            1 => 65536, // Caso especial para 65536
            size => u32::from(size),
        };

        if !is_valid_page_size(page_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Tamaño de página inválido: {}", page_size),
            ));
        }

        Ok(Header {
            page_size,
            write_version: buffer[18],
            read_version: buffer[19],
            reserved_space: buffer[20],
            max_payload_fraction: buffer[21],
            min_payload_fraction: buffer[22],
            leaf_payload_fraction: buffer[23],
            change_counter: u32::from_be_bytes([buffer[24], buffer[25], buffer[26], buffer[27]]),
            database_size: u32::from_be_bytes([buffer[28], buffer[29], buffer[30], buffer[31]]),
            first_freelist_trunk_page: u32::from_be_bytes([buffer[32], buffer[33], buffer[34], buffer[35]]),
            freelist_pages: u32::from_be_bytes([buffer[36], buffer[37], buffer[38], buffer[39]]),
            schema_cookie: u32::from_be_bytes([buffer[40], buffer[41], buffer[42], buffer[43]]),
            schema_format_number: u32::from_be_bytes([buffer[44], buffer[45], buffer[46], buffer[47]]),
            default_cache_size: u32::from_be_bytes([buffer[48], buffer[49], buffer[50], buffer[51]]),
            largest_root_btree_page: u32::from_be_bytes([buffer[52], buffer[53], buffer[54], buffer[55]]),
            text_encoding: u32::from_be_bytes([buffer[56], buffer[57], buffer[58], buffer[59]]),
            user_version: u32::from_be_bytes([buffer[60], buffer[61], buffer[62], buffer[63]]),
            incremental_vacuum_mode: u32::from_be_bytes([buffer[64], buffer[65], buffer[66], buffer[67]]),
            application_id: u32::from_be_bytes([buffer[68], buffer[69], buffer[70], buffer[71]]),
            reserved: {
                let mut reserved = [0u8; 20];
                reserved.copy_from_slice(&buffer[72..92]);
                reserved
            },
            version_valid_for: u32::from_be_bytes([buffer[92], buffer[93], buffer[94], buffer[95]]),
            sqlite_version_number: u32::from_be_bytes([buffer[96], buffer[97], buffer[98], buffer[99]]),
        })
    }

    /// Escribe el encabezado en un destino.
    ///
    /// # Parámetros
    /// * `writer` - Destino que implementa `Write`.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al escribir los datos.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut buffer = [0u8; HEADER_SIZE];

        // Escribir la cadena de inicio
        buffer[0..16].copy_from_slice(SQLITE_HEADER_STRING);

        // Escribir el tamaño de página
        let page_size_bytes = if self.page_size == 65536 {
            [0, 1] // Caso especial para 65536
        } else {
            ((self.page_size) as u16).to_be_bytes()
        };
        buffer[16..18].copy_from_slice(&page_size_bytes);

        // Escribir el resto de campos
        buffer[18] = self.write_version;
        buffer[19] = self.read_version;
        buffer[20] = self.reserved_space;
        buffer[21] = self.max_payload_fraction;
        buffer[22] = self.min_payload_fraction;
        buffer[23] = self.leaf_payload_fraction;
        buffer[24..28].copy_from_slice(&self.change_counter.to_be_bytes());
        buffer[28..32].copy_from_slice(&self.database_size.to_be_bytes());
        buffer[32..36].copy_from_slice(&self.first_freelist_trunk_page.to_be_bytes());
        buffer[36..40].copy_from_slice(&self.freelist_pages.to_be_bytes());
        buffer[40..44].copy_from_slice(&self.schema_cookie.to_be_bytes());
        buffer[44..48].copy_from_slice(&self.schema_format_number.to_be_bytes());
        buffer[48..52].copy_from_slice(&self.default_cache_size.to_be_bytes());
        buffer[52..56].copy_from_slice(&self.largest_root_btree_page.to_be_bytes());
        buffer[56..60].copy_from_slice(&self.text_encoding.to_be_bytes());
        buffer[60..64].copy_from_slice(&self.user_version.to_be_bytes());
        buffer[64..68].copy_from_slice(&self.incremental_vacuum_mode.to_be_bytes());
        buffer[68..72].copy_from_slice(&self.application_id.to_be_bytes());
        buffer[72..92].copy_from_slice(&self.reserved);
        buffer[92..96].copy_from_slice(&self.version_valid_for.to_be_bytes());
        buffer[96..100].copy_from_slice(&self.sqlite_version_number.to_be_bytes());

        writer.write_all(&buffer)
    }

    /// Incrementa el contador de cambios.
    pub fn increment_change_counter(&mut self) {
        self.change_counter = self.change_counter.wrapping_add(1);
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "SQLite Database Header:")?;
        writeln!(f, "  Page Size: {} bytes", self.page_size)?;
        writeln!(f, "  Write Version: {}", self.write_version)?;
        writeln!(f, "  Read Version: {}", self.read_version)?;
        writeln!(f, "  Reserved Space: {} bytes", self.reserved_space)?;
        writeln!(f, "  Change Counter: {}", self.change_counter)?;
        writeln!(f, "  Database Size: {} pages", self.database_size)?;
        writeln!(f, "  Schema Format Number: {}", self.schema_format_number)?;
        writeln!(f, "  Text Encoding: {}", text_encoding_to_string(self.text_encoding))?;
        writeln!(f, "  User Version: {}", self.user_version)?;
        writeln!(f, "  Application ID: {:#x}", self.application_id)?;
        writeln!(f, "  SQLite Version: {}", self.sqlite_version_number)
    }
}

/// Verifica si un tamaño de página es válido.
///
/// # Parámetros
/// * `size` - Tamaño de página a verificar.
///
/// # Retorno
/// `true` si el tamaño es una potencia de 2 entre 512 y 65536, `false` en caso contrario.
fn is_valid_page_size(size: u32) -> bool {
    if size < 512 || size > 65536 {
        return false;
    }
    
    // Verificar si es potencia de 2
    (size & (size - 1)) == 0
}

/// Convierte el código de codificación de texto a una cadena descriptiva.
///
/// # Parámetros
/// * `encoding` - Código de codificación de texto.
///
/// # Retorno
/// Una cadena que describe la codificación.
fn text_encoding_to_string(encoding: u32) -> String {
    match encoding {
        1 => "UTF-8".to_string(),
        2 => "UTF-16le".to_string(),
        3 => "UTF-16be".to_string(),
        _ => format!("Unknown ({})", encoding),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_header_default() {
        let header = Header::default();
        assert_eq!(header.page_size, 4096);
        assert_eq!(header.write_version, 1);
        assert_eq!(header.read_version, 1);
        assert_eq!(header.schema_format_number, 4);
    }

    #[test]
    fn test_with_page_size_valid() {
        let header = Header::with_page_size(8192).unwrap();
        assert_eq!(header.page_size, 8192);
    }

    #[test]
    fn test_with_page_size_invalid() {
        let result = Header::with_page_size(1000);
        assert!(result.is_err());
    }

    #[test]
    fn test_header_serialization() {
        let header = Header::default();
        let mut buffer = Vec::new();
        
        // Escribir el encabezado en el buffer
        header.write_to(&mut buffer).unwrap();
        
        // Leer el encabezado del buffer
        let mut cursor = Cursor::new(buffer);
        let read_header = Header::read_from(&mut cursor).unwrap();
        
        // Verificar que los valores coincidan
        assert_eq!(header.page_size, read_header.page_size);
        assert_eq!(header.write_version, read_header.write_version);
        assert_eq!(header.read_version, read_header.read_version);
        assert_eq!(header.schema_format_number, read_header.schema_format_number);
    }

    #[test]
    fn test_is_valid_page_size() {
        // Tamaños válidos (potencias de 2 entre 512 y 65536)
        assert!(is_valid_page_size(512));
        assert!(is_valid_page_size(1024));
        assert!(is_valid_page_size(4096));
        assert!(is_valid_page_size(8192));
        assert!(is_valid_page_size(16384));
        assert!(is_valid_page_size(32768));
        assert!(is_valid_page_size(65536));
        
        // Tamaños inválidos
        assert!(!is_valid_page_size(511)); // Menor que el mínimo
        assert!(!is_valid_page_size(513)); // No es potencia de 2
        assert!(!is_valid_page_size(1000)); // No es potencia de 2
        assert!(!is_valid_page_size(65537)); // Mayor que el máximo
    }
}
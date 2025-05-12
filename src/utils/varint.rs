//! # VarInt Module
//! 
//! Este módulo implementa la codificación y decodificación de enteros de longitud variable
//! (varint) utilizados en el formato de archivo SQLite.
//! 
//! Los varints son una forma de codificar enteros que utiliza menos bytes para valores pequeños,
//! lo que ahorra espacio en el archivo de base de datos.

use std::io::{self, Read, Write};

/// Tamaño máximo en bytes que puede ocupar un varint.
pub const MAX_VARINT_SIZE: usize = 9;

/// Codifica un entero de 64 bits con signo en formato varint.
///
/// # Parámetros
/// * `value` - El valor a codificar.
/// * `writer` - Destino donde se escribirá el valor codificado.
///
/// # Errores
/// Retorna un error si hay problemas al escribir en el destino.
///
/// # Retorno
/// Número de bytes escritos.
pub fn encode_varint<W: Write>(value: i64, writer: &mut W) -> io::Result<usize> {
    let mut uvalue = value as u64;
    let mut bytes_written = 0;
    
    // Para valores pequeños (0-127), usar un solo byte
    if uvalue <= 0x7F {
        writer.write_all(&[uvalue as u8])?;
        return Ok(1);
    }
    
    // Para valores que caben en 8 bytes
    if uvalue <= 0xFFFFFFFFFFFFFF {
        let mut buffer = [0u8; 8];
        let mut i = 0;
        
        // Codificar 7 bits por byte, con el bit más significativo como indicador
        while uvalue >= 0x80 {
            buffer[i] = 0x80 | (uvalue & 0x7F) as u8;
            uvalue >>= 7;
            i += 1;
        }
        
        // Último byte sin bit indicador
        buffer[i] = uvalue as u8;
        
        // Escribir los bytes utilizados
        writer.write_all(&buffer[0..=i])?;
        bytes_written = i + 1;
    } else {
        // Para valores que requieren 9 bytes
        let mut buffer = [0u8; 9];
        
        // Los primeros 8 bytes tienen el bit más significativo activo
        for i in 0..8 {
            buffer[i] = 0x80 | ((uvalue >> (7 * i)) & 0x7F) as u8;
        }
        
        // El noveno byte contiene el último bit
        buffer[8] = (uvalue >> 56) as u8;
        
        writer.write_all(&buffer)?;
        bytes_written = 9;
    }
    
    Ok(bytes_written)
}

/// Decodifica un entero de 64 bits con signo en formato varint.
///
/// # Parámetros
/// * `reader` - Origen de datos que implementa `Read`.
///
/// # Errores
/// Retorna un error si hay problemas al leer los datos o si el varint no es válido.
///
/// # Retorno
/// Tupla con el valor decodificado y el número de bytes leídos.
pub fn decode_varint<R: Read>(reader: &mut R) -> io::Result<(i64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    let mut bytes_read = 0;
    
    for _ in 0..MAX_VARINT_SIZE {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        bytes_read += 1;
        
        // Extraer 7 bits de datos
        let value = (byte[0] & 0x7F) as u64;
        result |= value << shift;
        
        // Si el bit más significativo no está activo, este es el último byte
        if byte[0] & 0x80 == 0 {
            return Ok((result as i64, bytes_read));
        }
        
        // Cada byte aporta 7 bits
        shift += 7;
        
        // Para el último byte (9º), no usamos el bit indicador
        if bytes_read == 8 {
            let mut last_byte = [0u8; 1];
            reader.read_exact(&mut last_byte)?;
            bytes_read += 1;
            
            // El último byte se desplaza 56 bits (8 bytes * 7 bits)
            result |= (last_byte[0] as u64) << 56;
            return Ok((result as i64, bytes_read));
        }
    }
    
    // No debería llegar aquí, pero por si acaso
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Varint inválido o demasiado largo",
    ))
}

/// Calcula el tamaño en bytes que ocuparía un valor codificado como varint.
///
/// # Parámetros
/// * `value` - El valor para el que se calculará el tamaño.
///
/// # Retorno
/// El número de bytes que ocuparía el valor codificado.
pub fn varint_size(value: i64) -> usize {
    let uvalue = value as u64;
    
    if uvalue <= 0x7F {
        1
    } else if uvalue <= 0x3FFF {
        2
    } else if uvalue <= 0x1FFFFF {
        3
    } else if uvalue <= 0xFFFFFFF {
        4
    } else if uvalue <= 0x7FFFFFFFF {
        5
    } else if uvalue <= 0x3FFFFFFFFFF {
        6
    } else if uvalue <= 0x1FFFFFFFFFFFF {
        7
    } else if uvalue <= 0xFFFFFFFFFFFFFF {
        8
    } else {
        9
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_varint_encode_decode_small() {
        // Valores pequeños (1 byte)
        let test_values = [0, 1, 42, 127];
        
        for &value in &test_values {
            let mut buffer = Vec::new();
            let bytes_written = encode_varint(value, &mut buffer).unwrap();
            
            assert_eq!(bytes_written, 1);
            assert_eq!(buffer.len(), 1);
            
            let mut cursor = Cursor::new(buffer);
            let (decoded, bytes_read) = decode_varint(&mut cursor).unwrap();
            
            assert_eq!(decoded, value);
            assert_eq!(bytes_read, 1);
        }
    }

    #[test]
    fn test_varint_encode_decode_medium() {
        // Valores medianos (múltiples bytes)
        let test_values = [128, 255, 256, 16383, 16384, 65535, 65536];
        
        for &value in &test_values {
            let mut buffer = Vec::new();
            let bytes_written = encode_varint(value, &mut buffer).unwrap();
            
            assert!(bytes_written > 1);
            assert_eq!(buffer.len(), bytes_written);
            
            let mut cursor = Cursor::new(buffer);
            let (decoded, bytes_read) = decode_varint(&mut cursor).unwrap();
            
            assert_eq!(decoded, value);
            assert_eq!(bytes_read, bytes_written);
        }
    }

    #[test]
    fn test_varint_encode_decode_large() {
        // Valores grandes
        let test_values = [
            2_i64.pow(32) - 1,   // Máximo u32
            2_i64.pow(32),       // Mínimo valor que requiere 5 bytes
            2_i64.pow(56) - 1,   // Máximo valor que cabe en 8 bytes
            2_i64.pow(56),       // Mínimo valor que requiere 9 bytes
            i64::MAX,            // Máximo i64
        ];
        
        for &value in &test_values {
            let mut buffer = Vec::new();
            let bytes_written = encode_varint(value, &mut buffer).unwrap();
            
            let expected_size = varint_size(value);
            assert_eq!(bytes_written, expected_size);
            assert_eq!(buffer.len(), expected_size);
            
            let mut cursor = Cursor::new(buffer);
            let (decoded, bytes_read) = decode_varint(&mut cursor).unwrap();
            
            assert_eq!(decoded, value);
            assert_eq!(bytes_read, expected_size);
        }
    }

    #[test]
    fn test_varint_size() {
        // Prueba para cada caso de tamaño
        assert_eq!(varint_size(0), 1);
        assert_eq!(varint_size(127), 1);
        assert_eq!(varint_size(128), 2);
        assert_eq!(varint_size(16383), 2);
        assert_eq!(varint_size(16384), 3);
        assert_eq!(varint_size(2097151), 3);
        assert_eq!(varint_size(2097152), 4);
        assert_eq!(varint_size(268435455), 4);
        assert_eq!(varint_size(268435456), 5);
        assert_eq!(varint_size(34359738367), 5);
        assert_eq!(varint_size(34359738368), 6);
        assert_eq!(varint_size(4398046511103), 6);
        assert_eq!(varint_size(4398046511104), 7);
        assert_eq!(varint_size(562949953421311), 7);
        assert_eq!(varint_size(562949953421312), 8);
        assert_eq!(varint_size(72057594037927935), 8);
        assert_eq!(varint_size(72057594037927936), 9);
        assert_eq!(varint_size(i64::MAX), 9);
    }
}
//! # Record Module
//! 
//! Este módulo implementa la estructura de registro que se almacena en las celdas
//! de los árboles B-Tree de SQLite. Un registro contiene los valores de una fila de una tabla.

use std::io::{self, Read, Write, Cursor};

use crate::utils::serialization::{SqliteValue, serialize_values, deserialize_values};

/// Representa un registro de una tabla en la base de datos.
///
/// Un registro es una colección de valores que corresponden a las columnas de una tabla.
#[derive(Debug, Clone)]
pub struct Record {
    /// Valores de las columnas.
    pub values: Vec<SqliteValue>,
}

impl Record {
    /// Crea un nuevo registro vacío.
    pub fn new() -> Self {
        Record { values: Vec::new() }
    }

    /// Crea un nuevo registro con los valores especificados.
    ///
    /// # Parámetros
    /// * `values` - Valores de las columnas.
    pub fn with_values(values: Vec<SqliteValue>) -> Self {
        Record { values }
    }

    /// Serializa el registro en formato binario.
    ///
    /// # Parámetros
    /// * `writer` - Destino donde se escribirá el registro serializado.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al escribir los datos.
    ///
    /// # Retorno
    /// Número de bytes escritos.
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        serialize_values(&self.values, writer)
    }

    /// Serializa el registro en un vector de bytes.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al serializar.
    ///
    /// # Retorno
    /// Vector con los datos serializados.
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.serialize(&mut buffer)?;
        Ok(buffer)
    }

    /// Deserializa un registro desde formato binario.
    ///
    /// # Parámetros
    /// * `reader` - Origen de datos que implementa `Read`.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al leer los datos o si el formato no es válido.
    ///
    /// # Retorno
    /// Tupla con el registro deserializado y el número de bytes leídos.
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<(Self, usize)> {
        let (values, bytes_read) = deserialize_values(reader)?;
        Ok((Record { values }, bytes_read))
    }

    /// Deserializa un registro desde un slice de bytes.
    ///
    /// # Parámetros
    /// * `data` - Slice de bytes que contiene el registro serializado.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al deserializar o si el formato no es válido.
    ///
    /// # Retorno
    /// Tupla con el registro deserializado y el número de bytes leídos.
    pub fn from_bytes(data: &[u8]) -> io::Result<(Self, usize)> {
        let mut cursor = Cursor::new(data);
        Self::deserialize(&mut cursor)
    }

    /// Añade un valor al registro.
    ///
    /// # Parámetros
    /// * `value` - Valor a añadir.
    pub fn add_value(&mut self, value: SqliteValue) {
        self.values.push(value);
    }

    /// Obtiene el valor en la posición especificada.
    ///
    /// # Parámetros
    /// * `index` - Índice del valor (comenzando desde 0).
    ///
    /// # Retorno
    /// Referencia al valor, o `None` si el índice está fuera de rango.
    pub fn get_value(&self, index: usize) -> Option<&SqliteValue> {
        self.values.get(index)
    }

    /// Establece el valor en la posición especificada.
    ///
    /// # Parámetros
    /// * `index` - Índice del valor (comenzando desde 0).
    /// * `value` - Nuevo valor.
    ///
    /// # Retorno
    /// `true` si se estableció el valor, `false` si el índice está fuera de rango.
    pub fn set_value(&mut self, index: usize, value: SqliteValue) -> bool {
        if index >= self.values.len() {
            return false;
        }
        
        self.values[index] = value;
        true
    }

    /// Obtiene el número de valores en el registro.
    ///
    /// # Retorno
    /// Número de valores.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Verifica si el registro está vacío.
    ///
    /// # Retorno
    /// `true` si el registro no contiene valores, `false` en caso contrario.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Calcula el tamaño en bytes que ocuparía el registro serializado.
    ///
    /// # Retorno
    /// Tamaño en bytes.
    pub fn serialized_size(&self) -> usize {
        // Tamaño del varint que indica el número de valores
        let count_size = crate::utils::varint_size(self.values.len() as i64);
        
        // Sumar el tamaño de cada valor
        let values_size = self.values.iter()
            .map(|v| v.serialized_size())
            .sum::<usize>();
        
        count_size + values_size
    }
}

impl Default for Record {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::serialization::SqliteValue;

    #[test]
    fn test_record_new() {
        let record = Record::new();
        assert!(record.is_empty());
        assert_eq!(record.len(), 0);
    }

    #[test]
    fn test_record_with_values() {
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::String("Hello".to_string()),
            SqliteValue::Blob(vec![1, 2, 3]),
        ];
        
        let record = Record::with_values(values.clone());
        assert_eq!(record.len(), 3);
        assert!(!record.is_empty());
        
        match record.get_value(0) {
            Some(SqliteValue::Integer(v)) => assert_eq!(*v, 42),
            _ => panic!("Valor incorrecto"),
        }
        
        match record.get_value(1) {
            Some(SqliteValue::String(s)) => assert_eq!(s, "Hello"),
            _ => panic!("Valor incorrecto"),
        }
        
        match record.get_value(2) {
            Some(SqliteValue::Blob(b)) => assert_eq!(b, &[1, 2, 3]),
            _ => panic!("Valor incorrecto"),
        }
    }

    #[test]
    fn test_record_add_value() {
        let mut record = Record::new();
        assert!(record.is_empty());
        
        record.add_value(SqliteValue::Integer(42));
        assert_eq!(record.len(), 1);
        
        record.add_value(SqliteValue::String("Hello".to_string()));
        assert_eq!(record.len(), 2);
        
        match record.get_value(0) {
            Some(SqliteValue::Integer(v)) => assert_eq!(*v, 42),
            _ => panic!("Valor incorrecto"),
        }
        
        match record.get_value(1) {
            Some(SqliteValue::String(s)) => assert_eq!(s, "Hello"),
            _ => panic!("Valor incorrecto"),
        }
    }

    #[test]
    fn test_record_set_value() {
        let mut record = Record::with_values(vec![
            SqliteValue::Integer(42),
            SqliteValue::String("Hello".to_string()),
        ]);
        
        // Establecer un valor existente
        assert!(record.set_value(0, SqliteValue::Integer(99)));
        
        match record.get_value(0) {
            Some(SqliteValue::Integer(v)) => assert_eq!(*v, 99),
            _ => panic!("Valor incorrecto"),
        }
        
        // Intentar establecer un valor fuera de rango
        assert!(!record.set_value(2, SqliteValue::Null));
    }

    #[test]
    fn test_record_serialization() {
        let record = Record::with_values(vec![
            SqliteValue::Integer(42),
            SqliteValue::String("Hello".to_string()),
            SqliteValue::Blob(vec![1, 2, 3]),
            SqliteValue::Null,
            SqliteValue::Float(3.14159),
        ]);
        
        // Serializar a bytes
        let bytes = record.to_bytes().unwrap();
        assert!(!bytes.is_empty());
        
        // Deserializar de vuelta
        let (deserialized, bytes_read) = Record::from_bytes(&bytes).unwrap();
        assert_eq!(bytes_read, bytes.len());
        assert_eq!(deserialized.len(), record.len());
        
        // Verificar que los valores son iguales
        for i in 0..record.len() {
            match (record.get_value(i), deserialized.get_value(i)) {
                (Some(SqliteValue::Integer(a)), Some(SqliteValue::Integer(b))) => assert_eq!(a, b),
                (Some(SqliteValue::String(a)), Some(SqliteValue::String(b))) => assert_eq!(a, b),
                (Some(SqliteValue::Blob(a)), Some(SqliteValue::Blob(b))) => assert_eq!(a, b),
                (Some(SqliteValue::Null), Some(SqliteValue::Null)) => {},
                (Some(SqliteValue::Float(a)), Some(SqliteValue::Float(b))) => assert!((a - b).abs() < f64::EPSILON),
                _ => panic!("Valores no coinciden en el índice {}", i),
            }
        }
    }

    #[test]
    fn test_record_serialized_size() {
        let record = Record::with_values(vec![
            SqliteValue::Integer(42),
            SqliteValue::String("Hello".to_string()),
            SqliteValue::Blob(vec![1, 2, 3]),
        ]);
        
        let size = record.serialized_size();
        let bytes = record.to_bytes().unwrap();
        
        assert_eq!(size, bytes.len());
    }
}
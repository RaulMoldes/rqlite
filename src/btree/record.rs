//! # Record Module
//! 
//! This module defines the `Record` struct, which represents a row in a SQLite database table.
//! It provides methods for creating, serializing, deserializing, and manipulating records.

use std::io::{self, Read, Write, Cursor};

use crate::utils::serialization::{SqliteValue, serialize_values, deserialize_values};

/// Represents a record in a SQLite database table.
///
/// A record is a collection of values, each corresponding to a column in the table.
// The `Record` struct is used internally by the SQLite engine to manage data storage and retrieval.
#[derive(Debug, Clone)]
pub struct Record {
    /// Values of the columns in the record.
    /// Each value corresponds to a column in the table.
    pub values: Vec<SqliteValue>,
}

impl Record {
    /// Creates a new empty record.
    pub fn new() -> Self {
        Record { values: Vec::new() }
    }

    /// Creates a new record with the specified values.
    /// 
    /// # Parameters
    /// * `values` - Vector of values to initialize the record.
    /// 
    /// # Returns
    /// A new `Record` instance with the specified values.
    pub fn with_values(values: Vec<SqliteValue>) -> Self {
        Record { values }
    }

    /// Serializes the record to a binary format.
    /// 
    /// # Parameters
    /// * `writer` - Destination for the serialized data, implementing `Write`.
    /// 
    /// # Errors
    /// Returns an error if there are issues writing the data.
    /// 
    /// # Returns
    /// Number of bytes written.
    /// 
    /// Note: see `serialize_values` for details on the serialization format (module `utils::serialization.rs`).
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        serialize_values(&self.values, writer)
    }

    /// Serializes the record to a byte vector.
    /// Just a different interface for convenience.
    /// 
    /// # Returns
    /// A vector of bytes representing the serialized record.
    /// 
    /// # Errors
    /// Returns an error if there are issues writing the data.
    /// 
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.serialize(&mut buffer)?;
        Ok(buffer)
    }

    /// Deserializes a record from a binary format.
    /// 
    /// # Parameters
    /// * `reader` - Source of the serialized data, implementing `Read`.
    /// 
    /// # Errors
    /// Returns an error if there are issues reading the data or if the format is invalid.
    /// 
    /// # Returns
    /// A tuple with the deserialized record and the number of bytes read.
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<(Self, usize)> {
        let (values, bytes_read) = deserialize_values(reader)?;
        Ok((Record { values }, bytes_read))
    }

    /// Deserializes a record from a byte slice.
    /// I used the same approach as `serialize` to avoid unnecessary allocations.
    /// 
    /// # Parameters
    /// * `data` - Slice of bytes representing the serialized record.
    /// 
    /// # Errors
    /// Returns an error if there are issues reading the data or if the format is invalid.
    /// 
    /// # Returns
    /// A tuple with the deserialized record and the number of bytes read.
    pub fn from_bytes(data: &[u8]) -> io::Result<(Self, usize)> {
        let mut cursor = Cursor::new(data);
        Self::deserialize(&mut cursor)
    }

    /// Adds a new value to the record.
    /// 
    /// # Parameters
    /// * `value` - Value to add to the record.
    /// 
    /// # Returns
    /// None
    pub fn add_value(&mut self, value: SqliteValue) {
        self.values.push(value);
    }

    /// Obtains the value at the specified index.
    /// 
    /// # Parameters
    /// * `index` - Index of the value (starting from 0).
    /// 
    /// # Returns
    /// An `Option` containing the value if it exists, or `None` if the index is out of range.
    /// 
    /// # Note
    /// The index is zero-based, so the first value is at index 0.
    pub fn get_value(&self, index: usize) -> Option<&SqliteValue> {
        self.values.get(index)
    }

    /// Sets the value at the specified index.
    /// 
    /// # Parameters
    /// * `index` - Index of the value to set (starting from 0).
    /// 
    /// # Returns
    /// `true` if the value was set successfully, `false` if the index is out of range.
    /// 
    /// # Note
    /// The index is zero-based, so the first value is at index 0.
    pub fn set_value(&mut self, index: usize, value: SqliteValue) -> bool {
        if index >= self.values.len() {
            return false;
        }
        
        self.values[index] = value;
        true
    }

    /// Obtains the number of values in the record.
    /// 
    /// # Returns
    /// The number of values in the record.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Verifies if the record is empty.
    /// 
    /// # Returns
    /// `true` if the record is empty, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Calculates the serialized size of the record.
    /// 
    /// # Returns
    /// The size in bytes of the serialized record.
    pub fn serialized_size(&self) -> usize {
        // Size of the varint that stores the number of values
        // (see `serialize_values` for details)
        // Remember that in sqlite, integers are variable length, so we need to calculate the size of the varint
        // that stores the number of values.
        let count_size = crate::utils::varint_size(self.values.len() as i64);
        
        // Add the size of each value
        // (see `serialize_values` for details)
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

use rqlite_engine::{Database, btree::Record, utils::serialization::SqliteValue};
use tempfile::tempdir;
use std::io;

/*
#[test]
fn test_database_with_btree() -> io::Result<()> {
    // Crear una base de datos temporal
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    
    // Inicializar la base de datos con un tamaño de página de 4096 bytes
    let mut db = Database::create(&db_path, 4096, 0)?;
    
    // Crear un árbol B-Tree de tabla
    let mut table = db.create_table()?;
    
    // Insertar algunos registros
    for i in 1..=100 {
        let mut record = Record::new();
        
        // Añadir algunos valores al registro
        record.add_value(SqliteValue::Integer(i));
        record.add_value(SqliteValue::String(format!("Registro {}", i)));
        
        if i % 2 == 0 {
            record.add_value(SqliteValue::Float(i as f64 / 10.0));
        } else {
            record.add_value(SqliteValue::Null);
        }
        
        // Insertar el registro con un rowid igual a i
        table.insert(i, &record)?;
    }
    
    // Guardar los cambios
    db.commit()?;
    
    // Buscar algunos registros
    for i in [10, 25, 50, 75, 100] {
        let found = table.find(i)?;
        assert!(found.is_some());
        
        let record = found.unwrap();
        
        // Verificar los valores del registro
        match record.get_value(0) {
            Some(SqliteValue::Integer(value)) => assert_eq!(*value, i),
            _ => panic!("Valor incorrecto"),
        }
        
        match record.get_value(1) {
            Some(SqliteValue::String(text)) => assert_eq!(text, &format!("Registro {}", i)),
            _ => panic!("Valor incorrecto"),
        }
        
        if i % 2 == 0 {
            match record.get_value(2) {
                Some(SqliteValue::Float(value)) => assert_eq!(*value, i as f64 / 10.0),
                _ => panic!("Valor incorrecto"),
            }
        } else {
            match record.get_value(2) {
                Some(SqliteValue::Null) => {},
                _ => panic!("Valor incorrecto"),
            }
        }
    }
    
    // Eliminar algunos registros
    table.delete(25)?;
    table.delete(75)?;
    
    // Guardar los cambios
    db.commit()?;
    
    // Verificar que los registros eliminados ya no existen
    assert!(table.find(25)?.is_none());
    assert!(table.find(75)?.is_none());
    
    // Verificar que los demás registros siguen existiendo
    assert!(table.find(10)?.is_some());
    assert!(table.find(50)?.is_some());
    assert!(table.find(100)?.is_some());
    
    // Cerrar la base de datos
    db.close()?;
    
    // Reabrir la base de datos
    let mut db = Database::open(&db_path)?;
    
    // Recuperar la página raíz y abrir el árbol B-Tree
    let root_page = 2; // Asumimos que es la segunda página, en una implementación real habría un catálogo de tablas
    let table = db.open_btree(root_page, rqlite_engine::btree::TreeType::Table)?;
    
    // Verificar que los registros siguen como esperamos después de reabrir
    assert!(table.find(25)?.is_none());
    assert!(table.find(75)?.is_none());
    
    assert!(table.find(10)?.is_some());
    assert!(table.find(50)?.is_some());
    assert!(table.find(100)?.is_some());
    
    Ok(())
}

#[test]
fn test_large_database() -> io::Result<()> {
    // Crear una base de datos temporal
    let dir = tempdir()?;
    let db_path = dir.path().join("large.db");
    
    // Inicializar la base de datos con un tamaño de página pequeño para provocar más divisiones
    let mut db = Database::create(&db_path, 1024, 0)?;
    
    // Crear un árbol B-Tree de tabla
    let mut table = db.create_table()?;
    
    // Insertar muchos registros para forzar divisiones en el árbol
    for i in 1..=1000 {
        let mut record = Record::new();
        
        // Crear un registro con un payload grande
        record.add_value(SqliteValue::Integer(i));
        record.add_value(SqliteValue::String(format!("Registro con un texto muy largo para forzar divisiones en el árbol: {}", i)));
        
        // Para algunos registros, crear un payload aún más grande
        if i % 10 == 0 {
            // Añadir un blob grande
            let data = vec![i as u8; 500]; // 500 bytes
            record.add_value(SqliteValue::Blob(data));
        }
        
        // Insertar el registro
        table.insert(i, &record)?;
    }
    
    // Guardar los cambios
    db.commit()?;
    
    // Verificar que se pueden encontrar algunos registros
    for i in [1, 100, 500, 1000] {
        let found = table.find(i)?;
        assert!(found.is_some());
        
        let record = found.unwrap();
        
        match record.get_value(0) {
            Some(SqliteValue::Integer(value)) => assert_eq!(*value, i),
            _ => panic!("Valor incorrecto"),
        }
        
        // Los registros con i múltiplo de 10 tienen un blob adicional
        if i % 10 == 0 {
            assert_eq!(record.len(), 3);
            
            match record.get_value(2) {
                Some(SqliteValue::Blob(data)) => {
                    assert_eq!(data.len(), 500);
                    assert_eq!(data[0], i as u8);
                },
                _ => panic!("Valor incorrecto"),
            }
        } else {
            assert_eq!(record.len(), 2);
        }
    }
    
    // Eliminar la mitad de los registros
    for i in 1..=500 {
        table.delete(i)?;
    }
    
    // Guardar los cambios
    db.commit()?;
    
    // Verificar que los registros eliminados ya no existen
    for i in [1, 100, 500] {
        assert!(table.find(i)?.is_none());
    }
    
    // Verificar que los demás registros siguen existiendo
    for i in [501, 750, 1000] {
        assert!(table.find(i)?.is_some());
    }
    
    Ok(())
}

    */
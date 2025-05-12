use rqlite_engine::{Database, btree::Record, utils::serialization::SqliteValue};
use std::{io, fs, path::Path};

fn main() -> io::Result<()> {
    // Configurar la ruta de la base de datos
    let db_path = Path::new("simple.db");
    
    // Eliminar la base de datos si ya existe
    if db_path.exists() {
        fs::remove_file(db_path)?;
    }
    
    println!("Creando base de datos SQLite...");
    
    // Crear una nueva base de datos
    let mut db = Database::create(db_path, 4096, 0)?;
    
    // Imprimir información sobre la base de datos
    let header = db.get_header()?;
    println!("Base de datos creada con éxito:");
    println!("  Tamaño de página: {} bytes", header.page_size);
    println!("  Espacio reservado: {} bytes", header.reserved_space);
    println!("  Versión de escritura: {}", header.write_version);
    println!("  Versión de lectura: {}", header.read_version);
    
    // Crear una tabla (árbol B-Tree)
    println!("\nCreando tabla (árbol B-Tree)...");
    let mut table = db.create_table()?;
    
    // Insertar registros en la tabla
    println!("Insertando registros...");
    
    let registros = [
        ("Juan Pérez", 30, "juan@example.com", "Desarrollador"),
        ("María García", 28, "maria@example.com", "Diseñadora"),
        ("Carlos López", 35, "carlos@example.com", "Gerente"),
        ("Ana Martínez", 32, "ana@example.com", "Analista"),
        ("Roberto Sánchez", 40, "roberto@example.com", "Director"),
    ];
    
    for (i, &(nombre, edad, email, puesto)) in registros.iter().enumerate() {
        let rowid = (i + 1) as i64;
        
        // Crear un registro con los datos
        let mut record = Record::new();
        record.add_value(SqliteValue::String(nombre.to_string()));
        record.add_value(SqliteValue::Integer(edad));
        record.add_value(SqliteValue::String(email.to_string()));
        record.add_value(SqliteValue::String(puesto.to_string()));
        
        // Insertar el registro
        table.insert(rowid, &record)?;
        println!("  Registro #{} insertado: {} ({})", rowid, nombre, puesto);
    }
    
    // Guardar los cambios
    db.commit()?;
    println!("Cambios guardados.");
    
    // Buscar registros
    println!("\nBuscando registros...");
    
    // Buscar un registro por su rowid
    let rowid = 3;
    match table.find(rowid)? {
        Some(record) => {
            // Extraer los valores del registro
            let nombre = match record.get_value(0) {
                Some(SqliteValue::String(s)) => s.as_str(),
                _ => "Desconocido",
            };
            
            let edad = match record.get_value(1) {
                Some(SqliteValue::Integer(i)) => *i,
                _ => 0,
            };
            
            let email = match record.get_value(2) {
                Some(SqliteValue::String(s)) => s.as_str(),
                _ => "Desconocido",
            };
            
            let puesto = match record.get_value(3) {
                Some(SqliteValue::String(s)) => s.as_str(),
                _ => "Desconocido",
            };
            
            println!("  Registro encontrado (rowid={}): {} ({} años)", rowid, nombre, edad);
            println!("    Email: {}", email);
            println!("    Puesto: {}", puesto);
        },
        None => {
            println!("  No se encontró el registro con rowid={}", rowid);
        }
    }
    
    // Eliminar un registro
    println!("\nEliminando registro...");
    let rowid_to_delete = 2;
    if table.delete(rowid_to_delete)? {
        println!("  Registro con rowid={} eliminado.", rowid_to_delete);
    } else {
        println!("  No se encontró el registro con rowid={}", rowid_to_delete);
    }
    
    // Guardar los cambios
    db.commit()?;
    
    // Verificar que el registro fue eliminado
    match table.find(rowid_to_delete)? {
        Some(_) => println!("  ¡Error! El registro debería haber sido eliminado."),
        None => println!("  Verificado: el registro ya no existe."),
    }
    
    // Mostrar todos los registros restantes
    println!("\nRegistros restantes:");
    for i in 1..=5 {
        match table.find(i)? {
            Some(record) => {
                let nombre = match record.get_value(0) {
                    Some(SqliteValue::String(s)) => s.as_str(),
                    _ => "Desconocido",
                };
                
                let puesto = match record.get_value(3) {
                    Some(SqliteValue::String(s)) => s.as_str(),
                    _ => "Desconocido",
                };
                
                println!("  Registro #{}: {} ({})", i, nombre, puesto);
            },
            None => println!("  Registro #{}: [Eliminado]", i),
        }
    }
    
    // Cerrar la base de datos
    println!("\nCerrando base de datos...");
    db.close()?;
    println!("Base de datos cerrada correctamente.");
    
    println!("\nLa base de datos se ha guardado en: {}", db_path.display());
    
    Ok(())
}
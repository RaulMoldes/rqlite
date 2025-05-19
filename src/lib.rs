//! # SQLite Storage Engine
//!
//! Este crate implementa un motor de almacenamiento compatible con SQLite,
//! siguiendo el formato de archivo SQLite versión 3. La implementación se centra
//! en la parte de almacenamiento, incluyendo el manejo de páginas, árboles B-Tree
//! y serialización/deserialización de datos.
//!
//! ## Estructura
//!
//! - `header`: Implementación del encabezado de la base de datos.
//! - `page`: Definición de las páginas y tipos de páginas.
//! - `btree`: Implementación de los árboles B-Tree utilizados para tablas e índices.
//! - `storage`: Motor de almacenamiento para lectura/escritura de archivos.
//! - `utils`: Utilidades como varints y serialización.

pub mod header;
pub mod page;
pub mod storage;
pub mod tree;
pub mod utils;
/*
/// Versión del formato SQLite implementado.
pub const SQLITE_FORMAT_VERSION: &str = "3";

/// Estructura principal que representa una base de datos SQLite.
///
/// Esta estructura proporciona métodos para abrir, crear y manipular
/// bases de datos SQLite a nivel de almacenamiento.
pub struct Database {
    /// Pager para gestionar las páginas de la base de datos.
    pager: storage::Pager,
}

impl Database {
    /// Abre una base de datos existente.
    ///
    /// # Parámetros
    /// * `path` - Ruta al archivo de base de datos.
    ///
    /// # Errores
    /// Retorna un error si el archivo no existe, no se puede abrir, o no es un archivo SQLite válido.
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        let pager = storage::Pager::open(path)?;
        Ok(Database { pager })
    }

    /// Crea una nueva base de datos.
    ///
    /// # Parámetros
    /// * `path` - Ruta donde crear el archivo.
    /// * `page_size` - Tamaño de página en bytes (debe ser una potencia de 2 entre 512 y 65536).
    /// * `reserved_space` - Espacio reservado al final de cada página.
    ///
    /// # Errores
    /// Retorna un error si no se puede crear el archivo, si el tamaño de página es inválido,
    /// o si hay otros problemas de I/O.
    pub fn create<P: AsRef<std::path::Path>>(
        path: P,
        page_size: u32,
        reserved_space: u8,
    ) -> std::io::Result<Self> {
        let pager = storage::Pager::create(path, page_size, reserved_space)?;
        Ok(Database { pager })
    }

    /// Obtiene el encabezado de la base de datos.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al leer los datos.
    pub fn get_header(&mut self) -> std::io::Result<header::Header> {
        self.pager.get_header()
    }

    /// Actualiza el encabezado de la base de datos.
    ///
    /// # Parámetros
    /// * `header` - Nuevo encabezado.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al escribir los datos.
    pub fn update_header(&mut self, header: &header::Header) -> std::io::Result<()> {
        self.pager.update_header(header)
    }

    /// Crea un nuevo nodo B-Tree hoja.
    ///
    /// # Parámetros
    /// * `node_type` - Tipo de nodo hoja (TableLeaf o IndexLeaf).
    ///
    /// # Errores
    /// Retorna un error si no se puede crear la página o si el tipo no es una hoja.
    pub fn create_btree_leaf(&mut self, node_type: page::PageType) -> std::io::Result<btree::BTreeNode> {
        btree::BTreeNode::create_leaf(node_type, &mut self.pager as *mut _)
    }

    /// Crea un nuevo nodo B-Tree interior.
    ///
    /// # Parámetros
    /// * `node_type` - Tipo de nodo interior (TableInterior o IndexInterior).
    /// * `right_most_page` - Número de página del hijo más a la derecha.
    ///
    /// # Errores
    /// Retorna un error si no se puede crear la página o si el tipo no es interior.
    pub fn create_btree_interior(
        &mut self,
        node_type: page::PageType,
        right_most_page: u32,
    ) -> std::io::Result<btree::BTreeNode> {
        btree::BTreeNode::create_interior(node_type, right_most_page, &mut self.pager as *mut _)
    }

    /// Abre un nodo B-Tree existente.
    ///
    /// # Parámetros
    /// * `page_number` - Número de página donde se almacena el nodo.
    /// * `node_type` - Tipo de nodo esperado.
    ///
    /// # Errores
    /// Retorna un error si la página no existe o si el tipo no coincide.
    pub fn open_btree_node(
        &mut self,
        page_number: u32,
        node_type: page::PageType,
    ) -> std::io::Result<btree::BTreeNode> {
        btree::BTreeNode::open(page_number, node_type, &mut self.pager as *mut _)
    }

    /// Crea un nuevo árbol B-Tree de tabla.
    ///
    /// # Errores
    /// Retorna un error si hay problemas de I/O.
    ///
    /// # Retorno
    /// El árbol B-Tree creado.
    pub fn create_table(&mut self) -> std::io::Result<btree::BTree> {
        // Obtener los valores para la configuración del B-Tree desde el encabezado
        let header = self.get_header()?;

        btree::BTree::create(
            btree::TreeType::Table,
            &mut self.pager as *mut _,
            header.page_size,
            header.reserved_space,
            header.max_payload_fraction,
            header.min_payload_fraction,
        )
    }

    /// Crea un nuevo árbol B-Tree de índice.
    ///
    /// # Errores
    /// Retorna un error si hay problemas de I/O.
    ///
    /// # Retorno
    /// El árbol B-Tree creado.
    pub fn create_index(&mut self) -> std::io::Result<btree::BTree> {
        // Obtener los valores para la configuración del B-Tree desde el encabezado
        let header = self.get_header()?;

        btree::BTree::create(
            btree::TreeType::Index,
            &mut self.pager as *mut _,
            header.page_size,
            header.reserved_space,
            header.max_payload_fraction,
            header.min_payload_fraction,
        )
    }

    /// Abre un árbol B-Tree existente.
    ///
    /// # Parámetros
    /// * `root_page` - Número de página de la raíz del árbol.
    /// * `tree_type` - Tipo de árbol (tabla o índice).
    ///
    /// # Errores
    /// Retorna un error si la página raíz no existe o no es válida.
    ///
    /// # Retorno
    /// El árbol B-Tree.
    pub fn open_btree(
        &mut self,
        root_page: u32,
        tree_type: btree::TreeType,
    ) -> std::io::Result<btree::BTree> {
        // Obtener los valores para la configuración del B-Tree desde el encabezado
        let header = self.get_header()?;

        btree::BTree::open(
            root_page,
            tree_type,
            &mut self.pager as *mut _,
            header.page_size,
            header.reserved_space,
            header.max_payload_fraction,
            header.min_payload_fraction,
        )
    }

    /// Guarda todos los cambios pendientes a disco.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al escribir los datos.
    pub fn commit(&mut self) -> std::io::Result<()> {
        self.pager.flush()
    }

    /// Cierra la base de datos, guardando todos los cambios pendientes.
    ///
    /// # Errores
    /// Retorna un error si hay problemas al guardar los datos.
    pub fn close(mut self) -> std::io::Result<()> {
        self.pager.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::tempdir;

    #[test]
    fn test_create_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Crear una base de datos
        let db = Database::create(&db_path, 4096, 0);
        assert!(db.is_ok());

        // Verificar que el archivo existe
        assert!(db_path.exists());
    }

    #[test]
    fn test_open_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Crear una base de datos
        {
            let _db = Database::create(&db_path, 4096, 0).unwrap();
        }

        // Abrir la base de datos existente
        let db = Database::open(&db_path);
        assert!(db.is_ok());
    }

    #[test]
    fn test_create_btree_node() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Crear una base de datos
        let mut db = Database::create(&db_path, 4096, 0).unwrap();

        // Crear un nodo B-Tree hoja
        let node = db.create_btree_leaf(page::PageType::TableLeaf);
        assert!(node.is_ok());

        // Verificar que el nodo se creó correctamente
        let node = node.unwrap();
        assert_eq!(node.node_type, page::PageType::TableLeaf);
        assert_eq!(node.cell_count().unwrap(), 0);
    }

    #[test]
    fn test_create_and_open_btree_node() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Crear una base de datos
        let mut db = Database::create(&db_path, 4096, 0).unwrap();

        // Crear un nodo B-Tree hoja
        let node = db.create_btree_leaf(page::PageType::TableLeaf).unwrap();
        let page_number = node.page_number;

        // Guardar cambios
        db.commit().unwrap();

        // Abrir el nodo de nuevo
        let node2 = db.open_btree_node(page_number, page::PageType::TableLeaf);
        assert!(node2.is_ok());

        let node2 = node2.unwrap();
        assert_eq!(node2.node_type, page::PageType::TableLeaf);
        assert_eq!(node2.page_number, page_number);
    }

    #[test]
    fn test_header_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Crear una base de datos
        let mut db = Database::create(&db_path, 4096, 0).unwrap();

        // Leer el encabezado
        let mut header = db.get_header().unwrap();
        assert_eq!(header.page_size, 4096);

        // Modificar el encabezado
        header.user_version = 42;
        db.update_header(&header).unwrap();

        // Leer de nuevo el encabezado
        let header2 = db.get_header().unwrap();
        assert_eq!(header2.user_version, 42);
    }

    #[test]
    fn test_close_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Crear una base de datos
        let db = Database::create(&db_path, 4096, 0).unwrap();

        // Cerrar la base de datos
        let result = db.close();
        assert!(result.is_ok());
    }
}
    */

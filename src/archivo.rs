use std::fs::File;
use std::io::{self, BufReader};
/// Procesa la ruta para acceder a una tabla específica, agregando el nombre de la tabla a la ruta.
///
/// Este método modifica la ruta original añadiendo una barra y el nombre de la tabla en minúsculas.
///
/// # Argumentos
/// - `ruta`: La ruta base donde se encuentran las tablas.
/// - `tabla`: El nombre de la tabla que se desea agregar a la ruta.
///
/// # Retorno
/// Devuelve la ruta completa como un `String` que combina la ruta base y la tabla.

pub fn procesar_ruta(ruta: &str, tabla: &str) -> String {
    let mut ruta_modificada = String::new(); // Crear un nuevo String
    ruta_modificada.push_str(ruta); // Agregar la ruta original (sin clonar)
    ruta_modificada.push_str("/"); // Modificar
    ruta_modificada.push_str(&tabla.to_ascii_lowercase()); // Modificar
    return ruta_modificada;
}

/// Lee el archivo en la ruta especificada y devuelve un `BufReader` para procesarlo.
///
/// Abre el archivo indicado y crea un `BufReader` que permite la lectura eficiente del archivo.
///
/// # Argumentos
/// - `ruta_archivo`: La ruta del archivo que se desea leer.
///
/// # Retorno
/// Retorna `Result<BufReader<File>, io::Error>` que contiene el `BufReader` en caso de éxito, o un error de E/S en caso de fallo.

pub fn leer_archivo(ruta_archivo: &str) -> Result<BufReader<File>, io::Error> {
    let file = File::open(ruta_archivo)?;
    let reader = BufReader::new(file);
    Ok(reader)
}

/// Parsea una línea del archivo CSV y devuelve dos vectores con los campos originales y en minúsculas.
///
/// Esta función divide la línea en campos usando comas como delimitador y devuelve dos vectores:
/// uno con los campos tal como están y otro con los campos en minúsculas.
///
/// # Argumentos
/// - `linea`: La línea que se desea procesar.
///
/// # Retorno
/// Devuelve una tupla con dos vectores `Vec<String>`: el primero con los campos originales y el segundo con los campos en minúsculas.

pub fn parsear_linea_archivo(linea: &String) -> (Vec<String>, Vec<String>) {
    return (
        linea.split(",").map(|s| s.to_string()).collect(),
        linea
            .to_lowercase()
            .split(",")
            .map(|s| s.to_string())
            .collect(),
    );
}
use crate::delete::ConsultaDelete;
use crate::errores;
use crate::insert::ConsultaInsert;
use crate::select::ConsultaSelect;
use crate::update::ConsultaUpdate;
use std::collections::{HashMap, HashSet};

const SELECT: &str = "select";
const INSERT: &str = "insert";
const DELETE: &str = "delete";
const UPDATE: &str = "update";
const CARACTER_VACIO: &str = "";
pub trait Parseables {
    /// Función para parsear cualquier cosa que se encuentre en la consulta.
    /// Se encarga de buscar las palabras clave de inicio y final, y devolver los campos entre ellas.
    /// Además, se encarga de convertir los campos a minúsculas si se especifica.
    /// Parámetros:
    /// - `consulta`: La consulta SQL en formato `Vec<String>`.
    /// - `keywords_inicio`: Un vector de cadenas de texto que contiene las palabras clave de inicio.
    /// - `keyword_final`: Un conjunto de cadenas de texto que contiene las palabras clave finales.
    /// - `parseo_lower`: Un booleano que indica si se deben convertir los campos a minúsculas.
    /// - `opcional`: Un booleano que indica si las palabras clave de inicio son opcionales.
    //
    ///   Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`) que puede ser sintaxis que
    ///   puede ocurrir al parsear.

    fn parsear_cualquier_cosa(
        consulta: &[String],
        keywords_inicio: Vec<String>,
        keyword_final: HashSet<String>,
        parseo_lower: bool,
        opcional: bool, // parámetro para indicar si las palabras clave de inicio son opcionales
    ) -> Result<Vec<String>, errores::Errores> {
        let mut campos = Vec::new();
        let mut keyword_final_encontrada = false;

        let index = buscar_keywords_inicio_seguidas(consulta, &keywords_inicio, opcional)?;
        if index == 0 && opcional {
            return Ok(campos); // Si no se encuentran las palabras clave de inicio opcionales, devolver campos vacío
        }

        let mut index = index;
        while index < consulta.len() {
            let token = consulta[index].to_lowercase();
            if keyword_final.contains(&token) {
                keyword_final_encontrada = true;
                break;
            }
            campos.push(if parseo_lower {
                token
            } else {
                consulta[index].to_string()
            });
            index += 1;
        }

        if campos.is_empty() {
            return Err(errores::Errores::InvalidSyntax);
        }

        if keyword_final.contains(CARACTER_VACIO) || keyword_final_encontrada {
            Ok(campos)
        } else {
            Err(errores::Errores::InvalidSyntax)
        }
    }
}

fn buscar_keywords_inicio_seguidas(
    consulta: &[String],
    keywords_inicio: &[String],
    opcional: bool,
) -> Result<usize, errores::Errores> {
    let mut index = 0;
    let mut keyword_index = 0;

    while index < consulta.len() {
        if consulta[index].to_lowercase() == keywords_inicio[keyword_index].to_lowercase() {
            keyword_index += 1;
            if keyword_index == keywords_inicio.len() {
                return Ok(index + 1); // Se encontraron todas las palabras clave seguidas
            }
        } else if keyword_index > 0 {
            // Si se encontró solo una de las palabras clave, devolver error de sintaxis
            return Err(errores::Errores::InvalidSyntax);
        }
        index += 1;
    }

    if opcional && keyword_index == 0 {
        Ok(0) // Si las palabras clave de inicio son opcionales y no se encuentran, devolver 0
    } else {
        Err(errores::Errores::InvalidSyntax)
    }
}

// Trait para definir metodos comunes de las consultas posibles
pub trait MetodosConsulta {
    /// Verifica la validez de la consulta SQL.
    /// verifica que la tabla a la que se quiere inserta exista, así como los campos de la consulta no estén vacíos
    /// y que todos los campos solicitados sean válidos según los campos posibles definidos en la estructura.
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn verificar_validez_consulta(&mut self) -> Result<(), errores::Errores>;

    /// Procesa la consulta
    /// Se encarga de procesar la consulta SQL y realizar la operación correspondiente, segun el tipo de consulta.
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn procesar(&mut self) -> Result<(), errores::Errores>;
}

/// Enumeración que define los tipos de consultas SQL posibles.
/// Cada tipo de consulta tiene su propia estructura de datos asociada.

#[derive(Debug)]
pub enum SQLConsulta {
    Select(ConsultaSelect),
    Insert(ConsultaInsert),
    Delete(ConsultaDelete),
    Update(ConsultaUpdate),
}

impl SQLConsulta {
    /// Crea una nueva instancia de `SQLConsulta` a partir de una cadena de consulta SQL.
    /// Esta funcion delega la creación de la consulta a la estructura correspondiente.
    ///
    /// # Parámetros
    /// - `consulta`: La consulta SQL en formato `String`.
    /// - `ruta_tablas`: La ruta del archivo de la que se va a conseguir la tabla.
    ///
    /// # Retorno
    /// Una instancia de `SQLConsulta` si la consulta es válida, o un error de tipo `Errores`.

    pub fn crear_consulta(
        consulta: &str,
        ruta_tablas: &String,
    ) -> Result<SQLConsulta, errores::Errores> {
        // Primero eliminamos los espacios
        let consulta_limpia: Vec<String> = parsear_consulta_de_comando(consulta);
        if consulta_limpia.len() < 2 {
            Err(errores::Errores::InvalidSyntax)?
        }

        // Usamos match para decidir el tipo de consulta
        match consulta_limpia[0].to_lowercase().as_str() {
            SELECT => Ok(SQLConsulta::Select(ConsultaSelect::crear(
                &consulta_limpia,
                ruta_tablas,
            )?)),
            INSERT => Ok(SQLConsulta::Insert(ConsultaInsert::crear(
                &consulta_limpia,
                ruta_tablas,
            )?)),
            UPDATE => Ok(SQLConsulta::Update(ConsultaUpdate::crear(
                &consulta_limpia,
                ruta_tablas,
            )?)),
            DELETE => Ok(SQLConsulta::Delete(ConsultaDelete::crear(
                &consulta_limpia,
                ruta_tablas,
            )?)),
            _ => Err(errores::Errores::InvalidSyntax),
        }
    }

    /// Procesa la consulta
    /// Se encarga de procesar la consulta SQL y realizar la operación correspondiente, segun el tipo de consulta.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    pub fn procesar_consulta(&mut self) -> Result<(), errores::Errores> {
        match self.verificar_validez_consulta() {
            Ok(_) => {}
            Err(consulta_no_valida) => {
                Err(consulta_no_valida)?;
            }
        }

        match self {
            SQLConsulta::Select(consulta_select) => consulta_select.procesar(),
            SQLConsulta::Insert(consulta_insert) => consulta_insert.procesar(),
            SQLConsulta::Update(consulta_update) => consulta_update.procesar(),
            SQLConsulta::Delete(consulta_delete) => consulta_delete.procesar(),
        }
    }

    /// Verifica la validez de la consulta SQL.
    /// Delega la responsabilidad de verificar la validez de la consulta a la estructura correspondiente.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn verificar_validez_consulta(&mut self) -> Result<(), errores::Errores> {
        match self {
            SQLConsulta::Select(consulta_select) => consulta_select.verificar_validez_consulta(),
            SQLConsulta::Insert(consulta_insert) => consulta_insert.verificar_validez_consulta(),
            SQLConsulta::Update(consulta_update) => consulta_update.verificar_validez_consulta(),
            SQLConsulta::Delete(consulta_delete) => consulta_delete.verificar_validez_consulta(),
        }
    }
}

/// Función para mapear los campos de una tabla a un HashMap.
/// Se encarga de mapear los campos de una tabla a un HashMap, donde la clave es el nombre del campo y
/// el valor es el índice del campo, en el orden en que aparecen en la tabla.
/// ejemplo: linea parseado del archivo de tablas: "id,nombre,edad" -> HashMap {id: 0, nombre: 1, edad: 2}
/// Parámetros:
/// - `campos`: Un vector de cadenas de texto (`Vec<String>`) que contiene los campos de la tabla.
///
/// Retorna un HashMap con los campos mapeados.

pub fn mapear_campos(campos: &[String]) -> HashMap<String, usize> {
    let mut campos_mapeados: HashMap<String, usize> = HashMap::new();
    for (indice, campo) in campos.iter().enumerate() {
        let indice_i: usize = indice;
        campos_mapeados.insert(campo.to_string(), indice_i);
    }
    campos_mapeados
}
pub trait Verificaciones {
    /// Verifica que los campos de la consulta sean válidos.
    /// Se encarga de verificar que los campos de la consulta sean válidos, es decir, que estén en la lista de campos posibles.
    /// Parámetros:
    /// - `campos_validos`: Un HashMap que contiene los campos posibles.
    /// - `campos_consulta`: Un vector de cadenas de texto que contiene los campos de la consulta.
    ///
    ///  Retorna un booleano que indica si los campos de la consulta son válidos.

    fn verificar_campos_validos(
        campos_validos: &HashMap<String, usize>,
        campos_consulta: &mut Vec<String>,
    ) -> bool;

    fn verificar_orden_keywords(
        query: &[String],
        palabras_clave_consulta: Vec<&str>,
    ) -> Result<HashSet<String>, errores::Errores> {
        let mut keyword_positions = vec![];
        let mut found_keywords = std::collections::HashSet::new();

        // Verificar que cada palabra clave está en el lugar correcto y es única
        for keyword in &palabras_clave_consulta {
            // Buscar la posición de la palabra clave
            if let Some(pos) = query.iter().position(|t| t.to_lowercase() == *keyword) {
                // Verificar si la palabra clave ya fue encontrada (unicidad)
                if !found_keywords.insert(keyword.to_lowercase()) {
                    Err(errores::Errores::InvalidSyntax)?;
                }
                keyword_positions.push((keyword.to_lowercase(), pos));
            } else if keyword.to_lowercase() != "where"
                && keyword.to_lowercase() != "order"
                && keyword.to_lowercase() != "by"
            {
                //SELECT Y FROM SIEMPRE DEBEN ESTAR
                // WHERE y ORDER BY son opcionales
                Err(errores::Errores::InvalidSyntax)?;
            }
        }

        // Verificar que las palabras clave están en el orden correcto
        for i in 1..keyword_positions.len() {
            if keyword_positions[i].1 < keyword_positions[i - 1].1 {
                Err(errores::Errores::InvalidSyntax)?;
            }
        }
        Ok(found_keywords)
    }
}

/// Función para obtener los campos de una consulta en el orden por defecto.
/// Se encarga de obtener los campos de una consulta en el orden por defecto, es decir, en el orden en que aparecen en la tabla.
/// Parámetros:
/// - `campos`: Un HashMap que contiene los campos de la tabla.
///   Retorna un vector con los campos de la consulta en el orden por defecto.

pub fn obtener_campos_consulta_orden_por_defecto(campos: &HashMap<String, usize>) -> Vec<String> {
    // Convertimos el HashMap en un vector de pares (clave, valor)
    let mut vec: Vec<(&String, &usize)> = campos.iter().collect();

    // Ordenamos el vector por el valor
    vec.sort_by(|a, b| a.1.cmp(b.1));

    let mut campos_tabla: Vec<String> = Vec::new();
    // Iteramos sobre los pares ordenados
    for (key, _value) in vec {
        campos_tabla.push(key.to_string());
    }
    campos_tabla
}

/// Función para parsear una consulta de comando.
/// Se encarga de parsear una consulta de comando y devolver un vector con las palabras de la consulta.
/// Parámetros:
/// - `consulta`: Una cadena de texto que contiene la consulta de comando.
///     Retorna un vector con las palabras de la consulta.

pub fn parsear_consulta_de_comando(consulta: &str) -> Vec<String> {
    return consulta.split_whitespace().map(|s| s.to_string()).collect();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_mapear_campos() {
        let campos = vec!["id".to_string(), "nombre".to_string(), "edad".to_string()];
        let resultado = mapear_campos(&campos);

        let mut esperado = HashMap::new();
        esperado.insert("id".to_string(), 0);
        esperado.insert("nombre".to_string(), 1);
        esperado.insert("edad".to_string(), 2);

        assert_eq!(resultado, esperado);
    }

    #[test]
    fn test_obtener_campos_consulta_orden_por_defecto() {
        let mut campos = HashMap::new();
        campos.insert("nombre".to_string(), 1);
        campos.insert("id".to_string(), 0);
        campos.insert("edad".to_string(), 2);

        let resultado = obtener_campos_consulta_orden_por_defecto(&campos);
        let esperado = vec!["id".to_string(), "nombre".to_string(), "edad".to_string()];

        assert_eq!(resultado, esperado);
    }

    #[test]
    fn test_crear_consulta_select() {
        let consulta = "SELECT * FROM tabla".to_string();
        let ruta_tablas = "ruta/a/tablas".to_string();
        let resultado = SQLConsulta::crear_consulta(&consulta, &ruta_tablas);

        assert!(resultado.is_ok());
        match resultado.unwrap() {
            SQLConsulta::Select(_) => assert!(true),
            _ => assert!(false, "Se esperaba una consulta de tipo SELECT"),
        }
    }

    #[test]
    fn crear_consulta_select_con_diferentes_campos() {
        let consulta = "SELECT id, nombre FROM tabla".to_string();
        let ruta_tablas = "ruta/a/tablas".to_string();
        let resultado = SQLConsulta::crear_consulta(&consulta, &ruta_tablas);

        assert!(resultado.is_ok());
        match resultado.unwrap() {
            SQLConsulta::Select(_) => assert!(true),
            _ => assert!(false, "Se esperaba una consulta de tipo SELECT"),
        }
    }

    #[test]
    fn crear_consulta_select_invalida() {
        let consulta = "SELECT FROM tabla".to_string();
        let ruta_tablas = "ruta/a/tablas".to_string();
        let resultado = SQLConsulta::crear_consulta(&consulta, &ruta_tablas);

        assert!(resultado.is_err());
    }

    #[test]
    fn test_crear_consulta_insert_valida() {
        let consulta = "INSERT INTO tabla (id, nombre ) VALUES (1, 'John')".to_string();
        let ruta_tablas = "ruta/a/tablas".to_string();
        let resultado = SQLConsulta::crear_consulta(&consulta, &ruta_tablas);

        assert!(resultado.is_ok());
        match resultado.unwrap() {
            SQLConsulta::Insert(_) => assert!(true),
            _ => assert!(false, "Se esperaba una consulta de tipo INSERT"),
        }
    }

    #[test]
    fn crear_consulta_insert_valida_con_campos_y_valores_vacios() {
        let consulta = "INSERT INTO tabla (id, nombre) VALUES (,)".to_string();
        let ruta_tablas = "ruta/a/tablas".to_string();
        let resultado = SQLConsulta::crear_consulta(&consulta, &ruta_tablas);

        assert!(resultado.is_ok());
    }

    #[test]
    fn test_crear_consulta_insert_valida_() {
        let consulta = "INSERT INTO tabla VALUES (1, 'John')".to_string();
        let ruta_tablas = "ruta/a/tablas".to_string();
        let resultado = SQLConsulta::crear_consulta(&consulta, &ruta_tablas);

        assert!(resultado.is_ok());
    }

    #[test]
    fn test_crear_consulta_invalida() {
        let consulta = " * FROM tabla".to_string();
        let ruta_tablas = "ruta/a/tablas".to_string();
        let resultado = SQLConsulta::crear_consulta(&consulta, &ruta_tablas);

        assert!(resultado.is_err());
    }
}

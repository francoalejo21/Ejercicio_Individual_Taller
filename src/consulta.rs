use crate::{errores, parseos::parseo};
use crate::insert::ConsultaInsert;
use crate::select::ConsultaSelect;
use std::collections::{HashMap, HashSet};
use crate::update::ConsultaUpdate;
use crate::delete::ConsultaDelete;

pub trait Parseables {
    fn parsear_cualquier_cosa(
        consulta: &[String], 
        keywords_inicio: Vec<String>, 
        keyword_final: HashSet<String>, 
        caracteres_delimitadores: Vec<char>, 
        parseo_lower: bool,
        opcional: bool, // Nuevo parámetro para indicar si las palabras clave de inicio son opcionales
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
            campos.push(if parseo_lower { token } else { consulta[index].to_string() });
            index += 1;
        }
    
        if campos.is_empty() {
            return Err(errores::Errores::InvalidSyntax);
        }
    
        let campos_parseados = parseo(&campos, &caracteres_delimitadores);
        if keyword_final.contains("") || keyword_final_encontrada {
            Ok(campos_parseados)
        } else {
            Err(errores::Errores::InvalidSyntax)
        }
    }
}

fn buscar_keywords_inicio_seguidas(
    consulta: &[String], 
    keywords_inicio: &[String],
    opcional: bool // Nuevo parámetro para indicar si las palabras clave de inicio son opcionales
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
    /// Verifica si la consulta es válida.
    ///
    /// Se asegura de que la consulta contenga los campos necesarios y que los campos de la consulta
    /// coincidan con los campos válidos de la tabla.
    ///
    /// # Retorno
    /// - `Ok(())`: Si la consulta es válida.
    /// - `Err(errores::Errores::InvalidSyntax)`: Si faltan campos en la consulta.
    /// - `Err(errores::Errores::InvalidColumn)`: Si la consulta contiene columnas inválidas.

    fn verificar_validez_consulta(&mut self) -> Result<(), errores::Errores>;

    /// Procesa la consulta
    ///
    /// # Retorno
    /// Retorna `Ok(())` si la consulta fue exitosa o un error si hubo algún problema al procesarla.

    fn procesar(&mut self) -> Result<(), errores::Errores>;
}
#[derive(Debug)]
pub enum SQLConsulta {
    Select(ConsultaSelect),
    Insert(ConsultaInsert),
    Delete(ConsultaDelete),
    Update(ConsultaUpdate),
}

impl SQLConsulta {
    //Documentar cuando la tenga terminada
    pub fn crear_consulta(
        consulta: &str,
        ruta_tablas: &String,
    ) -> Result<SQLConsulta, errores::Errores> {
        // Primero eliminamos los espacios
        let consulta_limpia: Vec<String> = parsear_consulta_de_comando(consulta);
        if consulta_limpia.len() < 2{
            Err(errores::Errores::InvalidSyntax)?
        }
        let consultas = ["select", "insert", "into","delete", "from", "update"];
        // Usamos match para decidir el tipo de consulta
        match &consulta_limpia[0].to_lowercase() {
            tipo_consulta if tipo_consulta == consultas[0]   => {
                Ok(SQLConsulta::Select(
                ConsultaSelect::crear(&consulta_limpia, ruta_tablas)?,
                ))},                
            tipo_consulta if tipo_consulta == consultas[1] => match &consulta_limpia[1].to_lowercase(){
                tipo_consulta if tipo_consulta == consultas[2] => 
                Ok(SQLConsulta::Insert(ConsultaInsert::crear(&consulta_limpia, ruta_tablas)?)),
                _ => Err(errores::Errores::InvalidSyntax)?},
            tipo_consulta if tipo_consulta == consultas[5] =>{
                Ok(SQLConsulta::Update(ConsultaUpdate::crear(&consulta_limpia, ruta_tablas)?))},
            tipo_consulta if tipo_consulta == consultas[3] =>{
                Ok(SQLConsulta::Delete(ConsultaDelete::crear(&consulta_limpia, ruta_tablas)?))},
            _=> {
                // En caso de que no coincida con ninguna consulta soportada, retornamos un error
                Err(errores::Errores::InvalidSyntax)?
            }
        }
    }

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

    fn verificar_validez_consulta(&mut self) -> Result<(), errores::Errores> {
        match self {
            SQLConsulta::Select(consulta_select) => consulta_select.verificar_validez_consulta(),
            SQLConsulta::Insert(consulta_insert) => consulta_insert.verificar_validez_consulta(),
            SQLConsulta::Update(consulta_update) => consulta_update.verificar_validez_consulta(),
            SQLConsulta::Delete(consulta_delete) => consulta_delete.verificar_validez_consulta(),
        }
    }
}

pub fn mapear_campos(campos: &[String]) -> HashMap<String, usize> {
    let mut campos_mapeados: HashMap<String, usize> = HashMap::new();
    for (indice,campo) in campos.iter().enumerate() {
        let indice_i: usize = indice;
        campos_mapeados.insert(campo.to_string(), indice_i);
    }
    campos_mapeados
}
pub trait Verificaciones {
    fn verificar_campos_validos(
        campos_validos: &HashMap<String, usize>,
        campos_consulta: &mut Vec<String>,
    ) -> bool;
}

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

pub fn parsear_consulta_de_comando(consulta: &str) -> Vec<String> {
    return consulta
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();    
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
    fn test_crear_consulta_insert() {
        let consulta = "INSERT INTO tabla (id, nombre ) VALUES (1, 'John')".to_string();
        let ruta_tablas = "ruta/a/tablas".to_string();
        let resultado = SQLConsulta::crear_consulta(&consulta, &ruta_tablas);

        assert!(resultado.is_ok());
        match resultado.unwrap() {
            //SQLConsulta::Insert(_) => assert!(true),
            _ => assert!(false, "Se esperaba una consulta de tipo INSERT"),
        }
    }

    #[test]
    fn test_crear_consulta_invalida() {
        let consulta = " * FROM tabla".to_string();
        let ruta_tablas = "ruta/a/tablas".to_string();
        let resultado = SQLConsulta::crear_consulta(&consulta, &ruta_tablas);

        assert!(resultado.is_err());
        match resultado.unwrap() {
            SQLConsulta::Select(_) => assert!(true),
            _ => assert!(false, "Se esperaba una consulta válida"),
        }
    }
}

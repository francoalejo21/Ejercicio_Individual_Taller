use crate::errores;
use crate::insert::ConsultaInsert;
use crate::select::ConsultaSelect;
use std::collections::HashMap;

pub trait Parseables {
    fn parsear_campos(consulta: &Vec<String>, index: &mut usize) -> Vec<String>;
    fn parsear_tabla(consulta: &Vec<String>, index: &mut usize) -> String;
    fn parsear_restricciones(_consulta: &Vec<String>, _index: &mut usize) -> Vec<String> {
        Vec::new()
    }
    fn parsear_ordenamiento(_consulta: &Vec<String>, _index: &mut usize) -> Vec<String> {
        Vec::new()
    }
    fn parsear_valores(_consulta: &Vec<String>, _index: &mut usize) -> Vec<Vec<String>> {
        Vec::new()
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
    //Delete(ConsultaDelete),
    //Update(ConsultaUpdate),
}

impl SQLConsulta {
    //Documentar cuando la tenga terminada
    pub fn crear_consulta(
        consulta: &String,
        ruta_tablas: &String,
    ) -> Result<SQLConsulta, errores::Errores> {
        // Primero eliminamos los espacios al inicio y convertimos la consulta a minúsculas
        let consulta_limpia = &consulta.trim_start().to_lowercase();

        // Usamos match para decidir el tipo de consulta
        match consulta_limpia.as_str() {
            _ if consulta_limpia.starts_with("select") => Ok(SQLConsulta::Select(
                ConsultaSelect::crear(consulta_limpia, ruta_tablas),
            )),
            _ if consulta_limpia.starts_with("insert into") => Ok(SQLConsulta::Insert(
                ConsultaInsert::crear(consulta_limpia, ruta_tablas),
            )),
            _ => {
                // En caso de que no coincida con ninguna consulta soportada, retornamos un error
                return Err(errores::Errores::InvalidSyntax);
            }
        }
    }

    pub fn procesar_consulta(&mut self) -> Result<(), errores::Errores> {
        match self.verificar_validez_consulta() {
            Ok(_) => {}
            Err(consulta_no_valida) => {
                return Err(consulta_no_valida);
            }
        }

        match self {
            SQLConsulta::Select(consulta_select) => consulta_select.procesar(),
            SQLConsulta::Insert(consulta_insert) => consulta_insert.procesar(),
        }
    }

    fn verificar_validez_consulta(&mut self) -> Result<(), errores::Errores> {
        match self {
            SQLConsulta::Select(consulta_select) => consulta_select.verificar_validez_consulta(),
            SQLConsulta::Insert(consulta_insert) => consulta_insert.verificar_validez_consulta(),
        }
    }
}

pub fn mapear_campos(campos: &Vec<String>) -> HashMap<String, usize> {
    let mut campos_mapeados: HashMap<String, usize> = HashMap::new();
    let mut indice: usize = 0;
    for campo in campos {
        let indice_i: usize = indice;
        campos_mapeados.insert(campo.to_string(), indice_i);
        indice += 1;
    }
    return campos_mapeados;
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
            SQLConsulta::Insert(_) => assert!(true),
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

use crate::archivo::{self, leer_archivo, procesar_ruta};
use crate::consulta::{mapear_campos, obtener_campos_consulta_orden_por_defecto, MetodosConsulta, Parseables, Verificaciones};
use crate::errores;
use archivo::parsear_linea_archivo;
use std::{
    collections::HashMap,
    io::BufRead,
};
//TODO: implementar restricciones, ordenamiento y mejorar el parseo

/// Representa una consulta SQL de selección.
///
/// Esta estructura contiene la información necesaria para realizar una consulta
/// de selección en una base de datos. Incluye los campos que se desean seleccionar,
/// los posibles campos de la tabla, el nombre de la tabla, las restricciones aplicadas
/// a la consulta y el ordenamiento de los resultados.
///
/// # Campos
///
/// - `campos_consulta`: Un vector de cadenas de texto (`Vec<String>`) que especifica
///   los campos que se desean incluir en los resultados de la consulta.
/// - `campos_posibles`: Un mapa (`HashMap<String, usize>`) que asocia los nombres de
///   los campos de la tabla con sus índices. Este mapa permite la validación de campos
///   seleccionados y la referencia a los campos por su índice.
/// - `tabla`: Una cadena de texto (`String`) que indica el nombre de la tabla en la
///   que se realiza la consulta.
/// - `restricciones`: Un vector de cadenas de texto (`Vec<String>`) que contiene las
///   restricciones aplicadas a la consulta.
/// - `ordenamiento`: Un vector de cadenas de texto (`Vec<String>`) que especifica
///   el criterio de ordenamiento de los resultados. Los valores en este vector pueden
///   ser nombres de campos seguidos opcionalmente por la palabra clave `ASC` o `DESC`
///   para indicar el orden ascendente o descendente.
#[derive(Debug)]
pub struct ConsultaSelect {
    pub campos_consulta: Vec<String>,
    pub campos_posibles: HashMap<String, usize>,
    pub tabla: String,
    pub restricciones: Vec<String>,
    pub ordenamiento: Vec<String>,
    pub ruta_tabla : String,
}

impl ConsultaSelect {
    /// Crea una nueva instancia de `ConsultaSelect` a partir de una cadena de consulta SQL.
    ///
    /// Este método toma una consulta SQL en formato `String` y la procesa para extraer los
    /// campos de consulta, la tabla, las restricciones, y el ordenamiento.
    ///
    /// # Parámetros
    /// - `consulta`: La consulta SQL en formato `String`.
    ///
    /// # Retorno
    /// Retorna una instancia de `ConsultaSelect` con los campos, tabla, restricciones y
    /// ordenamiento extraídos.

    pub fn crear(consulta: &String, ruta_a_tablas: &String) -> ConsultaSelect {
        let consulta_parseada = &Self::parsear_consulta_de_comando_select(&consulta);
        let mut index = 1; //nos salteamos la palabra select
        let campos_consulta = Self::parsear_campos(consulta_parseada, &mut index);
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let tabla = Self::parsear_tabla(consulta_parseada, &mut index);
        let restricciones = Self::parsear_restricciones(consulta_parseada, &mut index);
        let ordenamiento = Self::parsear_ordenamiento(consulta_parseada, &mut index);
        let ruta_tabla = procesar_ruta(&ruta_a_tablas, &tabla);

        ConsultaSelect {
            campos_consulta,
            campos_posibles,
            tabla,
            restricciones,
            ordenamiento,
            ruta_tabla,
        }
    }
    /// Parsea una consulta SQL para obtener los distintos tokens.
    ///
    /// Convierte la consulta a minúsculas, reemplaza las comas por espacios y divide la cadena en
    /// palabras.
    ///
    /// # Parámetros
    /// - `consulta`: La consulta SQL en formato `String`.
    ///
    /// # Retorno
    /// Retorna un `Vec<String>` que contiene cada palabra de la consulta SQL.
    
    fn parsear_consulta_de_comando_select(consulta: &String) -> Vec<String> {
        return consulta
            .replace(",", " ")
            .to_lowercase()
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();
    }

}

impl Parseables for ConsultaSelect {
    /// Extrae los campos de consulta a partir de la consulta SQL.
    ///
    /// A partir de una lista de tokens, extrae los campos hasta que encuentre la palabra clave `FROM`.
    ///
    /// # Parámetros
    /// - `consulta`: Un vector de cadenas que representa la consulta SQL tokenizada.
    /// - `index`: Un índice mutable que se actualiza conforme se procesan los tokens.
    ///
    /// # Retorno
    /// Un `Vec<String>` que contiene los nombres de los campos a consultar.
   
    fn parsear_campos(consulta: &Vec<String>, index: &mut usize) -> Vec<String> {
        let mut campos: Vec<String> = Vec::new();
        while *index < consulta.len() && consulta[*index] != "from" {
            let campo = &consulta[*index];
            campos.push(campo.to_string());
            *index += 1;
        }
        campos
    }
    /// Extrae el nombre de la tabla a partir de la consulta SQL.
    ///
    /// Busca la palabra clave `FROM` en los tokens de la consulta y toma el siguiente token como el nombre de la tabla.
    ///
    /// # Parámetros
    /// - `consulta`: Un vector de cadenas que representa la consulta SQL tokenizada.
    /// - `index`: Un índice mutable que se actualiza conforme se procesa la consulta.
    ///
    /// # Retorno
    /// Una cadena de texto (`String`) que contiene el nombre de la tabla.

    fn parsear_tabla(consulta: &Vec<String>, index: &mut usize) -> String {
        let mut tabla = String::new();
        if consulta[*index] == "from" {
            *index += 1
        }
        if *index < consulta.len() {
            let tabla_consulta = &consulta[*index];
            *index += 1;
            tabla = tabla_consulta.to_string();
        }
        tabla
    }

    /// Extrae las restricciones a partir de la consulta SQL.
    ///
    /// Busca la palabra clave `WHERE` en los tokens de la consulta y toma los tokens siguientes como restricciones hasta
    /// encontrar la palabra clave `ORDER` o `BY`.
    ///
    /// # Parámetros
    /// - `consulta`: Un vector de cadenas que representa la consulta SQL tokenizada.
    /// - `index`: Un índice mutable que se actualiza conforme se procesan los tokens.
    ///
    /// # Retorno
    /// Un `Vec<String>` que contiene las restricciones de la consulta.`

    fn parsear_restricciones(consulta: &Vec<String>, index: &mut usize) -> Vec<String> {
        let mut restricciones = Vec::new();

        while *index < consulta.len() {
            let palabra = &consulta[*index];
            if palabra == "where" {
                *index += 1;
                while *index < consulta.len()
                    && consulta[*index] != "order"
                    && consulta[*index] != "by"
                {
                    let palabra = &consulta[*index];
                    restricciones.push(palabra.to_string());
                    *index += 1;
                }
                break;
            } else {
                *index += 1;
            }
        }
        restricciones
    }

    /// Extrae el criterio de ordenamiento a partir de la consulta SQL.
    ///
    /// Busca las palabras clave `ORDER` y `BY` en los tokens de la consulta y toma los tokens siguientes como criterios de
    /// ordenamiento.
    ///
    /// # Parámetros
    /// - `consulta`: Un vector de cadenas que representa la consulta SQL tokenizada.
    /// - `index`: Un índice mutable que se actualiza conforme se procesan los tokens.
    ///
    /// # Retorno
    /// Un `Vec<String>` que contiene los criterios de ordenamiento de la consulta.

    fn parsear_ordenamiento(consulta: &Vec<String>, index: &mut usize) -> Vec<String> {
        let mut ordenamiento = Vec::new();

        while *index < consulta.len() {
            let palabra = &consulta[*index];
            if palabra == "order" {
                *index += 1;
                if *index < consulta.len() && consulta[*index] == "by" {
                    *index += 1;
                    while *index < consulta.len() {
                        let palabra = &consulta[*index];
                        ordenamiento.push(palabra.to_string());
                        *index += 1;
                    }
                }
            }
        }
        ordenamiento
    }
}

impl MetodosConsulta for ConsultaSelect {
    /// Verifica la validez de la consulta SQL.
    ///
    /// Este método verifica que los campos de consulta no estén vacíos,que exista la tabla y que todos los campos
    /// solicitados sean válidos según los campos posibles definidos en la estructura.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn verificar_validez_consulta(&mut self) -> Result<(), errores::Errores> {
        match leer_archivo(&self.ruta_tabla){
            Ok(mut lector)=>{
        let mut nombres_campos = String::new();
        lector.read_line(&mut nombres_campos).map_err(|_| errores::Errores::Error)?;
        let (_, campos_validos) = &parsear_linea_archivo(&nombres_campos);
        self.campos_posibles = mapear_campos(campos_validos);
        },
            Err(_)=>return Err(errores::Errores::InvalidTable)
        };
        if self.campos_consulta.is_empty() {
            return Err(errores::Errores::InvalidSyntax);
        }
        let campos_posibles = &self.campos_posibles;
        if !ConsultaSelect::verificar_campos_validos(campos_posibles, &mut self.campos_consulta) {
            return Err(errores::Errores::InvalidColumn);
        }
        Ok(())
    }

    /// Procesa el contenido del archivo tabla y muestra los resultados de la consulta.
    ///
    /// Lee línea por línea del archivo proporcionado y muestra las líneas que cumplen con los campos seleccionados.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn procesar(&mut self) -> Result<(), errores::Errores> {
        //primera version select normal sin condiciones;
        let mut lector =
        leer_archivo(&self.ruta_tabla).map_err(|_| errores::Errores::InvalidTable)?;

        let mut nombres_campos = String::new();
        lector.read_line(&mut nombres_campos).map_err(|_| errores::Errores::Error)?;

        for registro in lector.lines() {
            let (registro_parseado, _) = match registro {
                Ok(registro) => parsear_linea_archivo(&registro),
                Err(_) => return Err(errores::Errores::Error),
            };

            let mut campos_seleccionados: Vec<&usize> = Vec::new();
            for campo in &self.campos_consulta {
                match self.campos_posibles.get(campo) {
                    Some(valor) => campos_seleccionados.push(valor),
                    None => return Err(errores::Errores::Error),
                };
            }

            let mut linea: Vec<&str> = Vec::new();
            for campo in campos_seleccionados {
                linea.push(&registro_parseado[*campo]);
            }
            let linea = linea.join(",");
            println!("{}", linea);
        }
        Ok(())
    }
}

impl Verificaciones for ConsultaSelect {
    /// verifica si los campos de la consulta son existen en la tabla
    ///
    /// # Parámetros
    /// - `campos_validos`: Todos los campos de la tabla que son válidos
    /// - `campos_consulta`: Todos los campos que se quieren seleccionar
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn verificar_campos_validos(
        campos_validos: &HashMap<String, usize>,
        campos_consulta: &mut Vec<String>,
    ) -> bool {
        if campos_consulta.len() == 1{
            if campos_consulta[0] == "*".to_string(){
                campos_consulta.pop(); //Me saco de encima el "*""
                //debo reemplazar ese caracter por todos los campos válidos
                let campos = &obtener_campos_consulta_orden_por_defecto(campos_validos);
                for campo in campos{
                    campos_consulta.push(campo.to_string());
                }
                return true;
            }   
        }
    
        for campo in campos_consulta {
            if !(campos_validos.contains_key(campo)) {
                return false;
            }
        }
        return true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_parsear_consulta_select() {
        let consulta = String::from("SELECT campo1, campo2 FROM tabla WHERE campo1 = 'valor1' ORDER BY campo2 DESC");
        let tokens = ConsultaSelect::parsear_consulta_de_comando_select(&consulta);

        assert_eq!(
            tokens,
            vec![
                "select", "campo1", "campo2", "from", "tabla", "where", "campo1", "=", "'valor1'", "order", "by", "campo2", "desc"
            ]
        );
    }

    #[test]
    fn test_crear_consulta_select() {
        let consulta = String::from("SELECT campo1, campo2 FROM tabla WHERE campo1 = 'valor1' ORDER BY campo2 DESC");
        let ruta_tabla = String::from("/ruta/a/tablas");

        let consulta_select = ConsultaSelect::crear(&consulta, &ruta_tabla);

        assert_eq!(consulta_select.campos_consulta, vec!["campo1", "campo2"]);
        assert_eq!(consulta_select.tabla, "tabla");
        assert_eq!(consulta_select.restricciones, vec!["campo1", "=", "'valor1'"]);
        assert_eq!(consulta_select.ordenamiento, vec!["campo2", "desc"]);
        assert_eq!(consulta_select.ruta_tabla, "/ruta/a/tablas/tabla");
    }

    #[test]
    fn test_verificar_campos_validos() {
        let mut campos_validos = HashMap::new();
        campos_validos.insert("campo1".to_string(), 0);
        campos_validos.insert("campo2".to_string(), 1);

        let mut campos_consulta = vec!["campo1".to_string(), "campo2".to_string()];
        let resultado = ConsultaSelect::verificar_campos_validos(&campos_validos, &mut campos_consulta);

        assert!(resultado);
    }

    #[test]
    fn test_verificar_campos_invalidos() {
        let mut campos_validos = HashMap::new();
        campos_validos.insert("campo1".to_string(), 0);

        let mut campos_consulta = vec!["campo1".to_string(), "campo3".to_string()];
        let resultado = ConsultaSelect::verificar_campos_validos(&campos_validos, &mut campos_consulta);

        assert!(!resultado);
    }

    #[test]
    fn test_verificar_consulta_valida() {
        let mut consulta = ConsultaSelect {
            campos_consulta: vec!["nombre".to_string()],
            campos_posibles: HashMap::from([
                ("nombre".to_string(), 0),
                ("edad".to_string(), 1),
                ("ciudad".to_string(), 2),
            ]),
            tabla: "personas".to_string(),
            restricciones: vec![],
            ordenamiento: vec![],
            ruta_tabla: "tablas/personas".to_string(),
        };

        let resultado = consulta.verificar_validez_consulta();
        assert!(resultado.is_ok());
    }

    #[test]
    fn test_verificar_consulta_invalida() {
        let mut consulta = ConsultaSelect {
            campos_consulta: vec!["campo_invalido".to_string()],
            campos_posibles: HashMap::new(),
            tabla: "tabla".to_string(),
            restricciones: vec![],
            ordenamiento: vec![],
            ruta_tabla: "/ruta/a/tablas/tabla".to_string(),
        };

        let resultado = consulta.verificar_validez_consulta();
        assert!(resultado.is_err());
    }
}
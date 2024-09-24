use crate::archivo::{leer_archivo, parsear_linea_archivo, procesar_ruta};
use crate::consulta::{mapear_campos, MetodosConsulta, Parseables, Verificaciones};
use crate::errores;
use std::fs::OpenOptions;
use std::path::Path;
use std::{
    collections::HashMap,
    io::{BufRead, BufWriter, Write},
};

/// Representa una consulta SQL de inserción.
///
/// Esta estructura contiene la información necesaria para realizar una consulta
/// de inserción en una base de datos. Incluye los campos a insertar, los valores a
/// insertar, la tabla en la que se insertarán los datos y la ruta del archivo
/// que se actualizará.
///
/// # Campos
///
/// - `campos_consulta`: Un vector de cadenas de texto (`Vec<String>`) que especifica
///   los campos en los que se van a insertar los datos.
/// - `campos_posibles`: Un mapa (`HashMap<String, usize>`) que asocia los nombres de los
///   campos de la tabla con sus índices. Este mapa permite la validación de campos.
/// - `valores`: Un vector de vectores de cadenas de texto (`Vec<Vec<String>>`) que contiene
///   los valores a insertar en los campos especificados.
/// - `tabla`: Una cadena de texto (`String`) que indica el nombre de la tabla en la
///   que se van a insertar los datos.
/// - `ruta_tabla`: Una cadena de texto (`String`) que indica la ruta del archivo que
///   se actualizará con los datos insertados.
#[derive(Debug)]
pub struct ConsultaInsert {
    pub campos_consulta: Vec<String>,
    pub campos_posibles: HashMap<String, usize>,
    pub valores: Vec<Vec<String>>,
    pub tabla: String,
    pub ruta_tabla: String,
}

impl ConsultaInsert {
    /// Crea una nueva instancia de `ConsultaInsert` a partir de una cadena de consulta SQL.
    ///
    /// Procesa la consulta SQL para extraer los campos donde insertar, los valores a insertar en dichos campos, la tabla en la que se van a insertar
    /// los datos, y la ruta del archivo tabla a modificar.
    ///
    /// # Parámetros
    /// - `consulta`: La consulta SQL en formato `String`.
    /// - `ruta`: La ruta del archivo en el que se van a insertar los datos.
    ///
    /// # Retorno
    /// Una instancia de `ConsultaInsert`

    pub fn crear(consulta: &String, ruta_a_tablas: &String) -> ConsultaInsert {
        let consulta_parseada = &Self::parsear_consulta_de_comando(&consulta);
        let mut index = 2; //nos salteamos las palabras:  insert into
        let tabla = Self::parsear_tabla(consulta_parseada, &mut index);
        let campos_consulta = Self::parsear_campos(consulta_parseada, &mut index);
        let valores = Self::parsear_valores(consulta_parseada, &mut index);
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let ruta_tabla = procesar_ruta(&ruta_a_tablas, &tabla);

        ConsultaInsert {
            campos_consulta,
            campos_posibles,
            valores,
            tabla,
            ruta_tabla,
        }
    }

    /// Parsea la consulta SQL para obtener los distintos tokens.
    ///
    /// Convierte la consulta, eliminando las comas y divide la cadena en palabras.
    ///
    /// # Parámetros
    /// - `consulta`: La consulta SQL en formato `String`.
    ///
    /// # Retorno
    /// Retorna un `Vec<String>` que contiene cada palabra de la consulta SQL.

    fn parsear_consulta_de_comando(consulta: &String) -> Vec<String> {
        return consulta
            .replace(",", "")
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();
    }
}

impl Parseables for ConsultaInsert {
    // Extrae los campos de la consulta SQL.
    ///
    /// A partir de una lista de tokens, extrae los campos entre los paréntesis.
    ///
    /// # Parámetros
    /// - `consulta`: Un vector de cadenas que representa la consulta SQL tokenizada.
    /// - `index`: Un índice mutable que se actualiza conforme se procesan los tokens.
    ///
    /// # Retorno
    /// Un `Vec<String>` que contiene los nombres de los campos a insertar.

    fn parsear_campos(consulta: &Vec<String>, index: &mut usize) -> Vec<String> {
        let mut campos: Vec<String> = Vec::new();
        if consulta[*index] == "(" {
            *index += 1;
        }

        while *index < consulta.len() && consulta[*index] != ")" {
            let campo = &consulta[*index];
            campos.push(campo.to_string());
            *index += 1;
        }
        campos
    }
    /// Extrae el nombre de la tabla a partir de la consulta SQL.
    ///
    /// Busca la palabra clave `INTO` en los tokens de la consulta y toma el siguiente token como el nombre de la tabla.
    ///
    /// # Parámetros
    /// - `consulta`: Un vector de cadenas que representa la consulta SQL tokenizada.
    /// - `index`: Un índice mutable que se actualiza conforme se procesa la consulta.
    ///
    /// # Retorno
    /// Una cadena de texto (`String`) que contiene el nombre de la tabla.

    fn parsear_tabla(consulta: &Vec<String>, index: &mut usize) -> String {
        let mut tabla = String::new();

        if *index < consulta.len() {
            let tabla_consulta = &consulta[*index];
            tabla = tabla_consulta.to_string();
            *index += 1;
        }
        tabla
    }

    /// Extrae los valores a insertar a partir de la consulta SQL.
    ///
    /// Busca la palabra clave `VALUES` en los tokens de la consulta y toma los tokens siguientes
    /// entre paréntesis como los valores a insertar.
    ///
    /// # Parámetros
    /// - `consulta`: Un vector de cadenas que representa la consulta SQL tokenizada.
    /// - `index`: Un índice mutable que se actualiza conforme se procesan los tokens.
    ///
    /// # Retorno
    /// Un `Vec<Vec<String>>` que contiene los valores a insertar.

    fn parsear_valores(_consulta: &Vec<String>, _index: &mut usize) -> Vec<Vec<String>> {
        let mut lista_valores: Vec<Vec<String>> = Vec::new();
        if _consulta[*_index] == ")" {
            *_index += 1;
        }
        if _consulta[*_index] == "values" {
            *_index += 1;
        }

        while *_index < _consulta.len() {
            if _consulta[*_index] == "(" {
                *_index += 1;
            }
            let mut valores = Vec::new();
            while *_index < _consulta.len() && _consulta[*_index] != ")" {
                let valor = &_consulta[*_index];

                valores.push(valor.to_string());
                *_index += 1;
            }
            lista_valores.push(valores);
            *_index += 1;
        }
        lista_valores
    }
}

impl MetodosConsulta for ConsultaInsert {
    /// Verifica la validez de la consulta SQL.
    ///TODO: verificar la validez de los valores a ingresar
    /// verifica que la tabla a la que se quiere inserta exista, así como los campos de la consulta no estén vacíos
    /// y que todos los campos solicitados sean válidos según los campos posibles definidos en la estructura.
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn verificar_validez_consulta(&mut self) -> Result<(), errores::Errores> {
        match leer_archivo(&self.ruta_tabla) {
            Ok(mut lector) => {
                let mut nombres_campos = String::new();
                lector
                    .read_line(&mut nombres_campos)
                    .map_err(|_| errores::Errores::Error)?;
                let (_, campos_validos) = parsear_linea_archivo(&nombres_campos);
                self.campos_posibles = mapear_campos(&campos_validos);
            }
            Err(_) => return Err(errores::Errores::InvalidTable),
        };

        if self.campos_consulta.is_empty() {
            return Err(errores::Errores::InvalidSyntax);
        }
        let campos_posibles = &self.campos_posibles;
        if !ConsultaInsert::verificar_campos_validos(campos_posibles, &mut self.campos_consulta) {
            return Err(errores::Errores::InvalidColumn);
        }
        Ok(())
    }

    /// Procesa el contenido de la consulta y agrega los valores al archivo correspondiente.
    ///
    /// Abre el archivo en modo append y escribe los valores de la consulta al final del archivo.
    ///
    /// # Parámetros
    /// - `lector`: Un `BufReader<File>` que proporciona acceso al archivo.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn procesar(&mut self) -> Result<(), errores::Errores> {
        // Abrir el archivo original en modo append (agregar al final)
        let ruta_archivo = Path::new(&self.ruta_tabla);
        let archivo_original = match OpenOptions::new().append(true).open(ruta_archivo) {
            Ok(file) => file,
            Err(_) => return Err(errores::Errores::Error),
        };
        let mut escritor = BufWriter::new(archivo_original);

        // Agregar valores al final del archivo
        for valores_fila in &self.valores {
            let linea = valores_fila.join(",");
            if let Err(_) = writeln!(escritor, "{}", linea) {
                return Err(errores::Errores::Error);
            }
        }

        // Asegurarse de escribir en el archivo
        match escritor.flush() {
            Ok(_) => {}
            Err(_) => return Err(errores::Errores::Error), //error al escribir
        }
        Ok(())
    }
}

impl Verificaciones for ConsultaInsert {
    fn verificar_campos_validos(
        campos_validos: &HashMap<String, usize>,
        campos_consulta: &mut Vec<String>,
    ) -> bool {
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
    fn test_verificacion_campos_validos() {
        let mut campos_validos: HashMap<String, usize> = HashMap::new();
        campos_validos.insert("nombre".to_string(), 0);
        campos_validos.insert("edad".to_string(), 1);

        let mut campos_consulta = vec!["nombre".to_string(), "edad".to_string()];
        assert!(ConsultaInsert::verificar_campos_validos(
            &campos_validos,
            &mut campos_consulta
        ));

        let mut campos_invalidos = vec!["nombre".to_string(), "altura".to_string()];
        assert!(!ConsultaInsert::verificar_campos_validos(
            &campos_validos,
            &mut campos_invalidos
        ));
    }
}

use crate::archivo::{leer_archivo, parsear_linea_archivo, procesar_ruta};
use crate::consulta::{
    mapear_campos, obtener_campos_consulta_orden_por_defecto, MetodosConsulta, Parseables,
    Verificaciones,
};
use crate::errores;
use crate::parseos::{eliminar_comas, parseo, remover_comillas, unir_literales_spliteados};
use crate::select::verificar_sintaxis_campos;
use crate::select::ConsultaSelect;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::path::Path;
use std::{
    collections::HashMap,
    io::{BufRead, BufWriter, Write},
};

const CARACTERES_DELIMITADORES: &[char] = &['(', ')', ',', ';'];
const INSERT: &str = "insert";
const INTO: &str = "into";
const VALUES: &str = "values";
const PARENTESIS_ABIERTO: &str = "(";
const PARENTESIS_CERRADO: &str = ")";
const CARACTER_VACIO: &str = "";
const PUNTO_COMA: &str = ";";
const COMA: &str = ",";
const COMILLA_SIMPLE: &str = "'";
const INTEGER: &str = "Integer";
const STRING: &str = "String";

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
/// - `valores`: Un vector de cadenas de texto (`Vec<String>`) que contiene los
///   los valores a insertar en los campos especificados.
/// - `tabla`: Una cadena de texto (`String`) que indica el nombre de la tabla en la
///   que se van a insertar los datos.
/// - `ruta_tabla`: Una cadena de texto (`String`) que indica la ruta del archivo que
///   se actualizará con los datos insertados.
#[derive(Debug)]
pub struct ConsultaInsert {
    pub campos_consulta: Vec<String>,
    pub campos_posibles: HashMap<String, usize>,
    pub valores: Vec<String>,
    pub campos_mapeados_valores: Vec<HashMap<String, String>>,
    pub tabla: Vec<String>,
    pub ruta_tabla: String,
}

impl ConsultaInsert {
    /// Crea una nueva instancia de `ConsultaInsert` a partir de una cadena de consulta SQL.
    ///
    /// Procesa la consulta SQL para extraer los campos donde insertar, los valores a insertar en dichos campos, la tabla en la que se van a insertar
    /// los datos, y la ruta del archivo tabla a modificar.
    ///
    /// # Parámetros
    /// - `consulta`: La consulta SQL en formato de vector de cadenas de texto.
    /// - `ruta`: La ruta del archivo en el que se van a insertar los datos en la tabla.
    ///
    /// # Retorno
    /// Una instancia de `ConsultaInsert` si la consulta es válida, o un error de tipo `Errores`.

    pub fn crear(
        consulta: &Vec<String>,
        ruta_a_tablas: &String,
    ) -> Result<ConsultaInsert, errores::Errores> {
        let palabras_reservadas = vec![INSERT, INTO, VALUES];
        Self::verificar_orden_keywords(consulta, palabras_reservadas)?;
        let consulta_spliteada = &parseo(consulta, CARACTERES_DELIMITADORES);
        let consulta_spliteada = &unir_literales_spliteados(consulta_spliteada);
        let tabla = Self::parsear_cualquier_cosa(
            consulta_spliteada,
            vec![String::from(INSERT), String::from(INTO)],
            HashSet::from([PARENTESIS_ABIERTO.to_string()]),
            false,
            false,
        )?;
        let campos_consulta = Self::parsear_cualquier_cosa(
            consulta_spliteada,
            vec![String::from(PARENTESIS_ABIERTO)],
            HashSet::from([PARENTESIS_CERRADO.to_string()]),
            true,
            false,
        )?;
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let ruta_tabla = ruta_a_tablas.to_string();
        let valores: Vec<String> = Self::parsear_cualquier_cosa(
            consulta_spliteada,
            vec![String::from(VALUES)],
            HashSet::from([CARACTER_VACIO.to_string(), PUNTO_COMA.to_string()]),
            false,
            false,
        )?;
        let campos_mapeados_valores: Vec<HashMap<String, String>> = Vec::new();
        Ok(ConsultaInsert {
            campos_consulta,
            campos_posibles,
            valores,
            campos_mapeados_valores,
            tabla,
            ruta_tabla,
        })
    }
}

impl Parseables for ConsultaInsert {}

impl MetodosConsulta for ConsultaInsert {
    /// Verifica la validez de la consulta INSERT.
    /// Verifica que la consulta tenga un solo nombre de tabla, que la ruta de la tabla sea válida, que la tabla exista y que los campos y valores de la consulta sean válidos.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn verificar_validez_consulta(&mut self) -> Result<(), errores::Errores> {
        if self.tabla.len() != 1 {
            Err(errores::Errores::InvalidSyntax)?;
        }
        self.ruta_tabla = procesar_ruta(&self.ruta_tabla, &self.tabla[0]);
        let mut lector =
            leer_archivo(&self.ruta_tabla).map_err(|_| errores::Errores::InvalidTable)?;
        let mut nombres_campos = String::new();
        lector
            .read_line(&mut nombres_campos)
            .map_err(|_| errores::Errores::Error)?;
        let (_, campos_validos) = parsear_linea_archivo(&nombres_campos);
        self.campos_posibles = mapear_campos(&campos_validos);
        let mut tipos_datos = String::new();
        lector
            .read_line(&mut tipos_datos)
            .map_err(|_| errores::Errores::Error)?;
        let (_, tipos_datos) = parsear_linea_archivo(&tipos_datos);
        let tipos_datos = mapear_tipos_datos(&campos_validos, &tipos_datos);
        verificar_sintaxis_campos(&self.campos_consulta)?;
        self.campos_consulta = eliminar_comas(&self.campos_consulta);
        if !ConsultaSelect::verificar_campos_validos(
            &self.campos_posibles,
            &mut self.campos_consulta,
        ) {
            Err(errores::Errores::InvalidColumn)?;
        }
        self.valores = unir_literales_spliteados(&self.valores);
        verificar_sintaxis_valores(&self.valores)?;
        let vector_valores = construir_vector_valores(&self.valores);
        verificar_cantidad_valores_validos(&vector_valores, &self.campos_consulta)?;
        let campos_mapeados_valores = mapear_campos_valores(&vector_valores, &self.campos_consulta);
        verificar_valores_tipo_valido(&campos_mapeados_valores, &tipos_datos)?;
        self.campos_mapeados_valores = campos_mapeados_valores;
        Ok(())
    }

    /// Procesa el contenido de la consulta y agrega los valores al archivo correspondiente.
    ///
    /// Abre el archivo en modo append y escribe los valores de la consulta al final del archivo.
    /// Si ocurre algún error al abrir o escribir en el archivo, se retorna un error.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn procesar(&mut self) -> Result<(), errores::Errores> {
        // Abrir el archivo original en modo append (agregar al final)
        let ruta_archivo = Path::new(&self.ruta_tabla);
        let archivo_original = match OpenOptions::new().append(true).open(ruta_archivo) {
            Ok(file) => file,
            Err(_) => Err(errores::Errores::Error)?, //error al abrir el archivo
        };
        let mut escritor = BufWriter::new(archivo_original);
        // Agregar valores al final del archivo
        for valores_fila in &self.campos_mapeados_valores {
            let campos_tabla = &obtener_campos_consulta_orden_por_defecto(&self.campos_posibles);
            let mut linea: Vec<String> = Vec::new();
            for campo in campos_tabla {
                let valor = match valores_fila.get(campo) {
                    Some(valor) => valor,
                    None => &CARACTER_VACIO.to_string(), //si no esta ingreso una cadena vacia
                };
                let valor = remover_comillas(valor);
                linea.push(valor.to_string());
            }
            let linea = linea.join(COMA);
            if writeln!(escritor, "{}", linea).is_err() {
                Err(errores::Errores::Error)?;
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
        true
    }
}

fn verificar_valores_tipo_valido(
    campos_mapeados_valores: &Vec<HashMap<String, String>>,
    tipos_datos: &HashMap<String, String>,
) -> Result<(), errores::Errores> {
    for campos_mapeados in campos_mapeados_valores {
        for (campo, valor) in campos_mapeados {
            if valor.is_empty() {
                continue;
            }
            match tipos_datos.get(campo) {
                Some(tipo) => {
                    if tipo == INTEGER && valor.parse::<i32>().is_err() {
                        Err(errores::Errores::Error)?;
                    }
                    if tipo == STRING
                        && (!valor.starts_with(COMILLA_SIMPLE) && !valor.ends_with(COMILLA_SIMPLE))
                    {
                        Err(errores::Errores::Error)?;
                    }
                }
                None => Err(errores::Errores::Error)?,
            }
        }
    }
    Ok(())
}

fn mapear_campos_valores(
    vector_valores: &Vec<Vec<String>>,
    campos_consulta: &[String],
) -> Vec<HashMap<String, String>> {
    let mut campos_mapeados_valores: Vec<HashMap<String, String>> = Vec::new();
    for fila_valores in vector_valores {
        let mut campos_mapeados_valores_fila: HashMap<String, String> = HashMap::new();
        for (indice, valor) in fila_valores.iter().enumerate() {
            campos_mapeados_valores_fila
                .insert(campos_consulta[indice].to_string(), valor.to_string());
        }
        campos_mapeados_valores.push(campos_mapeados_valores_fila);
    }
    campos_mapeados_valores
}

fn mapear_tipos_datos(columnas: &[String], columna1: &[String]) -> HashMap<String, String> {
    let mut campos_mapeados_tipos_de_datos: HashMap<String, String> = HashMap::new();
    for (indice, campo) in columna1.iter().enumerate() {
        match campo.chars().all(char::is_numeric) {
            true => campos_mapeados_tipos_de_datos
                .insert(columnas[indice].to_string(), INTEGER.to_string()),
            false => campos_mapeados_tipos_de_datos
                .insert(columnas[indice].to_string(), STRING.to_string()),
        };
    }
    campos_mapeados_tipos_de_datos
}

fn verificar_sintaxis_valores(valores: &Vec<String>) -> Result<(), errores::Errores> {
    let mut parentesis_abierto = 0;

    for valor in valores {
        for c in valor.chars() {
            match c {
                '(' => {
                    parentesis_abierto += 1;
                }
                ')' => {
                    if parentesis_abierto == 0 {
                        return Err(errores::Errores::InvalidSyntax); // Cierre de paréntesis sin apertura
                    }
                    parentesis_abierto -= 1;
                }
                _ => {
                    // Ignorar otros caracteres
                }
            }
        }
    }

    if parentesis_abierto != 0 {
        return Err(errores::Errores::InvalidSyntax); // Hay paréntesis abiertos que no se han cerrado
    }

    Ok(()) // La sintaxis es válida
}

fn construir_vector_valores(valores: &Vec<String>) -> Vec<Vec<String>> {
    let mut vector_valores: Vec<Vec<String>> = Vec::new();
    let mut fila_valores: Vec<String> = Vec::new();
    let mut parentesis_abierto = false;
    let mut ultimo: Option<String> = None;
    for valor in valores {
        match valor.as_str() {
            PARENTESIS_ABIERTO => {
                // Iniciar una nueva fila
                parentesis_abierto = true;
                fila_valores.clear(); // Limpiar la fila al iniciar
            }
            PARENTESIS_CERRADO => {
                // Finalizar la fila actual solo si hay valores en ella
                if ultimo == Some(COMA.to_string()) {
                    fila_valores.push(String::new()); // Campo vacío por coma
                }
                vector_valores.push(fila_valores.clone());
                fila_valores.clear();
                parentesis_abierto = false;
            }
            COMA => {
                // Agregar un campo vacío solo si estamos dentro de paréntesis
                if parentesis_abierto && ultimo == Some(PARENTESIS_ABIERTO.to_string())
                    || ultimo == Some(COMA.to_string())
                {
                    fila_valores.push(String::new()); // Campo vacío por coma
                }
            }
            _ => {
                // Agregar el valor si estamos dentro de paréntesis
                if parentesis_abierto {
                    fila_valores.push(valor.to_string());
                }
            }
        }
        ultimo = Some(valor.to_string());
    }

    vector_valores
}

fn verificar_cantidad_valores_validos(
    vector_valores: &Vec<Vec<String>>,
    campos_consulta: &[String],
) -> Result<(), errores::Errores> {
    //verificar que la cantidad a valores a insertar sean la misma que la cantidad de campos
    for fila_valores in vector_valores {
        if fila_valores.len() != campos_consulta.len() {
            Err(errores::Errores::InvalidSyntax)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crear_consulta_insert_valida() {
        let consulta = vec![
            "insert".to_string(),
            "into".to_string(),
            "tabla".to_string(),
            "(".to_string(),
            "campo1".to_string(),
            ",".to_string(),
            "campo2".to_string(),
            ")".to_string(),
            "values".to_string(),
            "(".to_string(),
            "'valor1'".to_string(),
            ",".to_string(),
            "'valor2'".to_string(),
            ")".to_string(),
            ";".to_string(),
        ];
        let ruta = "ruta/a/tablas".to_string();
        let resultado = ConsultaInsert::crear(&consulta, &ruta);
        assert!(resultado.is_ok());
    }

    #[test]
    fn test_crear_consulta_insert_invalida() {
        let consulta = vec![
            "insert".to_string(),
            "into".to_string(),
            "(".to_string(),
            "campo1".to_string(),
            ",".to_string(),
            "campo2".to_string(),
            ")".to_string(),
            "values".to_string(),
            "(".to_string(),
            "'valor1'".to_string(),
            ",".to_string(),
            "'valor2'".to_string(),
            ")".to_string(),
            ";".to_string(),
        ];
        let ruta = "ruta/a/tablas".to_string();
        let resultado = ConsultaInsert::crear(&consulta, &ruta);
        assert!(resultado.is_err());
    }

    #[test]
    fn test_verificar_validez_consulta() {
        let consulta = vec![
            "insert".to_string(),
            "into".to_string(),
            "personas".to_string(),
            "(".to_string(),
            "nombre".to_string(),
            ",".to_string(),
            "edad".to_string(),
            ")".to_string(),
            "values".to_string(),
            "(".to_string(),
            "'Franco'".to_string(),
            ",".to_string(),
            "19".to_string(),
            ")".to_string(),
            ";".to_string(),
        ];
        let ruta = "tablas".to_string();
        let mut consulta_insert = ConsultaInsert::crear(&consulta, &ruta).unwrap();
        let resultado = consulta_insert.verificar_validez_consulta();
        assert!(resultado.is_ok());
    }

    #[test]
    fn test_verificar_validez_consulta_invalida() {
        let consulta = vec![
            "insert".to_string(),
            "into".to_string(),
            "personas".to_string(),
            "(".to_string(),
            "nombre".to_string(),
            ",".to_string(),
            "edad".to_string(),
            ")".to_string(),
            "values".to_string(),
            "(".to_string(),
            "'19'".to_string(),
            ",".to_string(),
            "Edgard".to_string(),
            ")".to_string(),
            ";".to_string(),
        ];
        let ruta = "tablas".to_string();
        let mut consulta_insert = ConsultaInsert::crear(&consulta, &ruta).unwrap();
        let resultado = consulta_insert.verificar_validez_consulta();
        assert!(resultado.is_err());
    }

    #[test]
    fn test_procesar_consulta_insert() {
        let consulta = vec![
            "insert".to_string(),
            "into".to_string(),
            "personas".to_string(),
            "(".to_string(),
            "nombre".to_string(),
            ",".to_string(),
            "edad".to_string(),
            ")".to_string(),
            "values".to_string(),
            "(".to_string(),
            "'Francisco'".to_string(),
            ",".to_string(),
            "25".to_string(),
            ")".to_string(),
            ";".to_string(),
        ];
        let ruta = "tablas".to_string();
        let mut consulta_insert = ConsultaInsert::crear(&consulta, &ruta).unwrap();
        consulta_insert.verificar_validez_consulta().unwrap();
        let resultado = consulta_insert.procesar();
        assert!(resultado.is_ok());
    }

    #[test]
    fn test_procesar_consulta_insert_invalida() {
        let consulta = vec![
            "insert".to_string(),
            "into".to_string(),
            "personas".to_string(),
            "(".to_string(),
            "edad".to_string(),
            ",".to_string(),
            "ciudades".to_string(),
            ")".to_string(),
            "values".to_string(),
            "(".to_string(),
            "'2'".to_string(),
            ",".to_string(),
            "30".to_string(), //tipo de dato incorrecto
            ")".to_string(),
            ";".to_string(),
        ];
        let ruta = "ruta/a/tablas".to_string();
        let mut consulta_insert = ConsultaInsert::crear(&consulta, &ruta).unwrap();
        let resultado = consulta_insert.procesar();
        assert!(resultado.is_err());
    }
}

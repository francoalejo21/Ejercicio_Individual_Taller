use crate::abe::ArbolExpresiones;
use crate::archivo::{leer_archivo, parsear_linea_archivo, procesar_ruta};
use crate::consulta::{mapear_campos, MetodosConsulta, Parseables, Verificaciones};
use crate::errores;
use crate::parseos::{
    convertir_lower_case_restricciones, parseo, remover_comillas, unir_literales_spliteados,
    unir_operadores_que_deben_ir_juntos,
};
use crate::validador_where::ValidadorOperandosValidos;
use crate::validador_where::ValidadorSintaxis;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::{
    collections::HashMap,
    io::{BufRead, BufWriter, Write},
};

const CARACTERES_DELIMITADORES: &[char] = &['=', ',', ';', '<', '>', '(', ')'];
const IGUAL: &str = "=";
const COMILLA_SIMPLE: &str = "'";
const UPDATE: &str = "update";
const SET: &str = "set";
const WHERE: &str = "where";
const CARACTER_VACIO: &str = "";
const PUNTO_COMA: &str = ";";
const COMA: &str = ",";
const INTEGER: &str = "Integer";
const STRING: &str = "String";

/// Representa una consulta SQL de actualizacion.
///
/// Esta estructura contiene la información necesaria para realizar una consulta
/// de actualizacion en una base de datos.
///
/// # Campos
///
/// - `campos_consulta`: Un vector de cadenas de texto (`Vec<String>`) que contiene los
///     nombres de los campos en los que se van a actualizar como también los valores a actualizar.
/// - `campos_posibles`: Un mapa (`HashMap<String, usize>`) que asocia los nombres de los
///   campos de la tabla con sus índices. Este mapa permite la validación de campos.
/// - `campos_mapeados_valores`: Un mapa (`HashMap<String, String>`) que asocia los nombres de los
///     campos de la tabla con los valores que se van a actualizar.
/// - `tabla`: Una cadena de texto (`String`) que indica el nombre de la tabla en la
///   que se van a actualizar los valores de los campos.
/// - `ruta_tabla`: Una cadena de texto (`String`) que indica la ruta del archivo que
///   se actualizará con los datos actualizados.
/// - `condiciones`: Un vector de cadenas de texto (`Vec<String>`) que contiene las
///   condiciones que deben cumplir los datos a actualizar.

#[derive(Debug)]
pub struct ConsultaUpdate {
    pub campos_consulta: Vec<String>,
    pub campos_posibles: HashMap<String, usize>,
    pub campos_mapeados_valores: HashMap<String, String>,
    pub tabla: Vec<String>,
    pub ruta_tabla: String,
    pub condiciones: Vec<String>,
}

impl ConsultaUpdate {
    /// Crea una nueva consulta de tipo UPDATE con los campos posibles a actualizar, la tabla en la que se van a actualizar los datos, la ruta del archivo tabla a modificar y las condiciones
    /// que deben cumplir los datos a actualizar.
    /// Verifica la validez de la consulta en el sentido de si las keywords estan correctamente ingresadas
    /// y si la consulta cumple con la sintaxis de UPDATE SET WHERE.
    ///
    /// # Parámetros
    /// - `consulta`: Un `Vec<String>` que contiene las palabras de la consulta SQL.
    /// - `ruta_a_tablas`: Un `String` que contiene la ruta de la tabla a modificar.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`), entonces devuelve una consulta de tipo UPDATE, o el tipo de error (`Err`).

    pub fn crear(
        consulta: &Vec<String>,
        ruta_a_tablas: &String,
    ) -> Result<ConsultaUpdate, errores::Errores> {
        let palabras_reservadas = vec![UPDATE, SET, WHERE];
        Self::verificar_orden_keywords(consulta, palabras_reservadas)?;
        let consulta_spliteada = &parseo(consulta, CARACTERES_DELIMITADORES);
        let consulta_spliteada = &unir_literales_spliteados(consulta_spliteada);
        let consulta_spliteada = &unir_operadores_que_deben_ir_juntos(consulta_spliteada);
        let tabla = Self::parsear_cualquier_cosa(
            consulta_spliteada,
            vec![String::from(UPDATE)],
            HashSet::from([SET.to_string()]),
            false,
            false,
        )?;
        let campos_consulta = Self::parsear_cualquier_cosa(
            consulta_spliteada,
            vec![String::from(SET)],
            HashSet::from([
                WHERE.to_string(),
                CARACTER_VACIO.to_string(),
                PUNTO_COMA.to_string(),
            ]),
            false,
            false,
        )?;
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let ruta_tabla = ruta_a_tablas.to_string();
        let campos_mapeados_valores: HashMap<String, String> = HashMap::new();
        let condiciones: Vec<String> = Self::parsear_cualquier_cosa(
            consulta_spliteada,
            vec![String::from(WHERE)],
            HashSet::from([CARACTER_VACIO.to_string(), PUNTO_COMA.to_string()]),
            false,
            true,
        )?;
        Ok(ConsultaUpdate {
            campos_consulta,
            campos_posibles,
            campos_mapeados_valores,
            tabla,
            ruta_tabla,
            condiciones,
        })
    }
}

impl Parseables for ConsultaUpdate {}
impl MetodosConsulta for ConsultaUpdate {
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

        let campos_valores =
            construir_vector_campos_comparador_igual_valores(&self.campos_consulta);
        verificar_sintaxis_campos_valores(&campos_valores)?;

        let campo_valores_validados = verificar_campos_validos_y_valores_validos(
            campos_valores,
            &self.campos_posibles,
            &tipos_datos,
        )?;

        let campos_mapeados_valores = mapear_campos_valores_terna(&campo_valores_validados);

        self.campos_mapeados_valores = campos_mapeados_valores;

        //verificamos que la condicion where sea valida y los operandos sean validos
        self.condiciones =
            convertir_lower_case_restricciones(&self.condiciones, &self.campos_posibles);
        let mut validador_where = ValidadorSintaxis::new(&self.condiciones);
        if self.condiciones.is_empty() {
            if !validador_where.validar() {
                return Err(errores::Errores::InvalidSyntax);
            }
            let operandos = validador_where.obtener_operandos();
            let validador_operandos_validos =
                ValidadorOperandosValidos::new(&operandos, &self.campos_posibles);
            validador_operandos_validos.validar()?;
        }
        Ok(())
    }

    /// Procesa la consulta de actualización y modifica el archivo de la tabla con los datos actualizados.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`), entonces devuelve un mensaje de éxito, o el tipo de error (`Err`).
    fn procesar(&mut self) -> Result<(), errores::Errores> {
        let ruta_archivo = Path::new(&self.ruta_tabla);
        let archivo_original = match File::open(ruta_archivo) {
            Ok(file) => file,
            Err(_) => return Err(errores::Errores::Error), // Error al abrir el archivo
        };
        let lector = BufReader::new(archivo_original);

        // Crear un archivo temporal para escribir los cambios
        let ruta_temporal = ruta_archivo.with_extension("tmp");
        let archivo_temporal = match File::create(&ruta_temporal) {
            Ok(file) => file,
            Err(_) => return Err(errores::Errores::Error), // Error al crear el archivo temporal
        };
        let mut escritor = BufWriter::new(archivo_temporal);
        let mut modificados = 0;
        let mut arbol_exp = ArbolExpresiones::new();
        arbol_exp.crear_abe(&self.condiciones);

        if arbol_exp.arbol_vacio() {
            // Si el árbol de expresiones está vacío, sobrescribir el archivo con los campos y valores de campos_mapeados_valores
            let mut nueva_linea: Vec<String> =
                vec![CARACTER_VACIO.to_string(); self.campos_posibles.len()];
            for (campo, valor) in &self.campos_mapeados_valores {
                let mut valor_parseado = valor.to_string();
                valor_parseado = remover_comillas(&valor_parseado);
                if let Some(indice) = self.campos_posibles.get(campo) {
                    nueva_linea[*indice] = valor_parseado;
                }
            }
            let linea_modificada = nueva_linea.join(COMA);
            writeln!(escritor, "{}", linea_modificada).map_err(|_| errores::Errores::Error)?;
        // Error al escribir la línea
        } else {
            for linea in lector.lines() {
                let linea = linea.map_err(|_| errores::Errores::Error)?; // Error al leer la línea
                let (mut campos, _) = parsear_linea_archivo(&linea);

                // Verificar si la línea cumple con las condiciones WHERE
                if arbol_exp.evalua(&self.campos_posibles, &campos) {
                    // La línea cumple con las condiciones, modificarla
                    for (campo, valor) in &self.campos_mapeados_valores {
                        let mut valor_parseado = valor.to_string();
                        valor_parseado = remover_comillas(&valor_parseado);
                        if let Some(indice) = self.campos_posibles.get(campo) {
                            campos[*indice] = valor_parseado;
                        }
                    }
                    modificados += 1;
                }
                let linea_modificada = campos.join(COMA);
                if writeln!(escritor, "{}", linea_modificada).is_err() {
                    Err(errores::Errores::Error)?; // Error al escribir la línea
                }
            }
        }
        if modificados == 0 {
            Err(errores::Errores::Error)?;
        }
        // Asegurarse de escribir en el archivo
        escritor.flush().map_err(|_| errores::Errores::Error)?; // Error al escribir

        // Reemplazar el archivo original con el archivo temporal
        fs::rename(ruta_temporal, ruta_archivo).map_err(|_| errores::Errores::Error)?; // Error al renombrar el archivo

        Ok(())
    }
}

impl Verificaciones for ConsultaUpdate {
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

fn verificar_sintaxis_campos_valores(
    campos_valores: &Vec<Vec<String>>,
) -> Result<(), errores::Errores> {
    for vec in campos_valores {
        if vec.len() != 3 {
            Err(errores::Errores::InvalidSyntax)?;
        }
    }

    for vec in campos_valores {
        let campo = &vec[0];
        let operador = &vec[1];
        if campo.is_empty() {
            Err(errores::Errores::InvalidSyntax)?;
        }
        if operador != IGUAL {
            Err(errores::Errores::InvalidSyntax)?;
        }
    }
    Ok(())
}

fn verificar_campos_validos_y_valores_validos(
    vector_campos_valores: Vec<Vec<String>>,
    campos_posibles: &HashMap<String, usize>,
    tipos_datos: &HashMap<String, String>,
) -> Result<Vec<Vec<String>>, errores::Errores> {
    let mut vector_campos_valores_validados = Vec::new();

    for campos_valores in vector_campos_valores {
        let mut campos_valores_validados = campos_valores.clone();
        let campo = campos_valores_validados[0].to_lowercase();
        let valor = &campos_valores_validados[2];
        if valor.is_empty() {
            if !campos_posibles.contains_key(&campo) {
                Err(errores::Errores::InvalidColumn)?;
            }
            {};
        } else if campo.starts_with(COMILLA_SIMPLE) && campo.ends_with(COMILLA_SIMPLE) {
            return Err(errores::Errores::InvalidSyntax);
        } else if !campos_posibles.contains_key(&campo) {
            return Err(errores::Errores::InvalidColumn);
        } else if valor.starts_with(COMILLA_SIMPLE) && valor.ends_with(COMILLA_SIMPLE) {
            if let Some(tipo) = tipos_datos.get(&campo) {
                if tipo == INTEGER {
                    Err(errores::Errores::Error)?;
                }
            }
        } else if let Some(tipo) = tipos_datos.get(&campo) {
            if tipo == STRING {
                Err(errores::Errores::Error)?;
            }
        }

        campos_valores_validados[0] = campo;
        vector_campos_valores_validados.push(campos_valores_validados);
    }

    Ok(vector_campos_valores_validados)
}

fn construir_vector_campos_comparador_igual_valores(valores: &Vec<String>) -> Vec<Vec<String>> {
    let mut vector_terna: Vec<Vec<String>> = Vec::new();
    let mut fila_campos_igual_valores: Vec<String> = Vec::new();
    let mut esperando_valor = false; // Indicador para saber si falta un valor después del '='

    for valor in valores {
        match valor.as_str() {
            IGUAL => {
                if fila_campos_igual_valores.len() == 1 {
                    // Si tenemos un campo antes de IGUAL, añadimos IGUAL y esperamos un valor
                    fila_campos_igual_valores.push(IGUAL.to_string());
                    esperando_valor = true;
                }
            }
            COMA => {
                if esperando_valor {
                    // Si estamos esperando un valor y viene una coma, significa que el valor está vacío
                    fila_campos_igual_valores.push(CARACTER_VACIO.to_string());
                    esperando_valor = false;
                }
                // Agregar la terna actual y limpiar
                vector_terna.push(fila_campos_igual_valores.clone());
                fila_campos_igual_valores.clear();
            }
            _ => {
                // Cualquier otro valor se añade a la terna actual
                fila_campos_igual_valores.push(valor.to_string());
                if esperando_valor {
                    esperando_valor = false; // Ya recibimos el valor después del IGUAL
                }
            }
        }
    }

    // Si al final queda algún valor o terna sin procesar, lo añadimos
    if esperando_valor {
        // Si quedó un IGUAL esperando un valor al final, agregamos un valor vacío
        fila_campos_igual_valores.push(CARACTER_VACIO.to_string());
    }
    if !fila_campos_igual_valores.is_empty() {
        vector_terna.push(fila_campos_igual_valores);
    }

    vector_terna
}

fn mapear_campos_valores_terna(vector_valores: &Vec<Vec<String>>) -> HashMap<String, String> {
    //recibe un vector de ternas donde cada terna es [campo, =, valor] y lo mapea a un vector de hashmap donde cada hashmap es [campo, valor]
    let mut campos_mapeados_valores_fila = HashMap::new();

    for terna in vector_valores {
        let campo = &terna[0];
        let valor = &terna[2];
        campos_mapeados_valores_fila.insert(campo.to_string(), valor.to_string());
    }
    campos_mapeados_valores_fila
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crear_consulta_update_valida() {
        let consulta = vec![
            "update".to_string(),
            "tabla".to_string(),
            "set".to_string(),
            "campo1".to_string(),
            "=".to_string(),
            "valor1".to_string(),
            ",".to_string(),
            "campo2".to_string(),
            "=".to_string(),
            "valor2".to_string(),
            "where".to_string(),
            "campo3".to_string(),
            "=".to_string(),
            "valor3".to_string(),
        ];
        let ruta_a_tablas = "ruta/a/tablas".to_string();
        let resultado = ConsultaUpdate::crear(&consulta, &ruta_a_tablas);
        assert!(resultado.is_ok());
    }

    #[test]
    fn test_crear_consulta_update_invalida_sintaxis() {
        let consulta = vec![
            "set".to_string(),
            "tabla".to_string(),
            "update".to_string(),
            "campo1".to_string(),
            "=".to_string(),
            "valor1".to_string(),
            ",".to_string(),
            "campo2".to_string(),
            "valor2".to_string(), // Falta el "="
            "where".to_string(),
            "campo3".to_string(),
            "=".to_string(),
            "valor3".to_string(),
        ];
        let ruta_a_tablas = "ruta/a/tablas".to_string();
        let resultado = ConsultaUpdate::crear(&consulta, &ruta_a_tablas);
        assert!(resultado.is_err());
    }

    #[test]
    fn test_verificar_validez_consulta() {
        let mut consulta_update = ConsultaUpdate {
            campos_consulta: vec![
                "nombre".to_string(),
                "=".to_string(),
                "''Federico'".to_string(),
            ],
            campos_posibles: HashMap::from([("nombre".to_string(), 0)]),
            campos_mapeados_valores: HashMap::new(),
            tabla: vec!["clientes".to_string()],
            ruta_tabla: "tablas".to_string(),
            condiciones: vec![
                "nombre".to_string(),
                "=".to_string(),
                "'Federico'".to_string(),
            ],
        };
        let resultado = consulta_update.verificar_validez_consulta();
        assert!(resultado.is_ok());
    }

    #[test]
    fn test_verificar_validez_consulta_invalida() {
        let mut consulta_update = ConsultaUpdate {
            campos_consulta: vec![
                "campo1".to_string(),
                "=".to_string(),
                "'valor1'".to_string(),
            ],
            campos_posibles: HashMap::new(),
            campos_mapeados_valores: HashMap::new(),
            tabla: vec!["ordenes".to_string()],
            ruta_tabla: "tablas".to_string(),
            condiciones: vec![
                "campo1".to_string(),
                "=".to_string(),
                "'valor1'".to_string(),
            ],
        };
        //deberia fallar campo1 no es un campo valido
        let resultado = consulta_update.verificar_validez_consulta();
        assert!(resultado.is_err());
    }

    #[test]
    fn test_procesar_consulta_update_invalida() {
        let mut consulta_update = ConsultaUpdate {
            campos_consulta: vec!["....".to_string(), "=".to_string(), "valor1".to_string()],
            campos_posibles: HashMap::new(),
            campos_mapeados_valores: HashMap::new(),
            tabla: vec!["tabla".to_string()],
            ruta_tabla: "tablas".to_string(),
            condiciones: vec!["campo1".to_string(), "=".to_string(), "valor1".to_string()],
        };
        let resultado = consulta_update.procesar();
        assert!(resultado.is_err());
    }

    #[test]
    fn test_verificar_campos_validos() {
        let campos_validos = HashMap::from([("campo1".to_string(), 0)]);
        let mut campos_consulta = vec!["campo1".to_string()];
        let resultado =
            ConsultaUpdate::verificar_campos_validos(&campos_validos, &mut campos_consulta);
        assert!(resultado);
    }

    #[test]
    fn test_verificar_campos_invalidos() {
        let campos_validos = HashMap::new();
        let mut campos_consulta = vec!["campo1".to_string()];
        let resultado =
            ConsultaUpdate::verificar_campos_validos(&campos_validos, &mut campos_consulta);
        assert!(!resultado);
    }
}

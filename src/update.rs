use crate::archivo::{leer_archivo, parsear_linea_archivo, procesar_ruta};
use crate::consulta::{mapear_campos, MetodosConsulta, Parseables, Verificaciones};
use crate::validador_where::ValidadorSintaxis;
use crate::verificaciones_sintaxis::verificar_orden_keywords;
use std::collections::HashSet;
use crate::select::convertir_lower_case_restricciones;
use crate::validador_where::ValidadorOperandosValidos;
use std::fs;
use std::io::BufReader;
use crate::abe::ArbolExpresiones;
use crate::errores;
use crate::parseos::{parseo,unir_literales_spliteados,remover_comillas};
use std::fs::File;
use std::path::Path;
use std::{
    collections::HashMap,
    io::{BufRead, BufWriter, Write},
};

const CARACTERES_DELIMITADORES: &[char] = &['=',',',';','<','>','(',')'];
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
pub struct ConsultaUpdate {
    pub campos_consulta: Vec<String>,
    pub campos_posibles: HashMap<String, usize>,
    pub campos_mapeados_valores :HashMap<String,String>,
    pub tabla: Vec<String>,
    pub ruta_tabla: String,
    pub condiciones: Vec<String>,
}

impl ConsultaUpdate {
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

    pub fn crear(consulta: &Vec<String>, ruta_a_tablas: &String) -> Result<ConsultaUpdate,errores::Errores> {
        let palabras_reservadas = vec![UPDATE, SET, WHERE];  
        verificar_orden_keywords(consulta, palabras_reservadas)?;
        let consulta_spliteada = &parseo(consulta, CARACTERES_DELIMITADORES);
        let consulta_spliteada = &unir_literales_spliteados(consulta_spliteada);
        let tabla = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from(UPDATE)], HashSet::from([SET.to_string()]), false, false)?;        
        let campos_consulta = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from(SET)], HashSet::from([WHERE.to_string(),CARACTER_VACIO.to_string(),PUNTO_COMA.to_string()]), false, false)?;
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let ruta_tabla = ruta_a_tablas.to_string(); 
        let campos_mapeados_valores: HashMap<String,String> = HashMap::new();
        let condiciones: Vec<String> = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from(WHERE)], HashSet::from([CARACTER_VACIO.to_string(),PUNTO_COMA.to_string()]), false, true)?;
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

impl Parseables for ConsultaUpdate {
}

impl MetodosConsulta for ConsultaUpdate {
    /// Verifica la validez de la consulta SQL.
    ///TODO: verificar la validez de los valores a ingresar
    /// verifica que la tabla a la que se quiere inserta exista, así como los campos de la consulta no estén vacíos
    /// y que todos los campos solicitados sean válidos según los campos posibles definidos en la estructura.
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn verificar_validez_consulta(&mut self) -> Result<(), errores::Errores> {
        if self.tabla.len() != 1 {
            Err(errores::Errores::InvalidSyntax)?;
        }
        self.ruta_tabla = procesar_ruta(&self.ruta_tabla, &self.tabla[0]);
        let mut lector = leer_archivo(&self.ruta_tabla).map_err(|_| errores::Errores::InvalidTable)?;
        let mut nombres_campos = String::new();
        lector.read_line(&mut nombres_campos).map_err(|_| errores::Errores::Error)?;
        
        let (_, campos_validos) = parsear_linea_archivo(&nombres_campos);
        self.campos_posibles = mapear_campos(&campos_validos);                
        
        let mut tipos_datos = String::new();
        lector.read_line(&mut tipos_datos).map_err(|_| errores::Errores::Error)?;
        let (_,tipos_datos) = parsear_linea_archivo(&tipos_datos);
        let tipos_datos = mapear_tipos_datos(&campos_validos,&tipos_datos);

        let campos_valores= construir_vector_campos_comparador_igual_valores(&self.campos_consulta);
        verificar_sintaxis_campos_valores(&campos_valores)?;
        
        
        let campo_valores_validados  = verificar_campos_validos_y_valores_validos(campos_valores, &self.campos_posibles, &tipos_datos)?;

        let campos_mapeados_valores = mapear_campos_valores_terna(&campo_valores_validados);


        self.campos_mapeados_valores = campos_mapeados_valores;
    
        //verificamos que la condicion where sea valida y los operandos sean validos
        self.condiciones = convertir_lower_case_restricciones(&self.condiciones, &self.campos_posibles);
        let mut validador_where = ValidadorSintaxis::new(&self.condiciones);
        if self.condiciones.is_empty() {
            if !validador_where.validar(){
                return Err(errores::Errores::InvalidSyntax);
            }
            let operandos = validador_where.obtener_operandos();
            let validador_operandos_validos = ValidadorOperandosValidos::new(&operandos, &self.campos_posibles);
            validador_operandos_validos.validar()?;
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
            let mut nueva_linea: Vec<String> = vec![CARACTER_VACIO.to_string(); self.campos_posibles.len()];
            for (campo, valor) in &self.campos_mapeados_valores {
                let mut valor_parseado = valor.to_string();
                valor_parseado = remover_comillas(&valor_parseado);
                if let Some(indice) = self.campos_posibles.get(campo) {
                    nueva_linea[*indice] = valor_parseado;
                }
            }
            let linea_modificada = nueva_linea.join(COMA);
            writeln!(escritor, "{}", linea_modificada).map_err(|_| errores::Errores::Error)?; // Error al escribir la línea
        } else {
            for linea in lector.lines() {
                let linea = linea.map_err(|_| errores::Errores::Error)?; // Error al leer la línea
                let  (mut campos,_) = parsear_linea_archivo(&linea);

                // Verificar si la línea cumple con las condiciones WHERE
                if arbol_exp.evalua( &self.campos_posibles, &campos) {
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

fn mapear_tipos_datos(columnas :&[String], columna1 :&[String])->HashMap<String,String>{
    let mut campos_mapeados_tipos_de_datos: HashMap<String, String> = HashMap::new();
    for (indice,campo) in columna1.iter().enumerate(){
        match campo.chars().all(char::is_numeric){
            true => campos_mapeados_tipos_de_datos.insert(columnas[indice].to_string(), INTEGER.to_string()),
            false => campos_mapeados_tipos_de_datos.insert(columnas[indice].to_string(), STRING.to_string())
            };
    }       
    campos_mapeados_tipos_de_datos
}

fn verificar_sintaxis_campos_valores(campos_valores: &Vec<Vec<String>>) -> Result<(), errores::Errores> {
    for vec in campos_valores {
        if vec.len() != 3 {
            Err(errores::Errores::InvalidSyntax)?;
        }
    }
    //hay por lo menos tres elementos en cada fila
    // El vector debe seguir el patrón: campo = valor, campo = valor, ...
    for  vec in campos_valores {
        let campo = &vec[0];
        let operador = &vec[1];
        // Verificar que el campo no esté vacío
        if campo.is_empty() {
            Err(errores::Errores::InvalidSyntax)?;
        }
        // Verificar que el segundo elemento sea '='
        if operador != IGUAL {
            Err(errores::Errores::InvalidSyntax)?;
        }
    }
    Ok(())
}

fn verificar_campos_validos_y_valores_validos(
    vector_campos_valores: Vec<Vec<String>>,
    campos_posibles: &HashMap<String, usize>,
    tipos_datos: &HashMap<String, String>
) -> Result<Vec<Vec<String>>, errores::Errores> {
    // Aclaración: cada elemento del vector_campos_valores está compuesto por [campo, =, valor]
    // donde tenemos que chequear que campo sea válido y valor sea válido.
    // Para esto verificamos que campo no sea un literal y que valor sea un literal y además que campo esté en los campos posibles
    // y que valor sea un tipo de dato posible para ese campo.
    // Cabe aclarar que valor puede ser un valor nulo.
    // Además, quiero que modifique los campos de campos_valores[0] a lowercase.

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
        }else if campo.starts_with(COMILLA_SIMPLE) && campo.ends_with(COMILLA_SIMPLE) {
            return Err(errores::Errores::InvalidSyntax);
        }else if !campos_posibles.contains_key(&campo) {
            return Err(errores::Errores::InvalidColumn);
        }else if valor.starts_with(COMILLA_SIMPLE) && valor.ends_with(COMILLA_SIMPLE) {
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

fn mapear_campos_valores_terna(vector_valores: &Vec<Vec<String>>)->HashMap<String, String>{
    //recibe un vector de ternas donde cada terna es [campo, =, valor] y lo mapea a un vector de hashmap donde cada hashmap es [campo, valor]
    let mut campos_mapeados_valores_fila = HashMap::new();

    for terna in vector_valores {
        let campo = &terna[0];
        let valor = &terna[2];
        campos_mapeados_valores_fila.insert(campo.to_string(), valor.to_string());
    }
    campos_mapeados_valores_fila
}
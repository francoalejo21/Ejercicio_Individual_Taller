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
use crate::parseos::{parseo,unir_literales_spliteados};
use std::fs::File;
use std::path::Path;
use std::{
    collections::HashMap,
    io::{BufRead, BufWriter, Write},
};

const CARACTERES_DELIMITADORES: &[char] = &[';',',','=','<','>','(',')'];
const DELETE: &str = "delete";
const FROM: &str = "from";
const WHERE: &str = "where";
const CARACTER_VACIO: &str = "";
const PUNTO_COMA: &str = ";";

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
pub struct ConsultaDelete {
    pub campos_posibles: HashMap<String, usize>,
    pub tabla: Vec<String>,
    pub ruta_tabla: String,
    pub condiciones: Vec<String>,
}

impl ConsultaDelete {
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
    /// Una instancia de `ConsultaDelete` si la consulta es válida, o un error de tipo `Errores`.

    pub fn crear(consulta: &Vec<String>, ruta_a_tablas: &String) -> Result<ConsultaDelete,errores::Errores> {
        let palabras_reservadas = vec![DELETE, FROM, WHERE];  
        verificar_orden_keywords(consulta, palabras_reservadas)?;
        let consulta_spliteada = &parseo(consulta, CARACTERES_DELIMITADORES);
        let consulta_spliteada = &unir_literales_spliteados(consulta_spliteada);
        let tabla = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from(DELETE), String::from(FROM)], HashSet::from([WHERE.to_string(), CARACTER_VACIO.to_string(), PUNTO_COMA.to_string()]), false, false)?;        
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let ruta_tabla = ruta_a_tablas.to_string(); 
        let condiciones: Vec<String> = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from(WHERE)], HashSet::from([CARACTER_VACIO.to_string(),PUNTO_COMA.to_string()]), false, true)?;
        Ok(ConsultaDelete {
            campos_posibles,
            tabla,
            ruta_tabla,
            condiciones,
        })
    }
}

impl Parseables for ConsultaDelete {

}

impl MetodosConsulta for ConsultaDelete {
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
        
        //verificamos que la condicion where sea valida y los operandos sean validos
        self.condiciones = convertir_lower_case_restricciones(&self.condiciones, &self.campos_posibles);
        let mut validador_where = ValidadorSintaxis::new(&self.condiciones);
        if !self.condiciones.is_empty(){
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
        let archivo_original = File::open(ruta_archivo).map_err(|_| errores::Errores::Error)?;
        let lector = BufReader::new(archivo_original);

        // Crear un archivo temporal para escribir los cambios
        let ruta_temporal = ruta_archivo.with_extension("tmp");
        let archivo_temporal = File::create(&ruta_temporal).map_err(|_| errores::Errores::Error)?;
        let mut escritor = BufWriter::new(archivo_temporal);
        let mut eliminados = 0;
        let mut arbol_exp = ArbolExpresiones::new();
        arbol_exp.crear_abe(&self.condiciones);

        for linea in lector.lines() {
            let linea = linea.map_err(|_| errores::Errores::Error)?;
            let (campos,_) = parsear_linea_archivo(&linea);

            // Si no hay condiciones, eliminar todas las líneas
            if arbol_exp.arbol_vacio() {
                continue;
            }

            // Verificar si la línea cumple con las condiciones WHERE
            if arbol_exp.evalua(&self.campos_posibles, &campos) {
                // La línea cumple con las condiciones, no escribirla en el archivo temporal
                eliminados+=1;
            } else {
                // La línea no cumple con las condiciones, escribirla en el archivo temporal
                writeln!(escritor, "{}", linea).map_err(|_| errores::Errores::Error)?;
            }
        }
        if eliminados == 0 {
            Err(errores::Errores::Error)?;
        }
        // Asegurarse de escribir en el archivo
        escritor.flush().map_err(|_| errores::Errores::Error)?;
        // Reemplazar el archivo original con el archivo temporal
        fs::rename(ruta_temporal, ruta_archivo).map_err(|_| errores::Errores::Error)?;

        Ok(())
    }
}

impl Verificaciones for ConsultaDelete {
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

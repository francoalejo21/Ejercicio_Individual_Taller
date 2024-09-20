use crate::archivo::{leer_archivo, parsear_linea_archivo, procesar_ruta};
use crate::consulta::{mapear_campos, obtener_campos_consulta_orden_por_defecto, MetodosConsulta, Parseables, Verificaciones};
use crate::validador_where::ValidadorSintaxis;
use crate::verificaciones_sintaxis::verificar_orden_keywords;
use std::collections::HashSet;
use crate::select::{convertir_lower_case_restricciones, eliminar_comas};
use crate::select::ConsultaSelect;
use crate::validador_where::ValidadorOperandosValidos;
use std::fs;
use std::io::BufReader;
use crate::abe::ArbolExpresiones;
use crate::errores;
use crate::parseos::parseo;
use std::fs::{OpenOptions, File};
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
    /// Una instancia de `ConsultaInsert`

    pub fn crear(consulta: &Vec<String>, ruta_a_tablas: &String) -> Result<ConsultaDelete,errores::Errores> {
        let palabras_reservadas = vec!["delete", "from", "where"];  
        verificar_orden_keywords(consulta, palabras_reservadas)?;
        let mut caracteres_delimitadores = vec!['=',','];
        let consulta_spliteada = &parseo(consulta, &caracteres_delimitadores);
        let consulta_spliteada = &unir_literales_spliteados(consulta_spliteada);
        let tabla = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from("delete"), String::from("from")], HashSet::from(["where".to_string(), "".to_string()]), caracteres_delimitadores, false, false)?;        
        println!("tabla {:?}", tabla);
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let ruta_tabla = ruta_a_tablas.to_string(); 
        caracteres_delimitadores = vec!['=','<','>','(',')'];
        println!("{:?}" ,consulta_spliteada);
        let condiciones: Vec<String> = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from("where")], HashSet::from(["".to_string()]), caracteres_delimitadores, false, true)?;
        println!("condiciones {:?}", condiciones);
        println!("Pude crea consulta update");
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
        if self.condiciones.len() != 0 {
            if !validador_where.validar(){
                return Err(errores::Errores::InvalidSyntax);
            }
            let operandos = validador_where.obtener_operandos();
            let validador_operandos_validos = ValidadorOperandosValidos::new(&operandos, &self.campos_posibles);
            validador_operandos_validos.validar()?;
        }
        Ok(())
        
        //tambien debo verificar que la clausula where sea valida, llamo a mi validador de where y operandos

        //tambien debo tener un archivo nuevo donde voy escribiendo todo modificado y luego tiro el anterior archivo y
        //renombro el nuevo archivo con el nombre del anterior

        //tambien debo considerar que si no hay condicion where entonces debo actualizar todos los registros, es decir tirar todo
        //y volver a escribir lo que estoy modificando
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

        let mut arbol_exp = ArbolExpresiones::new();
        arbol_exp.crear_abe(&self.condiciones);
        println!("arbol de expresiones {:?}", arbol_exp);
        println!("campos posibles de la consulta{:?}", self.campos_posibles);
        println!("condiciones de la consulta{:?}", self.condiciones);

        for linea in lector.lines() {
            let linea = linea.map_err(|_| errores::Errores::Error)?;
            let (campos,_) = parsear_linea_archivo(&linea);

            // Si no hay condiciones, eliminar todas las líneas
            if arbol_exp.arbol_vacio() {
                println!("arbol_vacio");
                continue;
            }

            // Verificar si la línea cumple con las condiciones WHERE
            if arbol_exp.evalua(&self.campos_posibles, &campos) {
                // La línea cumple con las condiciones, no escribirla en el archivo temporal
                println!("{:?}", self.campos_posibles);
                println!("Eliminando línea en forma debugeada: {:?}", campos);
                println!("Eliminando línea: {}", linea);
            } else {
                // La línea no cumple con las condiciones, escribirla en el archivo temporal
                print!("Escribiendo línea en forma debugeada: {:?}", linea);
                println!("Escribiendo línea: {}", linea);
                writeln!(escritor, "{}", linea).map_err(|_| errores::Errores::Error)?;

                // Asegurarse de escribir en el archivo
                escritor.flush().map_err(|_| errores::Errores::Error)?;
            }
        }

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
        return true;
    }
}
fn unir_literales_spliteados(consulta_spliteada: &Vec<String>) -> Vec<String> {
    let mut valores: Vec<String> = Vec::new();
    let mut literal: Vec<String> = Vec::new();
    let mut parado_en_literal = false;

    for campo in consulta_spliteada {
        if campo.starts_with("'") && campo.ends_with("'") && campo.len() > 1 {
            // Literal completo, lo agregamos directamente
            valores.push(campo.to_string());
        } else if campo.starts_with("'") && !parado_en_literal {
            // Empieza un nuevo literal
            literal.push(campo.to_string());
            parado_en_literal = true;
        } else if campo.ends_with("'") && parado_en_literal {
            // Termina el literal actual
            literal.push(campo.to_string());
            valores.push(literal.join(" "));  // Une todo el literal
            literal.clear();
            parado_en_literal = false;
        } else if parado_en_literal {
            // Parte de un literal en proceso de unión
            literal.push(campo.to_string());
        } else {
            // Campo normal que no es un literal
            valores.push(campo.to_string());
        }
    }

    // Si el literal no se cerró correctamente, lo agregamos igual
    if !literal.is_empty() {
        valores.push(literal.join(" "));
    }

    valores
}

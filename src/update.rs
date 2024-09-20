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
        let palabras_reservadas = vec!["update", "set", "where"];  
        verificar_orden_keywords(consulta, palabras_reservadas)?;
        let mut caracteres_delimitadores = vec!['=',','];
        let consulta_spliteada = &parseo(consulta, &caracteres_delimitadores);
        let consulta_spliteada = &unir_literales_spliteados(consulta_spliteada);
        let tabla = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from("update")], HashSet::from(["set".to_string()]), caracteres_delimitadores, false, false)?;        
        

        println!("tabla {:?}", tabla);
        caracteres_delimitadores = vec![','];
        let campos_consulta = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from("set")], HashSet::from(["where".to_string(),"".to_string()]), caracteres_delimitadores, false, false)?;
        println!("campos_consulta {:?}", campos_consulta);
        
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let ruta_tabla = ruta_a_tablas.to_string(); 

        let campos_mapeados_valores: HashMap<String,String> = HashMap::new();

        caracteres_delimitadores = vec!['=','<','>','(',')'];
        println!("{:?}" ,consulta_spliteada);
        let condiciones: Vec<String> = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from("where")], HashSet::from(["".to_string()]), caracteres_delimitadores, false, true)?;
        println!("condiciones {:?}", condiciones);
        println!("Pude crea consulta update");
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
    /* fn parsear_valores(_consulta: &Vec<String>, _index: &mut usize) -> Vec<Vec<String>> {
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
    } */
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
        


        /////7777
        let mut tipos_datos = String::new();
        lector.read_line(&mut tipos_datos).map_err(|_| errores::Errores::Error)?;
        let (_,tipos_datos) = parsear_linea_archivo(&tipos_datos);
        println!("linea parseada {:?}",tipos_datos);
        let tipos_datos = mapear_tipos_datos(&campos_validos,&tipos_datos);
        ////77777
                        
        
        
        ///esto cambia ya que los campos son: campo1 = valor1, campo2 = valor2, campo3 = valor3
        ///  y debo revisar que el campo sea valido y que el valor sea valido
        /// y tambien que solo haya entre dos campos un igual cada campo,valor esta separado por una coma
        /// y cada campo,valor esta separado por un igual
        println!("campos consulta antes de constuir vector {:?}", self.campos_consulta);
        let campos_valores= construir_vector_campos_comparador_igual_valores(&self.campos_consulta);
        verificar_sintaxis_campos_valores(&campos_valores)?;
        println!("campos consulta despues de construir vector {:?}", self.campos_consulta);
        
        let campo_valores_validados  = verificar_campos_validos_y_valores_validos(campos_valores, &self.campos_posibles, &tipos_datos)?;
        let campos_mapeados_valores = mapear_campos_valores_terna(&campo_valores_validados);
        self.campos_mapeados_valores = campos_mapeados_valores;
        
        println!("campos mapeados valores__ {:?}", self.campos_mapeados_valores);
        
        
        
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

        let mut arbol_exp = ArbolExpresiones::new();
        arbol_exp.crear_abe(&self.condiciones);

        if arbol_exp.arbol_vacio() {
            // Si el árbol de expresiones está vacío, sobrescribir el archivo con los campos y valores de campos_mapeados_valores
            let mut nueva_linea: Vec<String> = vec!["".to_string(); self.campos_posibles.len()];
            for (campo, valor) in &self.campos_mapeados_valores {
                let mut valor_parseado = valor.to_string();
                valor_parseado = remover_comillas(&valor_parseado);
                if let Some(indice) = self.campos_posibles.get(campo) {
                    nueva_linea[*indice] = valor_parseado;
                }
            }
            let linea_modificada = nueva_linea.join(",");
            writeln!(escritor, "{}", linea_modificada).map_err(|_| errores::Errores::Error)?; // Error al escribir la línea
        } else {
            for linea in lector.lines() {
                let linea = linea.map_err(|_| errores::Errores::Error)?; // Error al leer la línea
                let  (_,mut campos) = parsear_linea_archivo(&linea);

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
                }
                let linea_modificada = campos.join(",");
                if let Err(_) = writeln!(escritor, "{}", linea_modificada) {
                    return Err(errores::Errores::Error); // Error al escribir la línea
                }
            }
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
        return true;
    }
}

fn verificar_valores_tipo_valido(campos_mapeados_valores: &Vec<HashMap<String,String>>, tipos_datos: &HashMap<String,String>)->Result<(),errores::Errores>{
    println!("los tipos de datos disponibles son{:?}",tipos_datos);
    for campos_mapeados in campos_mapeados_valores {
        for (campo, valor) in campos_mapeados {
            println!("campo {} valor {}", campo, valor);
            if valor == ""{
                continue;
            }
            match tipos_datos.get(campo) {
                Some(tipo) => {
                    if tipo == "Integer" {
                        if !valor.chars().all(char::is_numeric) {
                            Err(errores::Errores::Error)?;
                        }
                    }
                    if tipo == "String" {
                        if !valor.starts_with("'") || !valor.ends_with("'") {
                            Err(errores::Errores::Error)?;
                        }
                    }
                }
                None => Err(errores::Errores::Error)?,
            }
        }
    }
    Ok(())
}

fn mapear_campos_valores(vector_valores: &Vec<Vec<String>>, campos_consulta : &Vec<String>)->Vec<HashMap<String, String>>{
    let mut campos_mapeados_valores: Vec<HashMap<String, String>> = Vec::new();
    for fila_valores in vector_valores{
        let mut campos_mapeados_valores_fila: HashMap<String, String> = HashMap::new();
        for (indice, valor) in fila_valores.iter().enumerate(){
            campos_mapeados_valores_fila.insert(campos_consulta[indice].to_string(), valor.to_string());
        }
        campos_mapeados_valores.push(campos_mapeados_valores_fila);
    }
    campos_mapeados_valores
}

fn mapear_tipos_datos(columnas :&Vec<String>, columna1 :&Vec<String>)->HashMap<String,String>{
    let mut campos_mapeados_tipos_de_datos: HashMap<String, String> = HashMap::new();
    let mut indice :usize= 0;
    for campo in columna1{
        match campo.chars().all(char::is_numeric){
            true => campos_mapeados_tipos_de_datos.insert(columnas[indice].to_string(), "Integer".to_string()),
            false => campos_mapeados_tipos_de_datos.insert(columnas[indice].to_string(), "String".to_string())
            };
        indice += 1;
    }       
    campos_mapeados_tipos_de_datos
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
    let mut ultimo:Option<String> = None;
    for valor in valores {
        match valor.as_str() {
            "(" => {
                // Iniciar una nueva fila
                parentesis_abierto = true;
                fila_valores.clear(); // Limpiar la fila al iniciar
            }
            ")" => {
                // Finalizar la fila actual solo si hay valores en ella
                if ultimo == Some(",".to_string()){
                    fila_valores.push(String::new()); // Campo vacío por coma
                }
                vector_valores.push(fila_valores.clone());
                fila_valores.clear();
                parentesis_abierto = false;
            }
            "," => {
                // Agregar un campo vacío solo si estamos dentro de paréntesis
                if parentesis_abierto && ultimo == Some("(".to_string()) || ultimo == Some(",".to_string()) {
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


fn rellenar_valores_vacios_entre_comas_de_cada_filas(valores: &Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut vector_valores: Vec<Vec<String>> = Vec::new();

    for fila_valores in valores {
        let mut fila_valores_rellenados: Vec<String> = Vec::new();
        let mut esperando_valor = true;

        for valor in fila_valores {
            if valor == "," {
                if esperando_valor {
                    fila_valores_rellenados.push("NULL".to_string());
                }
                esperando_valor = true;
            } else {
                fila_valores_rellenados.push(valor.to_string());
                esperando_valor = false;
            }
        }

        if esperando_valor {
            fila_valores_rellenados.push("NULL".to_string());
        }

        vector_valores.push(fila_valores_rellenados);
    }

    vector_valores
}
fn verificar_cantidad_valores_validos(vector_valores: &Vec<Vec<String>>, campos_consulta: &Vec<String>) -> Result<(), errores::Errores> {
    //verificar que la cantidad a  valores a insertar sean la misma que la cantidad de campos
    for fila_valores in vector_valores {
        if fila_valores.len() != campos_consulta.len() {
            Err(errores::Errores::InvalidSyntax)?;
        }
    }
    Ok(())
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
            return Err(errores::Errores::InvalidSyntax)?;
        }
        // Verificar que el segundo elemento sea '='
        if operador != "=" {
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

    println!("campos posibles {:?}", campos_posibles);
    println!("tipos de datos {:?}", tipos_datos);
    println!("campos valores {:?}", vector_campos_valores);

    let mut vector_campos_valores_validados = Vec::new();

    for campos_valores in vector_campos_valores {
        let mut campos_valores_validados = campos_valores.clone();
        let campo = campos_valores_validados[0].to_lowercase();
        let valor = &campos_valores_validados[2];
        println!("campo {:?}", campo);
        if campo.starts_with("'") && campo.ends_with("'") {
            return Err(errores::Errores::InvalidSyntax);
        }
        if !campos_posibles.contains_key(&campo) {
            println!("no esta en los campos posibles :  {:?}", campo);
            return Err(errores::Errores::InvalidColumn);
        }
        if valor.starts_with("'") && valor.ends_with("'") {
            match tipos_datos.get(&campo) {
                Some(tipo) => {
                    if tipo == "Integer" {
                        return Err(errores::Errores::Error);
                    }
                }
                None => {} // Nada porque puede haber valores vacíos
            }
        } else {
            match tipos_datos.get(&campo) {
                Some(tipo) => {
                    if tipo == "String" {
                        return Err(errores::Errores::Error);
                    }
                }
                None => {} // Nada porque puede haber valores vacíos
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
    let mut ultimo:Option<String> = None;
    println!("campos_y_valores_antes de  crear vector{:?}", valores);
    for valor in valores {
        match valor.as_str() {
            "," => {
                if ultimo == Some("=".to_string()) || ultimo == None{
                    fila_campos_igual_valores.push(String::new()); // Campo vacío por coma
                }
                vector_terna.push(fila_campos_igual_valores.clone());
                println!("fila campos igual valores {:?}", fila_campos_igual_valores);
                fila_campos_igual_valores.clear();
            }
            _ => {
                // Agregar el valor si estamos dentro de paréntesis
                fila_campos_igual_valores.push(valor.to_string());
                println!("fila campos igual valores {:?}", fila_campos_igual_valores);
            }
        }
        ultimo = Some(valor.to_string());
    }
    if ultimo == Some(",".to_string()){
        fila_campos_igual_valores.push(String::new()); // Campo vacío por coma
        vector_terna.push(fila_campos_igual_valores);
    }
    //si quedo algo que no es una coma al final de la lista
    else if !fila_campos_igual_valores.is_empty(){
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

fn remover_comillas(valor :&String)->String{
    let mut valor_parseado = valor.to_string();
    if valor_parseado.starts_with("'") && valor_parseado.ends_with("'") {
        valor_parseado = valor_parseado[1..valor_parseado.len()-1].to_string();
    }
    valor_parseado
}
/*
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
*/

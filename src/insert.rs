use crate::archivo::{leer_archivo, parsear_linea_archivo, procesar_ruta};
use crate::consulta::{mapear_campos, obtener_campos_consulta_orden_por_defecto, MetodosConsulta, Parseables, Verificaciones};
use crate::verificaciones_sintaxis::verificar_orden_keywords;
use std::collections::HashSet;
use crate::select::{eliminar_comas, verificar_sintaxis_campos};
use crate::select::ConsultaSelect;
 
use crate::errores;
use crate::parseos::parseo;
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
    pub valores: Vec<String>,
    pub vector_valores: Vec<Vec<String>>,
    pub campos_mapeados_tipos_de_datos :Vec<HashMap<String,String>>,
    pub campos_mapeados_valores :Vec<HashMap<String,String>>,
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
    /// - `consulta`: La consulta SQL en formato `String`.
    /// - `ruta`: La ruta del archivo en el que se van a insertar los datos.
    ///
    /// # Retorno
    /// Una instancia de `ConsultaInsert`

    pub fn crear(consulta: &Vec<String>, ruta_a_tablas: &String) -> Result<ConsultaInsert,errores::Errores> {
        let palabras_reservadas = vec!["insert", "into", "values"];  
        verificar_orden_keywords(consulta, palabras_reservadas)?;
        let mut caracteres_delimitadores = vec!['(', ')', ','];
        let consulta_spliteada = &parseo(consulta, &caracteres_delimitadores);
        let consulta_spliteada = &unir_literales_spliteados(consulta_spliteada);
        let tabla = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from("insert"),String::from("into")], HashSet::from(["(".to_string()]), caracteres_delimitadores, false,false)?;        
        caracteres_delimitadores = vec![','];
        let campos_consulta = Self::parsear_cualquier_cosa(consulta_spliteada, vec![String::from("(")], HashSet::from([")".to_string()]), caracteres_delimitadores, true, false)?;
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let vector_valores: Vec<Vec<String>> = Vec::new();
        let ruta_tabla = ruta_a_tablas.to_string(); 
        let valores: Vec<String> = Self::parsear_cualquier_cosa(consulta, vec![String::from("values")], HashSet::from(["".to_string()]), vec![','], false, false)?;
        let campos_mapeados_tipos_de_datos: Vec<HashMap<String,String>> = Vec::new();
        let campos_mapeados_valores: Vec<HashMap<String,String>> = Vec::new();
        Ok(ConsultaInsert {
            campos_consulta,
            campos_posibles,
            valores,
            vector_valores,
            campos_mapeados_tipos_de_datos,
            campos_mapeados_valores,
            tabla,
            ruta_tabla,
        })
    }
}

impl Parseables for ConsultaInsert {
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

impl MetodosConsulta for ConsultaInsert {
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
                        
        
        verificar_sintaxis_campos(&self.campos_consulta)?;
        self.campos_consulta = eliminar_comas(&self.campos_consulta);
        if !ConsultaSelect::verificar_campos_validos(&self.campos_posibles, &mut self.campos_consulta) {
            Err(errores::Errores::InvalidColumn)?;
        }
        println!("campos valores literales {:?}", self.valores);
        println!("campos {:?}", self.campos_consulta);
        self.valores = unir_literales_spliteados(&self.valores);
        println!("valores_antes_verificar_sintaxis{:?}",self.valores);
        verificar_sintaxis_valores(&self.valores)?;
        println!("valores {:?}", self.valores);
        let vector_valores = construir_vector_valores(&self.valores);
        println!("vector_valores_construido_con_vacios{:?}",vector_valores);
        //let vector_valores = rellenar_valores_vacios_entre_comas_de_cada_filas(&vector_valores);
        verificar_cantidad_valores_validos(&vector_valores, &self.campos_consulta)?;
        println!("vector_valores{:?}",vector_valores);
        println!("{:?}",tipos_datos);
        let campos_mapeados_valores = mapear_campos_valores(&vector_valores, &self.campos_consulta);

        println!("campos mapeados valores____{:?}", campos_mapeados_valores);
        verificar_valores_tipo_valido(&campos_mapeados_valores, &tipos_datos)?;
        self.campos_mapeados_valores = campos_mapeados_valores;

        println!("campos mapeados valores {:?}", self.campos_mapeados_valores);
        println!("campos mapeados tipos de datos {:?}", tipos_datos);
        println!("campos consulta {:?}", self.campos_consulta);
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
                    None => &"".to_string(),  //si no esta ingreso una cadena vacia                    
                };
                if valor.starts_with("'") && valor.ends_with("'") {
                    let mut valor_str = valor.clone();
                    valor_str.remove(0);
                    valor_str.pop();
                    linea.push(valor_str);
                } else {
                    linea.push(valor.to_string());
                } 
            }
            let linea = linea.join(",");
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
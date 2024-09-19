use crate::archivo::{self, leer_archivo, procesar_ruta};
use crate::consulta::{
    mapear_campos, obtener_campos_consulta_orden_por_defecto, MetodosConsulta, Parseables,
    Verificaciones,
};

use crate::abe::ArbolExpresiones;
use crate::validador_where::ValidadorOperandosValidos;
use crate::ordenamiento;
use crate::parseos::parseo;
use crate::verificaciones_sintaxis::verificar_orden_keywords;
use crate::{errores, validador_where::ValidadorSintaxis};
use archivo::parsear_linea_archivo;

use std::{collections::{HashMap, HashSet}, io::BufRead};
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
    pub tabla: Vec<String>,
    pub campos_posibles: HashMap<String, usize>,
    pub restricciones: Vec<String>,//vector con la sintaxis de la restriccion
    pub ordenamiento: Vec<String>, //vector con la sintaxis del ordenamiento
    pub ruta_tabla: String,
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

    pub fn crear(consulta: &Vec<String>, ruta_a_tablas: &String) -> Result<ConsultaSelect,errores::Errores> {
        let palabras_reservadas = vec!["select", "from", "where", "order", "by"];        
        verificar_orden_keywords(consulta, palabras_reservadas)?;
        let mut caracteres_delimitadores: Vec<char> = vec![','];
        let campos_consulta = Self::parsear_cualquier_cosa(consulta, vec![String::from("select")], HashSet::from(["from".to_string()]), caracteres_delimitadores, true)?;
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let ruta_tabla = ruta_a_tablas.to_string(); 
        caracteres_delimitadores = vec![]; //no hay delimitadores
        let tabla: Vec<String> = Self::parsear_cualquier_cosa(consulta, vec![String::from("from")], HashSet::from(["where".to_string(),"order".to_string(), "".to_string()]), caracteres_delimitadores, false)?;
        caracteres_delimitadores = vec!['=','<','>','(',')'];
        let restricciones: Vec<String> = Self::parsear_cualquier_cosa(consulta, vec![String::from("where")], HashSet::from(["order".to_string(), "".to_string()]), caracteres_delimitadores, false)?;
        caracteres_delimitadores = vec![','];
        let ordenamiento: Vec<String> = Self::parsear_cualquier_cosa(consulta, vec![String::from("order"),String::from("by")], HashSet::from(["".to_string()]), caracteres_delimitadores, true)?;
        Ok(ConsultaSelect {
            campos_consulta,
            tabla,
            campos_posibles,
            restricciones,
            ordenamiento,
            ruta_tabla,
        })
    }
}

impl Parseables for ConsultaSelect {

    fn parsear_cualquier_cosa(
        consulta: &Vec<String>, 
        keywords_inicio: Vec<String>, 
        keyword_final: HashSet<String>, 
        caracteres_delimitadores: Vec<char>, 
        parseo_lower: bool,
    ) -> Result<Vec<String>, errores::Errores> {
        let mut index = 0;
        let mut campos = Vec::new();
        let mut keyword_final_encontrada = false;
    
        // Busco el/las keywords de inicio y las salteo
        let mut i_keywords_inicio = 0;
        while index < consulta.len() && i_keywords_inicio < keywords_inicio.len() {
            if consulta[index].to_lowercase() == keywords_inicio[i_keywords_inicio] {
                i_keywords_inicio += 1;
            }
            index += 1;
        }
    
        // Si no encontre ninguna keyword de inicio devuelvo vacio
        if i_keywords_inicio == 0 {
            return Ok(campos);
        }
    
        // Si no encontre todas las keywords de inicio devuelvo error
        if i_keywords_inicio != keywords_inicio.len() {
            return Err(errores::Errores::InvalidSyntax);
        }
    
        while index < consulta.len() {
            let token = consulta[index].to_lowercase();
            if keyword_final.contains(&token) {
                keyword_final_encontrada = true;
                break;
            }
            campos.push(if parseo_lower { token } else { consulta[index].to_string()});
            index += 1;
        }
    
        if campos.is_empty() {
            return Err(errores::Errores::InvalidSyntax);
        }
    
        let campos_parseados = parseo(&campos, &caracteres_delimitadores);
        if keyword_final.contains("") || keyword_final_encontrada {
            Ok(campos_parseados)
        } else {
            Err(errores::Errores::InvalidSyntax)
        }
    }
}
impl MetodosConsulta for ConsultaSelect {
    /// Verifica la validez de la consulta SQL.
    ///
    /// Este método verifica que los campos de consulta no estén vacíos, que exista la tabla y que todos los campos
    /// solicitados sean válidos según los campos posibles definidos en la estructura.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`) o el tipo de error (`Err`).

    fn verificar_validez_consulta(&mut self) -> Result<(), errores::Errores> {
        if self.tabla.len() != 1 {
            return Err(errores::Errores::InvalidSyntax);
        }
        self.ruta_tabla = procesar_ruta(&self.ruta_tabla, &self.tabla[0]);
        let mut lector = leer_archivo(&self.ruta_tabla).map_err(|_| errores::Errores::InvalidTable)?;
        let mut nombres_campos = String::new();
        lector.read_line(&mut nombres_campos).map_err(|_| errores::Errores::Error)?;
        let (_, campos_validos) = parsear_linea_archivo(&nombres_campos);
        self.campos_posibles = mapear_campos(&campos_validos);
        verificar_sintaxis_campos(&self.campos_consulta)?;
        self.campos_consulta = eliminar_comas(&self.campos_consulta);
        if !ConsultaSelect::verificar_campos_validos(&self.campos_posibles, &mut self.campos_consulta) {
            return Err(errores::Errores::InvalidColumn);
        }
        self.restricciones = convertir_lower_case(&self.restricciones, &self.campos_posibles);
        let mut validador_where = ValidadorSintaxis::new(&self.restricciones);
        if self.restricciones.len() != 0 {
            if !validador_where.validar(){
                return Err(errores::Errores::InvalidSyntax);
            }
            let operandos = validador_where.obtener_operandos();
            let validador_operandos_validos = ValidadorOperandosValidos::new(&operandos, &self.campos_posibles);
            validador_operandos_validos.validar()?;
        }
        if self.ordenamiento.len() != 0 {
            verificar_sintaxis_ordenamiento(&self.ordenamiento)?;
            if !verificar_campos_validos_ordenamientos(&self.ordenamiento, &self.campos_posibles) {
                Err(errores::Errores::InvalidColumn)?
            }
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
        let mut lector = leer_archivo(&self.ruta_tabla).map_err(|_| errores::Errores::InvalidTable)?;
        let mut nombres_campos = String::new();
        lector.read_line(&mut nombres_campos).map_err(|_| errores::Errores::Error)?;

        let mut arbol_exp = ArbolExpresiones::new();
        arbol_exp.crear_abe(&self.restricciones);

        let ordenamientos = obtener_ordenamientos(&self.ordenamiento);
        let mut vector_almacenar: Vec<Vec<String>> = Vec::new();

        for registro in lector.lines() {
            let (registro_parseado, _) = registro.map_err(|_| errores::Errores::Error).and_then(|r| Ok(parsear_linea_archivo(&r)))?;
            let campos_seleccionados: Vec<&usize> = self.campos_consulta.iter()
                .map(|campo| self.campos_posibles.get(campo).ok_or(errores::Errores::Error))
                .collect::<Result<_, _>>()?;

            if !arbol_exp.arbol_vacio() {
                if !arbol_exp.evalua(&self.campos_posibles, &registro_parseado) {
                    continue;
                }
            }

            let linea: Vec<String> = campos_seleccionados.iter()
                .map(|&&campo| registro_parseado[campo].to_string())
                .collect();

            if ordenamientos.is_empty() {
                println!("{}", linea.join(","));
            } else {
                vector_almacenar.push(linea);
            }
        }

        if !ordenamientos.is_empty() {
            let orden_ordenamientos = reemplazar_string_por_usize(ordenamientos, &self.campos_posibles);
            ordenamiento::ordenar_consultas_multiples(&mut vector_almacenar, orden_ordenamientos);
            for linea in vector_almacenar {
                println!("{}", linea.join(","));
            }
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
        if campos_consulta.len() == 1 {
            if campos_consulta[0] == "*".to_string() {
                campos_consulta.pop(); //Me saco de encima el "*""
                                       //debo reemplazar ese caracter por todos los campos válidos
                let campos = &obtener_campos_consulta_orden_por_defecto(campos_validos);
                for campo in campos {
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

fn verificar_sintaxis_campos(campos: &Vec<String>)->Result<(),errores::Errores>{
    // iteramos el vector de campos si en la primera posicion hay una coma o en la ultima devolver error, o si hay dos comas seguidas
    // entre campos devolver error tambien
    let mut index: usize = 0;
    while index < campos.len(){
        if campos[index] == ","{
            if index == 0 || index == campos.len()-1{
                Err(errores::Errores::InvalidSyntax)?
            }
            if campos[index+1] == ","{
                Err(errores::Errores::InvalidSyntax)?
            }
        }
        index += 1;
    }
    Ok(())
}

fn eliminar_comas(campos : &Vec<String>)-> Vec<String>{
    //iterar sobre el vector de campos y eliminar las comas
    let mut campos_limpio: Vec<String> = Vec::new();
    for campo in campos{
        if campo != ","{
            campos_limpio.push(campo.to_string());
        }
    }
    campos_limpio
}

fn verificar_sintaxis_ordenamiento(ordenamiento: &Vec<String>) -> Result<(), errores::Errores> {
    if ordenamiento.is_empty() {
        return Ok(()); // No hay ordenamiento, no hay errores
    }

    let mut index = 0;
    let mut esperando_coma = false;
    let mut esperando_asc_desc = false;

    while index < ordenamiento.len() {
        let campo_actual = &ordenamiento[index].to_lowercase();

        if campo_actual == "," {
            if index == 0 || index == ordenamiento.len() - 1 || ordenamiento[index + 1] == "," {
                return Err(errores::Errores::InvalidSyntax);
            }
            esperando_coma = false;
            esperando_asc_desc = false;
        } else {
            if esperando_coma && !esperando_asc_desc {
                return Err(errores::Errores::InvalidSyntax);
            }
            if esperando_asc_desc {
                if campo_actual != "asc" && campo_actual != "desc" {
                    return Err(errores::Errores::InvalidSyntax);
                }
                esperando_asc_desc = false;
                esperando_coma = true;
            } else {
                esperando_asc_desc = index < ordenamiento.len() - 1 && ordenamiento[index + 1] != ",";
                esperando_coma = !esperando_asc_desc;
            }
        }
        index += 1;
    }

    if esperando_asc_desc {
        return Err(errores::Errores::InvalidSyntax);
    }

    Ok(())
}

fn verificar_campos_validos_ordenamientos(ordenamiento : &Vec<String>, campos_mapeados: &HashMap<String,usize>)->bool{
    //asumiendo que la sintaxis de los ordenamientos es correcta, iterar sobre el vector de ordenamientos y si algun campo no es un campo de la tabla devolver false
    for campo in ordenamiento{
        if !campos_mapeados.contains_key(campo) && campo != "asc" && campo != "desc" && campo != ","{
            return false;
        }
    }
    return true;
}

fn obtener_ordenamientos(ordenamientos: &Vec<String>) -> Vec<(String, bool)> {
    let mut ordenamientos_devolver: Vec<(String, bool)> = Vec::new();
    
    let mut campo: Option<String> = None;
    let mut ordenamiento = true; // Por defecto es ASC

    for orden in ordenamientos {
        if orden == "," {
            // Asegurarse de que haya un campo antes de la coma
            if let Some(campo_valido) = campo {
                ordenamientos_devolver.push((campo_valido, ordenamiento));
                campo = None;
                ordenamiento = true; // Reiniciar para el próximo campo
            }
            continue;
        }

        if orden == "asc" {
            ordenamiento = true; // Orden ASC
        } else if orden == "desc" {
            ordenamiento = false; // Orden DESC
        } else {
            // Si es un campo, lo guardamos para el próximo ordenamiento
            campo = Some(orden.to_string());
        }
    }

    // Agregar el último campo si no había una coma al final
    if let Some(campo_valido) = campo {
        ordenamientos_devolver.push((campo_valido, ordenamiento));
    }

    ordenamientos_devolver
}



fn convertir_lower_case(restricciones: &Vec<String>, campos_mapeados: &HashMap<String,usize>)->Vec<String>{
    //iteramos sobre las restricciones y si el campo es un campo de la tabla lo convertimos a minusculas y si es un operador and or not
    // tambien casteamos estos a lower case
    println!("campos mapeados : {:?}",campos_mapeados);
    let mut restricciones_lower: Vec<String> = Vec::new();
    for restriccion in restricciones{
        if campos_mapeados.contains_key(&restriccion.to_lowercase()) && !es_literal(restriccion) && !restriccion.chars().all(char::is_numeric){
            restricciones_lower.push(restriccion.to_lowercase());
        }else{
            if restriccion.to_lowercase() == "and" || restriccion.to_lowercase() == "or" || restriccion.to_lowercase() == "not"{
                restricciones_lower.push(restriccion.to_lowercase());
            }
            else {
                restricciones_lower.push(restriccion.to_string());
            }
        }
    }  
    restricciones_lower
}

fn es_literal(operando: &String) -> bool {
    operando.starts_with("'") && operando.ends_with("'")
}

fn reemplazar_string_por_usize(ordenamientos: Vec<(String,bool)>, campos_posibles: &HashMap<String,usize>)->Vec<(usize,bool)>{
    //iterar sobre el vector de ordenamientos y reemplazar los strings por los usizes
    let mut ordenamientos_usize: Vec<(usize,bool)> = Vec::new();
    for (campo,orden) in ordenamientos{
        let campo_usize = match campos_posibles.get(&campo) {
            Some(valor)=> valor,
            None=> continue
        };
        ordenamientos_usize.push((*campo_usize,orden));
    }
    ordenamientos_usize
}
/* 
#[cfg(test)]
mod tests {
    use crate::consulta;

    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_parsear_consulta_select() {
        let consulta = String::from(
            "SELECT campo1,campo2 FROM tabla WHERE campo1 = 'valor1' ORDER BY campo2 DESC",
        );
        let tokens = parsear_consulta_de_comando(&consulta);

        assert_eq!(
            tokens,
            vec![
                "SELECT", "campo1,campo2", "FROM", "tabla", "WHERE", "campo1", "=", "'valor1'",
                "ORDER", "BY", "campo2", "DESC"
            ]
        );
    }

    #[test]
     fn test_parsear_campos_caso_campos_pegados() {
        let consulta = String::from(
            "SELECT campo1,campo2,campo3 FROM tabla",
        );
        let consulta_parseada = parsear_consulta_de_comando(&consulta);
        let campos = ConsultaSelect::parsear_campos(&consulta_parseada);

        assert_eq!(
            campos,
            vec!["campo1","campo2","campo3"]
        );
    }

    #[test]
     fn test_parsear_campos_caso_campos_separados() {
        let consulta = String::from(
            "SELECT campo1, campo2, campo3 FROM tabla WHERE campo1 = 'valor1' ORDER BY campo2 DESC",
        );
        let consulta_parseada = parsear_consulta_de_comando(&consulta);
        let campos = ConsultaSelect::parsear_campos(&consulta_parseada);

        assert_eq!(
            campos,
            vec!["campo1","campo2","campo3"]
        );
    }
    #[test]
    fn test_parsear_campos_caso_separados() {
        let consulta = String::from(
            "SELECT campo1 ,campo2 ,campo3 FROM tabla",
        );
        let consulta_parseada = parsear_consulta_de_comando(&consulta);
        let campos = ConsultaSelect::parsear_campos(&consulta_parseada);

        assert_eq!(
            campos,
            vec!["campo1","campo2","campo3"]
        );
    }

    #[test]
    fn test_parsear_tablas() {
        let consulta = String::from(
            "SELECT campo1 ,campo2 ,campo3 FROM tabla1, tabla2, tabla3",
        );
        let consulta_parseada = parsear_consulta_de_comando(&consulta);
        let tablas = ConsultaSelect::parsear_tabla(&consulta_parseada);

        assert_eq!(
            tablas,
            vec!["tabla1,","tabla2,","tabla3"]
        );
    }
    #[test]
    fn test_parsear_tablas_con_corte_where() {
        let consulta = String::from(
            "SELECT campo1 ,campo2 ,campo3 FROM tabla1, tabla2, tabla3, tabla4, tabla5 WHERE ....",
        );
        let consulta_parseada = parsear_consulta_de_comando(&consulta);
        let tablas = ConsultaSelect::parsear_tabla(&consulta_parseada);

        assert_eq!(
            tablas,
            vec!["tabla1,","tabla2,","tabla3,", "tabla4,", "tabla5"]
        );
    }

    #[test]
    fn test_parsear_condiciones() {
        
        let consulta = String::from(
            "SELECT campo1, campo2, campo3 FROM tabla WHERE campo1 = 'valor1' AND campo2 = 'valor2'",
        );
        let consulta_parseada = parsear_consulta_de_comando(&consulta);
        let restricciones = ConsultaSelect::parsear_restricciones(&consulta_parseada);

        assert_eq!(
            restricciones,
            vec!["campo1", "=","'valor1'","AND", "campo2", "=", "'valor2'"]
        );
    }

    #[test]
    fn test_parsear_condiciones_con_corte_order() {
        let consulta = String::from(
            "SELECT campo1, campo2, campo3 FROM tabla WHERE campo1 = 'valor1' AND campo2 = 'valor2' OR 1 = 1 ORDER BY ....",
        );
        let consulta_parseada = parsear_consulta_de_comando(&consulta);
        let restricciones = ConsultaSelect::parsear_restricciones(&consulta_parseada);

        assert_eq!(
            restricciones,
            vec!["campo1", "=","'valor1'","AND", "campo2", "=", "'valor2'", "OR", "1","=", "1"]
        );
    }

    #[test]
    fn test_parsear_ordenamiento() {
        let consulta = String::from(
            "SELECT campo1, campo2, campo3 ORDER BY campo1 DESC, campo2",
        );
        let consulta_parseada = parsear_consulta_de_comando(&consulta);
        let ordenamiento = ConsultaSelect::parsear_ordenamiento(&consulta_parseada);

        assert_eq!(
            ordenamiento,
            vec!["campo1", "DESC,", "campo2"]
        );
    }

*/
/*
    #[test]
    fn test_verificar_ruta_tabla() {
       
        let ruta_tabla = String::from("/ruta/a/tablas");

        let consulta_select = ConsultaSelect::crear(&consulta, &ruta_tabla);

        assert_eq!(consulta_select.campos_consulta, vec!["campo1", "campo2"]);
        assert_eq!(consulta_select.tabla, "tabla");
        assert_eq!(
            consulta_select.restricciones,
            vec!["campo1", "=", "'valor1'"]
        );
        assert_eq!(consulta_select.ordenamiento, vec!["campo2", "desc"]);
        assert_eq!(consulta_select.ruta_tabla, "/ruta/a/tablas/tabla");
    }
 
    #[test]
    fn test_verificar_campos_validos() {
        let mut campos_validos = HashMap::new();
        campos_validos.insert("campo1".to_string(), 0);
        campos_validos.insert("campo2".to_string(), 1);

        let mut campos_consulta = vec!["campo1".to_string(), "campo2".to_string()];
        let resultado =
            ConsultaSelect::verificar_campos_validos(&campos_validos, &mut campos_consulta);

        assert!(resultado);
    }

    #[test]
    fn test_verificar_campos_invalidos() {
        let mut campos_validos = HashMap::new();
        campos_validos.insert("campo1".to_string(), 0);

        let mut campos_consulta = vec!["campo1".to_string(), "campo3".to_string()];
        let resultado =
            ConsultaSelect::verificar_campos_validos(&campos_validos, &mut campos_consulta);

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
 */
use crate::archivo::{self, leer_archivo, procesar_ruta};
use crate::consulta::{
    mapear_campos, obtener_campos_consulta_orden_por_defecto, MetodosConsulta, Parseables, Verificaciones
};

use crate::abe::ArbolExpresiones;
use crate::validador_where::ValidadorOperandosValidos;
use crate::verificaciones_sintaxis::verificar_orden_keywords;
use crate::{errores, validador_where::ValidadorSintaxis};
use archivo::parsear_linea_archivo;
use crate::parseos::parseo;
use crate::parseos::unir_literales_spliteados;
use std::{collections::{HashMap, HashSet}, io::BufRead};

const CARACTERES_DELIMITADORES: &[char] = &[';', ',', '=', '<', '>', '(', ')'];
const TODO: &str = "*";
const COMILLA_SIMPLE: &str = "'";
const SELECT: &str = "select";
const FROM: &str = "from";
const WHERE: &str = "where";
const ORDER: &str = "order";
const BY: &str = "by";
const CARACTER_VACIO: &str = "";
const PUNTO_COMA: &str = ";";
const COMA: &str = ",";
const ASCENDENTE: &str = "asc";
const DESCENDENTE: &str = "desc";


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
        let palabras_reservadas = vec![SELECT, FROM, WHERE, ORDER, BY];        
        verificar_orden_keywords(consulta, palabras_reservadas)?;
        let consulta_spliteada = &parseo(consulta, CARACTERES_DELIMITADORES);
        let consulta = &unir_literales_spliteados(consulta_spliteada);
        let campos_consulta = Self::parsear_cualquier_cosa(consulta, vec![String::from(SELECT)], HashSet::from([FROM.to_string()]), true, false)?;
        let campos_posibles: HashMap<String, usize> = HashMap::new();
        let ruta_tabla = ruta_a_tablas.to_string(); 
        let tabla: Vec<String> = Self::parsear_cualquier_cosa(consulta, vec![String::from(FROM)], HashSet::from([WHERE.to_string(),ORDER.to_string(), CARACTER_VACIO.to_string(),PUNTO_COMA.to_string()]), false, false)?;
        let restricciones: Vec<String> = Self::parsear_cualquier_cosa(consulta, vec![String::from(WHERE)], HashSet::from([ORDER.to_string(), CARACTER_VACIO.to_string(),PUNTO_COMA.to_string()]), false, true)?;
        let ordenamiento: Vec<String> = Self::parsear_cualquier_cosa(consulta, vec![String::from(ORDER),String::from(BY)], HashSet::from([CARACTER_VACIO.to_string(), PUNTO_COMA.to_string()]), true, true)?;
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
        self.restricciones = convertir_lower_case_restricciones(&self.restricciones, &self.campos_posibles);
        let mut validador_where = ValidadorSintaxis::new(&self.restricciones);
        if !self.restricciones.is_empty(){
            if !validador_where.validar(){
                return Err(errores::Errores::InvalidSyntax);
            }
            let operandos = validador_where.obtener_operandos();
            let validador_operandos_validos = ValidadorOperandosValidos::new(&operandos, &self.campos_posibles);
            validador_operandos_validos.validar()?;
        }
        if !self.ordenamiento.is_empty(){
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
        let mut seleccionados = 0;
        for registro in lector.lines() {
            let (registro_parseado, _) = registro.map_err(|_| errores::Errores::Error).map(|r| parsear_linea_archivo(&r))?;
            let campos_seleccionados: Vec<&usize> = self.campos_consulta.iter()
                .map(|campo| self.campos_posibles.get(campo).ok_or(errores::Errores::Error))
                .collect::<Result<_, _>>()?;

            if !arbol_exp.arbol_vacio() && !arbol_exp.evalua(&self.campos_posibles, &registro_parseado) {
                continue;
            }
            seleccionados+=1;
            let linea: Vec<String> = campos_seleccionados.iter()
                .map(|&&campo| registro_parseado[campo].to_string())
                .collect();

            if ordenamientos.is_empty() {
                println!("{}", linea.join(COMA));
            } else {
                vector_almacenar.push(linea);
            }
        }

        if !ordenamientos.is_empty() {
            let orden_ordenamientos = reemplazar_string_por_usize(ordenamientos, &self.campos_posibles);
            ordenar_consultas_multiples(&mut vector_almacenar, orden_ordenamientos);
            for linea in vector_almacenar {
                println!("{}", linea.join(COMA));
            }
        }
        if seleccionados == 0{
            Err(errores::Errores::Error)?
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
        if campos_consulta.len() == 1 && campos_consulta[0] == TODO{
            campos_consulta.pop(); //Me saco de encima el "*CARACTER_VACIO
                                    //debo reemplazar ese caracter por todos los campos válidos
            let campos = &obtener_campos_consulta_orden_por_defecto(campos_validos);
            for campo in campos {
                campos_consulta.push(campo.to_string());
            }
            return true;
        }

        for campo in campos_consulta {
            if !(campos_validos.contains_key(campo)) {
                return false;
            }
        }
        true
    }
}

pub fn verificar_sintaxis_campos(campos: &[String])->Result<(),errores::Errores>{
    // iteramos el vector de campos si en la primera posicion hay una coma o en la ultima devolver error, o si hay dos comas seguidas
    // entre campos devolver error tambien
    let mut index: usize = 0;
    while index < campos.len(){
        if campos[index] == COMA{
            if index == 0 || index == campos.len()-1{
                Err(errores::Errores::InvalidSyntax)?
            }
            if campos[index+1] == COMA{
                Err(errores::Errores::InvalidSyntax)?
            }
        }
        index += 1;
    }
    Ok(())
}

pub fn eliminar_comas(campos : &Vec<String>)-> Vec<String>{
    //iterar sobre el vector de campos y eliminar las comas
    let mut campos_limpio: Vec<String> = Vec::new();
    for campo in campos{
        if campo != COMA{
            campos_limpio.push(campo.to_string());
        }
    }
    campos_limpio
}

fn verificar_sintaxis_ordenamiento(ordenamiento: &[String]) -> Result<(), errores::Errores> {
    if ordenamiento.is_empty() {
        return Ok(()); // No hay ordenamiento, no hay errores
    }

    let mut index = 0;
    let mut esperando_coma = false;
    let mut esperando_asc_desc = false;

    while index < ordenamiento.len() {
        let campo_actual = &ordenamiento[index].to_lowercase();

        if campo_actual == COMA {
            if index == 0 || index == ordenamiento.len() - 1 || ordenamiento[index + 1] == COMA {
                return Err(errores::Errores::InvalidSyntax);
            }
            esperando_coma = false;
            esperando_asc_desc = false;
        } else {
            if esperando_coma && !esperando_asc_desc {
                return Err(errores::Errores::InvalidSyntax);
            }
            if esperando_asc_desc {
                if campo_actual != ASCENDENTE && campo_actual != DESCENDENTE {
                    return Err(errores::Errores::InvalidSyntax);
                }
                esperando_asc_desc = false;
                esperando_coma = true;
            } else {
                esperando_asc_desc = index < ordenamiento.len() - 1 && ordenamiento[index + 1] != COMA;
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
        if !campos_mapeados.contains_key(campo) && campo != ASCENDENTE && campo != DESCENDENTE && campo != COMA{
            return false;
        }
    }
    true
}

fn obtener_ordenamientos(ordenamientos: &Vec<String>) -> Vec<(String, bool)> {
    let mut ordenamientos_devolver: Vec<(String, bool)> = Vec::new();
    
    let mut campo: Option<String> = None;
    let mut ordenamiento = true; // Por defecto es ASC

    for orden in ordenamientos {
        if orden == COMA {
            // Asegurarse de que haya un campo antes de la coma
            if let Some(campo_valido) = campo {
                ordenamientos_devolver.push((campo_valido, ordenamiento));
                campo = None;
                ordenamiento = true; // Reiniciar para el próximo campo
            }
            continue;
        }

        if orden == ASCENDENTE {
            ordenamiento = true; // Orden ASC
        } else if orden == DESCENDENTE {
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



pub fn convertir_lower_case_restricciones(restricciones: &Vec<String>, campos_mapeados: &HashMap<String, usize>) -> Vec<String> {
    // Iteramos sobre las restricciones y si el campo es un campo de la tabla lo convertimos a minúsculas y si es un operador and, or, not
    // también casteamos estos a lower case
    let mut restricciones_lower: Vec<String> = Vec::new();
    for restriccion in restricciones {
        let restriccion_lower = restriccion.to_lowercase();
        if campos_mapeados.contains_key(&restriccion_lower) && !es_literal(restriccion) && !restriccion.chars().all(char::is_numeric) || ["and", "or", "not"].contains(&restriccion_lower.as_str()) {
            restricciones_lower.push(restriccion_lower);
        } else {
            restricciones_lower.push(restriccion.to_string());
        }
    }
    restricciones_lower
}

fn es_literal(operando: &str) -> bool {
    operando.starts_with(COMILLA_SIMPLE) && operando.ends_with(COMILLA_SIMPLE)
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

fn ordenar_consultas_multiples(
    filas: &mut [Vec<String>], 
    columnas_orden: Vec<(usize, bool)>
) {
    filas.sort_by(|a, b| {
        for (columna_orden, ascendente) in &columnas_orden {
            let valor_a = &a[*columna_orden];
            let valor_b = &b[*columna_orden];

            // Comparación adicional si alguna columna es vacía
            let cmp = match (valor_a.is_empty(), valor_b.is_empty()) {
                (true, false) => std::cmp::Ordering::Less,    // La columna vacía es menor
                (false, true) => std::cmp::Ordering::Greater, // La columna vacía es mayor
                (true, true) => std::cmp::Ordering::Equal,    // Ambas son vacías, son iguales
                _ => valor_a.cmp(valor_b),                    // Comparar normalmente si no están vacías
            };

            if cmp != std::cmp::Ordering::Equal {
                return if *ascendente {
                    cmp
                } else {
                    cmp.reverse()
                };
            }
        }
        std::cmp::Ordering::Equal
    });
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
                ORDER, "BY", "campo2", DESCENDENTE
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
            vec!["campo1COMAcampo2COMAcampo3"]
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
            vec!["campo1COMAcampo2COMAcampo3"]
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
            vec!["campo1COMAcampo2COMAcampo3"]
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
            vec!["tabla1,COMAtabla2,COMAtabla3"]
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
            vec!["tabla1,COMAtabla2,COMAtabla3,", "tabla4,", "tabla5"]
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
            vec!["campo1", "=COMA'valor1'COMAAND", "campo2", "=", "'valor2'"]
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
            vec!["campo1", "=COMA'valor1'COMAAND", "campo2", "=", "'valor2'", "OR", "1COMA=", "1"]
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
        assert_eq!(consulta_select.ordenamiento, vec!["campo2", DESCENDENTE]);
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
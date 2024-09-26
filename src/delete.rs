use crate::abe::ArbolExpresiones;
use crate::archivo::{leer_archivo, parsear_linea_archivo, procesar_ruta};
use crate::consulta::{mapear_campos, MetodosConsulta, Parseables, Verificaciones};
use crate::errores;
use crate::parseos::{
    convertir_lower_case_restricciones, parseo, unir_literales_spliteados,
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

const CARACTERES_DELIMITADORES: &[char] = &[';', ',', '=', '<', '>', '(', ')'];
const DELETE: &str = "delete";
const FROM: &str = "from";
const WHERE: &str = "where";
const CARACTER_VACIO: &str = "";
const PUNTO_COMA: &str = ";";

/// Estructura que representa una consulta SQL de tipo DELETE.
/// Contiene los campos posibles a eliminar, la tabla en la que se van a eliminar los datos, la ruta del archivo tabla a modificar y las condiciones
/// que deben cumplir los datos a eliminar.

#[derive(Debug)]
pub struct ConsultaDelete {
    pub campos_posibles: HashMap<String, usize>,
    pub tabla: Vec<String>,
    pub ruta_tabla: String,
    pub condiciones: Vec<String>,
}

impl ConsultaDelete {
    /// Crea una nueva consulta de tipo DELETE con los campos posibles a eliminar, la tabla en la que se van a eliminar los datos, la ruta del archivo tabla a modificar y las condiciones
    /// que deben cumplir los datos a eliminar.
    /// Verifica la validez de la consulta en el sentido de si las keywords estan correctamente ingresadas
    /// y si la consulta cumple con la sintaxis de DELETE FROM WHERE.
    ///
    /// # Parámetros
    /// - `consulta`: Un `Vec<String>` que contiene las palabras de la consulta SQL.
    /// - `ruta_a_tablas`: Un `String` que contiene la ruta de la tabla a modificar.
    ///
    /// # Retorno
    /// Retorna un `Result` que indica el éxito (`Ok`), entonces devuelve una consulta de tipo DELETE, o el tipo de error (`Err`).

    pub fn crear(
        consulta: &Vec<String>,
        ruta_a_tablas: &String,
    ) -> Result<ConsultaDelete, errores::Errores> {
        let palabras_reservadas = vec![DELETE, FROM, WHERE];
        Self::verificar_orden_keywords(consulta, palabras_reservadas)?;
        let consulta_spliteada = &parseo(consulta, CARACTERES_DELIMITADORES);
        let consulta_spliteada = &unir_literales_spliteados(consulta_spliteada);
        let consulta_spliteada = &unir_operadores_que_deben_ir_juntos(consulta_spliteada);
        let tabla = Self::parsear_cualquier_cosa(
            consulta_spliteada,
            vec![String::from(DELETE), String::from(FROM)],
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
        let condiciones: Vec<String> = Self::parsear_cualquier_cosa(
            consulta_spliteada,
            vec![String::from(WHERE)],
            HashSet::from([CARACTER_VACIO.to_string(), PUNTO_COMA.to_string()]),
            false,
            true,
        )?;
        Ok(ConsultaDelete {
            campos_posibles,
            tabla,
            ruta_tabla,
            condiciones,
        })
    }
}

impl Parseables for ConsultaDelete {}

impl MetodosConsulta for ConsultaDelete {
    /// Verifica la validez de la consulta DELETE.
    /// Verifica que la consulta tenga un solo nombre de tabla, que la ruta de la tabla sea válida, que la tabla exista y que las condiciones de la consulta sean válidas.
    /// que las condiciones de la consulta sean válidas, como tambien los operandos de las condiciones.
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

        //verificamos que la condicion where sea valida y los operandos sean validos
        self.condiciones =
            convertir_lower_case_restricciones(&self.condiciones, &self.campos_posibles);
        let mut validador_where = ValidadorSintaxis::new(&self.condiciones);
        if !self.condiciones.is_empty() {
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

    /// Procesa la consulta DELETE.
    /// Lee el archivo de la tabla a modificar, crea un archivo temporal para escribir los cambios, elimina las líneas que cumplen con las condiciones de la consulta y reemplaza el archivo original con el archivo temporal.
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
            let (campos, _) = parsear_linea_archivo(&linea);

            // Si no hay condiciones, eliminar todas las líneas
            if arbol_exp.arbol_vacio() {
                continue;
            }

            // Verificar si la línea cumple con las condiciones WHERE
            if arbol_exp.evalua(&self.campos_posibles, &campos) {
                // La línea cumple con las condiciones, no escribirla en el archivo temporal
                eliminados += 1;
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
    /// Verifica que los campos de la consulta DELETE sean válidos.
    /// Para esto se verifica que los campos de la consulta estén en el Hashmap de campos válidos.
    ///
    /// Parámetros:
    /// - `campos_validos`: Un `HashMap<String, usize>` que contiene los campos válidos.
    /// - `campos_consulta`: Un `Vec<String>` que contiene los campos de la consulta.
    ///
    /// Retorno:
    /// Retorna un `bool` que indica si los campos de la consulta son válidos (`true`) o no (`false`).

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errores::Errores;

    #[test]
    fn test_crear_consulta_delete_valida() {
        let consulta = vec![
            "delete".to_string(),
            "from".to_string(),
            "tabla".to_string(),
            "where".to_string(),
            "campo".to_string(),
            "=".to_string(),
            "valor".to_string(),
        ];
        let ruta_a_tablas = "ruta/a/tablas".to_string();
        let resultado = ConsultaDelete::crear(&consulta, &ruta_a_tablas);
        assert!(resultado.is_ok());
    }

    #[test]
    fn test_crear_consulta_delete_invalida_sintaxis() {
        let consulta = vec![
            "delete".to_string(),
            "tabla".to_string(),
            "from".to_string(),
            "where".to_string(),
            "campo".to_string(),
            "=".to_string(),
            "valor".to_string(),
        ];
        let ruta_a_tablas = "ruta/a/tablas".to_string();
        let resultado = ConsultaDelete::crear(&consulta, &ruta_a_tablas);
        assert!(matches!(resultado, Err(Errores::InvalidSyntax)));
    }

    #[test]
    fn test_verificar_validez_consulta_tabla_inexistente() {
        let mut consulta_delete = ConsultaDelete {
            campos_posibles: HashMap::new(),
            tabla: vec!["tabla_inexistente".to_string()],
            ruta_tabla: "ruta/a/tablas".to_string(),
            condiciones: vec!["campo = valor".to_string()],
        };
        let resultado = consulta_delete.verificar_validez_consulta();
        assert!(matches!(resultado, Err(Errores::InvalidTable)));
    }

    #[test]
    fn test_verificar_campos_validos() {
        let campos_validos = HashMap::from([("campo1".to_string(), 0), ("campo2".to_string(), 1)]);
        let mut campos_consulta = vec!["campo1".to_string(), "campo2".to_string()];
        let resultado =
            ConsultaDelete::verificar_campos_validos(&campos_validos, &mut campos_consulta);
        assert!(resultado);
    }

    #[test]
    fn test_verificar_campos_invalidos() {
        let campos_validos = HashMap::from([("campo1".to_string(), 0), ("campo2".to_string(), 1)]);
        let mut campos_consulta = vec!["campo1".to_string(), "campo3".to_string()];
        let resultado =
            ConsultaDelete::verificar_campos_validos(&campos_validos, &mut campos_consulta);
        assert!(!resultado);
    }
}

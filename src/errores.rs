#[derive(Debug,PartialEq)]

/// Enumeración de posibles errores que pueden ocurrir durante la ejecución de las consultas SQL.
///
/// - `InvalidSyntax`: Error de sintaxis en la consulta.
/// - `InvalidTable`: La tabla especificada no es válida o no existe.
/// - `InvalidColumn`: La columna especificada no es válida.
/// - `Error`: Error genérico.
pub enum Errores {
    InvalidSyntax,
    InvalidTable,
    InvalidColumn,
    Error,
}

impl Errores {
    /// Imprime una descripción del error específico.
    ///
    /// Esta función proporciona un mensaje descriptivo para cada tipo de error.
    ///
    /// # Ejemplo
    /// ```
    /// Errores::InvalidSyntax.imprimir_desc();  // "[INVALID_SYNTAX] : [sintaxis invalida, por favor ingresa correctamente la consulta]"
    /// ```

    pub fn imprimir_desc(self) {
        match self {
            Errores::InvalidSyntax => {
                println!("[INVALID_SYNTAX] : [sintaxis invalida, por favor ingresa correctamente la consulta]")
            }
            Errores::InvalidTable => {
                println!("[INVALID_TABLE] : [tabla invalida o no existe]")
            }
            Errores::InvalidColumn => {
                println!("[INVALID_COLUMN] : [columna invalida, por favor ingrese un campo válido]")
            }
            Errores::Error => {
                println!("[ERROR] : [Error, se produjo un error al procesar la consulta]")
            }
        }
    }
}
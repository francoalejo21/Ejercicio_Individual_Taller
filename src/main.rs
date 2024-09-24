mod archivo;
mod consulta;
mod delete;
mod errores;
mod insert;
mod select;
mod update;

/// Función principal que se encarga de manejar la ejecución del programa.
///
/// Esta función llama a `ejecutar` y gestiona cualquier error que ocurra durante la ejecución,
/// imprimiendo la descripción del error cuando es necesario.

fn main() {
    match ejecutar() {
        Ok(_) => {}
        Err(error) => error.imprimir_desc(),
    };
}

/// Ejecuta la lógica principal del programa, gestionando la consulta SQL y procesando el archivo correspondiente.
///
/// Este método realiza las siguientes acciones:
/// 1. Obtiene los argumentos del programa.
/// 2. Verifica si la cantidad de argumentos es válida.
/// 3. Parsea la consulta SQL.
/// 4. Procesa la consulta y genera los resultados.
///
/// # Retorno
/// - `Ok(())`: Si todo se ejecuta correctamente.
/// - `Err(errores::Errores)`: Si ocurre algún error durante la ejecución.

fn ejecutar() -> Result<(), errores::Errores> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        return Err(errores::Errores::Error);
    }

    let ruta_tablas: &String = &args[1];
    let consulta_sin_parsear = &args[2];

    let mut consulta = consulta::SQLConsulta::crear_consulta(consulta_sin_parsear, ruta_tablas)
        .map_err(|_| errores::Errores::Error)?;

    consulta.procesar_consulta()?;
    Ok(())
}

use std::collections::HashMap;
const COMILLAS_SIMPLES: &str = "'";
const ESPACIO: &str = " ";
const COMA: &str = ",";
const MAYOR: &str = ">";
const MENOR: &str = "<";
const IGUAL: &str = "=";

pub fn parseo(condiciones: &Vec<String>, caracteres: &[char]) -> Vec<String> {
    // Vector para almacenar los tokens resultantes
    let mut tokens: Vec<String> = Vec::new();

    // Recorrer cada condición en el vector
    for condicion in condiciones {
        let mut token = String::new(); // Token temporal para acumular caracteres

        // Recorrer cada carácter en la condición
        for c in condicion.chars() {
            // Si encontramos un operador o paréntesis
            if caracteres.contains(&c) || c == '(' || c == ')' {
                // Añadimos el token acumulado (si no está vacío) antes del operador
                if !token.is_empty() {
                    tokens.push(token.clone());
                    token.clear(); // Limpiar el token acumulado
                }

                // Añadimos el operador o paréntesis como un token separado
                tokens.push(c.to_string());
            } else if c == ' ' {
                // Si encontramos un espacio, añadimos el token acumulado (si no está vacío)
                if !token.is_empty() {
                    tokens.push(token.clone());
                    token.clear(); // Limpiar el token acumulado
                }
            } else {
                // Si es un carácter de operando (campo o valor), lo acumulamos en el token
                token.push(c);
            }
        }

        // Añadir el último token acumulado al final (si no está vacío)
        if !token.is_empty() {
            tokens.push(token);
        }
    }

    tokens
}

/// Une los literales que fueron spliteados por espacios
/// Ejemplo: ["'Hola", "mundo'", "cómo", "estás?"] -> ["'Hola mundo'", "cómo", "estás?"]
///
/// # Argumentos
/// * `consulta_spliteada` - Vector de strings con la consulta spliteada por espacios
///
/// # Retorno
/// Vector de strings con los literales unidos

pub fn unir_literales_spliteados(consulta_spliteada: &Vec<String>) -> Vec<String> {
    let mut valores: Vec<String> = Vec::new();
    let mut literal: Vec<String> = Vec::new();
    let mut parado_en_literal = false;

    for campo in consulta_spliteada {
        if campo.starts_with(COMILLAS_SIMPLES)
            && campo.ends_with(COMILLAS_SIMPLES)
            && campo.len() > 1
        {
            // Literal completo, lo agregamos directamente
            valores.push(campo.to_string());
        } else if campo.starts_with(COMILLAS_SIMPLES) && !parado_en_literal {
            // Empieza un nuevo literal
            literal.push(campo.to_string());
            parado_en_literal = true;
        } else if campo.ends_with(COMILLAS_SIMPLES) && parado_en_literal {
            // Termina el literal actual
            literal.push(campo.to_string());
            valores.push(literal.join(ESPACIO)); // Une todo el literal
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
        valores.push(literal.join(ESPACIO));
    }

    valores
}

/// Remueve las comillas simples al inicio y al final de un valor
/// Ejemplo: "'Hola mundo'" -> "Hola mundo"
///
/// # Argumentos
/// * `valor` - Referencia a un string con el valor a remover las comillas
///
/// # Retorno
/// String con las comillas removidas

pub fn remover_comillas(valor: &String) -> String {
    let mut valor_parseado = valor.to_string();
    if valor_parseado.starts_with(COMILLAS_SIMPLES) && valor_parseado.ends_with(COMILLAS_SIMPLES) {
        valor_parseado = valor_parseado[1..valor_parseado.len() - 1].to_string();
    }
    valor_parseado
}

/// Elimina las comas de un vector de campos y retorna un nuevo vector sin ellas
/// Ejemplo: ["campo1", ",", "campo2"] -> ["campo1", "campo2"]
///
/// # Argumentos
/// * `campos` - Referencia a un vector de strings con los campos a limpiar
///
/// # Retorno
/// Vector de strings con las comas eliminadas

pub fn eliminar_comas(campos: &Vec<String>) -> Vec<String> {
    //iterar sobre el vector de campos y eliminar las comas
    let mut campos_limpio: Vec<String> = Vec::new();
    for campo in campos {
        if campo != COMA {
            campos_limpio.push(campo.to_string());
        }
    }
    campos_limpio
}

pub fn convertir_lower_case_restricciones(
    restricciones: &Vec<String>,
    campos_mapeados: &HashMap<String, usize>,
) -> Vec<String> {
    // Iteramos sobre las restricciones y si el campo es un campo de la tabla  o un operador and , or , not lo convertimos a minúsculas.
    let mut restricciones_lower: Vec<String> = Vec::new();
    for restriccion in restricciones {
        let restriccion_lower = restriccion.to_lowercase();
        if campos_mapeados.contains_key(&restriccion_lower)
            && !es_literal(restriccion)
            && !restriccion.chars().all(char::is_numeric)
            || ["and", "or", "not"].contains(&restriccion_lower.as_str())
        {
            restricciones_lower.push(restriccion_lower);
        } else {
            restricciones_lower.push(restriccion.to_string());
        }
    }
    restricciones_lower
}

fn es_literal(operando: &str) -> bool {
    operando.starts_with(COMILLAS_SIMPLES) && operando.ends_with(COMILLAS_SIMPLES)
}

pub fn unir_operadores_que_deben_ir_juntos(consulta_spliteada: &[String]) -> Vec<String> {
    //si se encuentran operadores > y = se unen en >= o si se encuentran operadores < y = se unen en <=
    let mut consulta_unida: Vec<String> = Vec::new();
    let mut i = 0;
    while i < consulta_spliteada.len() {
        let campo = consulta_spliteada[i].to_string();
        if i + 1 < consulta_spliteada.len() {
            let siguiente_campo = consulta_spliteada[i + 1].to_string();
            if (campo == MAYOR || campo == MENOR) && siguiente_campo == IGUAL {
                consulta_unida.push(format!("{}{}", campo, siguiente_campo));
                i += 1;
            } else {
                consulta_unida.push(campo);
            }
        } else {
            consulta_unida.push(campo);
        }
        i += 1;
    }
    consulta_unida
}
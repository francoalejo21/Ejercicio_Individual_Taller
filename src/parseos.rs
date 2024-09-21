const COMILLAS_SIMPLES: &str = "'";
const ESPACIO: &str = " ";

pub fn parseo(condiciones: &Vec<String>, caracteres: &[char]) -> Vec<String> {

    // Vector para almacenar los tokens resultantes
    let mut tokens: Vec<String> = Vec::new();

    // Recorrer cada condición en el vector
    for condicion in condiciones {
        let mut token = String::new();  // Token temporal para acumular caracteres

        // Recorrer cada carácter en la condición
        for c in condicion.chars() {
            // Si encontramos un operador o paréntesis
            if caracteres.contains(&c) || c == '(' || c == ')' {
                // Añadimos el token acumulado (si no está vacío) antes del operador
                if !token.is_empty() {
                    tokens.push(token.clone());
                    token.clear();  // Limpiar el token acumulado
                }

                // Añadimos el operador o paréntesis como un token separado
                tokens.push(c.to_string());
            } else if c == ' ' {
                // Si encontramos un espacio, añadimos el token acumulado (si no está vacío)
                if !token.is_empty() {
                    tokens.push(token.clone());
                    token.clear();  // Limpiar el token acumulado
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

pub fn unir_literales_spliteados(consulta_spliteada: &Vec<String>) -> Vec<String> {
    let mut valores: Vec<String> = Vec::new();
    let mut literal: Vec<String> = Vec::new();
    let mut parado_en_literal = false;

    for campo in consulta_spliteada {
        if campo.starts_with(COMILLAS_SIMPLES) && campo.ends_with(COMILLAS_SIMPLES) && campo.len() > 1 {
            // Literal completo, lo agregamos directamente
            valores.push(campo.to_string());
        } else if campo.starts_with(COMILLAS_SIMPLES) && !parado_en_literal {
            // Empieza un nuevo literal
            literal.push(campo.to_string());
            parado_en_literal = true;
        } else if campo.ends_with(COMILLAS_SIMPLES) && parado_en_literal {
            // Termina el literal actual
            literal.push(campo.to_string());
            valores.push(literal.join(ESPACIO));  // Une todo el literal
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

pub fn remover_comillas(valor :&String)->String{
    let mut valor_parseado = valor.to_string();
    if valor_parseado.starts_with(COMILLAS_SIMPLES) && valor_parseado.ends_with(COMILLAS_SIMPLES) {
        valor_parseado = valor_parseado[1..valor_parseado.len()-1].to_string();
    }
    valor_parseado
}
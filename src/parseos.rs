pub fn parseo(condiciones: &Vec<String>, caracteres: &Vec<char>) -> Vec<String> {
    // Operadores que no deben estar separados de los operandos
    //let operadores_que_no_tienen_que_estar_separados_operando: Vec<char> = vec!['=', '>', '<', '(', ')'];

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

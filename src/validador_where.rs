use std::collections::{HashMap, HashSet};

use crate::errores;

pub struct ValidadorSintaxis {
    tokens: Vec<String>,
    operadores_binarios: HashSet<String>,
    parentesis_abiertos: i32,
    operandos: Vec<String>,
}

impl ValidadorSintaxis {
    pub fn new(_tokens: &Vec<String>) -> Self {
        let operadores_binarios = vec!["and", "or", "=", ">", "<"]
            .into_iter()
            .map(String::from)
            .collect();
        let mut tokens = Vec::new();
        for token in _tokens {
            tokens.push(token.to_string());
        }
        ValidadorSintaxis {
            tokens,
            operadores_binarios,
            parentesis_abiertos: 0,
            operandos: Vec::new(),
        }
    }

    pub fn obtener_operandos(&self) -> Vec<String> {
        self.operandos.clone()
    }

    pub fn validar(&mut self) -> bool {
        if self.tokens.is_empty() {
            return true;
        }
        let mut ultimo_token: Option<&str> = None;
        for token in &self.tokens {
            match token.as_str() {
                "(" => {
                    self.parentesis_abiertos += 1;
                    if let Some(ultimo) = ultimo_token {
                        if !["and", "or", "not", "("].contains(&ultimo) {
                            return false;
                        }
                    }
                }
                ")" => {
                    self.parentesis_abiertos -= 1;
                    if self.parentesis_abiertos < 0
                        || matches!(ultimo_token, Some(ultimo) if self.operadores_binarios.contains(ultimo) || ultimo == "not" || ultimo == "(")
                    {
                        return false;
                    }
                }
                "and" | "or" | ">" | "<" | "=" => {
                    if match ultimo_token {
                        None => true,
                        Some(ultimo) => {
                            self.operadores_binarios.contains(ultimo)
                                || ultimo == "not"
                                || ultimo == "("
                        }
                    } {
                        return false;
                    }
                }
                "not" => {
                    if let Some(ultimo) = ultimo_token {
                        if !["(", "and", "or"].contains(&ultimo) {
                            return false;
                        }
                    }
                }
                _ => {
                    self.operandos.push(token.clone());
                    if let Some(ultimo) = ultimo_token {
                        if !["(", "and", "or", "not", ">", "<", "="].contains(&ultimo) {
                            return false;
                        }
                    }
                }
            }
            ultimo_token = Some(token.as_str());
        }
        self.parentesis_abiertos == 0
            && matches!(ultimo_token, Some(ultimo) if !self.operadores_binarios.contains(ultimo) && ultimo != "not" && ultimo != "(")
    }
}

pub struct ValidadorOperandosValidos {
    operandos: Vec<String>,
    campos_tabla: HashSet<String>,
}

impl ValidadorOperandosValidos {
    pub fn new(_operandos: &Vec<String>, _campos_tabla: &HashMap<String, usize>) -> Self {
        let mut operandos = Vec::new();
        for operando in _operandos {
            operandos.push(operando.to_string());
        }
        let mut campos_tabla: HashSet<String> = HashSet::new();
        for key in _campos_tabla.keys() {
            campos_tabla.insert(key.to_string());
        }

        ValidadorOperandosValidos {
            operandos,
            campos_tabla,
        }
    }

    pub fn validar(&self) -> Result<(), errores::Errores> {
        if self.operandos.is_empty() || self.operandos.len() < 2 {
            Err(errores::Errores::InvalidSyntax)?;
        }
        for operando in &self.operandos {
            if !self.campos_tabla.contains(&operando.to_lowercase())
                && !self.es_literal(operando)
                && !operando.chars().all(char::is_numeric)
            {
                Err(errores::Errores::InvalidColumn)?;
            }
        }
        Ok(())
    }

    fn es_literal(&self, operando: &str) -> bool {
        operando.starts_with("'") && operando.ends_with("'")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validador_expresiones_validas() {
        // Prueba válida
        let tokens_validos1 = vec![
            "not".to_string(),
            "(".to_string(),
            "True".to_string(),
            "and".to_string(),
            "False".to_string(),
            ")".to_string(),
            "or".to_string(),
            "False".to_string(),
        ];
        let mut validador_valid1 = ValidadorSintaxis::new(&tokens_validos1);
        assert!(validador_valid1.validar());

        // Prueba válida con operadores de comparación
        let tokens_validos2 = vec![
            "not".to_string(),
            "(".to_string(),
            "True".to_string(),
            "=".to_string(),
            "False".to_string(),
            "and".to_string(),
            "True".to_string(),
            ">".to_string(),
            "False".to_string(),
            ")".to_string(),
        ];
        let mut validador_valid2 = ValidadorSintaxis::new(&tokens_validos2);
        assert!(validador_valid2.validar());

        // Prueba válida con operadores lógicos y paréntesis anidados
        let tokens_validos3 = vec![
            "(".to_string(),
            "True".to_string(),
            "and".to_string(),
            "False".to_string(),
            ")".to_string(),
            "or".to_string(),
            "(".to_string(),
            "not".to_string(),
            "True".to_string(),
            "or".to_string(),
            "False".to_string(),
            ")".to_string(),
        ];
        let mut validador_valid3 = ValidadorSintaxis::new(&tokens_validos3);
        assert!(validador_valid3.validar());
    }

    #[test]
    fn test_validador_expresiones_invalidas() {
        // Prueba inválida (paréntesis mal balanceados)
        let tokens_invalidos1 = vec![
            "not".to_string(),
            "(".to_string(),
            "True".to_string(),
            "and".to_string(),
            "False".to_string(),
            "or".to_string(),
            "False".to_string(),
        ];
        let mut validador_invalid1 = ValidadorSintaxis::new(&tokens_invalidos1);
        assert!(!validador_invalid1.validar());

        // Prueba inválida (operadores consecutivos)
        let tokens_invalidos2 = vec![
            "True".to_string(),
            "and".to_string(),
            "or".to_string(),
            "False".to_string(),
        ];
        let mut validador_invalid2 = ValidadorSintaxis::new(&tokens_invalidos2);
        assert!(!validador_invalid2.validar());

        // Prueba inválida (operador al final)
        let tokens_invalidos3 = vec![
            "True".to_string(),
            "and".to_string(),
            "False".to_string(),
            "or".to_string(),
        ];
        let mut validador_invalid3 = ValidadorSintaxis::new(&tokens_invalidos3);
        assert!(!validador_invalid3.validar());

        // Prueba inválida (operandos seguidos)
        let tokens_invalidos4 = vec!["True".to_string(), "False".to_string()];
        let mut validador_invalid4 = ValidadorSintaxis::new(&tokens_invalidos4);
        assert!(!validador_invalid4.validar());

        // Prueba inválida (operador sin suficiente operando antes)
        let tokens_invalidos5 = vec!["=".to_string(), "True".to_string()];
        let mut validador_invalid5 = ValidadorSintaxis::new(&tokens_invalidos5);
        assert!(!validador_invalid5.validar());

        // Prueba inválida (operador sin suficiente operando después)
        let tokens_invalidos6 = vec!["True".to_string(), "=".to_string()];
        let mut validador_invalid6 = ValidadorSintaxis::new(&tokens_invalidos6);
        assert!(!validador_invalid6.validar());

        // Prueba inválida (paréntesis sin operandos)
        let tokens_invalidos7 = vec![
            "(".to_string(),
            "not".to_string(),
            ")".to_string(),
            "True".to_string(),
            "and".to_string(),
            "False".to_string(),
            "or".to_string(),
            "False".to_string(),
        ];
        let mut validador_invalid7 = ValidadorSintaxis::new(&tokens_invalidos7);
        assert!(!validador_invalid7.validar());

        // Prueba inválida (falta operando antes de "or")
        let tokens_invalidos8 = vec![
            "True".to_string(),
            "and".to_string(),
            "not".to_string(),
            "(".to_string(),
            "or".to_string(),
            "False".to_string(),
            ")".to_string(),
        ];
        let mut validador_invalid8 = ValidadorSintaxis::new(&tokens_invalidos8);
        assert!(!validador_invalid8.validar());
    }

    #[test]
    fn test_pruebas_adicionales_invalidas() {
        let casos_invalidos = vec![
            vec!["not", "(", "True", "and", "False"], // Caso 0, falta paréntesis de cierre
            vec!["True", "and", "False", ")"],        // Caso 1, paréntesis desbalanceados
            vec!["and", "True", "or", "False"],       // Caso 2, operador al inicio
            vec!["True", "and", "False", "or"],       // Caso 3, operador al final
            vec!["True", "and", "or", "False"],       // Caso 4, operadores consecutivos
            vec!["True", "and", "(", ")", "or", "False"], // Caso 5, paréntesis vacíos
            vec!["True", "and", "not", "or", "False"], // Caso 6, "not" seguido por operador
            vec!["not", "and", "True"],               // Caso 7, operadores consecutivos
            vec!["not"],                              // Caso 8, solo "not" sin operando
            vec!["True", ">", "=", "False"], // Caso 9, operadores comparativos consecutivos
            vec!["True", "and", "False", ">"], // Caso 10, operador sin operando después
            vec!["True", "and", "(", "True", "or", ")"], // Caso 11, falta operando antes de ")"
            vec!["(", "True", "and", "False"], // Caso 12, falta paréntesis de cierre
            vec!["=", "True"],               // Caso 13, operador sin operando antes
        ];

        for (i, tokens) in casos_invalidos.iter().enumerate() {
            let tokens: Vec<String> = tokens.iter().map(|&t| t.to_string()).collect();
            let mut validador = ValidadorSintaxis::new(&tokens);
            assert!(
                !validador.validar(),
                "Error en la prueba {}: {:?} debería ser inválida",
                i,
                tokens
            );
        }
    }

    #[test]
    fn test_pruebas_adicionales_validas() {
        let casos_validos = vec![
            vec!["True", "and", "(", "not", "False", "or", "True", ")"],
            vec![
                "not", "(", "True", "=", "False", "and", "True", ">", "False", ")",
            ],
            vec![
                "(", "True", "and", "False", ")", "or", "(", "not", "True", "or", "False", ")",
            ],
            vec![
                "not", "(", "True", "=", "False", ")", "and", "True", "or", "False",
            ],
            vec![
                "(", "(", "True", "or", "False", ")", "and", "not", "(", "False", "or", "True",
                ")", ")",
            ],
        ];

        for (i, tokens) in casos_validos.iter().enumerate() {
            let tokens: Vec<String> = tokens.iter().map(|&t| t.to_string()).collect();
            let mut validador = ValidadorSintaxis::new(&tokens);
            assert!(
                validador.validar(),
                "Error en la prueba {}: {:?} debería ser válida",
                i,
                tokens
            );
        }
    }
}

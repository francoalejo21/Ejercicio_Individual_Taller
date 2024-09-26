use std::collections::HashMap;
use std::vec::Vec;

const MAYOR_IGUAL: &str = ">=";
const MENOR_IGUAL: &str = "<=";
const IGUAL: &str = "=";
const MAYOR: &str = ">";
const MENOR: &str = "<";
const AND: &str = "and";
const NOT: &str = "not";
const OR: &str = "or";
const PARENTESIS_APERTURA: &str = "(";
const PARENTESIS_CIERRE: &str = ")";
const CARACTER_VACIO: &str = "";
const COMILLA_SIMPLE: &str = "'";

#[derive(Debug, PartialEq, PartialOrd)]
enum TiposDatos {
    Entero(i32),
    String(String),
}

#[derive(Debug, Clone)]
struct NodoArbolExpresiones {
    dato: Option<String>,
    izquierdo: Option<Box<NodoArbolExpresiones>>,
    derecho: Option<Box<NodoArbolExpresiones>>,
}

impl NodoArbolExpresiones {
    fn new() -> Self {
        NodoArbolExpresiones {
            dato: None,
            izquierdo: None,
            derecho: None,
        }
    }
}

/// Representa un arbol de expresiones binarias y unarias.
///
/// El struct `ArbolExpresiones` se utiliza para definir una estructura de árbol donde cada nodo
/// puede contener una expresión. El nodo raíz es opcional, lo que significa que el árbol puede estar vacío.
///
/// Campos:
///
/// * `raiz`: Un `Option` que contiene un `Box` que apunta al nodo raíz del árbol de expresiones.
///          Si es `None`, el árbol está vacío.
#[derive(Debug, Clone)]
pub struct ArbolExpresiones {
    raiz: Option<Box<NodoArbolExpresiones>>,
}

impl ArbolExpresiones {
    /// Crea un nuevo árbol de expresiones vacío.
    pub fn new() -> Self {
        ArbolExpresiones { raiz: None }
    }

    /// Verifica si el árbol de expresiones está vacío.
    pub fn arbol_vacio(&self) -> bool {
        self.raiz.is_none()
    }

    fn crear_sub_arbol(
        &self,
        operando2: Box<NodoArbolExpresiones>,
        operando1: Box<NodoArbolExpresiones>,
        operador: Box<NodoArbolExpresiones>,
    ) -> Box<NodoArbolExpresiones> {
        let mut operador = operador;
        operador.izquierdo = Some(operando1);
        operador.derecho = Some(operando2);
        operador
    }

    fn crear_sub_arbol_unario(
        &self,
        operando: Box<NodoArbolExpresiones>,
        operador: Box<NodoArbolExpresiones>,
    ) -> Box<NodoArbolExpresiones> {
        let mut operador = operador;
        operador.izquierdo = Some(operando);
        operador
    }

    fn prioridad(&self, caracter: &str) -> u8 {
        match caracter {
            IGUAL | MAYOR | MENOR | MAYOR_IGUAL | MENOR_IGUAL=> 4,
            NOT => 3,
            AND => 2,
            OR => 1,
            _ => 0,
        }
    }

    fn es_operador(&self, caracter: &str) -> bool {
        matches!(
            caracter,
            PARENTESIS_APERTURA | PARENTESIS_CIERRE | IGUAL | MAYOR | MENOR | MAYOR_IGUAL| MENOR_IGUAL | NOT | AND | OR
        )
    }

    /// Crea un árbol de expresiones a partir de una lista de palabras.
    /// donde en esa lista hay operadores y operandos a partir de los cuales se creará el árbol.
    pub fn crear_abe(&mut self, palabras: &Vec<String>) {
        let mut pila_operandos: Vec<Box<NodoArbolExpresiones>> = Vec::new();
        let mut pila_operadores: Vec<Box<NodoArbolExpresiones>> = Vec::new();

        for palabra in palabras {
            let mut token = Box::new(NodoArbolExpresiones::new());
            token.dato = Some(palabra.to_string());

            if !self.es_operador(palabra) {
                pila_operandos.push(token);
            } else if palabra == PARENTESIS_APERTURA {
                pila_operadores.push(token);
            } else if palabra == PARENTESIS_CIERRE {
                let mut tope = match pila_operadores.last() {
                    Some(tope) => tope,
                    None => break,
                };
                let mut dato = match &tope.dato {
                    Some(dato) => dato,
                    None => break,
                };
                while !pila_operadores.is_empty() && dato != PARENTESIS_APERTURA {
                    if dato == NOT {
                        let (operando, operador) =
                            match (pila_operandos.pop(), pila_operadores.pop()) {
                                (Some(operando), Some(operador)) => (operando, operador),
                                _ => break,
                            };
                        let nuevo_operando = self.crear_sub_arbol_unario(operando, operador);
                        pila_operandos.push(nuevo_operando);
                    } else {
                        let (operando2, operando1, operador) = match (
                            pila_operandos.pop(),
                            pila_operandos.pop(),
                            pila_operadores.pop(),
                        ) {
                            (Some(operando2), Some(operando1), Some(operador)) => {
                                (operando2, operando1, operador)
                            }
                            _ => break,
                        };
                        let nuevo_operando = self.crear_sub_arbol(operando2, operando1, operador);
                        pila_operandos.push(nuevo_operando);
                    }
                    tope = match pila_operadores.last() {
                        Some(tope) => tope,
                        _ => break,
                    };
                    dato = match &tope.dato {
                        Some(dato) => dato,
                        None => break,
                    };
                }
                pila_operadores.pop(); // Elimina el "("
            } else {
                if pila_operadores.is_empty() {
                    pila_operadores.push(token);
                    continue;
                }
                let mut tope = match pila_operadores.last() {
                    Some(tope) => tope,
                    None => break,
                };
                let mut dato = match &tope.dato {
                    Some(dato) => dato,
                    None => break,
                };
                while !pila_operadores.is_empty() && self.prioridad(palabra) <= self.prioridad(dato)
                {
                    if dato == NOT {
                        let (operando, operador) =
                            match (pila_operandos.pop(), pila_operadores.pop()) {
                                (Some(operando), Some(operador)) => (operando, operador),
                                _ => break,
                            };
                        let nuevo_operando = self.crear_sub_arbol_unario(operando, operador);
                        pila_operandos.push(nuevo_operando);
                    } else {
                        let (operando2, operando1, operador) = match (
                            pila_operandos.pop(),
                            pila_operandos.pop(),
                            pila_operadores.pop(),
                        ) {
                            (Some(operando2), Some(operando1), Some(operador)) => {
                                (operando2, operando1, operador)
                            }
                            _ => break,
                        };
                        let nuevo_operando = self.crear_sub_arbol(operando2, operando1, operador);
                        pila_operandos.push(nuevo_operando);
                    }
                    if pila_operadores.is_empty() {
                        break;
                    }
                    tope = match pila_operadores.last() {
                        Some(tope) => tope,
                        _ => break,
                    };
                    dato = match &tope.dato {
                        Some(dato) => dato,
                        None => break,
                    };
                }
                pila_operadores.push(token);
            }
        }

        while !pila_operadores.is_empty() {
            let tope = match pila_operadores.last() {
                Some(tope) => tope,
                None => break,
            };
            let dato = match &tope.dato {
                Some(dato) => dato,
                None => break,
            };
            if dato == NOT {
                let (operando, operador) = match (pila_operandos.pop(), pila_operadores.pop()) {
                    (Some(operando), Some(operador)) => (operando, operador),
                    _ => break,
                };
                let nuevo_operando = self.crear_sub_arbol_unario(operando, operador);
                pila_operandos.push(nuevo_operando);
            } else {
                let (operando2, operando1, operador) = match (
                    pila_operandos.pop(),
                    pila_operandos.pop(),
                    pila_operadores.pop(),
                ) {
                    (Some(operando2), Some(operando1), Some(operador)) => {
                        (operando2, operando1, operador)
                    }
                    _ => break,
                };
                let nuevo_operando = self.crear_sub_arbol(operando2, operando1, operador);
                pila_operandos.push(nuevo_operando);
            }
        }

        if let Some(raiz) = pila_operandos.pop() {
            if raiz.dato.is_some() {
                self.raiz = Some(raiz);
            }
        }
    }

    pub fn evalua(
        &self,
        campos_mapeados: &HashMap<String, usize>,
        campos_fila_actual: &[String],
    ) -> bool {
        if let Some(raiz) = &self.raiz {
            let (_, booleano) = self.evalua_expresion(raiz, campos_mapeados, campos_fila_actual);
            return booleano;
        }
        false
    }

    fn evalua_expresion(
        &self,
        sub_arbol: &NodoArbolExpresiones,
        campos_mapeados: &HashMap<String, usize>,
        campos_fila_actual: &[String],
    ) -> (TiposDatos, bool) {
        let mut caracter = match &sub_arbol.dato {
            Some(dato) => dato.to_string(),
            None => return (TiposDatos::String(CARACTER_VACIO.to_string()), false), // No hay nodo
        };

        if !self.es_operador(&caracter) {
            // Ver si podemos parsear a int o string
            if es_cadena_literal(&caracter) {
                remover_comillas_simples(&mut caracter);
                return (TiposDatos::String(caracter.to_string()), false); // Aquí devolveríamos la cadena sin las comillas simples
            }
            if let Ok(numero) = caracter.parse::<i32>() {
                return (TiposDatos::Entero(numero), false);
            }
            // Buscar en los campos mapeados
            if let Some(&indice) = campos_mapeados.get(&caracter) {
                let valor = &campos_fila_actual[indice];
                if let Ok(numero) = valor.parse::<i32>() {
                    return (TiposDatos::Entero(numero), false);
                }
                return (TiposDatos::String(valor.to_string()), false);
            }
        } else {
            return self.evalua_operador(&caracter, sub_arbol, campos_mapeados, campos_fila_actual);
        }

        (TiposDatos::String("".to_string()), false)
    }

    fn evalua_operador(
        &self,
        operador: &str,
        sub_arbol: &NodoArbolExpresiones,
        campos_mapeados: &HashMap<String, usize>,
        campos_fila_actual: &[String],
    ) -> (TiposDatos, bool) {
        let (dato_izq, booleano_izq) = match sub_arbol.izquierdo.as_ref() {
            Some(izquierdo) => {
                self.evalua_expresion(izquierdo, campos_mapeados, campos_fila_actual)
            }
            None => return (TiposDatos::String(CARACTER_VACIO.to_string()), true), // Manejo del caso None
        };

        let (dato_der, booleano_der) = match sub_arbol.derecho.as_ref() {
            Some(derecho) => {
                self.evalua_expresion(derecho, campos_mapeados, campos_fila_actual)
            },
            None => {
                if operador != NOT {
                    return (TiposDatos::String(CARACTER_VACIO.to_string()), true) // Manejo del caso None
                }
                (TiposDatos::String(CARACTER_VACIO.to_string()), true)
            }
        };

        match operador {
            NOT => (dato_izq, !booleano_izq),
            MAYOR_IGUAL => (
                TiposDatos::String(CARACTER_VACIO.to_string()),
                dato_izq >= dato_der,
            ),
            MENOR_IGUAL => (
                TiposDatos::String(CARACTER_VACIO.to_string()),
                dato_izq <= dato_der,
            ),
            IGUAL => (
                TiposDatos::String(CARACTER_VACIO.to_string()),
                dato_izq == dato_der,
            ),
            MAYOR => (
                TiposDatos::String(CARACTER_VACIO.to_string()),
                dato_izq > dato_der,
            ),
            MENOR => (
                TiposDatos::String(CARACTER_VACIO.to_string()),
                dato_izq < dato_der,
            ),
            AND => (
                TiposDatos::String(CARACTER_VACIO.to_string()),
                booleano_izq && booleano_der,
            ),
            OR => (
                TiposDatos::String(CARACTER_VACIO.to_string()),
                booleano_izq || booleano_der,
            ),
            _ => (TiposDatos::String(CARACTER_VACIO.to_string()), false), // Operador no reconocido
        }
    }
}
fn es_cadena_literal(operando: &str) -> bool {
    operando.starts_with(COMILLA_SIMPLE) && operando.ends_with(COMILLA_SIMPLE)
}

fn remover_comillas_simples(cadena: &mut String) {
    cadena.remove(0);
    cadena.pop();
}
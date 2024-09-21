use std::vec::Vec;
use std::collections::HashMap;
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
#[derive(Debug, Clone)]
pub struct ArbolExpresiones {
    raiz: Option<Box<NodoArbolExpresiones>>,
}

impl ArbolExpresiones {
    pub fn new() -> Self {
        ArbolExpresiones { raiz: None }
    }

    pub fn arbol_vacio(&self) -> bool {
        self.raiz.is_none()
    }
    /*
    fn reinicializar(&mut self) {
        self.raiz = None;
    }*/

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
            "=" | ">" | "<" => 4,
            "not" => 3,
            "and" => 2,
            "or" => 1,
            _ => 0,
        }
    }

    fn es_operador(&self, caracter: &str) -> bool {
        matches!(caracter, "(" | ")" | "=" | ">" | "<" | "not" | "and" | "or")
    }

    pub fn crear_abe(&mut self, palabras: &Vec<String>) {
        let mut pila_operandos: Vec<Box<NodoArbolExpresiones>> = Vec::new();
        let mut pila_operadores: Vec<Box<NodoArbolExpresiones>> = Vec::new();

        for palabra in palabras {
            let mut token = Box::new(NodoArbolExpresiones::new());
            token.dato = Some(palabra.to_string());

            if !self.es_operador(palabra) {
                pila_operandos.push(token);
            } else if palabra == "(" { 
                pila_operadores.push(token);
                } else if palabra == ")" {
                    let mut tope = match pila_operadores.last() {
                        Some(tope) => tope,
                        None => break,
                    };
                    let mut dato = match &tope.dato {
                        Some(dato) => dato,
                        None => break,                        
                    };
                    while !pila_operadores.is_empty() && dato != "(" {
                        if dato == "not" {
                            let (operando, operador) = match (pila_operandos.pop(), pila_operadores.pop()) {
                                (Some(operando), Some(operador)) => (operando, operador),
                                _ => break,
                            };
                            let nuevo_operando = self.crear_sub_arbol_unario(operando, operador);
                            pila_operandos.push(nuevo_operando);
                        } else {
                            let (operando2, operando1, operador) = match (pila_operandos.pop(), pila_operandos.pop(), pila_operadores.pop()) {
                                (Some(operando2), Some(operando1), Some(operador)) => (operando2, operando1, operador),
                                _ => break,
                            };
                            let nuevo_operando =
                                self.crear_sub_arbol(operando2, operando1, operador);
                            pila_operandos.push(nuevo_operando);
                        }
                        tope = match pila_operadores.last(){
                            Some(tope) => tope,
                            _ => break,
                        };
                        dato = match &tope.dato{
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
                    let mut tope = match pila_operadores.last(){
                        Some(tope) => tope,
                        None => break,
                    };
                    let mut dato = match &tope.dato{
                        Some(dato) => dato,
                        None => break,
                    };
                    while !pila_operadores.is_empty() && self.prioridad(palabra) <= self.prioridad(dato){
                        if dato == "not" {
                            let (operando, operador) = match(pila_operandos.pop(),pila_operadores.pop()){
                                (Some(operando), Some(operador)) => (operando, operador),
                                _ => break,
                            };
                            let nuevo_operando = self.crear_sub_arbol_unario(operando, operador);
                            pila_operandos.push(nuevo_operando);
                        } else {
                            let (operando2, operando1, operador) = match (pila_operandos.pop(), pila_operandos.pop(), pila_operadores.pop()) {
                                (Some(operando2), Some(operando1), Some(operador)) => (operando2, operando1, operador),
                                _ => break,
                            };
                            let nuevo_operando =
                                self.crear_sub_arbol(operando2, operando1, operador);
                            pila_operandos.push(nuevo_operando);
                        }
                        if pila_operadores.is_empty() {
                            break;
                        }
                        tope = match pila_operadores.last(){
                            Some(tope) => tope,
                            _ => break,
                        };
                        dato = match &tope.dato{
                            Some(dato) => dato,
                            None => break,
                        }; 
                    }
                    pila_operadores.push(token);
                }
        }

        while !pila_operadores.is_empty() {
            let tope = match pila_operadores.last(){
                Some(tope) => tope,
                None => break,
            };
            let dato = match &tope.dato{
                Some(dato) => dato,
                None => break,
            };
            if dato == "not" {
                let (operando, operador) = match(pila_operandos.pop(),pila_operadores.pop()){
                    (Some(operando), Some(operador)) => (operando, operador),
                    _ => break,
                };
                let nuevo_operando = self.crear_sub_arbol_unario(operando, operador);
                pila_operandos.push(nuevo_operando);
            } else {
                let (operando2, operando1, operador) = match (pila_operandos.pop(), pila_operandos.pop(), pila_operadores.pop()) {
                    (Some(operando2), Some(operando1), Some(operador)) => (operando2, operando1, operador),
                    _ => break,
                };
                let nuevo_operando =
                    self.crear_sub_arbol(operando2, operando1, operador);
                pila_operandos.push(nuevo_operando);
            }
        }

        if let Some(raiz) = pila_operandos.pop() {
            if raiz.dato.is_some() {
                self.raiz = Some(raiz);
            }
        }
    }

    pub fn evalua(&self, campos_mapeados: &HashMap<String, usize>, campos_fila_actual: &[String]) -> bool {
        if let Some(raiz) = &self.raiz {
            let (_, booleano) = self.evalua_expresion(raiz, campos_mapeados, campos_fila_actual);
            return booleano;
        }
        false
    }

    fn evalua_expresion(&self, sub_arbol: &NodoArbolExpresiones, campos_mapeados: &HashMap<String, usize>, campos_fila_actual: &[String]) -> (TiposDatos,bool) {
        let mut caracter = match &sub_arbol.dato {
            Some(dato) => dato.clone(),
            None => return (TiposDatos::String("".to_string()), false)//No hay nodo
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
        } else if caracter == "not" {
            let (dato_izq, booleano_izq) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            return (dato_izq, !booleano_izq);
        } else if caracter == "=" {
            let (dato_izq, _) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            let (dato_der , _) = self.evalua_expresion(sub_arbol.derecho.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            if dato_izq == dato_der{
                return (TiposDatos::String("".to_string()), true);
            }
            else{
                return (TiposDatos::String("".to_string()), false);
            }
        } else if caracter == ">" {
            let (dato_izq, _) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            let (dato_der , _) = self.evalua_expresion(sub_arbol.derecho.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            if dato_izq > dato_der{
                return (TiposDatos::String("".to_string()), true);
            }
            else{
                return (TiposDatos::String("".to_string()), false);
            }
        } else if caracter == "<" {
            let (dato_izq, _) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            let (dato_der , _) = self.evalua_expresion(sub_arbol.derecho.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            if dato_izq < dato_der{
                return (TiposDatos::String("".to_string()), true);
            }
            else{
                return (TiposDatos::String("".to_string()), false);
            }
        } else if caracter == "and" {
            let (_, booleano_izq) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            let (_ , booleano_der) = self.evalua_expresion(sub_arbol.derecho.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            if booleano_izq && booleano_der{
                return (TiposDatos::String("".to_string()), true);
            }
            else{
                return (TiposDatos::String("".to_string()), false);
            }
        } else if caracter == "or" {
            let (_, booleano_izq) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            let (_ , booleano_der) = self.evalua_expresion(sub_arbol.derecho.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            if booleano_izq || booleano_der{
                return (TiposDatos::String("".to_string()), true);
            }
            else{
                return (TiposDatos::String("".to_string()), false);
            }
        }

        (TiposDatos::String("".to_string()), false)
    }

}
fn es_cadena_literal(operando: &str) -> bool {
    operando.starts_with("'") && operando.ends_with("'")  // Aquí puedes definir tu lógica de cadena literal
}

fn remover_comillas_simples(cadena : &mut String){
    cadena.remove(0);
    cadena.pop();
}
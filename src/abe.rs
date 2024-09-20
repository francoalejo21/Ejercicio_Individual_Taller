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

            if !self.es_operador(&palabra) {
                pila_operandos.push(token);
            } else {
                if palabra == "(" {
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
                    while !pila_operadores.is_empty() && self.prioridad(&palabra) <= self.prioridad(dato){
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

        match pila_operandos.pop() {
            Some(raiz) => match raiz.dato {
                Some(_) => self.raiz = Some(raiz),
                None => {}
            },
            None => {}
        }
    }

    pub fn evalua(&self, campos_mapeados: &HashMap<String, usize>, campos_fila_actual: &Vec<String>) -> bool {
        if let Some(raiz) = &self.raiz {
            let (_, booleano) = self.evalua_expresion(raiz, campos_mapeados, campos_fila_actual);
            return booleano;
        }
        false
    }

    fn evalua_expresion(&self, sub_arbol: &Box<NodoArbolExpresiones>, campos_mapeados: &HashMap<String, usize>, campos_fila_actual: &[String]) -> (TiposDatos,bool) {
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
            match caracter.parse::<i32>(){
                Ok(numero) => return (TiposDatos::Entero(numero), false),
                Err(_) => {},
            };
            // Buscar en los campos mapeados
            if let Some(&indice) = campos_mapeados.get(&caracter) {
                let valor = &campos_fila_actual[indice];
                match valor.parse::<i32>(){
                    Ok(numero) => return (TiposDatos::Entero(numero), false),
                    Err(_) => {},
                };  
                return (TiposDatos::String(valor.to_string()), false);
            }
        } else if caracter == "not" {
            let (dato_izq, booleano_izq) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            return (dato_izq, !booleano_izq);
        } else if caracter == "=" {
            let (dato_izq, booleano_izq) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            let (dato_der , booleano_der) = self.evalua_expresion(sub_arbol.derecho.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            if dato_izq == dato_der{
                return (TiposDatos::String("".to_string()), true);
            }
            else{
                return (TiposDatos::String("".to_string()), false);
            }
        } else if caracter == ">" {
            let (dato_izq, booleano_izq) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            let (dato_der , booleano_der) = self.evalua_expresion(sub_arbol.derecho.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            if dato_izq > dato_der{
                return (TiposDatos::String("".to_string()), true);
            }
            else{
                return (TiposDatos::String("".to_string()), false);
            }
        } else if caracter == "<" {
            let (dato_izq, booleano_izq) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            let (dato_der , booleano_der) = self.evalua_expresion(sub_arbol.derecho.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            if dato_izq < dato_der{
                return (TiposDatos::String("".to_string()), true);
            }
            else{
                return (TiposDatos::String("".to_string()), false);
            }
        } else if caracter == "and" {
            let (dato_izq, booleano_izq) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            let (dato_der , booleano_der) = self.evalua_expresion(sub_arbol.derecho.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            if booleano_izq && booleano_der{
                return (TiposDatos::String("".to_string()), true);
            }
            else{
                return (TiposDatos::String("".to_string()), false);
            }
        } else if caracter == "or" {
            let (dato_izq, booleano_izq) = self.evalua_expresion(sub_arbol.izquierdo.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            let (dato_der , booleano_der) = self.evalua_expresion(sub_arbol.derecho.as_ref().unwrap(), campos_mapeados, campos_fila_actual);
            if booleano_izq || booleano_der{
                return (TiposDatos::String("".to_string()), true);
            }
            else{
                return (TiposDatos::String("".to_string()), false);
            }
        }

        return (TiposDatos::String("".to_string()), false)
    }

}
fn es_cadena_literal(operando: &str) -> bool {
    operando.starts_with("'") && operando.ends_with("'")  // Aquí puedes definir tu lógica de cadena literal
}

fn main() {
    let simulacion_tabla_csv_parseada: Vec<Vec<String>> = vec![
        vec!["1".to_string(), "Juan".to_string(), "Juan".to_string(), "juan.perez@email.com".to_string()],
        vec!["2".to_string(), "Ana".to_string(), "López".to_string(), "ana.lopez@email.com".to_string()],
        vec!["3".to_string(), "Carlos".to_string(), "Gómez".to_string(), "carlos.gomez@email.com".to_string()],
        vec!["4".to_string(), "María".to_string(), "Rodríguez".to_string(), "maria.rodriguez@email.com".to_string()],
        vec!["5".to_string(), "José".to_string(), "López".to_string(), "jose.lopez@email.com".to_string()],
        vec!["6".to_string(), "Laura".to_string(), "Fernández".to_string(), "laura.fernandez@email.com".to_string()],
    ];

    let campos_consulta: Vec<String> = vec!["nombre".to_string(), "apellido".to_string()];
    /* let restriccion: Vec<String> = vec![
        "(".to_string(), "id".to_string(), ">".to_string(), "2".to_string(), "and".to_string(), "id".to_string(), "<".to_string(), "7".to_string(), ")".to_string(), "or".to_string(), "(".to_string(), "nombre".to_string(), ">".to_string(), "'A'".to_string(), "and".to_string(), "not".to_string(), "apellido".to_string(), "<".to_string(), "'Z'".to_string(), ")".to_string()
    ]; */

    let restriccion: Vec<String> = vec![
        "(".to_string(), "nombre".to_string(), "=".to_string(), "apellido".to_string(), ")".to_string()
    ];
    //let validador = ValidadorSintaxis::new(&restriccion);
    //assert(validador.validar());  // True, expresión valida
    //assert(validador.obtener_operandos() == ["id", "5", "id", "7", "nombre", "A|", "apellido", "Z|"]);  // True, operandos extraidos correctamente
    //let validadorOperandos = ValidadorOperandosValidos(validador.obtener_operandos(),campos_consulta);
    //assert(validadorOperandos.validar());  // True, operandos validos

    let campos_mapeados = HashMap::from([("id".to_string(), 0), ("nombre".to_string(), 1), ("apellido".to_string(), 2), ("email".to_string(), 3)]);
    for fila in simulacion_tabla_csv_parseada {
        //println!("{:?}", fila);
        let mut arbol_expresiones = ArbolExpresiones::new();
        arbol_expresiones.crear_abe(&restriccion);
        //println!("{:?}",arbol_expresiones.raiz );
        let condicion_restriccion = arbol_expresiones.evalua(&campos_mapeados, &fila);
        //println!("{}", condicion_restriccion);
        if condicion_restriccion { //si se cumple la condicion where
            let mut linea:Vec<String> = Vec::new();
            for campo in &campos_consulta {
                let campo_seleccionado = match campos_mapeados.get(campo){
                    Some(indice) => indice,
                    None => &0,
                };
                let indice = match fila.get(*campo_seleccionado){
                    Some(valor) => valor,
                    None => &"".to_string(),
                };
                linea.push(indice.to_string());
            }
            let linea = linea.join(",");
            println!("{}", linea);
        }
    }
}

fn remover_comillas_simples(cadena : &mut String){
    cadena.remove(0);
    cadena.pop();
}
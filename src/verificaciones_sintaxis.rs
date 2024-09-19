use std::collections::HashSet;

use crate::errores;

pub fn verificar_orden_keywords(query: &Vec<String>, palabras_clave_consulta : Vec<&str>) -> Result<HashSet<String>,errores::Errores> {
    // Define las palabras clave esperadas en el orden correcto
    //let keywords_order = vec!["select", "from", "where", "order", "by"];
    
    let mut keyword_positions = vec![];
    let mut found_keywords = std::collections::HashSet::new();
    
    // Verificar que cada palabra clave está en el lugar correcto y es única
    for keyword in &palabras_clave_consulta {
        // Buscar la posición de la palabra clave
        if let Some(pos) = query.iter().position(|t| t.to_lowercase() == *keyword) {
            // Verificar si la palabra clave ya fue encontrada (unicidad)
            if !found_keywords.insert(keyword.to_lowercase()) {
                println!("Error de sintaxis: La palabra clave '{}' está duplicada.", keyword);
                Err(errores::Errores::InvalidSyntax)?;
            }
            keyword_positions.push((keyword.to_lowercase(), pos));
        } else if keyword.to_lowercase() != "where" && keyword.to_lowercase() != "order" && keyword.to_lowercase() != "by" { //SELECT Y FROM SIEMPRE DEBEN ESTAR
            // WHERE y ORDER BY son opcionales
            println!("Error de sintaxis: La palabra clave '{}'no está presente en la consulta.", keyword.to_lowercase());
                Err(errores::Errores::InvalidSyntax)?;
        }
    }

    // Verificar que las palabras clave están en el orden correcto
    for i in 1..keyword_positions.len() {
        if keyword_positions[i].1 < keyword_positions[i - 1].1 {
            println!("Error de sintaxis: algunas palabras clave están en el orden incorrecto: '{}' y '{}'.",keyword_positions[i - 1].0,keyword_positions[i].0);
            Err(errores::Errores::InvalidSyntax)?;            
        }
    }
    Ok(found_keywords)
}
/* 
fn main() {
    let query = "SELECT campo1, campo2 FROM tabla WHERE campo1 = 1 ORDER BY campo2";
    
    match verificar_orden_keywords(query) {
        Ok(_) => println!("Consulta SQL válida."),
        Err(e) => println!("{}", e),
    }
}
}

fn main() {
    let query = "SELECT campo1, campo2 FROM tabla WHERE campo1 = 1 ORDER BY campo2";
    match verificar_orden_keywords(query) {
        Ok(_) => println!("Consulta SQL válida."),
        Err(e) => println!("{}", e),
    }
}*/

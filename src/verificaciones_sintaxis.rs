use std::collections::HashSet;

use crate::errores;

pub fn verificar_orden_keywords(query: &[String], palabras_clave_consulta : Vec<&str>) -> Result<HashSet<String>,errores::Errores> {
    let mut keyword_positions = vec![];
    let mut found_keywords = std::collections::HashSet::new();
    
    // Verificar que cada palabra clave está en el lugar correcto y es única
    for keyword in &palabras_clave_consulta {
        // Buscar la posición de la palabra clave
        if let Some(pos) = query.iter().position(|t| t.to_lowercase() == *keyword) {
            // Verificar si la palabra clave ya fue encontrada (unicidad)
            if !found_keywords.insert(keyword.to_lowercase()) {
                Err(errores::Errores::InvalidSyntax)?;
            }
            keyword_positions.push((keyword.to_lowercase(), pos));
        } else if keyword.to_lowercase() != "where" && keyword.to_lowercase() != "order" && keyword.to_lowercase() != "by" { //SELECT Y FROM SIEMPRE DEBEN ESTAR
            // WHERE y ORDER BY son opcionales
            Err(errores::Errores::InvalidSyntax)?;
        }
    }

    // Verificar que las palabras clave están en el orden correcto
    for i in 1..keyword_positions.len() {
        if keyword_positions[i].1 < keyword_positions[i - 1].1 {
            Err(errores::Errores::InvalidSyntax)?;            
        }
    }
    Ok(found_keywords)
}
pub struct ordenamiento{
    
}


pub fn ordenar_consultas_multiples(
    filas: &mut Vec<Vec<String>>, 
    columnas_orden: Vec<(usize, bool)>
) {
    filas.sort_by(|a, b| {
        for (columna_orden, ascendente) in &columnas_orden {
            let valor_a = &a[*columna_orden];
            let valor_b = &b[*columna_orden];

            // Comparación adicional si alguna columna es vacía
            let cmp = match (valor_a.is_empty(), valor_b.is_empty()) {
                (true, false) => std::cmp::Ordering::Less,    // La columna vacía es menor
                (false, true) => std::cmp::Ordering::Greater, // La columna vacía es mayor
                (true, true) => std::cmp::Ordering::Equal,    // Ambas son vacías, son iguales
                _ => valor_a.cmp(valor_b),                    // Comparar normalmente si no están vacías
            };

            if cmp != std::cmp::Ordering::Equal {
                return if *ascendente {
                    cmp
                } else {
                    cmp.reverse()
                };
            }
        }
        std::cmp::Ordering::Equal
    });
}
/* 
fn main() {
    let mut filas = vec![
        vec!["2".to_string(), "Juan".to_string(), "30".to_string()],
        vec!["1".to_string(), "".to_string(), "25".to_string()],
        vec!["".to_string(), "Carlos".to_string(), "".to_string()],
        vec!["2".to_string(), "Ana".to_string(), "".to_string()],
    ];

    let columnas_orden = vec![
        (0, true),  // Ordenar por la primera columna ascendente
        (1, false), // Luego por la segunda columna descendente
        (2, true),  // Finalmente por la tercera columna ascendente
    ];

    ordenar_consultas_multiples(&mut filas, columnas_orden);

    for fila in filas {
        println!("{:?}", fila);
    }
}*/
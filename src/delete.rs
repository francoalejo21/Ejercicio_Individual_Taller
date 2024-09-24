/*use std::{collections::HashMap, fs::File, io::{BufWriter, Write}};
use std::path::Path;
use std::fs::OpenOptions;
use crate::errores;
use crate::consulta::{Parseables,MetodosConsulta,verificar_campos_validos};
use std::io::BufReader;


pub struct ConsultaDelete {
    pub tabla: String,
    pub ruta_tabla:String,
    pub restricciones : Vec<String>,
    pub campos_posibles : Vec<String>,
}


impl ConsultaDelete {
    pub fn crear(consulta: String, ruta : String) -> ConsultaDelete {
        // Aquí implementarías la lógica para parsear una consulta Insert
        let consulta_parseada = &Self::parsear_consulta_de_comando(&consulta);
        let mut index = 2; //nos salteamos las palabras insert into
        let tabla = Self::parsear_tabla(consulta_parseada, &mut index);
        let restricciones = Self::parsear_restricciones(consulta_parseada, &mut index);
        let campos_posibles: HashMap<String,usize> = HashMap::new() ;
        let ruta_tabla = ruta;

        ConsultaDelete {
            tabla,
            ruta_tabla,
            restricciones,
            campos_posibles,
        }
    }

    fn parsear_consulta_de_comando(consulta: &String) -> Vec<String> {
        return consulta.replace(",", "").to_lowercase().split_whitespace().map(|s| s.to_string()).collect(); //elimino las comas y los espacios
    }
}

impl Parseables for ConsultaDelete {

    fn parsear_campos(consulta: &Vec<String>, index: &mut usize) -> Vec<String> {
        let mut campos: Vec<String> = Vec::new();
        if consulta[*index] == "("  {
            *index+=1;
        }

        while *index < consulta.len() && consulta[*index] != ")" {
            let campo = &consulta[*index];
            campos.push(campo.to_string());
            *index += 1;
        }
        campos
    }

    fn parsear_tabla(consulta: &Vec<String>, index: &mut usize) -> String {
        let mut tabla = String::new();

        if *index < consulta.len() {
            let tabla_consulta = &consulta[*index];
            tabla = tabla_consulta.to_string();
            *index += 1;
        }
        tabla
    }

    fn parsear_valores(_consulta: &Vec<String>, _index: &mut usize)-> Vec<Vec<String>> {
        let mut lista_valores: Vec<Vec<String>> = Vec::new();
        if _consulta[*_index] == "values"  {
            *_index+=1;
        }

        while *_index < _consulta.len(){
            if _consulta[*_index] == "("{
                *_index+=1;
            }
            let mut valores = Vec::new();
            while *_index < _consulta.len() && _consulta[*_index] != ")"{
                let valor = &_consulta[*_index];

                valores.push(valor.to_string());
                *_index += 1;
            }
            lista_valores.push(valores);
            *_index += 1;
        }
        lista_valores
    }
}

impl MetodosConsulta for ConsultaDelete {

    fn verificar_sintaxis(&self) -> Result<(), errores::Errores> {
        let campos_posibles = &self.campos_posibles;
        if !verificar_campos_validos(campos_posibles, &self.campos_consulta){
            return Err(errores::Errores::InvalidColumn);
        }
        Ok(())
    }

    fn procesar(&self, lector: &mut BufReader<File>) -> Result<(), errores::Errores> {
        // Abrir el archivo original en modo append (agregar al final)
        let ruta_archivo = Path::new(&self.ruta_tabla);
        let archivo_original = match OpenOptions::new().append(true).open(ruta_archivo) {
            Ok(file) => file,
            Err(_) => return Err(errores::Errores::Error),
        };
        let mut escritor = BufWriter::new(archivo_original);

        // Agregar valores al final del archivo
        for valores_fila in &self.valores {
            let linea = valores_fila.join(",");
            if let Err(e) = writeln!(escritor, "{}", linea) {
                return Err(errores::Errores::Error);
            }
        }

        // Asegurarse de escribir en el archivo
        escritor.flush().unwrap();

        Ok(())
    }

    fn ver_tabla_consulta(&self)->String{
        let tabla_consulta = &self.tabla;
        return tabla_consulta.to_string()
    }

    fn agregar_campos_validos(&mut self, campos: HashMap<String,usize>){
        self.campos_posibles = campos
    }

}*/



use std::error::Error;
use std::str;
use std::fs::File;
use std::io::Read;

use std::time::{Duration, Instant};
use num_traits::cast::*;

use titlecase::titlecase;

use serde::{Serialize, Serializer};

// Equivalente à série "Estabelecimentos";
// Mas com alguns campos adicionais;
#[derive(Debug, Serialize, Clone)]
struct CNPJ {
    cnpj: String,
    cnpj_n: i32,
    nome_fantasia: String,
    razao_social: Option<String>,
    email: String,
    cnaes: String,
    data_abertura: String,
    municipio: String,
    telefone1: String,
    telefone2: String,
    socio: Option<String>
}

struct Socio {
    cnpj_n: i32,
    nome: String
}

struct Empresa {
    cnpj_n: i32,
    nome: String
}


macro_rules! read_list {
    ($name:ident, $fname:expr, $fn:expr) => {
        let mut $name = vec![];
        for file in read_series($fname).unwrap() {
            let mut reader = csv::ReaderBuilder::new().delimiter(b';').from_path(file).unwrap();

            for record in reader.byte_records() {
                match $fn(record) {
                    Ok(res) => $name.push(res),
                    Err(_) => {}
                }
            }
        }
    }
}

macro_rules! check_aux_list {
    ($result: ident, $idx:ident, $vector:ident, $field:ident, $miss:ident) => {
        for s in $idx..$vector.len() {
            if $result.cnpj_n == $vector[s].cnpj_n {
                $idx = s;
                $result.$field = Some($vector[s].nome.clone());
                break;
            }

            if $result.cnpj_n < $vector[s].cnpj_n {
                $miss += 1;
                break;
            }
        }
    }
}




// type CNAEs = Vec<String>;

// impl Serialize for CNAEs {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//         where S: Serializer,
//     {

//     }
// }
fn read_field(record: &csv::ByteRecord, i: usize) -> Result<String, Box<dyn Error>> {
    Ok(str::from_utf8(&record[i])?.to_string())
}

fn read_telefone(record: &csv::ByteRecord, i: usize) -> Result<String, Box<dyn Error>> {
    let a = read_field(record, i)?;
    let b = read_field(record, i + 1)?;

    if b == "" {
        Ok(b)
    } else {
        Ok(format!("({}) {}", a ,b))
    }
}

fn read_cnaes(record: &csv::ByteRecord) -> Result<String, Box<dyn Error>> {

    let primary = read_field(record, 11)?;

    let mut secondary = read_field(record, 12)?.split(",").map(str::to_string).collect();

    let mut cnaes: Vec<String> = vec![primary];

    cnaes.append(&mut secondary);

    Ok(cnaes.join(","))
}
fn read_record(record: &csv::ByteRecord) -> Result<CNPJ, Box<dyn Error>> {
  Ok(
      CNPJ {
          cnpj: format!("{}{}{}", read_field(&record, 0)?, read_field(&record, 1)?, read_field(&record, 2)?),
          cnpj_n: read_field(&record, 0)?.parse::<i32>().unwrap(),
          nome_fantasia: titlecase(&read_field(&record, 4)?),
          email: read_field(&record, 27)?.to_lowercase(),
          cnaes: read_cnaes(&record)?,
          data_abertura: read_field(&record, 10)?,
          municipio: read_field(&record, 20)?,
          telefone1: read_telefone(&record, 21)?,
          telefone2: read_telefone(&record, 23)?,
          razao_social: None,
          socio: None,
      })
}

fn read_from_file(path: &str, cnaes: Vec<String>) -> Result<Vec<CNPJ>, Box<dyn Error>> {
    // Creates a new csv `Reader` from a file
    let mut reader = csv::ReaderBuilder::new().delimiter(b';').from_path(path)?;

    // Retrieve and print header record
    //let headers = reader.headers()?;
    //println!("{:?}", headers);

    let mut total = 0;
    let mut ok = 0;
    // `.deserialize` returns an iterator of the internal
    // record structure deserialized
    //
    let mut output = vec![];
    let mut errors = 0;
    for result in reader.byte_records() {
        let record = result?;

        let situationr = read_field(&record, 5)?;
        let email = &record[27];

        if email != [] {

            if situationr == "02" {
                match read_record(&record) {
                    Err(_) => errors += 1,
                    Ok(rec) => output.push(rec)
                }
                    //println!("{:?}", k);
                ok += 1;
            }
        };

        total += 1;
        //println!("{:?}", record);
    }

    show_number_of_records(ok, total);
    show_number_of_records(errors, total);

    Ok(output)
}

fn read_series(identifier: &str) -> Result<Vec<String>, Box<dyn Error>> {

    let mut output = vec![];
    let base: &str = "../dataset/";
    for idx in 0..10 {
        let res = format!("../dataset/{}{}.csv", identifier, idx);
        output.push(res);
    }
    // let fpath = base.push_str(identifier);

    Ok(output)

}

// Load target CNAE list;
fn load_cnaes() -> Result<Vec<String>, Box<dyn Error>> {
    let mut file = File::open("../cnaes.txt")?;
    let mut contents = String::new();

    file.read_to_string(&mut contents)?;

    let mut output = vec![];
    for line in contents.split("\n") {
        let cnae: String = line.split(" ").next().ok_or("")?.to_string();

        if !cnae.is_empty() {
            output.push(cnae);
        }
    }

    Ok(output)
}

fn show_number_of_records(x: usize, t: usize) {
    println!("Number of records: {:?}/{:?}  {:}%", x, t, x.to_f32().unwrap() / t.to_f32().unwrap() * 100.0);
}

fn load_email_blacklist() -> Vec<String> {
    let mut file = File::open("../email-blacklist.txt").unwrap();

    let mut contents = String::new();

    file.read_to_string(&mut contents).unwrap();

    contents.split("\n")
        .map(str::to_string)
        .filter(|x| x.len() > 0)
        .collect()

}

fn filter_records(records: Vec<CNPJ>, email_blacklist: Vec<String>, cnaes: Vec<String>) -> Vec<CNPJ> {

    let mut output: Vec<CNPJ> = vec![];

    for record in records.iter() {
        let mut accepted = true;

        // EMAIL antipattern filtering;
        for antipattern in email_blacklist.iter() {
            if record.email.contains(antipattern) {
                accepted = false;
                // println!("Reject: {:?}", record.email);
                break;
            }
        }
        if !accepted { continue; }

        // CNAEs filtering;
        accepted = false;
        for cnae in record.cnaes.split(",").map(str::to_string) {
            if cnaes.contains(&cnae) {
                accepted = true;
                break;
            }
        }

        if accepted {
            output.push(record.clone());
        }

    }

    output
}

fn filter_unique_email(records: Vec<CNPJ>) -> Vec<CNPJ> {

    let mut output = vec![];
    for r in 0..records.len() {
        if r == 0 || records[r].email != records[r - 1].email {
            output.push(records[r].clone());
        }
    }
    output
}

fn write_output(records: Vec<CNPJ>) -> Result<(), Box<dyn Error>> {

    let n = 50000;
    let mut outputs: Vec<Vec<CNPJ>> = vec![];

    let mut current: Vec<CNPJ> = vec![];

    for record in records {
        current.push(record.clone());

        if current.len() == n {
            outputs.push(current.clone());
            current.clear();
        }
    }

    if !current.is_empty() {
        outputs.push(current);
    }

    println!("{}", outputs.len());
    for o in 0..outputs.len() {
        let mut wtr = csv::Writer::from_path(format!("../output/output{}.csv", o + 1))?;
        for record in &outputs[o] {
            match wtr.serialize(record) {
                Ok(_) => wtr.flush()?,
                Err(_) => println!("{:?}", record)
            };
        }
    }

    Ok(())
}

fn read_socio(record: Result<csv::ByteRecord, csv::Error>) -> Result<Socio, Box<dyn Error>> {
    let res = record.unwrap();
    Ok(
        Socio {
            cnpj_n: read_field(&res, 0)?.parse::<i32>().unwrap(),
            nome: titlecase(&read_field(&res, 2)?)
        })
}

fn read_empresa(record: Result<csv::ByteRecord, csv::Error>) -> Result<Empresa, Box<dyn Error>> {
    let res = record.unwrap();
    Ok(
        Empresa {
            cnpj_n: read_field(&res, 0)?.parse::<i32>().unwrap(),
            nome: titlecase(&read_field(&res, 1)?)
        })
}

fn enhance_with_extra_data(mut records: Vec<CNPJ>) -> Vec<CNPJ> {

    read_list!(socios, "Socios", read_socio);
    read_list!(empresas, "Empresas", read_empresa);


    socios.sort_by(|a, b| a.cnpj_n.cmp(&b.cnpj_n));
    empresas.sort_by(|a, b| a.cnpj_n.cmp(&b.cnpj_n));

    records.sort_by(|a, b| a.cnpj_n.cmp(&b.cnpj_n));

    let mut output = vec![];
    let mut i = 0;

    let now = Instant::now();
    let mut socio_idx = 0;
    let mut empresa_idx = 0;
    let mut socio_nf = 0;
    let mut empresa_nf = 0;
    let mut t;

    for result in &mut records {
        // Macro fest!
        check_aux_list!(result, socio_idx, socios, socio, socio_nf);
        check_aux_list!(result, empresa_idx, empresas, razao_social, empresa_nf);

        if i % 1000 == 0 {
            t = now.elapsed().as_secs() + 1;
            let eta =  i / t * 60; //t / i * ($vector.len().to_u64().unwrap() - i) / 3600;
            println!("{}    {}s    ETA {}r/min         miss soc:{} emp:{}", i, t, eta, socio_nf, empresa_nf);
        }
        i += 1;
        output.push(result.clone());
    }

    output
}

fn main() {

    let now = Instant::now();

    // 0. Carregar configurações;
    let cnaes = load_cnaes().ok().unwrap();
    let email_blacklist = load_email_blacklist();

    println!("{:?}", cnaes);
    println!("{:?}", email_blacklist);

    // 1. Extrair todos os CNPJs que:
    //
    // - tem e-mails
    // - estão em situação ativa
    // - pertencem aos CNAEs de interesse
    //
    // Isso tudo é feito e conferido olhando apenas
    // a série de arquivos 'Estabelecimentos';
    let file_series = read_series("Estabelecimentos").unwrap();

    let mut results: Vec<CNPJ> = vec![];
    for file in file_series {
        println!("Running for {:?}", file);

        match read_from_file(&file, cnaes.clone()) {
            Ok(k) => {
                println!("{} seconds elapsed on primary filter.", now.elapsed().as_secs());
                let mut res1 = filter_records(k, email_blacklist.clone(), cnaes.clone());

                println!("{:?}", res1.len());
                results.append(&mut res1);

                println!("{} seconds elapsed on secondary filter.", now.elapsed().as_secs());

            },
            Err(e) => eprintln!("{}", e)
        }
    }

    println!("{} seconds elapsed before sorting.", now.elapsed().as_secs());

    // 2. Remover os CNPJs que contém e-mails duplicados;
    results.sort_by(|a, b| a.email.cmp(&b.email));
    println!("Extracted {:} CNPJs", results.len());

    results = filter_unique_email(results);
    println!("{} seconds elapsed.", now.elapsed().as_secs());

    println!("Extracted {:} CNPJs, after removing duplicate e-mails.", results.len());

    // 3. Enriquecer os dados com informações de outras tabelas;
    results = enhance_with_extra_data(results);

    // 4. Ordenar CNPJs por data (mais recentes primeiro);
    results.sort_by(|a, b| b.data_abertura.cmp(&a.data_abertura));

    // 5. Escrever saída;
    write_output(results).unwrap();

    println!("{} seconds elapsed!", now.elapsed().as_secs());

}

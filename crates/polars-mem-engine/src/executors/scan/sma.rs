use polars_core::frame::DataFrame;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::io::{Read, Write};

const SMA_BASE_FOLDER: &str = "/Users/u.oezdemir/Desktop/thesis/data";

#[derive(Serialize, Deserialize, Debug)]
pub struct SMAHeader {
    pub version: String,
    pub predicate_count: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OutlierEntry {
    pub predicate: String,
    pub min: f64,
    pub max: f64,
    pub lower_threshold: f64,
    pub upper_threshold: f64,
    pub outliers: Option<DataFrame>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SMA {
    pub header: SMAHeader,
    pub results: HashMap<String, OutlierEntry>, // Key: predicate, Value: OutlierEntry
}

impl SMA {
    fn new() -> Self {
        SMA {
            header: SMAHeader { version: "SMA".to_string(), predicate_count: 0 },
            results: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct SMAManager {
    smas: HashSet<String> // keep tracks of existed SMA files
}

impl SMAManager {
    pub fn new() -> Self {
        Self {
            smas: HashSet::new(),
        }
    }

    pub fn insert_sma(&mut self, file_name: String) {
        self.smas.insert(file_name);
    }

    pub fn sma_file_exist(&mut self, file_name: &str) -> bool {
        if self.smas.contains(file_name) {
            return true;
        }
        let file_path_in_base_folder = format!("{}/{}", SMA_BASE_FOLDER, file_name);
        let file_exists_in_base_folder = std::path::Path::new(&file_path_in_base_folder).exists();
        // Sync with the hashset
        if file_exists_in_base_folder {
            self.smas.insert(file_name.to_string());
            return true;
        }
        false
    }

    // Finds if the sma file and the sma entry for given predicate exists.
    pub fn can_retrieve_from_sma(&mut self, file_path: &str) -> bool
    {
        if !self.sma_file_exist(file_path) {
            println!("SMA does not exist for file: {}", file_path);
            return false;
        }
        true
    }
    pub fn deserialize_sma_file(&self, path: &str) ->  Result<SMA, io::Error> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let decoded_data: SMA = bincode::deserialize(&buffer)
            .expect("Failed to deserialize SMA struct");
        Ok(decoded_data)
    }

    pub fn create_sma_file(&mut self, file_name: &str) -> Result<(), io::Error> {
        let file_path_in_base_folder = format!("{}/{}", SMA_BASE_FOLDER, file_name);
        let mut file = File::create(file_path_in_base_folder)?;
        let encoded_data = bincode::serialize(&SMA::new())
            .expect("Failed to serialize SMA struct");
        file.write_all(&encoded_data)?;
        self.insert_sma(file_name.to_string());
        Ok(())
    }

    pub fn update_sma_file(&mut self, file_name: &str, sma: SMA) -> Result<(), io::Error> {
        let file_path_in_base_folder = format!("{}/{}", SMA_BASE_FOLDER, file_name);
        let mut file = File::create(file_path_in_base_folder)?;
        let encoded_data = bincode::serialize(&sma)
            .expect("Failed to serialize SMA struct");
        file.write_all(&encoded_data)?;
        Ok(())
    }
}
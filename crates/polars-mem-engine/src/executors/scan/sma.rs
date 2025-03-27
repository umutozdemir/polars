use polars_core::frame::DataFrame;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{Read, Write};
use polars_io::prelude::FileMetadataRef;
use polars_parquet::parquet::metadata::ColumnOrder;
use crate::ScanPredicate;

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
    pub outliers: Option<Vec<DataFrame>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SMA {
    pub header: SMAHeader,
    pub results: Vec<OutlierEntry>, // List of Outlier information per query
}

impl SMA {
    fn new() -> Self {
        SMA {
            header: SMAHeader { version: "SMA".to_string(), predicate_count: 0 },
            results: vec![],
        }
    }

    pub fn add_result(&mut self, predicate: String, min: f64, max: f64, lower_threshold: f64, upper_threshold: f64, outliers: Option<Vec<DataFrame>>) {
        self.results.push(OutlierEntry {
            predicate,
            min,
            max,
            lower_threshold,
            upper_threshold,
            outliers,
        })
    }

    pub fn get_results(&self) -> &Vec<OutlierEntry> {
        &self.results
    }

    pub fn get_results_mut(&mut self) -> &mut Vec<OutlierEntry> {
        &mut self.results
    }

    pub fn get_header(&self) -> &SMAHeader {
        &self.header
    }

    pub fn get_header_mut(&mut self) -> &mut SMAHeader {
        &mut self.header
    }

    pub fn get_header_version(&self) -> &String {
        &self.header.version
    }

    pub fn get_header_predicate_count(&self) -> &usize {
        &self.header.predicate_count
    }

    pub fn write_to_file(&self, path: &str) -> Result<(), io::Error> {
        let mut file = File::create(path)?;
        let encoded_data = bincode::serialize(self)
            .expect("Failed to serialize SMA struct");
        file.write_all(&encoded_data)?;
        Ok(())
    }
    
    pub fn read_from_file(path: &str) ->  Result<Self, io::Error> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let decoded_data: SMA = bincode::deserialize(&buffer)
            .expect("Failed to deserialize SMA struct");
        Ok(decoded_data)
    }
}

#[derive(Debug)]
pub struct SMAManager {
    smas: HashMap<String, SMA>, // key as a file name, values as SMA object
}

impl SMAManager {
    pub fn new() -> Self {
        Self {
            smas: HashMap::new(),
        }
    }

    pub fn insert_sma(&mut self, file_name: String, sma: SMA) {
        self.smas.insert(file_name, sma);
    }

    pub fn get_sma(&self, file_name: &str) -> Option<&SMA> {
        self.smas.get(file_name)
    }

    pub fn get_sma_mut(&mut self, file_name: &str) -> Option<&mut SMA> {
        self.smas.get_mut(file_name)
    }

    pub fn can_use_sma(&self, file_name: &str) -> bool {
        self.smas.contains_key(file_name)
    }

    pub fn can_retrieve_from_sma(&self, predicates: Option<ScanPredicate>, metadata: Option<FileMetadataRef>) -> bool {
        let predicate_column;
        if let Some(predicates) = predicates {
            predicate_column = predicates.live_columns.iter().next().cloned();
            println!("predicate_column: {:?}", predicate_column);
        } else {
            eprintln!("predicates is None");
            return false
        }

        let mut global_min = f64::INFINITY;
        let mut global_max = f64::NEG_INFINITY;

        if let Some(metadata) = metadata {
            for (index, order) in metadata.column_orders.iter().enumerate() {
                println!("Column {}: {:?}", index, order);
            }

            if let Some(column_name) = predicate_column {
                for (row_group_index, row_group) in metadata.row_groups.iter().enumerate() {
                    let column_metadata =  row_group.column_by_name(column_name.as_str());
                        let column_meta_data = column_metadata.unwrap().metadata();
                            if let Some(statistics) = &column_meta_data.statistics {
                                while let Some(v) = statistics.max_value.clone() {
                                    v.iter().for_each(|v| global_max = global_max.max(*v as f64));
                                }
                                while let Some(v) = statistics.min_value.clone() {
                                    v.iter().for_each(|v| global_min = global_min.min(*v as f64));
                                }
                                println!(
                                    "Row Group: {}, Column: {}, Min: {}, Max: {}",
                                    row_group_index, column_name, global_min, global_max
                                );
                            }
                }

                if global_min == f64::INFINITY || global_max == f64::NEG_INFINITY {
                    eprintln!("No valid min/max values found for column '{}'.", column_name);
                } else {
                    println!(
                        "Final min/max values for column '{}': Min: {}, Max: {}",
                        column_name, global_min, global_max
                    );
                }

        } else {
            eprintln!("given column has no metadata");
        }


        let (lower_threshold, upper_threshold) = Self::calculate_thresholds_with_iqr(global_min, global_max);


        // Go over the values of the column that predicate applied such as column 'a'
        // then calculate its outliers and store it in the sma such as:
        // new sma will be created, predicate name is 'a' set min-max-low-upp thresholds
        // if this query called again then we will find it from sma, check the query filter value
        // for example if query predicate is a > 50 then we would know that let's say values higher
        // than 45 is outlier so we can return the query result from outlier field of the sma.
        // if values higher than 50 is not outliers then we may still apply some optimizations as we
        // somehow know statistics per row group per column but currently polars do not support
        // reading of specific row groups. TODO: Let's implement that as further steps but keep it
        // simple for now.
        //
        true
    } else {
            println!("predicates is None");
            false
        }
    }

    fn calculate_thresholds_with_iqr(min: f64, max: f64) -> (f64, f64) {
        let first_quartile = min + (max - min) * 0.25;
        let third_quartile = min + (max - min) * 0.75;
        let iqr = third_quartile - first_quartile;
        let lower_threshold = first_quartile - 1.5 * iqr;
        let upper_threshold = third_quartile + 1.5 * iqr;
        (lower_threshold, upper_threshold)
    }

    fn calculate_thresholds_with_empirical(min: f64, max: f64) -> (f64, f64) {
        let mean = (min + max) / 2.0;
        let std = (max - min) / 6.0;
        let lower_threshold = mean - 3.0 * std;
        let upper_threshold = mean + 3.0 * std;
        (lower_threshold, upper_threshold)
    }

     fn find_outliers(column_data: &[f64]) -> Vec<f64> {
         const OUTLIER_MULTIPLIER: f64 = 1.5;

         // Basic statistics
         let min = column_data.iter().cloned().fold(f64::INFINITY, f64::min);
         let max = column_data.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
         let mean = column_data.iter().sum::<f64>() / column_data.len() as f64;

         // Determine thresholds for outliers (e.g. upper = mean + 1.5 * range)
         let range = max - min;
         let lower_threshold = mean - OUTLIER_MULTIPLIER * range;
         let upper_threshold = mean + OUTLIER_MULTIPLIER * range;

         // Identify outliers
         let outliers: Vec<_> = column_data
             .iter()
             .filter(|value| **value < lower_threshold || **value > upper_threshold)
             .cloned()
             .collect();
        outliers
     }
}